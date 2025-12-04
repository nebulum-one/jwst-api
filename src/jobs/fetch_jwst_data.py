"""
Fast, paginated JWST PUBLIC fetcher (observation-level pagination).

Usage:
    # default: limit=50, offset=0
    python -m src.jobs.fetch_jwst_data

    # custom
    python -m src.jobs.fetch_jwst_data --limit 100 --offset 200

Notes:
- Batch commit size is defaulted to 20 (your current batch size).
- The script will only process the requested slice (offset:offset+limit).
- For each observation processed we fetch the product list and only store PUBLIC products.
"""

from __future__ import annotations
import argparse
import sys
import time
from datetime import datetime, timezone
from typing import Optional, Callable

from astroquery.mast import Observations
from astropy.time import Time
import numpy as np

# allow project imports when run as module
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.db.database import SessionLocal, init_db
from src.db.models import JWSTObservation

# -------------------------
# Configuration defaults
# -------------------------
DEFAULT_LIMIT = 50
DEFAULT_OFFSET = 0
BATCH_COMMIT = 20  # keep your current batch size

# -------------------------
# Utility helpers
# -------------------------

def safe_value(v):
    """Convert numpy/Masked values to Python native or None."""
    try:
        if v is None:
            return None
        # numpy masked array
        if isinstance(v, np.ma.MaskedArray):
            if v.size == 0:
                return None
            if getattr(v, "mask", False).all():
                return None
            try:
                return v.item()
            except Exception:
                arr = np.asarray(v)
                # if scalar-like
                if arr.size == 1:
                    return arr.flatten()[0].item()
                return arr.tolist()
        # MaskedConstant
        try:
            from numpy.ma.core import MaskedConstant
            if isinstance(v, MaskedConstant):
                return None
        except Exception:
            pass
    except Exception:
        pass

    # numpy scalar -> python scalar
    try:
        if isinstance(v, np.generic):
            return v.item()
    except Exception:
        pass

    return v

def mast_uri_to_http(uri: Optional[str]) -> Optional[str]:
    """Convert a mast: URI to HTTP download endpoint or return None."""
    if not uri:
        return None
    uri = safe_value(uri)
    if not isinstance(uri, str):
        return None
    if uri.startswith("mast:"):
        return f"https://mast.stsci.edu/api/v0.1/Download/file?uri={uri}"
    return uri

def retry(func: Callable[[], any], retries: int = 5, backoff_factor: float = 2.0, initial_delay: float = 1.0):
    """Generic retry wrapper with exponential backoff."""
    delay = initial_delay
    for attempt in range(1, retries + 1):
        try:
            return func()
        except Exception as e:
            if attempt == retries:
                raise
            print(f"⚠️ Attempt {attempt} failed: {e} — retrying in {delay:.1f}s")
            time.sleep(delay)
            delay *= backoff_factor

def fetch_product_table_for_obs(obs_row):
    """Fetch product list for an observation with retries."""
    def _call():
        return Observations.get_product_list(obs_row)
    return retry(_call, retries=5, backoff_factor=2.0, initial_delay=1.0)

def filter_public_products(product_table):
    """Return list of rows from product_table where dataRights == 'PUBLIC'."""
    public = []
    try:
        for p in product_table:
            rights = safe_value(p.get("dataRights", None))
            if isinstance(rights, str) and rights.strip().upper() == "PUBLIC":
                public.append(p)
    except Exception:
        pass
    return public

def pick_preview_from_products(public_products):
    """Pick a preview jpg/png (prefer productType == 'PREVIEW' then filename)."""
    if not public_products:
        return None

    # prefer explicit productType PREVIEW
    for p in public_products:
        pt = safe_value(p.get("productType"))
        if isinstance(pt, str) and pt.strip().upper() == "PREVIEW":
            uri = safe_value(p.get("dataURI"))
            http = mast_uri_to_http(uri)
            if http:
                return http

    # filename-based jpg/png
    for p in public_products:
        fname = safe_value(p.get("productFilename"))
        if isinstance(fname, str) and fname.lower().endswith((".jpg", ".jpeg", ".png")):
            http = mast_uri_to_http(safe_value(p.get("dataURI")))
            if http:
                return http

    # fallback: any image-like filename
    for p in public_products:
        fname = safe_value(p.get("productFilename"))
        if isinstance(fname, str) and any(ext in fname.lower() for ext in (".jpg", ".jpeg", ".png", ".tif")):
            http = mast_uri_to_http(safe_value(p.get("dataURI")))
            if http:
                return http

    return None

def pick_fits_from_products(public_products):
    """Pick first public FITS file (returns an HTTP URL)."""
    if not public_products:
        return None
    for p in public_products:
        fname = safe_value(p.get("productFilename"))
        if isinstance(fname, str) and fname.lower().endswith(".fits"):
            http = mast_uri_to_http(safe_value(p.get("dataURI")))
            if http:
                return http
    return None

def parse_mjd_to_datetime(mjd):
    """Convert MJD to timezone-aware UTC datetime, or None."""
    try:
        if mjd is None:
            return None
        val = safe_value(mjd)
        if val is None:
            return None
        dt = Time(val, format="mjd").to_datetime()
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

# -------------------------
# Main fetch function
# -------------------------

def fetch_jwst_observations(limit: int = DEFAULT_LIMIT, offset: int = DEFAULT_OFFSET, batch_commit: int = BATCH_COMMIT):
    """
    Fetch observations (observation-level pagination) and store only public products.
    Only processes observations in range offset:offset+limit (fast).
    """
    print(f"\n🚀 Fetch start — limit={limit}, offset={offset}, batch_commit={batch_commit}")

    # Try to set ROW_LIMIT to avoid downloading entire catalog where supported
    try:
        Observations.ROW_LIMIT = offset + limit
    except Exception:
        # some versions may not support; ignore
        pass

    init_db()
    db = SessionLocal()

    try:
        # Main MAST query (we ask for public observation-level rows)
        def _obs_query():
            return Observations.query_criteria(
                obs_collection="JWST",
                dataproduct_type="image",
                dataRights="PUBLIC",
                calib_level=[2, 3],
                # we avoid max_records because some astroquery versions warn it is unsupported
            )

        obs_table = retry(_obs_query, retries=5, backoff_factor=2.0, initial_delay=1.0)

        if obs_table is None:
            print("No observations returned.")
            return

        total_available = len(obs_table)
        print(f"Found {total_available} observation-level PUBLIC entries.")

        # slice to requested window (fast mode)
        start = int(offset)
        end = int(offset + limit)
        sliced = obs_table[start:end]
        print(f"Processing {len(sliced)} observations (slice {start}:{end}).")

        added = 0
        updated = 0
        processed = 0

        for row in sliced:
            processed += 1
            # MAST sometimes uses 'obs_id' or 'obsid' naming; be resilient
            obs_id = safe_value(row.get("obs_id") or row.get("obsid") or row.get("obs")) or None
            if not obs_id:
                # skip malformed row
                continue

            # fetch product list for this observation (only for the current processed slice)
            product_table = fetch_product_table_for_obs(row)
            public_products = filter_public_products(product_table)

            if not public_products:
                # nothing public for this observation - skip it
                print(f"  - {obs_id}: no PUBLIC products (skipped)")
                continue

            preview_url = pick_preview_from_products(public_products)
            fits_url = pick_fits_from_products(public_products)

            # parse values safely
            obs_date = parse_mjd_to_datetime(safe_value(row.get("t_min")))
            ra = safe_value(row.get("s_ra"))
            dec = safe_value(row.get("s_dec"))

            obs_data = {
                "obs_id": obs_id,
                "target_name": safe_value(row.get("target_name")) or "",
                "ra": float(ra) if ra is not None else None,
                "dec": float(dec) if dec is not None else None,
                "instrument": safe_value(row.get("instrument_name")) or "",
                "filter_name": safe_value(row.get("filters")) or "",
                "observation_date": obs_date,
                "proposal_id": safe_value(row.get("proposal_id")) or "",
                "exposure_time": float(safe_value(row.get("t_exptime"))) if safe_value(row.get("t_exptime")) is not None else None,
                "description": safe_value(row.get("obs_title")) or "",
                # metadata
                "dataproduct_type": safe_value(row.get("dataproduct_type")),
                "calib_level": int(safe_value(row.get("calib_level"))) if safe_value(row.get("calib_level")) is not None else None,
                "wavelength_region": safe_value(row.get("wavelength_region")),
                "pi_name": safe_value(row.get("proposal_pi")),
                "target_classification": safe_value(row.get("target_classification")),
                # Only public URLs
                "preview_url": preview_url,
                "fits_url": fits_url,
            }

            # Insert or update
            existing = db.query(JWSTObservation).filter(JWSTObservation.obs_id == obs_id).first()
            if existing:
                for k, v in obs_data.items():
                    setattr(existing, k, v)
                existing.updated_at = datetime.now(timezone.utc)
                updated += 1
            else:
                db.add(JWSTObservation(**obs_data))
                added += 1

            # periodic commit
            if (added + updated) % batch_commit == 0:
                db.commit()
                print(f"  Progress: processed={processed} added={added} updated={updated}")

        # final commit
        db.commit()
        total = db.query(JWSTObservation).count()
        print("\n✅ Fetch complete")
        print(f"   Added: {added}")
        print(f"   Updated: {updated}")
        print(f"   Total in DB: {total}\n")

    except Exception as exc:
        print(f"❌ Fatal error during fetch: {exc}")
        import traceback
        traceback.print_exc()
        db.rollback()
        raise
    finally:
        db.close()

# -------------------------
# CLI
# -------------------------

def parse_cli():
    p = argparse.ArgumentParser(description="Fetch JWST public observations into DB (paginated).")
    p.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="How many observations to process this run (default 50).")
    p.add_argument("--offset", type=int, default=DEFAULT_OFFSET, help="Start index for observations (default 0).")
    p.add_argument("--batch", type=int, default=BATCH_COMMIT, help="Batch commit size (default 20).")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_cli()
    fetch_jwst_observations(limit=args.limit, offset=args.offset, batch_commit=args.batch)
