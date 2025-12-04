"""
Robust JWST public-data fetcher for MAST.

Replaces src/jobs/fetch_jwst_data.py with:
 - retries + backoff for MAST calls
 - product-level filtering: only products where dataRights == "PUBLIC"
 - safe conversion of masked/NumPy values to Python native types
 - picks a public preview image (jpg/png) and a public .fits file when available
 - batch commits and clear logging
"""

import sys
import time
from datetime import datetime, timezone
from typing import Optional

from astroquery.mast import Observations
from astropy.time import Time
import numpy as np

# allow project imports
sys.path.append(__import__("os").path.dirname(__import__("os").path.dirname(__file__)))

from src.db.database import SessionLocal, init_db
from src.db.models import JWSTObservation


# ---------------------------
# Helpers
# ---------------------------

def safe_value(v):
    """
    Convert MaskedArray/MaskedConstant/numpy types to Python types or None.
    """
    try:
        # None stays None
        if v is None:
            return None

        # numpy masked values
        if isinstance(v, np.ma.MaskedArray):
            if v.size == 0:
                return None
            # scalar masked array
            if getattr(v, "mask", False).all():
                return None
            # convert to native
            try:
                return v.item()
            except Exception:
                # attempt to convert elementwise
                arr = np.asarray(v)
                return arr.tolist()

        # MaskedConstant (sometimes shows up)
        from numpy.ma.core import MaskedConstant
        if isinstance(v, MaskedConstant):
            return None
    except Exception:
        # if numpy isn't available or something odd, fall through
        pass

    # numpy scalars -> python scalars
    try:
        if isinstance(v, np.generic):
            return v.item()
    except Exception:
        pass

    return v


def mast_uri_to_http(uri: Optional[str]) -> Optional[str]:
    """Convert a mast: URI to the MAST HTTP download endpoint."""
    if not uri:
        return None
    uri = safe_value(uri)
    if not isinstance(uri, str):
        return None
    if uri.startswith("mast:"):
        return f"https://mast.stsci.edu/api/v0.1/Download/file?uri={uri}"
    return uri


def retry(func, retries=5, backoff_factor=1.5, initial_delay=1.0, allowed_exceptions=(Exception,)):
    """
    Generic retry wrapper with exponential backoff.
    func should be a no-arg callable.
    """
    delay = initial_delay
    for attempt in range(1, retries + 1):
        try:
            return func()
        except allowed_exceptions as e:
            if attempt == retries:
                raise
            print(f"⚠️ Attempt {attempt} failed: {e} — retrying in {delay:.1f}s")
            time.sleep(delay)
            delay *= backoff_factor


def fetch_product_table_for_obs(obs_row):
    """
    Fetch product table for the observation row, with retries.
    Returns an astropy.table.Table (may be empty).
    """
    def _call():
        return Observations.get_product_list(obs_row)
    return retry(_call, retries=5, backoff_factor=2, initial_delay=1, allowed_exceptions=(Exception,))


def filter_public_products(product_table):
    """
    Given an astropy Table from get_product_list, return list of product rows
    where dataRights == 'PUBLIC'.
    We'll return a Python list of dict-like rows for easier handling.
    """
    public = []
    try:
        # product_table can be an astropy Table. iterate rows.
        for p in product_table:
            # p['dataRights'] could be masked or a numpy scalar
            rights = safe_value(p.get("dataRights", None))
            if rights and isinstance(rights, str) and rights.strip().upper() == "PUBLIC":
                public.append(p)
    except Exception:
        pass
    return public


def pick_preview_from_products(public_products):
    """
    Choose a preview image URL from public_products.
    Prefer files whose filename endswith jpg/jpeg/png or productType == 'PREVIEW'.
    """
    if not public_products:
        return None

    # 1) productType == 'PREVIEW' and dataURI present
    for p in public_products:
        pt = safe_value(p.get("productType"))
        if pt and isinstance(pt, str) and pt.upper() == "PREVIEW":
            uri = safe_value(p.get("dataURI"))
            http = mast_uri_to_http(uri)
            if http:
                return http

    # 2) filename-based (jpg/png)
    for p in public_products:
        fname = safe_value(p.get("productFilename"))
        if fname and isinstance(fname, str):
            lower = fname.lower()
            if lower.endswith(".jpg") or lower.endswith(".jpeg") or lower.endswith(".png"):
                uri = safe_value(p.get("dataURI"))
                http = mast_uri_to_http(uri)
                if http:
                    return http

    # 3) fallback: any product with image-like filename
    for p in public_products:
        fname = safe_value(p.get("productFilename"))
        if fname and isinstance(fname, str) and any(ext in fname.lower() for ext in (".jpg", ".jpeg", ".png", ".tif")):
            http = mast_uri_to_http(safe_value(p.get("dataURI")))
            if http:
                return http

    return None


def pick_fits_from_products(public_products):
    """Choose the first public FITS product (dataURI -> http)."""
    if not public_products:
        return None
    for p in public_products:
        fname = safe_value(p.get("productFilename"))
        if fname and isinstance(fname, str) and fname.lower().endswith(".fits"):
            uri = safe_value(p.get("dataURI"))
            http = mast_uri_to_http(uri)
            if http:
                return http
    return None


def parse_mjd_to_datetime(mjd):
    """Convert MJD numeric to timezone-aware datetime (UTC)."""
    try:
        if mjd is None:
            return None
        mjd_val = safe_value(mjd)
        if mjd_val is None:
            return None
        dt = Time(mjd_val, format="mjd").to_datetime()
        # ensure tz-aware UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


# ---------------------------
# Main fetcher
# ---------------------------

def fetch_jwst_observations(max_results: int = 100, batch_commit: int = 20):
    """
    Fetch JWST observations from MAST and store only PUBLIC products.
    """
    print(f"\n🚀 Starting fetch: max_results={max_results}")

    init_db()
    db = SessionLocal()

    try:
        # Use a safe query wrapper for MAST that retries
        def _query():
            # limit the rows returned to keep the response smaller
            # astroquery supports max_records param
            return Observations.query_criteria(
                obs_collection="JWST",
                dataproduct_type="image",
                dataRights="PUBLIC",   # observation-level public
                calib_level=[2, 3],
                max_records=max_results,
            )

        obs_table = retry(_query, retries=5, backoff_factor=2, initial_delay=1, allowed_exceptions=(Exception,))
        total_found = len(obs_table) if obs_table is not None else 0
        print(f"Found {total_found} (observation-level PUBLIC) JWST entries (capped to {max_results}).")

        added = 0
        updated = 0
        processed = 0

        for row in obs_table:
            processed += 1
            obs_id = safe_value(row.get("obs_id") or row.get("obsid") or row.get("obsid", None))
            if not obs_id:
                # skip malformed rows
                continue

            # fetch product list for this observation (with retries)
            product_table = fetch_product_table_for_obs(row)
            public_products = filter_public_products(product_table)

            # If there are no public products, skip this observation entirely
            if not public_products:
                # nothing public to save for this observation
                print(f"  - Skipping {obs_id}: no PUBLIC products")
                continue

            # choose preview and fits from the public products
            preview_url = pick_preview_from_products(public_products)
            fits_url = pick_fits_from_products(public_products)

            # prepare safe metadata values
            obs_date = parse_mjd_to_datetime(safe_value(row.get("t_min")))
            ra = safe_value(row.get("s_ra"))
            dec = safe_value(row.get("s_dec"))

            # Build the dict for model fields (tolerant to missing fields)
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

                # only public URLs (converted to HTTP download endpoint)
                "preview_url": preview_url,
                "fits_url": fits_url,
            }

            # Insert or update DB
            existing = db.query(JWSTObservation).filter(JWSTObservation.obs_id == obs_id).first()

            if existing:
                for k, v in obs_data.items():
                    setattr(existing, k, v)
                # timezone-aware updated timestamp
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

    except Exception as e:
        print(f"❌ Fatal error during fetch: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    # adjust the number you want to fetch in each run
    fetch_jwst_observations(max_results=50, batch_commit=20)
