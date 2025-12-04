"""
Fetch 50 PUBLIC JWST observations with valid preview + FITS URLs.
Optimized for speed and reliable URL extraction.
"""

import sys
import traceback
from datetime import datetime, UTC

from astroquery.mast import Observations
from sqlalchemy.orm import Session

from src.db.database import SessionLocal, init_db
from src.db.models import JWSTObservation


MAX_RESULTS = 50


# -------------------------------------------------------
# URL HELPERS
# -------------------------------------------------------

def mast_to_public_url(uri_or_path: str | None) -> str | None:
    """Convert a mast:JWST/... or filename into a public HTTPS download URL."""
    if not uri_or_path:
        return None

    # Already an HTTPS URL
    if uri_or_path.startswith("http"):
        return uri_or_path

    # Convert mast: URIs
    if uri_or_path.startswith("mast:"):
        return f"https://mast.stsci.edu/api/v0.1/Download/file?uri={uri_or_path}"

    # Convert product filenames
    # Example: jw01234...fits
    if uri_or_path.lower().endswith((".fits", ".jpg", ".jpeg", ".png")):
        return f"https://mast.stsci.edu/api/v0.1/Download/file?uri=mast:JWST/product/{uri_or_path}"

    return None


def extract_preview_url(prod: dict) -> str | None:
    """Select best preview URL."""
    # 1. Direct JPEG preview
    if prod.get("jpegURL"):
        return prod["jpegURL"]

    # 2. PNG
    if prod.get("pngURL"):
        return prod["pngURL"]

    # 3. Use dataURI if it's an image
    uri = prod.get("dataURI")
    if uri and prod.get("dataproduct_type") == "image":
        return mast_to_public_url(uri)

    # 4. Fallback to productFilename
    filename = prod.get("productFilename")
    if filename:
        return mast_to_public_url(filename)

    return None


def extract_fits_url(prod: dict) -> str | None:
    """Select best FITS file URL."""

    # 1. dataURI
    uri = prod.get("dataURI")
    if uri and uri.lower().endswith(".fits"):
        return mast_to_public_url(uri)

    # 2. productFilename
    filename = prod.get("productFilename")
    if filename and filename.lower().endswith(".fits"):
        return mast_to_public_url(filename)

    # 3. dataURL
    if prod.get("dataURL"):
        url = prod["dataURL"]
        if url.lower().endswith(".fits"):
            return url

    # 4. s3_uris (rare)
    s3 = prod.get("s3_uris")
    if s3:
        for uri in s3:
            if uri.lower().endswith(".fits"):
                return mast_to_public_url(uri)

    return None


# -------------------------------------------------------
# MAIN FETCH LOGIC
# -------------------------------------------------------

def fetch_public_jwst(limit=MAX_RESULTS):
    print(f"\n🚀 Fetching up to {limit} PUBLIC JWST observations...\n")

    # Fetch only public JWST observations
    obs_table = Observations.query_criteria(
        obs_collection="JWST",
        dataproduct_type=["image", "spectrum", "cube"],   # get all meaningful types
        calib_level=[1, 2, 3],                            # all calibration levels
        project="JWST",
        filters=["PUBLIC"]
    )

    print(f"Found {len(obs_table)} raw entries (public).")

    db = SessionLocal()
    added = 0
    updated = 0
    processed = 0

    for obs in obs_table:

        if added >= limit:
            break

        processed += 1

        obsid = obs.get("obsid") or obs.get("obs_id")
        if not obsid:
            continue

        # ---------------------------------------------------
        # Retrieve product list for this observation
        # ---------------------------------------------------
        products = Observations.get_product_list(obs)
        if len(products) == 0:
            continue

        preview = None
        fits = None

        # Scan for best preview + FITS URLs
        for prod in products:
            if not preview:
                preview = extract_preview_url(prod)
            if not fits:
                fits = extract_fits_url(prod)

            if preview and fits:
                break

        # Must have at least preview or fits URL to be useful
        if not preview and not fits:
            continue

        # ---------------------------------------------------
        # Insert or update DB row
        # ---------------------------------------------------
        existing: JWSTObservation = db.query(JWSTObservation).filter_by(obs_id=obsid).first()

        def parse_date(val):
            try:
                return datetime.strptime(val, "%Y-%m-%dT%H:%M:%S.%f")
            except:
                try:
                    return datetime.fromisoformat(val)
                except:
                    return None

        metadata = {
            "obs_id": obsid,
            "target_name": obs.get("target_name"),
            "ra": obs.get("s_ra"),
            "dec": obs.get("s_dec"),
            "instrument": obs.get("instrument_name"),
            "filter_name": obs.get("filter_name"),
            "observation_date": parse_date(obs.get("t_min")),
            "preview_url": preview,
            "fits_url": fits,
            "description": obs.get("intent", obs.get("description")),
            "proposal_id": str(obs.get("proposal_id")),
            "exposure_time": obs.get("t_exptime"),
            "dataproduct_type": obs.get("dataproduct_type"),
            "calib_level": obs.get("calib_level"),
            "wavelength_region": obs.get("em_min") and obs.get("em_max"),
            "pi_name": obs.get("proposal_pi"),
            "target_classification": obs.get("target_classification"),
            "updated_at": datetime.now(UTC),
        }

        if existing:
            for k, v in metadata.items():
                setattr(existing, k, v)
            updated += 1
        else:
            obj = JWSTObservation(**metadata)
            db.add(obj)
            added += 1

        if processed % 20 == 0:
            print(f"  Progress: processed={processed} added={added} updated={updated}")

    db.commit()
    db.close()

    print("\n✅ Fetch complete!")
    print(f"   Added: {added}")
    print(f"   Updated: {updated}")
    print(f"   Total processed: {processed}")
    print("")


# -------------------------------------------------------
# ENTRY POINT
# -------------------------------------------------------

if __name__ == "__main__":
    try:
        init_db()
        fetch_public_jwst()
    except Exception as e:
        print("❌ ERROR:")
        traceback.print_exc()
        sys.exit(1)
