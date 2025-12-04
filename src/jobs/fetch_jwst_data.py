"""
Fetch 50 PUBLIC JWST observations with valid preview + FITS URLs.
Optimized for speed and reliable URL extraction.
"""

import sys
import traceback
from datetime import datetime, UTC

from astroquery.mast import Observations
from sqlalchemy.orm import Session
from astropy.time import Time

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.db.database import SessionLocal, init_db
from src.db.models import JWSTObservation


MAX_RESULTS = 50


# -------------------------------------------------------
# HELPER FUNCTIONS
# -------------------------------------------------------

def clean_value(val):
    """Convert masked/nan values to None for database compatibility"""
    if val is None:
        return None
    # Check if it's a masked value (numpy/astropy masked arrays)
    if hasattr(val, 'mask'):
        if val.mask:
            return None
    # Convert numpy strings to regular strings
    if hasattr(val, 'item'):
        try:
            return val.item()
        except:
            pass
    return val


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
    if uri_or_path.lower().endswith((".fits", ".jpg", ".jpeg", ".png")):
        return f"https://mast.stsci.edu/api/v0.1/Download/file?uri=mast:JWST/product/{uri_or_path}"

    return None


def extract_preview_url(prod: dict) -> str | None:
    """Select best preview URL."""
    # 1. Direct JPEG preview
    if prod.get("jpegURL"):
        return mast_to_public_url(prod["jpegURL"])

    # 2. PNG
    if prod.get("pngURL"):
        return mast_to_public_url(prod["pngURL"])

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
            return mast_to_public_url(url)

    return None


# -------------------------------------------------------
# MAIN FETCH LOGIC
# -------------------------------------------------------

def fetch_public_jwst(limit=MAX_RESULTS):
    print(f"\n🚀 Fetching up to {limit} PUBLIC JWST observations...\n")

    # Fetch only public JWST observations - SIMPLE QUERY (fast!)
    print("Querying MAST archive...")
    obs_table = Observations.query_criteria(
        obs_collection="JWST",
        dataproduct_type=["image"],
        calib_level=[2, 3],
        dataRights="PUBLIC"
    )[:limit * 3]  # Get 3x limit to scan for valid URLs

    print(f"Found {len(obs_table)} observations to process.\n")

    db = SessionLocal()
    added = 0
    updated = 0
    processed = 0
    skipped = 0

    for obs in obs_table:
        if added >= limit:
            print(f"\nReached target of {limit} new observations. Stopping.")
            break

        processed += 1

        obsid = clean_value(obs.get("obsid") or obs.get("obs_id"))
        if not obsid:
            skipped += 1
            continue

        # Retrieve product list for this observation
        try:
            products = Observations.get_product_list(obs)
        except:
            skipped += 1
            continue

        if len(products) == 0:
            skipped += 1
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

        # Must have at least preview or fits URL
        if not preview and not fits:
            skipped += 1
            continue

        # Check if exists
        existing = db.query(JWSTObservation).filter_by(obs_id=obsid).first()

        # Parse date
        obs_date = None
        if obs.get('t_min'):
            try:
                t = Time(obs['t_min'], format='mjd')
                obs_date = t.datetime
            except:
                pass

        # Prepare metadata (CLEAN all values to avoid masked constants)
        metadata = {
            "obs_id": obsid,
            "target_name": clean_value(obs.get("target_name")),
            "ra": float(obs.get("s_ra")) if obs.get("s_ra") else None,
            "dec": float(obs.get("s_dec")) if obs.get("s_dec") else None,
            "instrument": clean_value(obs.get("instrument_name")),
            "filter_name": clean_value(obs.get("filters")),
            "observation_date": obs_date,
            "preview_url": preview,
            "fits_url": fits,
            "description": clean_value(obs.get("obs_title")),
            "proposal_id": clean_value(str(obs.get("proposal_id")) if obs.get("proposal_id") else None),
            "exposure_time": float(obs.get("t_exptime")) if obs.get("t_exptime") else None,
            "dataproduct_type": clean_value(obs.get("dataproduct_type")),
            "calib_level": int(obs.get("calib_level")) if obs.get("calib_level") else None,
            "wavelength_region": clean_value(obs.get("wavelength_region")),
            "pi_name": clean_value(obs.get("proposal_pi")),
            "target_classification": clean_value(obs.get("target_classification")),
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

        if (added + updated) % 10 == 0:
            db.commit()
            print(f"Progress: {added} added, {updated} updated, {skipped} skipped (processed {processed})")

    db.commit()
    db.close()

    print("\n✅ Fetch complete!")
    print(f"   Added: {added}")
    print(f"   Updated: {updated}")
    print(f"   Skipped: {skipped}")
    print(f"   Total processed: {processed}")
    print()


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
