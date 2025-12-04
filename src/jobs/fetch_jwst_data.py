"""
Fetch JWST PUBLIC observations and store them in the database.
Ensures only PUBLIC downloadable products are saved (no Access Denied).
"""

import sys
import os
from datetime import datetime
from astroquery.mast import Observations
from astropy.time import Time
from sqlalchemy.orm import Session

# Allow imports from project root
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.db.database import SessionLocal, init_db
from src.db.models import JWSTObservation


# -------------------------------------------------------
# Helpers
# -------------------------------------------------------

def safe_get(row, key, default=None):
    """Safely get a MAST field (avoids masked or missing values)."""
    try:
        value = row.get(key, default)
        if value is None:
            return default
        if hasattr(value, "dtype") and getattr(value, "mask", False):
            return default
        return value
    except Exception:
        return default


def get_public_products(obs_row):
    """
    Fetch MAST product list for an observation
    and return only PUBLIC downloadable products.
    """
    products = Observations.get_product_list(obs_row)

    public = products[products["dataRights"] == "PUBLIC"]

    # Return pandas-like table subset
    return public


def pick_preview(public_products):
    """Pick a preview image (JPEG or PNG) if available."""
    if len(public_products) == 0:
        return None

    # Common preview image types
    preview_types = ["jpg", "jpeg", "png"]

    for ext in preview_types:
        matches = public_products[
            public_products["productFilename"].str.lower().str.endswith(ext)
        ]
        if len(matches) > 0:
            uri = matches[0]["dataURI"]
            return f"https://mast.stsci.edu/api/v0.1/Download/file?uri={uri}"

    return None


def pick_fits(public_products):
    """Pick a PUBLIC science FITS file."""
    fits = public_products[
        public_products["productFilename"].str.lower().str.endswith(".fits")
    ]

    if len(fits) == 0:
        return None

    uri = fits[0]["dataURI"]
    return f"https://mast.stsci.edu/api/v0.1/Download/file?uri={uri}"


def parse_obs_date(row):
    """Convert MJD date into a proper datetime."""
    t_min = safe_get(row, "t_min")
    if not t_min:
        return None

    try:
        return Time(t_min, format="mjd").to_datetime()
    except Exception:
        return None


# -------------------------------------------------------
# Main fetcher
# -------------------------------------------------------

def fetch_jwst_observations(max_results=100):
    print(f"\n🚀 Fetching up to {max_results} PUBLIC JWST observations...\n")

    init_db()
    db = SessionLocal()

    try:
        # -- Query ONLY public observations --
        obs_table = Observations.query_criteria(
            obs_collection="JWST",
            dataRights="PUBLIC",
            dataproduct_type="image",
            calib_level=[2, 3],
        )

        print(f"Found {len(obs_table)} public JWST observations.")

        # Limit for this run
        obs_table = obs_table[:max_results]

        added = 0
        updated = 0

        for row in obs_table:
            obs_id = row["obs_id"]

            # Fetch PUBLIC products
            public_products = get_public_products(row)

            preview_url = pick_preview(public_products)
            fits_url = pick_fits(public_products)

            # Parse observation timestamp
            obs_date = parse_obs_date(row)

            # Build record
            obs_data = {
                "obs_id": obs_id,
                "target_name": safe_get(row, "target_name", ""),
                "ra": safe_get(row, "s_ra"),
                "dec": safe_get(row, "s_dec"),
                "instrument": safe_get(row, "instrument_name", ""),
                "filter_name": safe_get(row, "filters", ""),
                "observation_date": obs_date,
                "proposal_id": safe_get(row, "proposal_id", ""),
                "exposure_time": safe_get(row, "t_exptime"),
                "description": safe_get(row, "obs_title", ""),

                # Extra metadata safely accessed
                "dataproduct_type": safe_get(row, "dataproduct_type"),
                "calib_level": safe_get(row, "calib_level"),
                "wavelength_region": safe_get(row, "wavelength_region"),
                "pi_name": safe_get(row, "proposal_pi"),
                "target_classification": safe_get(row, "target_classification"),

                # PUBLIC URLs only
                "preview_url": preview_url,
                "fits_url": fits_url,
            }

            # Check existing entry
            existing = db.query(JWSTObservation).filter(
                JWSTObservation.obs_id == obs_id
            ).first()

            if existing:
                for key, value in obs_data.items():
                    setattr(existing, key, value)
                existing.updated_at = datetime.now(datetime.UTC)
                updated += 1
            else:
                db.add(JWSTObservation(**obs_data))
                added += 1

            if (added + updated) % 10 == 0:
                db.commit()
                print(f"Progress: {added} added, {updated} updated")

        db.commit()
        print("\n✅ DONE!")
        print(f"   Added:   {added}")
        print(f"   Updated: {updated}")
        count = db.query(JWSTObservation).count()
        print(f"   Total in DB: {count}\n")

    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    fetch_jwst_observations(max_results=50)
