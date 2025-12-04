"""
Fetch and store JWST observation data from NASA's MAST archive.
Only stores entries where both the OBSERVATION and its PRODUCT are PUBLIC.
"""

from astroquery.mast import Observations
from sqlalchemy.orm import Session
from datetime import datetime
import sys
import os

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.db.database import SessionLocal, init_db
from src.db.models import JWSTObservation


def fetch_jwst_observations(max_results=100):
    """
    Fetch JWST observations from MAST and store them in the database.
    Only includes files/products that are PUBLIC.
    """

    print(f"Starting JWST data fetch... (max {max_results} observations)")

    # Initialize DB
    init_db()
    db = SessionLocal()

    try:
        print("Querying MAST for JWST public observations...")

        obs_table = Observations.query_criteria(
            obs_collection="JWST",
            dataproduct_type="image",
            calib_level=[2, 3],        # Calibrated products only
            dataRights="PUBLIC"        # Observation must be PUBLIC
        )

        print(f"Found {len(obs_table)} public JWST observations.")
        obs_table = obs_table[:max_results]

        added_count = 0
        updated_count = 0

        for row in obs_table:
            obs_id = row["obs_id"]

            # -------------------------------------------------------
            # STEP 1 — Retrieve products and ensure at least ONE is public
            # -------------------------------------------------------
            products = Observations.get_product_list(row)

            # Filter products where *product-level* rights are PUBLIC
            public_products = products[products["dataRights"] == "PUBLIC"]

            if len(public_products) == 0:
                # Skip if the products for this observation are not public
                print(f"Skipping {obs_id}: no PUBLIC products found.")
                continue

            # Pick the first public product (usually best default)
            product = public_products[0]

            preview_url = product.get("jpegURL", "")
            fits_url = product.get("dataURL", "")

            # -------------------------------------------------------
            # STEP 2 — Parse observation date (convert MJD to datetime)
            # -------------------------------------------------------
            obs_date = None
            if row.get("t_min"):
                try:
                    from astropy.time import Time
                    obs_date = Time(row["t_min"], format="mjd").datetime
                except Exception:
                    pass

            # -------------------------------------------------------
            # STEP 3 — Build observation data model
            # -------------------------------------------------------
            obs_data = {
                "obs_id": obs_id,
                "target_name": row.get("target_name", ""),
                "ra": float(row["s_ra"]) if row.get("s_ra") else None,
                "dec": float(row["s_dec"]) if row.get("s_dec") else None,
                "instrument": row.get("instrument_name", ""),
                "filter_name": row.get("filters", ""),
                "observation_date": obs_date,
                "proposal_id": row.get("proposal_id", ""),
                "exposure_time": float(row["t_exptime"]) if row.get("t_exptime") else None,
                "description": row.get("obs_title", ""),

                # --- Metadata ---
                "dataproduct_type": row.get("dataproduct_type", ""),
                "calib_level": int(row["calib_level"]) if row.get("calib_level") else None,
                "wavelength_region": row.get("wavelength_region", ""),
                "pi_name": row.get("proposal_pi", ""),
                "target_classification": row.get("target_classification", ""),

                # --- PUBLIC PRODUCT URLs ONLY ---
                "preview_url": preview_url,
                "fits_url": fits_url
            }

            # -------------------------------------------------------
            # STEP 4 — Insert/update DB entry
            # -------------------------------------------------------
            existing = db.query(JWSTObservation).filter(
                JWSTObservation.obs_id == obs_id
            ).first()

            if existing:
                for key, value in obs_data.items():
                    setattr(existing, key, value)
                existing.updated_at = datetime.utcnow()
                updated_count += 1
            else:
                db.add(JWSTObservation(**obs_data))
                added_count += 1

            # Periodic commit to avoid losing work
            if (added_count + updated_count) % 10 == 0:
                db.commit()
                print(f"Progress: {added_count} added, {updated_count} updated")

        # Final commit
        db.commit()

        print("\n✅ Fetch complete!")
        print(f"   Added: {added_count}")
        print(f"   Updated: {updated_count}")
        print(f"   Total observations in DB: {db.query(JWSTObservation).count()}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
        raise

    finally:
        db.close()


if __name__ == "__main__":
    fetch_jwst_observations(max_results=50)
