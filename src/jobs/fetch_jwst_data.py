"""
Job for fetching JWST observation data from NASA's MAST archive
Run this script to populate the database with JWST observations

Usage: python src/jobs/fetch_jwst_data.py
"""

from astroquery.mast import Observations
from astropy.time import Time
from datetime import datetime, UTC
import sys
import os
import traceback

# Add parent directory to path to import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.db.database import SessionLocal, init_db
from src.db.models import JWSTObservation


# Configuration
MAX_RESULTS = 50  # How many observations to add/update per run


# -------------------------------------------------------
# URL CONVERSION HELPERS
# -------------------------------------------------------

def mast_to_public_url(uri_or_path: str | None) -> str | None:
    """Convert a mast:JWST/... URI or filename into a public HTTPS download URL."""
    if not uri_or_path:
        return None

    # Already an HTTPS URL
    if uri_or_path.startswith("http"):
        return uri_or_path

    # Convert mast: URIs
    if uri_or_path.startswith("mast:"):
        return f"https://mast.stsci.edu/api/v0.1/Download/file?uri={uri_or_path}"

    # Convert product filenames (e.g., jw01234...fits)
    if uri_or_path.lower().endswith((".fits", ".jpg", ".jpeg", ".png")):
        return f"https://mast.stsci.edu/api/v0.1/Download/file?uri=mast:JWST/product/{uri_or_path}"

    return None


def extract_preview_url(prod: dict) -> str | None:
    """Extract the best preview image URL from a product."""
    # Priority: jpegURL > pngURL > dataURI (if image) > productFilename
    
    if prod.get("jpegURL"):
        return mast_to_public_url(prod["jpegURL"])
    
    if prod.get("pngURL"):
        return mast_to_public_url(prod["pngURL"])
    
    uri = prod.get("dataURI")
    if uri and prod.get("dataproduct_type") == "image":
        return mast_to_public_url(uri)
    
    filename = prod.get("productFilename")
    if filename and filename.lower().endswith((".jpg", ".jpeg", ".png")):
        return mast_to_public_url(filename)

    return None


def extract_fits_url(prod: dict) -> str | None:
    """Extract the best FITS file URL from a product."""
    # Priority: dataURI > productFilename > dataURL
    
    uri = prod.get("dataURI")
    if uri and uri.lower().endswith(".fits"):
        return mast_to_public_url(uri)
    
    filename = prod.get("productFilename")
    if filename and filename.lower().endswith(".fits"):
        return mast_to_public_url(filename)
    
    data_url = prod.get("dataURL")
    if data_url and data_url.lower().endswith(".fits"):
        return mast_to_public_url(data_url)

    return None


# -------------------------------------------------------
# MAIN FETCH LOGIC
# -------------------------------------------------------

def fetch_jwst_observations(max_results=MAX_RESULTS):
    """
    Fetch JWST observations from MAST and store in database
    
    Args:
        max_results: Maximum number of observations to add per run
    """
    print(f"\n🚀 Starting JWST data fetch (max {max_results} observations)...\n")
    
    # Initialize database
    init_db()
    
    # Create database session
    db = SessionLocal()
    
    try:
        # Query MAST for PUBLIC JWST observations
        # Limit to 3x max_results to scan through for valid URLs
        SCAN_LIMIT = max_results * 3
        
        print(f"Querying MAST archive (scanning up to {SCAN_LIMIT} entries)...")
obs_table = Observations.query_criteria(
    obs_collection="JWST",
    dataproduct_type=["image"],
    calib_level=[3]  # Only fully processed data
)[:150]  # Take first 150 only
        
        print(f"Found {len(obs_table)} public observations in MAST")
        
        # Limit how many we scan through
        obs_table = obs_table[:SCAN_LIMIT]
        print(f"Processing first {len(obs_table)} observations...\n")
        
        added_count = 0
        updated_count = 0
        processed_count = 0
        skipped_count = 0
        
        for row in obs_table:
            # Stop if we've added enough observations
            if added_count >= max_results:
                print(f"\nReached target of {max_results} new observations. Stopping.")
                break
            
            processed_count += 1
            
            obs_id = row.get('obs_id')
            if not obs_id:
                skipped_count += 1
                continue
            
            # Check if observation already exists
            existing = db.query(JWSTObservation).filter(
                JWSTObservation.obs_id == obs_id
            ).first()
            
            # Parse observation date from MJD
            obs_date = None
            if row.get('t_min'):
                try:
                    t = Time(row['t_min'], format='mjd')
                    obs_date = t.datetime
                except:
                    pass
            
            # Get product list to extract URLs
            preview_url = None
            fits_url = None
            
            try:
                products = Observations.get_product_list(row)
                
                # Scan products for best preview and FITS URLs
                for prod in products:
                    if not preview_url:
                        preview_url = extract_preview_url(prod)
                    if not fits_url:
                        fits_url = extract_fits_url(prod)
                    
                    # Stop if we have both
                    if preview_url and fits_url:
                        break
                
            except Exception as e:
                # If we can't get products, skip this observation
                skipped_count += 1
                continue
            
            # Skip if no valid URLs found
            if not preview_url and not fits_url:
                skipped_count += 1
                continue
            
            # Prepare observation data with ALL fields
            obs_data = {
                'obs_id': obs_id,
                'target_name': row.get('target_name', ''),
                'ra': float(row['s_ra']) if row.get('s_ra') else None,
                'dec': float(row['s_dec']) if row.get('s_dec') else None,
                'instrument': row.get('instrument_name', ''),
                'filter_name': row.get('filters', ''),
                'observation_date': obs_date,
                'proposal_id': row.get('proposal_id', ''),
                'exposure_time': float(row['t_exptime']) if row.get('t_exptime') else None,
                'description': row.get('obs_title', ''),
                # Metadata fields
                'dataproduct_type': row.get('dataproduct_type', ''),
                'calib_level': int(row['calib_level']) if row.get('calib_level') else None,
                'wavelength_region': row.get('wavelength_region', ''),
                'pi_name': row.get('proposal_pi', ''),
                'target_classification': row.get('target_classification', ''),
                # URLs
                'preview_url': preview_url,
                'fits_url': fits_url,
                'updated_at': datetime.now(UTC)
            }
            
            if existing:
                # Update existing observation
                for key, value in obs_data.items():
                    setattr(existing, key, value)
                updated_count += 1
            else:
                # Create new observation
                new_obs = JWSTObservation(**obs_data)
                db.add(new_obs)
                added_count += 1
            
            # Commit every 10 observations to save progress
            if (added_count + updated_count) % 10 == 0:
                db.commit()
                print(f"Progress: {added_count} added, {updated_count} updated, {skipped_count} skipped (processed {processed_count}/{len(obs_table)})")
        
        # Final commit
        db.commit()
        
        print(f"\n✅ Fetch complete!")
        print(f"   Added: {added_count} new observations")
        print(f"   Updated: {updated_count} existing observations")
        print(f"   Skipped: {skipped_count} (no valid URLs)")
        print(f"   Total in database: {db.query(JWSTObservation).count()}")
        print()
        
    except Exception as e:
        print(f"❌ Error fetching data: {e}")
        traceback.print_exc()
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    # You can adjust max_results here
    fetch_jwst_observations(max_results=50)
