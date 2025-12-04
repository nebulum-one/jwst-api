"""
Job for fetching JWST observation data from NASA's MAST archive
Run this script to populate the database with JWST observations
"""

from astroquery.mast import Observations
from sqlalchemy.orm import Session
from datetime import datetime
import sys
import os

# Add parent directory to path to import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.db.database import SessionLocal, init_db
from src.db.models import JWSTObservation


def mast_uri_to_url(uri):
    """Convert MAST URI to downloadable HTTP URL"""
    if uri and uri.startswith('mast:'):
        return f"https://mast.stsci.edu/api/v0.1/Download/file?uri={uri}"
    return uri


def fetch_jwst_observations(max_results=100):
    """
    Fetch JWST observations from MAST and store in database
    
    Args:
        max_results: Maximum number of observations to fetch per run
    """
    print(f"Starting JWST data fetch... (max {max_results} observations)")
    
    # Initialize database
    init_db()
    
    # Create database session
    db = SessionLocal()
    
    try:
        # Query MAST for JWST observations
        print("Querying MAST archive...")
        obs_table = Observations.query_criteria(
            obs_collection='JWST',
            dataproduct_type='image',
            calib_level=[2, 3],  # Calibrated data
            dataRights='PUBLIC'  # Only public data (no access denied errors)
        )
        
        print(f"Found {len(obs_table)} observations in MAST")
        
        # Limit results
        obs_table = obs_table[:max_results]
        
        added_count = 0
        updated_count = 0
        
        for row in obs_table:
            obs_id = row['obs_id']
            
            # Check if observation already exists
            existing = db.query(JWSTObservation).filter(
                JWSTObservation.obs_id == obs_id
            ).first()
            
            # Parse observation date
            obs_date = None
            if row.get('t_min'):
                try:
                    # Convert MJD to datetime
                    from astropy.time import Time
                    t = Time(row['t_min'], format='mjd')
                    obs_date = t.datetime
                except:
                    pass
            
            # Prepare observation data with ALL fields (using correct MAST field names)
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
                # New metadata fields (using CORRECT MAST field names)
                'dataproduct_type': row.get('dataproduct_type', ''),
                'calib_level': int(row['calib_level']) if row.get('calib_level') else None,
                'wavelength_region': row.get('wavelength_region', ''),
                'pi_name': row.get('proposal_pi', ''),  # MAST calls it 'proposal_pi'
                'target_classification': row.get('target_classification', ''),
                # Get URLs directly from MAST (they provide jpegURL and dataURL)
                'preview_url': row.get('jpegURL', ''),
                'fits_url': row.get('dataURL', '')
            }
            
            # URLs are now directly from MAST metadata (jpegURL and dataURL)
            # No need to fetch product list separately anymore!
            
            if existing:
                # Update existing observation with new data
                for key, value in obs_data.items():
                    setattr(existing, key, value)
                existing.updated_at = datetime.utcnow()
                updated_count += 1
            else:
                # Create new observation
                new_obs = JWSTObservation(**obs_data)
                db.add(new_obs)
                added_count += 1
            
            # Commit every 10 observations to avoid losing progress
            if (added_count + updated_count) % 10 == 0:
                db.commit()
                print(f"Progress: {added_count} added, {updated_count} updated")
        
        # Final commit
        db.commit()
        
        print(f"\n✅ Fetch complete!")
        print(f"   Added: {added_count} new observations")
        print(f"   Updated: {updated_count} existing observations")
        print(f"   Total in database: {db.query(JWSTObservation).count()}")
        
    except Exception as e:
        print(f"❌ Error fetching data: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    # You can adjust max_results here
    fetch_jwst_observations(max_results=50)
