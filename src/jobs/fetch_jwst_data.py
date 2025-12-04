"""
JWST Data Fetcher with Smart Monthly Batch Processing
Supports both IMAGE and SPECTRUM data with appropriate metadata.

Usage: python src/jobs/fetch_jwst_data.py
"""

import sys
import os
import json
import traceback
from datetime import datetime, UTC
from pathlib import Path

from astroquery.mast import Observations
from astropy.time import Time
from sqlalchemy.orm import Session

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.db.database import SessionLocal, init_db
from src.db.models import JWSTObservation


# Configuration
MAX_RESULTS = 50
PROGRESS_FILE = "progress.json"


# -------------------------------------------------------
# PROGRESS TRACKING
# -------------------------------------------------------

def load_progress():
    """Load progress from JSON file"""
    if not os.path.exists(PROGRESS_FILE):
        return {"completed_months": [], "total_observations": 0}
    
    try:
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    except:
        return {"completed_months": [], "total_observations": 0}


def save_progress(progress):
    """Save progress to JSON file"""
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, indent=2, fp=f)


def get_all_months():
    """Generate list of all months from Jan 2022 to current month"""
    months = []
    start_year = 2022
    current = datetime.now()
    
    for year in range(start_year, current.year + 1):
        for month in range(1, 13):
            # Stop at current month
            if year == current.year and month > current.month:
                break
            months.append(f"{year}-{month:02d}")
    
    return months


def get_next_month_to_process(progress):
    """Determine next month that hasn't been fetched"""
    all_months = get_all_months()
    completed = set(progress.get("completed_months", []))
    
    # Find first uncompleted month (going backwards from most recent)
    for month in reversed(all_months):
        if month not in completed:
            return month
    
    return None


def month_to_mjd_range(year_month):
    """Convert YYYY-MM string to MJD date range"""
    year, month = map(int, year_month.split('-'))
    
    # Start of month
    start_dt = datetime(year, month, 1)
    
    # End of month (start of next month)
    if month == 12:
        end_dt = datetime(year + 1, 1, 1)
    else:
        end_dt = datetime(year, month + 1, 1)
    
    # Convert to MJD
    start_mjd = Time(start_dt).mjd
    end_mjd = Time(end_dt).mjd
    
    return start_mjd, end_mjd


# -------------------------------------------------------
# HELPER FUNCTIONS
# -------------------------------------------------------

def clean_value(val):
    """Convert masked/nan values to None for database compatibility"""
    if val is None:
        return None
    if hasattr(val, 'mask') and val.mask:
        return None
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
    if uri_or_path.startswith("http"):
        return uri_or_path
    if uri_or_path.startswith("mast:"):
        return f"https://mast.stsci.edu/api/v0.1/Download/file?uri={uri_or_path}"
    if uri_or_path.lower().endswith((".fits", ".jpg", ".jpeg", ".png")):
        return f"https://mast.stsci.edu/api/v0.1/Download/file?uri=mast:JWST/product/{uri_or_path}"
    return None


def extract_preview_url(prod: dict) -> str | None:
    """Select best preview URL."""
    if prod.get("jpegURL"):
        return mast_to_public_url(prod["jpegURL"])
    if prod.get("pngURL"):
        return mast_to_public_url(prod["pngURL"])
    uri = prod.get("dataURI")
    if uri and prod.get("dataproduct_type") == "image":
        return mast_to_public_url(uri)
    filename = prod.get("productFilename")
    if filename:
        return mast_to_public_url(filename)
    return None


def extract_fits_url(prod: dict) -> str | None:
    """Select best FITS file URL."""
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


def extract_spectrum_metadata(obs: dict) -> dict:
    """
    Extract spectrum-specific metadata from observation.
    Returns dict with spectral resolution, wavelength range, grating, etc.
    """
    spectrum_meta = {}
    
    # Spectral resolution (if available)
    if obs.get('em_res_power'):
        spectrum_meta['spectral_resolution'] = float(obs['em_res_power'])
    
    # Wavelength range (convert from meters to microns if needed)
    if obs.get('em_min'):
        # MAST stores wavelengths in meters, convert to microns
        wl_min = float(obs['em_min'])
        spectrum_meta['wavelength_min'] = wl_min * 1e6 if wl_min < 0.001 else wl_min
    
    if obs.get('em_max'):
        wl_max = float(obs['em_max'])
        spectrum_meta['wavelength_max'] = wl_max * 1e6 if wl_max < 0.001 else wl_max
    
    # Grating/disperser information (often in filters field for spectra)
    filters = clean_value(obs.get("filters"))
    if filters:
        # JWST gratings: PRISM, G140M, G235M, G395M, G140H, G235H, G395H, etc.
        grating_keywords = ['PRISM', 'G140', 'G235', 'G395', 'G150', 'G235']
        for keyword in grating_keywords:
            if keyword in filters.upper():
                spectrum_meta['grating'] = filters
                break
    
    return spectrum_meta


# -------------------------------------------------------
# MAIN FETCH LOGIC
# -------------------------------------------------------

def fetch_month(year_month: str, limit=MAX_RESULTS):
    """Fetch JWST observations (images AND spectra) for a specific month"""
    
    print(f"\n📅 Processing: {year_month}")
    print("=" * 60)
    
    # Get MJD range for this month
    start_mjd, end_mjd = month_to_mjd_range(year_month)
    
    # Query MAST for this specific month - NOW INCLUDING SPECTRA
    print(f"🔍 Querying MAST for observations in {year_month}...")
    obs_table = Observations.query_criteria(
        obs_collection="JWST",
        dataproduct_type=["image", "spectrum"],  # ← NOW INCLUDES SPECTRA!
        calib_level=[2, 3],
        dataRights="PUBLIC",
        t_min=[start_mjd, end_mjd]
    )
    
    print(f"📊 Found {len(obs_table)} observations for {year_month}")
    
    if len(obs_table) == 0:
        print(f"⚠️  No observations found for {year_month} - marking as complete")
        return 0, 0, 0
    
    # Count by type
    image_count = sum(1 for obs in obs_table if obs.get('dataproduct_type') == 'image')
    spectrum_count = sum(1 for obs in obs_table if obs.get('dataproduct_type') == 'spectrum')
    print(f"   📷 Images: {image_count}")
    print(f"   📊 Spectra: {spectrum_count}")
    
    # Process observations
    db = SessionLocal()
    added = 0
    updated = 0
    skipped = 0
    processed = 0

    # Load existing obs_ids to skip updates
    existing_obs_ids = set(
        r[0] for r in db.query(JWSTObservation.obs_id)
                     .filter(JWSTObservation.obs_id.in_(
                         [clean_value(obs.get("obsid") or obs.get("obs_id")) for obs in obs_table]
                     ))
                     .all()
    )

    for obs in obs_table:
        processed += 1
        
        obsid = clean_value(obs.get("obsid") or obs.get("obs_id"))
        if not obsid:
            skipped += 1
            continue

        # Skip if already exists
        if obsid in existing_obs_ids:
            skipped += 1
            continue

        # Get product list
        try:
            products = Observations.get_product_list(obs)
        except:
            skipped += 1
            continue

        if len(products) == 0:
            skipped += 1
            continue

        # Extract URLs
        preview = None
        fits = None
        for prod in products:
            if not preview:
                preview = extract_preview_url(prod)
            if not fits:
                fits = extract_fits_url(prod)
            if preview and fits:
                break

        if not preview and not fits:
            skipped += 1
            continue

        # Parse date
        obs_date = None
        if obs.get('t_min'):
            try:
                t = Time(obs['t_min'], format='mjd')
                obs_date = t.datetime
            except:
                pass

        # Get data product type
        dataproduct_type = clean_value(obs.get("dataproduct_type"))

        # Prepare base metadata
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
            "dataproduct_type": dataproduct_type,
            "calib_level": int(obs.get("calib_level")) if obs.get("calib_level") else None,
            "wavelength_region": clean_value(obs.get("wavelength_region")),
            "pi_name": clean_value(obs.get("proposal_pi")),
            "target_classification": clean_value(obs.get("target_classification")),
            "updated_at": datetime.now(UTC),
        }

        # Add spectrum-specific metadata if this is a spectrum
        if dataproduct_type == "spectrum":
            spectrum_meta = extract_spectrum_metadata(obs)
            metadata.update({
                "spectral_resolution": spectrum_meta.get("spectral_resolution"),
                "wavelength_min": spectrum_meta.get("wavelength_min"),
                "wavelength_max": spectrum_meta.get("wavelength_max"),
                "grating": spectrum_meta.get("grating"),
                "dispersion_axis": None,  # Not readily available in MAST metadata
                "slit_width": None,  # Not readily available in MAST metadata
            })

        # Add new observation
        obj = JWSTObservation(**metadata)
        db.add(obj)
        added += 1

        # Commit in batches for progress
        if added % 25 == 0:
            db.commit()
            print(f"  Progress: {added} added, {skipped} skipped ({processed}/{len(obs_table)})")

    db.commit()
    db.close()

    return added, 0, skipped


# -------------------------------------------------------
# MAIN EXECUTION
# -------------------------------------------------------

def main():
    """Main execution with smart month selection"""
    
    print("\n" + "=" * 60)
    print("🔭 JWST DATA FETCHER - Images & Spectra")
    print("=" * 60)
    
    # Initialize database
    init_db()
    
    # Load progress
    progress = load_progress()
    
    # Show current progress
    all_months = get_all_months()
    completed_count = len(progress.get("completed_months", []))
    total_count = len(all_months)
    
    print(f"\n📊 Current Progress: {completed_count}/{total_count} months completed ({completed_count/total_count*100:.1f}%)")
    
    # Find next month to process
    next_month = get_next_month_to_process(progress)
    
    if not next_month:
        print("\n🎉 ALL MONTHS COMPLETED! Your database is fully backfilled!")
        print(f"📚 Total observations: {progress.get('total_observations', 0)}")
        return
    
    print(f"📅 Next month to process: {next_month}")
    print()
    
    try:
        # Fetch this month's data
        added, updated, skipped = fetch_month(next_month)
        
        # Update progress
        if next_month not in progress["completed_months"]:
            progress["completed_months"].append(next_month)
        
        # Get total observation count
        db = SessionLocal()
        total_obs = db.query(JWSTObservation).count()
        db.close()
        
        progress["total_observations"] = total_obs
        progress["last_run"] = datetime.now(UTC).isoformat()
        
        save_progress(progress)
        
        # Summary
        print("\n" + "=" * 60)
        print(f"✅ {next_month} COMPLETE!")
        print("=" * 60)
        print(f"   Added: {added} new observations")
        print(f"   Updated: {updated} existing observations")
        print(f"   Skipped: {skipped} (already in database or no valid URLs)")
        print(f"   Total in database: {total_obs}")
        print()
        print(f"📊 Overall Progress: {len(progress['completed_months'])}/{len(all_months)} months ({len(progress['completed_months'])/len(all_months)*100:.1f}%)")
        
        # Show next month
        next_next_month = get_next_month_to_process(progress)
        if next_next_month:
            print(f"📅 Next run will process: {next_next_month}")
            print("\n💡 Run the script again to continue!")
        else:
            print("\n🎉 ALL DONE! Database fully backfilled!")
        
        print("=" * 60 + "\n")
        
    except Exception as e:
        print(f"\n❌ ERROR processing {next_month}:")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
