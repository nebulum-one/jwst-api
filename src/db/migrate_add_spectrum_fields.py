"""
Migration script to add spectrum-specific fields to existing database
Run this ONCE after updating models.py

Usage: python src/db/migrate_add_spectrum_fields.py
"""

import sys
import os
from sqlalchemy import text

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.db.database import engine

def migrate():
    """Add new spectrum fields to observations table"""
    
    print("🔧 Adding spectrum-specific fields to database...")
    print("=" * 60)
    
    with engine.connect() as conn:
        # Add new columns
        columns_to_add = [
            "ALTER TABLE observations ADD COLUMN IF NOT EXISTS spectral_resolution FLOAT",
            "ALTER TABLE observations ADD COLUMN IF NOT EXISTS wavelength_min FLOAT",
            "ALTER TABLE observations ADD COLUMN IF NOT EXISTS wavelength_max FLOAT",
            "ALTER TABLE observations ADD COLUMN IF NOT EXISTS dispersion_axis INTEGER",
            "ALTER TABLE observations ADD COLUMN IF NOT EXISTS grating VARCHAR",
            "ALTER TABLE observations ADD COLUMN IF NOT EXISTS slit_width FLOAT",
            "CREATE INDEX IF NOT EXISTS idx_observations_dataproduct_type ON observations(dataproduct_type)",
            "CREATE INDEX IF NOT EXISTS idx_observations_grating ON observations(grating)"
        ]
        
        for sql in columns_to_add:
            try:
                conn.execute(text(sql))
                conn.commit()
                print(f"✅ {sql[:50]}...")
            except Exception as e:
                print(f"⚠️  Could not execute: {e}")
    
    print("=" * 60)
    print("✅ Migration complete! Spectrum fields added.")
    print("\nYou can now run: python src/jobs/fetch_jwst_data.py")

if __name__ == "__main__":
    migrate()
