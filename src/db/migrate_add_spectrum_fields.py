"""
Migration script to add spectrum-specific fields to existing database
Run this ONCE after updating models.py
"""

from sqlalchemy import text
from src.db.database import engine

def migrate():
    """Add new spectrum fields to observations table"""
    
    with engine.connect() as conn:
        # Add new columns
        columns_to_add = [
            "ALTER TABLE observations ADD COLUMN IF NOT EXISTS spectral_resolution FLOAT",
            "ALTER TABLE observations ADD COLUMN IF NOT EXISTS wavelength_min FLOAT",
            "ALTER TABLE observations ADD COLUMN IF NOT EXISTS wavelength_max FLOAT",
            "ALTER TABLE observations ADD COLUMN IF NOT EXISTS dispersion_axis INTEGER",
            "ALTER TABLE observations ADD COLUMN IF NOT EXISTS grating VARCHAR",
            "ALTER TABLE observations ADD COLUMN IF NOT EXISTS slit_width FLOAT",
        ]
        
        for sql in columns_to_add:
            try:
                conn.execute(text(sql))
                conn.commit()
                print(f"✅ Executed: {sql}")
            except Exception as e:
                print(f"⚠️  Could not execute {sql}: {e}")
    
    print("\n✅ Migration complete!")

if __name__ == "__main__":
    migrate()