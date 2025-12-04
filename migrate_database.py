"""
Database migration script to add new columns to existing observations table
Run this once to update your database schema
"""

import sys
import os
from sqlalchemy import text

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from src.db.database import engine, SessionLocal


def migrate_database():
    """Add new columns to the observations table"""
    print("Starting database migration...")
    print(f"Connecting to database...")
    
    migrations = [
        "ALTER TABLE observations ADD COLUMN IF NOT EXISTS dataproduct_type VARCHAR;",
        "ALTER TABLE observations ADD COLUMN IF NOT EXISTS calib_level INTEGER;",
        "ALTER TABLE observations ADD COLUMN IF NOT EXISTS wavelength_region VARCHAR;",
        "ALTER TABLE observations ADD COLUMN IF NOT EXISTS pi_name VARCHAR;",
        "ALTER TABLE observations ADD COLUMN IF NOT EXISTS target_classification VARCHAR;",
        "CREATE INDEX IF NOT EXISTS idx_observations_filter_name ON observations(filter_name);",
        "CREATE INDEX IF NOT EXISTS idx_observations_proposal_id ON observations(proposal_id);"
    ]
    
    try:
        db = SessionLocal()
        print("Database connection established!")
        
        for i, migration in enumerate(migrations, 1):
            print(f"[{i}/{len(migrations)}] Executing: {migration[:60]}...")
            db.execute(text(migration))
            db.commit()
            print(f"    ✓ Success")
        
        print("\n✅ Migration completed successfully!")
        print("Your database now has all the new fields.")
        
        db.close()
        
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    try:
        migrate_database()
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)