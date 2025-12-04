"""
Visual Progress Viewer for JWST Data Backfill
Shows detailed breakdown of what's completed and what remains.

Usage: python src/jobs/show_progress.py
"""

import json
import os
from datetime import datetime


PROGRESS_FILE = "progress.json"


def load_progress():
    """Load progress from JSON file"""
    if not os.path.exists(PROGRESS_FILE):
        return None
    
    try:
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    except:
        return None


def get_all_months():
    """Generate list of all months from Jan 2022 to current month"""
    months = []
    start_year = 2022
    current = datetime.now()
    
    for year in range(start_year, current.year + 1):
        for month in range(1, 13):
            if year == current.year and month > current.month:
                break
            months.append(f"{year}-{month:02d}")
    
    return months


def show_progress():
    """Display visual progress breakdown"""
    
    progress = load_progress()
    
    if not progress:
        print("\n⚠️  No progress file found. Run the fetch script first!")
        print("   python src/jobs/fetch_jwst_data.py\n")
        return
    
    all_months = get_all_months()
    completed = set(progress.get("completed_months", []))
    total_obs = progress.get("total_observations", 0)
    
    print("\n" + "=" * 70)
    print("🔭 JWST DATA BACKFILL PROGRESS")
    print("=" * 70)
    
    # Overall stats
    print(f"\n📊 Overall Progress: {len(completed)}/{len(all_months)} months ({len(completed)/len(all_months)*100:.1f}%)")
    print(f"📚 Total Observations: {total_obs:,}")
    
    if progress.get("last_run"):
        last_run = datetime.fromisoformat(progress["last_run"])
        print(f"🕐 Last Run: {last_run.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Year by year breakdown
    print("\n" + "-" * 70)
    print("BREAKDOWN BY YEAR:")
    print("-" * 70)
    
    years = {}
    for month in all_months:
        year = month[:4]
        if year not in years:
            years[year] = {"total": 0, "completed": 0, "months": []}
        years[year]["total"] += 1
        years[year]["months"].append(month)
        if month in completed:
            years[year]["completed"] += 1
    
    for year in sorted(years.keys()):
        data = years[year]
        pct = (data["completed"] / data["total"]) * 100
        
        # Progress bar
        bar_length = 30
        filled = int((data["completed"] / data["total"]) * bar_length)
        bar = "█" * filled + "░" * (bar_length - filled)
        
        print(f"\n{year}: {bar} {data['completed']}/{data['total']} ({pct:.0f}%)")
        
        # Show completed months
        completed_in_year = [m for m in data["months"] if m in completed]
        pending_in_year = [m for m in data["months"] if m not in completed]
        
        if completed_in_year:
            print(f"  ✅ Completed: {', '.join([m.split('-')[1] for m in completed_in_year])}")
        
        if pending_in_year:
            print(f"  ⏳ Pending: {', '.join([m.split('-')[1] for m in pending_in_year])}")
    
    # What's next
    print("\n" + "-" * 70)
    remaining = [m for m in reversed(all_months) if m not in completed]
    
    if remaining:
        print(f"📅 NEXT MONTH TO PROCESS: {remaining[0]}")
        print(f"⏳ Remaining: {len(remaining)} months")
        print("\n💡 Run this to continue:")
        print("   python src/jobs/fetch_jwst_data.py")
    else:
        print("🎉 ALL MONTHS COMPLETED!")
        print("   Your database is fully backfilled!")
    
    print("=" * 70 + "\n")


if __name__ == "__main__":
    show_progress()
    