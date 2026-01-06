import sys
import os
import subprocess
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# ==========================================
# 0. SETUP
# ==========================================
print("🎼 Full Power Parallel Orchestrator Initializing...")

current_file = Path(__file__).resolve()
project_root = current_file.parent
while not (project_root / 'src').exists():
    if project_root == project_root.parent:
        print("❌ Critical Error: Could not find Project Root.")
        sys.exit(1)
    project_root = project_root.parent

sys.path.append(str(project_root))

# ==========================================
# 1. CONFIGURATION (Path Mapping)
# ==========================================
SCRAPER_FT_DIR = project_root / "src" / "02_daily_performance" / "financial_times"
SCRAPER_YF_DIR = project_root / "src" / "02_daily_performance" / "yahoo_finance"
SCRAPER_SA_DIR = project_root / "src" / "02_daily_performance" / "stock_analysis"
SYNC_DIR = project_root / "src" / "05_db_synchronization" / "02_performance_sync"

PARALLEL_SCRAPERS = [
    ("01_ft_nav_scraper.py", SCRAPER_FT_DIR),
    ("01_yf_fund_nav_scraper.py", SCRAPER_YF_DIR),
    ("02_yf_etf_nav_scraper.py", SCRAPER_YF_DIR),  
    ("01_sa_nav_scraper.py", SCRAPER_SA_DIR)
]

REPAIR_SCRAPERS = [
    ("03_ft_nav_repair.py", SCRAPER_FT_DIR),       
    ("02_yf_fund_repair_scraper.py", SCRAPER_YF_DIR) 
]

SEQUENTIAL_STEPS = [
    ("00_performance_data_cleaner.py", SYNC_DIR),
    ("01_performance_validator.py", SYNC_DIR),
    ("03_daily_nav_loader.py", SYNC_DIR),
    ("05_performance_archiver.py", SYNC_DIR)
]

# ==========================================
# 2. CORE LOGIC
# ==========================================
def run_single_script(script_info):
    script_name, script_dir = script_info
    start_time = time.time()
    print(f"🚀 Started: {script_name}")
    
    try:
        subprocess.run(
            [sys.executable, str(script_dir / script_name)],
            check=True,
            text=True,
            cwd=str(script_dir)
        )
        duration = time.time() - start_time
        print(f"✅ Finished: {script_name} ({duration:.2f}s)")
        return True, script_name, duration
    except Exception as e:
        print(f"❌ Failed: {script_name} - {e}")
        return False, script_name, 0

def run_pipeline():
    total_start = time.time()
    print("=" * 75)
    print(f"🚀 STARTING FULL AUTOMATION PIPELINE (WITH REPAIR & ETF)")
    print("=" * 75)

    print("\n🌐 PHASE 1: Running Main Scrapers & ETF (Parallel)...")
    with ThreadPoolExecutor(max_workers=len(PARALLEL_SCRAPERS)) as executor:
        results_p1 = list(executor.map(run_single_script, PARALLEL_SCRAPERS))
 
    print("\n🔧 PHASE 2: Running Repair Scrapers (Parallel)...")
    with ThreadPoolExecutor(max_workers=len(REPAIR_SCRAPERS)) as executor:
        results_p2 = list(executor.map(run_single_script, REPAIR_SCRAPERS))

    for success, name, _ in (results_p1 + results_p2):
        if not success:
            print(f"🚨 Warning: {name} failed. Proceeding with caution...")

    print("\n⚙️  PHASE 3: Cleaning, Loading, and Archiving...")
    for script_info in SEQUENTIAL_STEPS:
        success, name, duration = run_single_script(script_info)
        if not success:
            print(f"🚨 Critical Failure in Step: {name}. Aborting Pipeline.")
            sys.exit(1)

    total_time = time.time() - total_start
    print("\n" + "=" * 75)
    print(f"🏆 MISSION ACCOMPLISHED: DATA SYNCED & ARCHIVED")
    print(f"⏱️  Total Pipeline Time: {total_time/60:.2f} minutes")
    print("=" * 75)

if __name__ == "__main__":
    run_pipeline()