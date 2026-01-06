import os
from pathlib import Path

# ==========================================
# 1. Project Configuration & Root Path
# ==========================================
# ถอยหลัง 3 สเต็ปจากไฟล์นี้ (src/utils/path_manager.py) เพื่อหา Root
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# ==========================================
# 2. Infrastructure Directories
# ==========================================
CONFIG_DIR = BASE_DIR / "config"
LOG_DIR = BASE_DIR / "logs"
SRC_DIR = BASE_DIR / "src"
OUTPUT_DIR = BASE_DIR / "outputs" # Folder Outputs กลาง

# --- 2.1 Validation Outputs Root ---
VALIDATION_DIR = BASE_DIR / "validation_output"

# --- Financial Times Validation ---
VAL_FT_DIR = VALIDATION_DIR / "Financial_Times"
VAL_FT_MASTER    = VAL_FT_DIR / "01_List_Master"
VAL_FT_NAV       = VAL_FT_DIR / "02_Daily_NAV"
VAL_FT_HIST      = VAL_FT_DIR / "02_Price_And_Dividend_History"
VAL_FT_STATIC    = VAL_FT_DIR / "03_Detail_Static"
VAL_FT_HOLDINGS  = VAL_FT_DIR / "04_Holdings"

# --- Stock Analysis Validation ---
VAL_SA_DIR = VALIDATION_DIR / "Stock_Analysis"
VAL_SA_MASTER    = VAL_SA_DIR / "01_List_Master"
VAL_SA_NAV       = VAL_SA_DIR / "02_Daily_NAV"
VAL_SA_HIST      = VAL_SA_DIR / "02_Price_And_Dividend_History"
VAL_SA_STATIC    = VAL_SA_DIR / "03_Detail_Static"
VAL_SA_HOLDINGS  = VAL_SA_DIR / "04_Holdings"

# --- Yahoo Finance Validation ---
VAL_YF_DIR = VALIDATION_DIR / "Yahoo_Finance"
VAL_YF_MASTER    = VAL_YF_DIR / "01_List_Master"
VAL_YF_NAV       = VAL_YF_DIR / "02_Daily_NAV"
VAL_YF_HIST      = VAL_YF_DIR / "02_Price_And_Dividend_History"
VAL_YF_STATIC    = VAL_YF_DIR / "03_Detail_Static"
VAL_YF_HOLDINGS  = VAL_YF_DIR / "04_Holdings"

# --- System & Queues Validation ---
VAL_SYS_DIR = VALIDATION_DIR / "System"
VAL_SYS_QUEUES  = VAL_SYS_DIR / "Queues"
VAL_SYS_DUPES   = VAL_SYS_QUEUES / "Duplicate_Reports"
VAL_SYS_REPORTS = VAL_SYS_QUEUES / "Validation_Reports"

# --- 2.2 Auth & Session Storage (NEW) ---
# เก็บไฟล์ session (json/cookies) ที่ login แล้วไว้ที่นี่
AUTH_DIR = OUTPUT_DIR / "auth"

# ==========================================
# 3. Data Store Directories
# ==========================================
DATA_STORE_DIR = BASE_DIR / "data_store"

DATA_MASTER_LIST_DIR = DATA_STORE_DIR / "01_master_list"
DATA_MASTER_READY_DIR = DATA_MASTER_LIST_DIR / "04_ready_to_load" # Stage 04 Support

DATA_PERFORMANCE_DIR = DATA_STORE_DIR / "02_performance"
DATA_STATIC_DETAILS_DIR = DATA_STORE_DIR / "03_static_details"
DATA_HOLDINGS_DIR = DATA_STORE_DIR / "04_holdings"

# ==========================================
# 4. Scraper Scripts Paths (Acquisition)
# ==========================================

# --- 4.1 Master List Acquisition ---
SRC_ACQUISITION_DIR = SRC_DIR / "01_master_list_acquisition"
SCRAPER_MASTER_FT = SRC_ACQUISITION_DIR / "01_ft_list_scraper.py"
SCRAPER_MASTER_YF = SRC_ACQUISITION_DIR / "02_yf_list_scraper.py"
SCRAPER_MASTER_SA = SRC_ACQUISITION_DIR / "03_sa_list_scraper.py"

# --- 4.2 Daily Performance ---
SRC_PERFORMANCE_DIR = SRC_DIR / "02_daily_performance"

# FT
PERF_FT_DIR = SRC_PERFORMANCE_DIR / "financial_times"
SCRAPER_PERF_FT_NAV     = PERF_FT_DIR / "01_ft_nav_scraper.py"
SCRAPER_PERF_FT_HISTORY = PERF_FT_DIR / "02_ft_history_scraper.py"
SCRAPER_PERF_FT_REPAIR  = PERF_FT_DIR / "03_ft_nav_repair.py"

# SA
PERF_SA_DIR = SRC_PERFORMANCE_DIR / "stock_analysis"
SCRAPER_PERF_SA_NAV      = PERF_SA_DIR / "01_sa_nav_scraper.py"
SCRAPER_PERF_SA_HISTORY  = PERF_SA_DIR / "02_sa_price_history_scraper.py"
SCRAPER_PERF_SA_DIVIDEND = PERF_SA_DIR / "03_sa_dividend_scraper.py"

# YF
PERF_YF_DIR = SRC_PERFORMANCE_DIR / "yahoo_finance"
SCRAPER_PERF_YF_FUND_NAV     = PERF_YF_DIR / "01_yf_fund_nav_scraper.py"
SCRAPER_PERF_YF_ETF_NAV      = PERF_YF_DIR / "02_yf_etf_nav_scraper.py"

# Repair & Sweep for YF
SCRAPER_PERF_YF_FUND_REPAIR  = PERF_YF_DIR / "02_yf_repair_scraper.py"
SCRAPER_PERF_YF_ETF_REPAIR   = PERF_YF_DIR / "02_yf_etf_repair_scraper.py"
SCRAPER_PERF_YF_FUND_SWEEP   = PERF_YF_DIR / "03_yf_final_sweep.py"
SCRAPER_PERF_YF_ETF_SWEEP    = PERF_YF_DIR / "03_yf_etf_final_sweep.py"

SCRAPER_PERF_YF_FUND_HISTORY = PERF_YF_DIR / "03_yf_fund_price_history_scraper.py"
SCRAPER_PERF_YF_ETF_HISTORY  = PERF_YF_DIR / "04_yf_etf_price_history_scraper.py"
SCRAPER_PERF_YF_FUND_DIV     = PERF_YF_DIR / "05_yf_fund_dividend_scraper.py"
SCRAPER_PERF_YF_ETF_DIV      = PERF_YF_DIR / "06_yf_etf_dividend_scraper.py"

# --- 4.3 Static Details ---
SRC_STATIC_DIR = SRC_DIR / "03_master_detail_static"

# FT
STATIC_FT_DIR = SRC_STATIC_DIR / "financial_times"
SCRAPER_STATIC_FT_IDENTITY = STATIC_FT_DIR / "01_ft_identity.py"
SCRAPER_STATIC_FT_FEES     = STATIC_FT_DIR / "02_ft_fees_ops.py"
SCRAPER_STATIC_FT_RISK     = STATIC_FT_DIR / "03_ft_risk_ratings.py"
SCRAPER_STATIC_FT_POLICY   = STATIC_FT_DIR / "04_ft_policy.py"

# SA (✅ Updated: เพิ่ม 01_sa_detail_scraper.py)
STATIC_SA_DIR = SRC_STATIC_DIR / "stock_analysis"
SCRAPER_STATIC_SA_DETAIL   = STATIC_SA_DIR / "01_sa_detail_scraper.py" 
SCRAPER_STATIC_SA_FEES     = STATIC_SA_DIR / "02_sa_fees_ops.py"
SCRAPER_STATIC_SA_RISK     = STATIC_SA_DIR / "03_sa_risk_ratings.py"
SCRAPER_STATIC_SA_POLICY   = STATIC_SA_DIR / "04_sa_policy.py"

# YF
STATIC_YF_DIR = SRC_STATIC_DIR / "yahoo_finance"
SCRAPER_STATIC_YF_IDENTITY = STATIC_YF_DIR / "01_yf_identity.py"
SCRAPER_STATIC_YF_FEES     = STATIC_YF_DIR / "02_yf_fees_ops.py"
SCRAPER_STATIC_YF_RISK     = STATIC_YF_DIR / "03_yf_risk_ratings.py"
SCRAPER_STATIC_YF_POLICY   = STATIC_YF_DIR / "04_yf_policy.py"

# --- 4.4 Holdings Acquisition ---
SRC_HOLDINGS_DIR = SRC_DIR / "04_holdings_acquisition"

# FT
HOLDINGS_FT_DIR = SRC_HOLDINGS_DIR / "financial_times"
SCRAPER_HOLDINGS_FT_HOLDINGS    = HOLDINGS_FT_DIR / "01_ft_holdings_scraper.py"
SCRAPER_HOLDINGS_FT_ALLOCATIONS = HOLDINGS_FT_DIR / "02_ft_allocations_scraper.py"

# SA
HOLDINGS_SA_DIR = SRC_HOLDINGS_DIR / "stock_analysis"
SCRAPER_HOLDINGS_SA_HOLDINGS    = HOLDINGS_SA_DIR / "01_sa_holdings_scraper.py"
SCRAPER_HOLDINGS_SA_ALLOCATIONS = HOLDINGS_SA_DIR / "02_sa_allocations_scraper.py"

# YF
HOLDINGS_YF_DIR = SRC_HOLDINGS_DIR / "yahoo_finance"
SCRAPER_HOLDINGS_YF_HOLDINGS    = HOLDINGS_YF_DIR / "01_yf_holdings_scraper.py"
SCRAPER_HOLDINGS_YF_ALLOCATIONS = HOLDINGS_YF_DIR / "02_yf_allocations_scraper.py"


# ==========================================
# 5. DB Synchronization Scripts
# ==========================================
SRC_DB_SYNC_DIR = SRC_DIR / "05_db_synchronization"

# --- 5.0 Main Pipeline ---
SYNC_MAIN_PIPELINE = SRC_DB_SYNC_DIR / "main_pipeline.py"

# --- 5.1 Master Sync ---
SYNC_MASTER_DIR = SRC_DB_SYNC_DIR / "01_master_sync"
SYNC_MASTER_CLEANER      = SYNC_MASTER_DIR / "00_master_list_cleaner.py"
SYNC_MASTER_CONSOLIDATOR = SYNC_MASTER_DIR / "01_source_consolidator.py"
SYNC_MASTER_VALIDATOR    = SYNC_MASTER_DIR / "02_master_list_validator.py"
SYNC_MASTER_REMEDIATOR   = SYNC_MASTER_DIR / "03_master_list_remediator.py"
SYNC_MASTER_LOADER       = SYNC_MASTER_DIR / "04_master_list_loader.py"
SYNC_MASTER_STATUS_MGR   = SYNC_MASTER_DIR / "05_status_manager.py"
SYNC_MASTER_ARCHIVER     = SYNC_MASTER_DIR / "06_master_data_archiver.py"
SYNC_MASTER_ORCHESTRATOR = SYNC_MASTER_DIR / "07_master_sync_orchestrator.py"

# --- 5.2 Performance Sync ---
SYNC_PERF_DIR = SRC_DB_SYNC_DIR / "02_performance_sync"
SYNC_PERF_CLEANER     = SYNC_PERF_DIR / "00_performance_data_cleaner.py"
SYNC_PERF_VALIDATOR   = SYNC_PERF_DIR / "01_performance_validator.py"
SYNC_PERF_HASHER      = SYNC_PERF_DIR / "02_history_hasher.py"
SYNC_PERF_LOADER_NAV  = SYNC_PERF_DIR / "03_daily_nav_loader.py"
SYNC_PERF_LOADER_DIV  = SYNC_PERF_DIR / "03_dividend_loader.py"
SYNC_PERF_LOADER_HIST = SYNC_PERF_DIR / "03_price_history_loader.py"
SYNC_PERF_GAP_CHECKER = SYNC_PERF_DIR / "04_nav_gap_checker.py"
SYNC_PERF_ARCHIVER    = SYNC_PERF_DIR / "05_performance_archiver.py"
SYNC_PERF_ORCHESTRATOR = SYNC_PERF_DIR / "06_performance_sync_orchestrator.py"

# --- 5.3 Detail Sync ---
SYNC_DETAIL_DIR = SRC_DB_SYNC_DIR / "03_detail_sync"
SYNC_DETAIL_CLEANER   = SYNC_DETAIL_DIR / "00_static_data_cleaner.py"
SYNC_DETAIL_VALIDATOR = SYNC_DETAIL_DIR / "01_detail_validator.py"
SYNC_DETAIL_HASHER    = SYNC_DETAIL_DIR / "02_static_hasher.py"
SYNC_DETAIL_LOADER    = SYNC_DETAIL_DIR / "03_fund_detail_loader.py"
SYNC_DETAIL_ARCHIVER  = SYNC_DETAIL_DIR / "04_detail_archiver.py"
SYNC_DETAIL_ORCHESTRATOR = SYNC_DETAIL_DIR / "05_detail_sync_orchestrator.py"

# --- 5.4 Holdings Sync ---
SYNC_HOLDINGS_DIR = SRC_DB_SYNC_DIR / "04_holdings_sync"
SYNC_HOLDINGS_CLEANER    = SYNC_HOLDINGS_DIR / "00_holdings_data_cleaner.py"
SYNC_HOLDINGS_INTEGRITY  = SYNC_HOLDINGS_DIR / "01_holdings_integrity_checker.py"
SYNC_HOLDINGS_HASHER     = SYNC_HOLDINGS_DIR / "02_holdings_hasher.py"
SYNC_HOLDINGS_LOADER     = SYNC_HOLDINGS_DIR / "03_holdings_loader.py"
SYNC_HOLDINGS_ALLOC_LOAD = SYNC_HOLDINGS_DIR / "04_allocations_loader.py"
SYNC_HOLDINGS_ARCHIVER   = SYNC_HOLDINGS_DIR / "05_holdings_archiver.py"
SYNC_HOLDINGS_ORCHESTRATOR = SYNC_HOLDINGS_DIR / "06_holdings_sync_orchestrator.py"

# ==========================================
# 6. Maintenance & Utilities
# ==========================================
SRC_UTILS_DIR = SRC_DIR / "utils"
UTILS_BROWSER_UTILS = SRC_UTILS_DIR / "browser_utils.py"
UTILS_DATA_CLEANER  = SRC_UTILS_DIR / "data_cleaner.py"
UTILS_DB_CONNECTOR  = SRC_UTILS_DIR / "db_connector.py"
UTILS_HASHER        = SRC_UTILS_DIR / "hasher.py"
UTILS_LOGGER        = SRC_UTILS_DIR / "logger.py"
UTILS_PATH_MANAGER  = SRC_UTILS_DIR / "path_manager.py"
UTILS_STATUS_MANAGER = SRC_UTILS_DIR / "status_manager.py"

SRC_MAINTENANCE_DIR = SRC_DIR / "maintenance"
MAINTENANCE_CLEANUP_OLD = SRC_MAINTENANCE_DIR / "cleanup_old_data.py"
MAINTENANCE_RETENTION   = SRC_MAINTENANCE_DIR / "retention_cleaner.py"


# ==========================================
# 7. Utility Functions
# ==========================================

class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

def get_project_root() -> Path:
    return BASE_DIR

def get_validation_path(source: str, category: str, filename: str) -> Path:
    target_dir = VALIDATION_DIR / source / category
    target_dir.mkdir(parents=True, exist_ok=True)
    return target_dir / filename

def ensure_dirs_exist():
    dirs = [
        CONFIG_DIR, LOG_DIR, VALIDATION_DIR, OUTPUT_DIR, AUTH_DIR,
        DATA_STORE_DIR, DATA_MASTER_LIST_DIR, 
        DATA_MASTER_READY_DIR,
        DATA_PERFORMANCE_DIR, DATA_STATIC_DETAILS_DIR, DATA_HOLDINGS_DIR,
        SRC_UTILS_DIR, SRC_MAINTENANCE_DIR,
        DATA_STORE_DIR / "archive" / "master_list"
    ]
    
    validation_dirs = [
        VAL_FT_DIR, VAL_FT_MASTER, VAL_FT_NAV, VAL_FT_HIST, VAL_FT_STATIC, VAL_FT_HOLDINGS,
        VAL_SA_DIR, VAL_SA_MASTER, VAL_SA_NAV, VAL_SA_HIST, VAL_SA_STATIC, VAL_SA_HOLDINGS,
        VAL_YF_DIR, VAL_YF_MASTER, VAL_YF_NAV, VAL_YF_HIST, VAL_YF_STATIC, VAL_YF_HOLDINGS,
        VAL_SYS_DIR, VAL_SYS_QUEUES, VAL_SYS_DUPES, VAL_SYS_REPORTS
    ]
    dirs.extend(validation_dirs)

    print(f"{Colors.BLUE}Directory Check:{Colors.ENDC}")
    for d in dirs:
        if not d.exists():
            d.mkdir(parents=True, exist_ok=True)
            print(f"   Created: {d}")
    print(f"   {Colors.GREEN}All directories ensured.{Colors.ENDC}\n")

def check_all_scripts_exist():
    script_groups = {
        "1. Acquisition (Master)": {
            "FT Master": SCRAPER_MASTER_FT,
            "YF Master": SCRAPER_MASTER_YF,
            "SA Master": SCRAPER_MASTER_SA
        },
        "2. Acquisition (Performance)": {
            "FT NAV": SCRAPER_PERF_FT_NAV, "FT Hist": SCRAPER_PERF_FT_HISTORY, "FT Repair": SCRAPER_PERF_FT_REPAIR,
            "SA NAV": SCRAPER_PERF_SA_NAV, "SA Hist": SCRAPER_PERF_SA_HISTORY, "SA Div": SCRAPER_PERF_SA_DIVIDEND,
            "YF Fund NAV": SCRAPER_PERF_YF_FUND_NAV, "YF ETF NAV": SCRAPER_PERF_YF_ETF_NAV,
            "YF Fund Repair": SCRAPER_PERF_YF_FUND_REPAIR, "YF ETF Repair": SCRAPER_PERF_YF_ETF_REPAIR,
            "YF Fund Sweep": SCRAPER_PERF_YF_FUND_SWEEP, "YF ETF Sweep": SCRAPER_PERF_YF_ETF_SWEEP,
            "YF Fund Hist": SCRAPER_PERF_YF_FUND_HISTORY, "YF ETF Hist": SCRAPER_PERF_YF_ETF_HISTORY,
            "YF Fund Div": SCRAPER_PERF_YF_FUND_DIV, "YF ETF Div": SCRAPER_PERF_YF_ETF_DIV,
        },
        "3. Acquisition (Static)": {
            "FT Identity": SCRAPER_STATIC_FT_IDENTITY, "FT Fees": SCRAPER_STATIC_FT_FEES,
            "FT Risk": SCRAPER_STATIC_FT_RISK, "FT Policy": SCRAPER_STATIC_FT_POLICY,
            
            # ✅ Updated Check List
            "SA Detail": SCRAPER_STATIC_SA_DETAIL, 
            "SA Fees": SCRAPER_STATIC_SA_FEES,
            "SA Risk": SCRAPER_STATIC_SA_RISK, "SA Policy": SCRAPER_STATIC_SA_POLICY,
            
            "YF Identity": SCRAPER_STATIC_YF_IDENTITY, "YF Fees": SCRAPER_STATIC_YF_FEES,
            "YF Risk": SCRAPER_STATIC_YF_RISK, "YF Policy": SCRAPER_STATIC_YF_POLICY,
        },
        "4. Acquisition (Holdings)": {
            "FT Main": SCRAPER_HOLDINGS_FT_HOLDINGS, "FT Alloc": SCRAPER_HOLDINGS_FT_ALLOCATIONS,
            "SA Main": SCRAPER_HOLDINGS_SA_HOLDINGS, "SA Alloc": SCRAPER_HOLDINGS_SA_ALLOCATIONS,
            "YF Main": SCRAPER_HOLDINGS_YF_HOLDINGS, "YF Alloc": SCRAPER_HOLDINGS_YF_ALLOCATIONS,
        },
        "5. Synchronization": {
            "Main Pipeline": SYNC_MAIN_PIPELINE,
            "Master Cleaner": SYNC_MASTER_CLEANER, "Master Consolidator": SYNC_MASTER_CONSOLIDATOR,
            "Master Validator": SYNC_MASTER_VALIDATOR, "Master Remediator": SYNC_MASTER_REMEDIATOR,
            "Master Loader": SYNC_MASTER_LOADER, "Master Status": SYNC_MASTER_STATUS_MGR,
            "Master Archiver": SYNC_MASTER_ARCHIVER, "Master Orchestrator": SYNC_MASTER_ORCHESTRATOR,
            "Perf Cleaner": SYNC_PERF_CLEANER, "Perf Validator": SYNC_PERF_VALIDATOR,
            "Perf Hasher": SYNC_PERF_HASHER, "Perf Loader NAV": SYNC_PERF_LOADER_NAV,
            "Perf Loader Div": SYNC_PERF_LOADER_DIV, "Perf Loader Hist": SYNC_PERF_LOADER_HIST,
            "Perf Gap Checker": SYNC_PERF_GAP_CHECKER, "Perf Archiver": SYNC_PERF_ARCHIVER,
            "Perf Orchestrator": SYNC_PERF_ORCHESTRATOR,
            "Detail Cleaner": SYNC_DETAIL_CLEANER, "Detail Validator": SYNC_DETAIL_VALIDATOR,
            "Detail Hasher": SYNC_DETAIL_HASHER, "Detail Loader": SYNC_DETAIL_LOADER,
            "Detail Archiver": SYNC_DETAIL_ARCHIVER, "Detail Orchestrator": SYNC_DETAIL_ORCHESTRATOR,
            "Holdings Cleaner": SYNC_HOLDINGS_CLEANER, "Holdings Integrity": SYNC_HOLDINGS_INTEGRITY,
            "Holdings Hasher": SYNC_HOLDINGS_HASHER, "Holdings Loader": SYNC_HOLDINGS_LOADER,
            "Holdings Alloc Load": SYNC_HOLDINGS_ALLOC_LOAD, "Holdings Archiver": SYNC_HOLDINGS_ARCHIVER,
            "Holdings Orchestrator": SYNC_HOLDINGS_ORCHESTRATOR,
        },
        "6. Utilities & Maint": {
            "Maint Clean Old": MAINTENANCE_CLEANUP_OLD, "Maint Retention": MAINTENANCE_RETENTION,
            "Util Browser": UTILS_BROWSER_UTILS, "Util Cleaner": UTILS_DATA_CLEANER,
            "Util DB": UTILS_DB_CONNECTOR, "Util Hasher": UTILS_HASHER,
            "Util Logger": UTILS_LOGGER, "Util PathMgr": UTILS_PATH_MANAGER,
            "Util Status Mgr": UTILS_STATUS_MANAGER,
        }
    }

    print(f"{'='*60}")
    print(f"{Colors.HEADER}🔎 SYSTEM INTEGRITY CHECK (Script Existence){Colors.ENDC}")
    print(f"{'='*60}")
    
    total_scripts = 0
    missing_count = 0

    for category, scripts in script_groups.items():
        print(f"\n{Colors.BOLD}--- {category} ---{Colors.ENDC}")
        for name, path in scripts.items():
            total_scripts += 1
            if path.exists():
                pass
            else:
                print(f"  {Colors.FAIL}❌ [MISSING] {name:<25}{Colors.ENDC}")
                print(f"     -> Expected: {path}")
                missing_count += 1

    print(f"\n{'='*60}")
    if missing_count == 0:
        print(f"{Colors.GREEN}✨ SUCCESS: All {total_scripts} scripts found successfully!{Colors.ENDC}")
    else:
        print(f"{Colors.FAIL}⚠️  FAILURE: Missing {missing_count} scripts out of {total_scripts}.{Colors.ENDC}")
        print("    Please check the paths above and verify file creation.")
    print(f"{'='*60}")

if __name__ == "__main__":
    ensure_dirs_exist()
    check_all_scripts_exist()