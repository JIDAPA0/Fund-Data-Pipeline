import os
import sys
import subprocess
import time
from pathlib import Path

# ==========================================
# 0. DB CONFIGURATION & SCHEMA CHECK
# ==========================================

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.maintenance import db_schema_check

MASTER_SYNC = BASE_DIR / "src" / "05_db_synchronization" / "01_master_sync" / "07_master_sync_orchestrator.py"
PERF_NAV_SYNC = BASE_DIR / "src" / "05_db_synchronization" / "02_performance_sync" / "05_performance_sync_orchestrator.py"
HOUSEKEEPING = BASE_DIR / "src" / "maintenance" / "data_store_housekeeping.py"


def run_script(path: Path) -> None:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(BASE_DIR)
    subprocess.run([sys.executable, str(path)], check=True, env=env)


# ==========================================
# 1. DATA FETCH (MODULE EXECUTION)
# ==========================================

def run_modules() -> None:
    run_script(MASTER_SYNC)
    run_script(PERF_NAV_SYNC)


# ==========================================
# 2. SAVE RESULTS (VERIFY + ARCHIVE/CLEANUP)
# ==========================================

def run_housekeeping() -> None:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(BASE_DIR)
    subprocess.run(
        [sys.executable, str(HOUSEKEEPING), "--run", "--allow-partial"],
        check=True,
        env=env,
    )


def main() -> None:
    start = time.time()

    db_schema_check.main()
    run_modules()
    run_housekeeping()

    elapsed = time.time() - start
    print(f"ZTBT pipeline completed in {elapsed:.2f}s")


if __name__ == "__main__":
    main()
