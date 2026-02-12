import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

from prefect import flow, task

BASE_DIR = Path(__file__).resolve().parent.parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

MODULES = {
    "master": {
        "name": "MASTER LIST SYNC (Module 01 + 05.01)",
        "path": "src/05_db_synchronization/01_master_sync/07_master_sync_orchestrator.py",
    },
    "performance": {
        "name": "PERFORMANCE SYNC (Module 02 + 05.02)",
        "path": "src/05_db_synchronization/02_performance_sync/05_performance_sync_orchestrator.py",
    },
    "detail": {
        "name": "DETAIL SYNC (Module 03 + 05.03)",
        "path": "src/05_db_synchronization/03_detail_sync/05_detail_sync_orchestrator.py",
    },
    "holdings": {
        "name": "HOLDINGS SYNC (Module 04 + 05.04)",
        "path": "src/05_db_synchronization/04_holdings_sync/06_holdings_sync_orchestrator.py",
    },
}
MODULE_ORDER = ["master", "performance", "detail", "holdings"]


@task(name="run-module", retries=0)
def run_module(module_key: str) -> bool:
    module = MODULES[module_key]
    full_path = BASE_DIR / module["path"]

    if not full_path.exists():
        print(f"Module file not found: {full_path}")
        return False

    print(f"[START] {module['name']}")
    start = time.time()

    env = os.environ.copy()
    env["PYTHONPATH"] = str(BASE_DIR)

    try:
        subprocess.run([sys.executable, str(full_path)], check=True, env=env)
        duration = round(time.time() - start, 2)
        print(f"[DONE] {module['name']} ({duration}s)")
        return True
    except subprocess.CalledProcessError:
        print(f"[FAIL] {module['name']}")
        return False


@flow(name="fund-module-pipeline")
def module_pipeline(module_key: str):
    if module_key not in MODULES:
        raise ValueError(f"Unsupported module: {module_key}")

    success = run_module(module_key)
    if not success:
        raise RuntimeError(f"Module failed: {module_key}")


@flow(name="fund-global-pipeline")
def global_pipeline():
    results = []
    for module_key in MODULE_ORDER:
        success = run_module(module_key)
        results.append((module_key, success))
        if not success:
            break

    failed = [m for m, ok in results if not ok]
    if failed:
        raise RuntimeError(f"Pipeline failed at module(s): {', '.join(failed)}")


def main():
    parser = argparse.ArgumentParser(description="Fund pipeline with Prefect")
    parser.add_argument(
        "--module",
        choices=["all", *MODULE_ORDER],
        default="all",
        help="Run all modules or a single module",
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Create a Prefect scheduled runner instead of immediate execution",
    )
    parser.add_argument("--cron", default="0 6 * * 1-5", help="Cron schedule for --serve")
    parser.add_argument("--timezone", default="Asia/Bangkok", help="Timezone for --serve")
    args = parser.parse_args()

    if args.serve:
        if args.module == "all":
            global_pipeline.serve(
                name="fund-global-pipeline",
                cron=args.cron,
                timezone=args.timezone,
            )
        else:
            module_pipeline.serve(
                name=f"fund-{args.module}-pipeline",
                cron=args.cron,
                timezone=args.timezone,
                parameters={"module_key": args.module},
            )
        return

    if args.module == "all":
        global_pipeline()
    else:
        module_pipeline(module_key=args.module)


if __name__ == "__main__":
    main()
