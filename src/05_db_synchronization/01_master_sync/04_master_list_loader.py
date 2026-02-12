import sys
from pathlib import Path
from datetime import datetime

import pandas as pd
from sqlalchemy import text

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.utils.path_manager import DATA_MASTER_LIST_DIR
from src.utils.logger import setup_logger, log_execution_summary
from src.utils.db_connector import get_db_engine, init_master_table
from src.utils.hasher import calculate_row_hash
from src.utils.status_manager import StatusManager

logger = setup_logger("05_sync_Loader")


def load_to_database():
    start_time = datetime.now().timestamp()
    today_str = datetime.now().strftime("%Y-%m-%d")

    logger.info("STARTING MASTER LIST LOADER (MySQL)")

    input_path = DATA_MASTER_LIST_DIR / "04_ready_to_load" / today_str / "master_list_final.csv"
    if not input_path.exists():
        logger.error(f"No file to load: {input_path}")
        return

    df = pd.read_csv(input_path)
    df = df.where(pd.notnull(df), None)

    total_rows = len(df)
    logger.info(f"Loaded {total_rows:,} rows ready to sync.")

    try:
        engine = get_db_engine()
        init_master_table(engine)

        data_to_upsert = []
        for _, row in df.iterrows():
            ticker = str(row["ticker"]).strip()
            asset_type = str(row["asset_type"]).strip()
            source = str(row.get("source", "Unknown")).strip()
            name = row.get("name")

            status = row.get("status") if row.get("status") else StatusManager.determine_initial_status(ticker, name, source)
            date_added = row.get("date_added", today_str)
            current_hash = calculate_row_hash(ticker, asset_type, source, name, status)

            data_to_upsert.append(
                {
                    "ticker": ticker,
                    "asset_type": asset_type,
                    "source": source,
                    "name": name,
                    "status": status,
                    "row_hash": current_hash,
                    "first_seen": date_added,
                    "last_seen": today_str,
                }
            )

        upsert_sql = text(
            """
            INSERT INTO stg_security_master (
                ticker, asset_type, source, name, status, row_hash, first_seen, last_seen, updated_at
            ) VALUES (
                :ticker, :asset_type, :source, :name, :status, :row_hash, :first_seen, :last_seen, NOW()
            )
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                status = VALUES(status),
                row_hash = VALUES(row_hash),
                last_seen = VALUES(last_seen),
                updated_at = NOW()
            """
        )

        with engine.begin() as conn:
            conn.execute(upsert_sql, data_to_upsert)

        logger.info(f"Sync successful. Processed {len(data_to_upsert):,} rows.")

    except Exception as e:
        logger.error(f"Database error: {e}")
        return

    log_execution_summary(logger, start_time, total_items=total_rows, status="Completed")


if __name__ == "__main__":
    load_to_database()
