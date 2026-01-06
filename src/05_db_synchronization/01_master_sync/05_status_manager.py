import sys
from pathlib import Path
from datetime import datetime
from sqlalchemy import text

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.utils.logger import setup_logger, log_execution_summary
from src.utils.db_connector import get_db_engine
from src.utils.status_manager import StatusManager

# ✅ [FIXED] แก้ชื่อ Logger
logger = setup_logger("05_sync_StatusMgr")

def manage_status_lifecycle():
    start_time = datetime.now().timestamp()
    
    # Get 7-day cutoff from Utils
    cutoff_date_str = StatusManager.get_inactive_cutoff_date()
    
    logger.info("🛡️ STARTING STATUS MANAGER")
    logger.info(f"   Criteria: Inactive if not seen since {cutoff_date_str} (7-day Grace Period)")
    
    inactive_count = 0
    promote_count = 0

    try:
        engine = get_db_engine()
        with engine.begin() as conn:
            # 1. Active -> Inactive
            sql_inactive = text(StatusManager.get_sql_update_inactive())
            result_inactive = conn.execute(sql_inactive, {"cutoff_date": cutoff_date_str})
            inactive_count = result_inactive.rowcount
            
            if inactive_count > 0:
                logger.info(f"📉 Marked {inactive_count:,} items INACTIVE.")

            # 2. New -> Active
            sql_promote = text(StatusManager.get_sql_promote_new_to_active())
            result_promote = conn.execute(sql_promote)
            promote_count = result_promote.rowcount
            
            if promote_count > 0:
                logger.info(f"📈 Promoted {promote_count:,} items NEW -> ACTIVE.")

    except Exception as e:
        logger.error(f"❌ Failed: {e}")
        return

    log_execution_summary(logger, start_time, total_items=inactive_count + promote_count, status="Completed")

if __name__ == "__main__":
    manage_status_lifecycle()