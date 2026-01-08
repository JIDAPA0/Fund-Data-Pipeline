import sys
import os
import json
import time
import pandas as pd
from datetime import datetime
from pathlib import Path
import re
import uuid
from sqlalchemy import text

# ==========================================
# 0. SETUP & DB CONNECTION
# ==========================================
print("🚀 Script: Price History Loader Starting...")

current_file = Path(__file__).resolve()
project_root = current_file.parent
while not (project_root / 'src').exists():
    if project_root == project_root.parent: break
    project_root = project_root.parent
sys.path.append(str(project_root))

from src.utils.db_connector import get_db_connection

# ==========================================
# 1. CONFIGURATION
# ==========================================
HASHED_BASE_DIR = project_root / "data" / "04_hashed" / "price_history"
TARGET_TABLE = "stg_price_history"
PROGRESS_PATH = project_root / "logs" / "price_history_progress.json"

# ==========================================
# 2. CORE LOADER LOGIC
# ==========================================

def upsert_to_db(df, engine):
    if df.empty: return

    temp_table = f"temp_{TARGET_TABLE}_{uuid.uuid4().hex}"
    
    try:
        df.to_sql(temp_table, engine, if_exists='fail', index=False)
        
        upsert_query = f"""
        INSERT INTO {TARGET_TABLE} (ticker, asset_type, source, date, open, high, low, close, adj_close, volume, row_hash, updated_at)
        SELECT
            ticker,
            asset_type,
            source,
            date::date,
            open::numeric,
            high::numeric,
            low::numeric,
            close::numeric,
            adj_close::numeric,
            volume::numeric,
            row_hash,
            updated_at::timestamp
        FROM {temp_table}
        ON CONFLICT (ticker, asset_type, source, date) 
        DO UPDATE SET 
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            adj_close = EXCLUDED.adj_close,
            volume = EXCLUDED.volume,
            row_hash = EXCLUDED.row_hash,
            updated_at = EXCLUDED.updated_at
        WHERE {TARGET_TABLE}.row_hash IS DISTINCT FROM EXCLUDED.row_hash;
        """
        
        with engine.begin() as conn:
            conn.execute(text(upsert_query))
        
        return len(df)
    finally:
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))

def cleanup_temp_tables(engine):
    try:
        with engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT tablename
                    FROM pg_tables
                    WHERE schemaname = 'public'
                      AND tablename LIKE 'temp_stg_price_history_%'
                    ORDER BY tablename
                    """
                )
            ).fetchall()
            for row in rows:
                name = row[0]
                conn.execute(text(f'DROP TABLE IF EXISTS public."{name}"'))
            if rows:
                print(f"🧹 Cleaned up {len(rows)} leftover temp tables.")
    except Exception as exc:
        print(f"⚠️  Temp table cleanup skipped: {exc}")

def discover_hashed_dirs(base_dir: Path) -> list[Path]:
    scan_dirs: list[Path] = []
    added = set()

    date_dirs = [
        d for d in base_dir.iterdir()
        if d.is_dir() and re.fullmatch(r"\d{4}-\d{2}-\d{2}", d.name)
    ]
    date_dirs.sort()
    preferred_date = date_dirs[-1] if date_dirs else None

    if preferred_date:
        ft_date_dir = preferred_date / "ft" / preferred_date.name
        if ft_date_dir.exists():
            scan_dirs.append(ft_date_dir)
            added.add("ft")
        sa_date_dir = preferred_date / "sa" / preferred_date.name
        if sa_date_dir.exists():
            scan_dirs.append(sa_date_dir)
            added.add("sa")

    if "ft" not in added:
        ft_dir = base_dir / "ft"
        if ft_dir.exists():
            scan_dirs.append(ft_dir)
            added.add("ft")
    if "sa" not in added:
        sa_dir = base_dir / "sa"
        if sa_dir.exists():
            scan_dirs.append(sa_dir)
            added.add("sa")

    return scan_dirs


def parse_number(value):
    if pd.isna(value):
        return None
    text = str(value).strip().lower()
    if text in {"", "nan", "none", "-"}:
        return None
    multiplier = 1.0
    if text.endswith("k"):
        multiplier = 1_000.0
        text = text[:-1]
    elif text.endswith("m"):
        multiplier = 1_000_000.0
        text = text[:-1]
    elif text.endswith("b"):
        multiplier = 1_000_000_000.0
        text = text[:-1]
    text = text.replace(",", "")
    try:
        return float(text) * multiplier
    except Exception:
        return None


def write_progress(processed, total, success, errors, total_rows, start_time):
    elapsed = time.time() - start_time
    pct = (processed / total * 100.0) if total else 0.0
    rate = processed / elapsed if elapsed > 0 else 0.0
    remaining = (total - processed) / rate if rate > 0 else None
    payload = {
        "timestamp": datetime.utcnow().isoformat(),
        "processed_files": processed,
        "total_files": total,
        "success_files": success,
        "error_files": errors,
        "total_rows_upserted": total_rows,
        "percent_complete": round(pct, 2),
        "elapsed_seconds": round(elapsed, 2),
        "eta_seconds": round(remaining, 2) if remaining is not None else None,
    }
    PROGRESS_PATH.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS_PATH.write_text(json.dumps(payload, ensure_ascii=False))


def main():
    engine = get_db_connection()
    cleanup_temp_tables(engine)
    
    scan_dirs = discover_hashed_dirs(HASHED_BASE_DIR)
    if not scan_dirs:
        print("⚠️ No hashed directories found to scan.")
        return

    print("📂 Scanning hashed files in:")
    for d in scan_dirs:
        print(f"   - {d}")

    all_hashed_files = []
    for d in scan_dirs:
        all_hashed_files.extend(d.rglob("*.csv"))
    
    if not all_hashed_files:
        print("⚠️ No hashed files found to upload.")
        return

    total_rows = 0
    processed = 0
    success_files = 0
    error_files = 0
    total_files = len(all_hashed_files)
    start_time = time.time()
    write_progress(processed, total_files, success_files, error_files, total_rows, start_time)
    
    for csv_file in all_hashed_files:
        try:
            df = pd.read_csv(csv_file)
            if df.empty:
                processed += 1
                continue

            # Align column names with DB schema and remove unusable rows
            df.columns = [c.strip().lower() for c in df.columns]
            rename_map = {
                "adj close": "adj_close",
                "adj. close": "adj_close",
                "adj_close": "adj_close",
                "change %": "change_pct",
                "change%": "change_pct",
            }
            df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
            if "change_pct" in df.columns:
                df = df.drop(columns=["change_pct"])

            required_cols = [
                "ticker",
                "asset_type",
                "source",
                "date",
                "open",
                "high",
                "low",
                "close",
                "adj_close",
                "volume",
                "row_hash",
                "updated_at",
            ]
            for col in required_cols:
                if col not in df.columns:
                    df[col] = None
            df = df[required_cols]

            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
                df = df.dropna(subset=["date"])

            numeric_cols = ["open", "high", "low", "close", "adj_close", "volume"]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = df[col].apply(parse_number)

            if "updated_at" in df.columns:
                df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
                df["updated_at"] = df["updated_at"].fillna(pd.Timestamp.utcnow())
            else:
                df["updated_at"] = pd.Timestamp.utcnow()

            if "row_hash" in df.columns:
                df["row_hash"] = df["row_hash"].fillna("").astype(str).str.strip()
                df = df[df["row_hash"] != ""]
            if "updated_at" in df.columns:
                df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
                df["updated_at"] = df["updated_at"].fillna(pd.Timestamp.utcnow())
            else:
                df["updated_at"] = pd.Timestamp.utcnow()
            if "row_hash" in df.columns:
                df["row_hash"] = df["row_hash"].fillna("").astype(str).str.strip()
                df = df[df["row_hash"] != ""]
            if df.empty:
                continue
            
            rows_added = upsert_to_db(df, engine)
            total_rows += rows_added
            success_files += 1
            processed += 1

            if processed % 50 == 0:
                percent = processed / total_files * 100 if total_files else 0
                print(
                    f"   📤 Uploaded {processed}/{total_files} files "
                    f"({percent:.2f}%) | Total rows: {total_rows}",
                    flush=True,
                )
                write_progress(processed, total_files, success_files, error_files, total_rows, start_time)
                
        except Exception as e:
            print(f"   ❌ Error uploading {csv_file.name}: {e}")
            error_files += 1
            processed += 1
            if processed % 50 == 0:
                percent = processed / total_files * 100 if total_files else 0
                print(
                    f"   ⚠️  Progress {processed}/{total_files} files "
                    f"({percent:.2f}%) | Errors: {error_files}",
                    flush=True,
                )
                write_progress(processed, total_files, success_files, error_files, total_rows, start_time)

    print(f"\n✨ {'='*30}")
    print(f"✅ LOAD COMPLETED!")
    print(f"📊 Total Files Processed: {processed}")
    print(f"📈 Total Rows Upserted: {total_rows}")
    print(f"{'='*30}")
    write_progress(processed, total_files, success_files, error_files, total_rows, start_time)

if __name__ == "__main__":
    main()
