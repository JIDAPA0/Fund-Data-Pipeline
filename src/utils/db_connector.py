import os
import sys
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from typing import Optional, List, Dict

from sqlalchemy.dialects.postgresql import insert as pg_insert

# ----------------------------------------------------------------------
# SETUP PATHS & ENV
# ----------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent.parent
ENV_PATH = BASE_DIR / ".env"

if ENV_PATH.exists():
    load_dotenv(dotenv_path=ENV_PATH)
else:
    print(f"⚠️  เตือน: ไม่เจอไฟล์ .env ที่ {ENV_PATH}")

# ----------------------------------------------------------------------
# DB CONNECTION UTILS
# ----------------------------------------------------------------------

def get_db_url() -> str:
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    dbname = os.getenv("DB_NAME")

    if not all([user, password, dbname]):
        raise ValueError("❌ ข้อมูลเชื่อมต่อไม่ครบ: กรุณาเช็ค DB_USER, DB_PASSWORD, DB_NAME ในไฟล์ .env")

    safe_password = quote_plus(password)
    return f"postgresql://{user}:{safe_password}@{host}:{port}/{dbname}"

def get_db_engine():
    try:
        db_url = get_db_url()
        engine = create_engine(
            db_url,
            isolation_level="AUTOCOMMIT",
            connect_args={'client_encoding': 'utf8'}
        )
        return engine
    except Exception as e:
        print(f"❌ สร้าง DB Engine ไม่สำเร็จ: {e}")
        raise

def get_db_connection():
    return get_db_engine()

def test_connection():
    try:
        engine = get_db_engine()
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            print(f"✅ เชื่อมต่อสำเร็จ! Test Query Result: {result.scalar()}")
        return True
    except Exception as e:
        print(f"❌ เชื่อมต่อล้มเหลว: {e}")
        return False

# ----------------------------------------------------------------------
# TABLE INITIALIZATION
# ----------------------------------------------------------------------

def init_master_table(engine):
    try:
        create_table_sql = text("""
            CREATE TABLE IF NOT EXISTS stg_security_master (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(50) NOT NULL,
                asset_type VARCHAR(50) NOT NULL,
                source VARCHAR(50) NOT NULL,
                name TEXT,
                status VARCHAR(20) DEFAULT 'active',
                row_hash VARCHAR(255),
                first_seen DATE,
                last_seen DATE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uq_stg_master_key UNIQUE (ticker, asset_type, source)
            );
        """)
        with engine.connect() as conn:
            conn.execute(create_table_sql)
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_stg_master_ticker ON stg_security_master(ticker);"))
    except Exception as e:
        print(f"❌ สร้างตาราง Master ไม่สำเร็จ: {e}")
        raise

def init_price_history_table(engine):
    try:
        create_table_sql = text("""
            CREATE TABLE IF NOT EXISTS stg_price_history (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(50) NOT NULL,
                asset_type VARCHAR(50) NOT NULL,
                source VARCHAR(50) NOT NULL,
                date DATE NOT NULL,
                open NUMERIC(18, 4),
                high NUMERIC(18, 4),
                low NUMERIC(18, 4),
                close NUMERIC(18, 4),
                adj_close NUMERIC(18, 4),
                volume BIGINT,
                name TEXT,
                status VARCHAR(20) DEFAULT 'active',
                row_hash VARCHAR(255),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uq_stg_price_key UNIQUE (ticker, asset_type, source, date)
            );
        """)
        with engine.connect() as conn:
            conn.execute(create_table_sql)
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_stg_price_ticker ON stg_price_history(ticker);"))
    except Exception as e:
        print(f"❌ สร้างตาราง Price History ไม่สำเร็จ: {e}")
        raise

def init_daily_nav_table(engine):
    try:
        create_table_sql = text("""
            CREATE TABLE IF NOT EXISTS stg_daily_nav (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(50) NOT NULL,
                asset_type VARCHAR(50) NOT NULL,
                source VARCHAR(50) NOT NULL,
                nav_price NUMERIC(18, 4),
                currency VARCHAR(10),
                as_of_date DATE NOT NULL,
                scrape_date DATE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uq_stg_daily_nav_key UNIQUE (ticker, asset_type, source, as_of_date)
            );
        """)
        with engine.connect() as conn:
            conn.execute(create_table_sql)
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_stg_nav_ticker ON stg_daily_nav(ticker);"))
    except Exception as e:
        print(f"❌ สร้างตาราง Daily NAV ไม่สำเร็จ: {e}")
        raise

def init_dividend_history_table(engine):
    try:
        create_table_sql = text("""
            CREATE TABLE IF NOT EXISTS stg_dividend_history (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(50),
                asset_type VARCHAR(50),
                source VARCHAR(50),
                ex_date DATE,
                payment_date DATE,
                amount NUMERIC(18, 6),
                currency VARCHAR(10),
                type VARCHAR(20) DEFAULT 'Cash',
                row_hash VARCHAR(255),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uq_stg_dividend_key UNIQUE (ticker, asset_type, source, ex_date, payment_date, amount, type)
            );
        """)
        with engine.connect() as conn:
            conn.execute(create_table_sql)
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_stg_div_ticker ON stg_dividend_history(ticker);"))
            print("✅ Dividend Table Initialized in Flexible Mode.")
    except Exception as e:
        print(f"❌ สร้างตาราง Dividend History ไม่สำเร็จ: {e}")
        raise

def init_allocations_table(engine):
    try:
        create_table_sql = text("""
            CREATE TABLE IF NOT EXISTS stg_allocations (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(20) NOT NULL,
                asset_type VARCHAR(20) NOT NULL,
                source VARCHAR(50) NOT NULL,
                allocation_type VARCHAR(50) NOT NULL,
                item_name VARCHAR(100) NOT NULL,
                value_net DECIMAL(10, 4),
                value_category_avg DECIMAL(10, 4),
                value_long DECIMAL(10, 4),
                value_short DECIMAL(10, 4),
                as_of_date DATE,
                row_hash VARCHAR(64),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uq_stg_allocations_key UNIQUE (ticker, asset_type, source, allocation_type, item_name, as_of_date)
            );
        """)
        with engine.connect() as conn:
            conn.execute(create_table_sql)
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_stg_alloc_ticker ON stg_allocations(ticker);"))
    except Exception as e:
        print(f"❌ สร้างตาราง stg_allocations ไม่สำเร็จ: {e}")
        raise

def init_fund_info_table(engine):
    try:
        create_table_sql = text("""
            CREATE TABLE IF NOT EXISTS stg_fund_info (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(20) NOT NULL,
                asset_type VARCHAR(20) NOT NULL,
                source VARCHAR(50) NOT NULL,
                name VARCHAR(255),
                isin_number VARCHAR(20),
                cusip_number VARCHAR(20),
                issuer VARCHAR(100),
                category VARCHAR(100),
                index_benchmark VARCHAR(255),
                inception_date DATE,
                exchange VARCHAR(100),
                region TEXT,
                country VARCHAR(100),
                leverage VARCHAR(20),
                options VARCHAR(20),
                shares_out DECIMAL(20, 2),
                market_cap_size VARCHAR(50),
                investment_style VARCHAR(50),
                row_hash VARCHAR(64),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uq_stg_fund_info_key UNIQUE (ticker, asset_type, source)
            );
        """)
        with engine.connect() as conn:
            conn.execute(create_table_sql)
    except Exception as e:
        print(f"❌ สร้างตาราง stg_fund_info ไม่สำเร็จ: {e}")
        raise

def init_fund_fees_table(engine):
    try:
        create_table_sql = text("""
            CREATE TABLE IF NOT EXISTS stg_fund_fees (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(20) NOT NULL,
                asset_type VARCHAR(20) NOT NULL,
                source VARCHAR(50) NOT NULL,
                expense_ratio DECIMAL(5, 4),
                initial_charge DECIMAL(5, 4),
                exit_charge DECIMAL(5, 4),
                assets_aum DECIMAL(20, 2),
                top_10_hold_pct DECIMAL(5, 2),
                holdings_count INT,
                holdings_turnover DECIMAL(5, 2),
                row_hash VARCHAR(64),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uq_stg_fund_fees_key UNIQUE (ticker, asset_type, source)
            );
        """)
        with engine.connect() as conn:
            conn.execute(create_table_sql)
    except Exception as e:
        print(f"❌ สร้างตาราง stg_fund_fees ไม่สำเร็จ: {e}")
        raise

def init_fund_risk_table(engine):
    try:
        create_table_sql = text("""
            CREATE TABLE IF NOT EXISTS stg_fund_risk (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(20) NOT NULL,
                asset_type VARCHAR(20) NOT NULL,
                source VARCHAR(50) NOT NULL,
                sharpe_ratio_1y DECIMAL(5, 2), sharpe_ratio_3y DECIMAL(5, 2), sharpe_ratio_5y DECIMAL(5, 2), sharpe_ratio_10y DECIMAL(5, 2),
                beta_1y DECIMAL(5, 2), beta_3y DECIMAL(5, 2), beta_5y DECIMAL(5, 2), beta_10y DECIMAL(5, 2),
                alpha_1y DECIMAL(5, 2), alpha_3y DECIMAL(5, 2), alpha_5y DECIMAL(5, 2), alpha_10y DECIMAL(5, 2),
                standard_dev_1y DECIMAL(5, 2), standard_dev_3y DECIMAL(5, 2), standard_dev_5y DECIMAL(5, 2), standard_dev_10y DECIMAL(5, 2),
                r_squared_1y DECIMAL(5, 2), r_squared_3y DECIMAL(5, 2), r_squared_5y DECIMAL(5, 2), r_squared_10y DECIMAL(5, 2),
                rsi_daily DECIMAL(5, 2), moving_avg_200 DECIMAL(10, 2), morningstar_rating INT,
                lipper_total_return_3y INT, lipper_total_return_5y INT, lipper_total_return_10y INT, lipper_total_return_overall INT,
                lipper_consistent_return_3y INT, lipper_consistent_return_5y INT, lipper_consistent_return_10y INT, lipper_consistent_return_overall INT,
                lipper_preservation_3y INT, lipper_preservation_5y INT, lipper_preservation_10y INT, lipper_preservation_overall INT,
                lipper_expense_3y INT, lipper_expense_5y INT, lipper_expense_10y INT, lipper_expense_overall INT,
                row_hash VARCHAR(64),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uq_stg_fund_risk_key UNIQUE (ticker, asset_type, source)
            );
        """)
        with engine.connect() as conn:
            conn.execute(create_table_sql)
    except Exception as e:
        print(f"❌ สร้างตาราง stg_fund_risk ไม่สำเร็จ: {e}")
        raise

def init_fund_policy_table(engine):
    try:
        create_table_sql = text("""
            CREATE TABLE IF NOT EXISTS stg_fund_policy (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(20) NOT NULL,
                asset_type VARCHAR(20) NOT NULL,
                source VARCHAR(50) NOT NULL,
                dividend_yield DECIMAL(5, 2),
                dividend_growth_1y DECIMAL(5, 2),
                dividend_growth_3y DECIMAL(5, 2),
                dividend_growth_5y DECIMAL(5, 2),
                dividend_growth_10y DECIMAL(5, 2),
                dividend_consecutive_years INT,
                payout_ratio DECIMAL(5, 2),
                total_return_ytd DECIMAL(5, 2),
                total_return_1y DECIMAL(5, 2),
                pe_ratio DECIMAL(5, 2),
                row_hash VARCHAR(64),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uq_stg_fund_policy_key UNIQUE (ticker, asset_type, source)
            );
        """)
        with engine.connect() as conn:
            conn.execute(create_table_sql)
    except Exception as e:
        print(f"❌ สร้างตาราง stg_fund_policy ไม่สำเร็จ: {e}")
        raise

def init_fund_holdings_table(engine):
    try:
        create_table_sql = text("""
            CREATE TABLE IF NOT EXISTS stg_fund_holdings (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(20) NOT NULL,
                asset_type VARCHAR(20) NOT NULL,
                source VARCHAR(50) NOT NULL,
                holding_ticker VARCHAR(20),
                holding_name VARCHAR(255) NOT NULL,
                holding_percentage DECIMAL(10, 4),
                shares_held DECIMAL(20, 2),
                market_value DECIMAL(20, 2),
                sector VARCHAR(100),
                country VARCHAR(100),
                as_of_date DATE,
                row_hash VARCHAR(64),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uq_stg_holdings_key UNIQUE (ticker, asset_type, source, holding_name, as_of_date)
            );
        """)
        with engine.connect() as conn:
            conn.execute(create_table_sql)
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_stg_hold_ticker ON stg_fund_holdings(ticker);"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_stg_hold_name ON stg_fund_holdings(holding_name);"))
    except Exception as e:
        print(f"❌ สร้างตาราง stg_fund_holdings ไม่สำเร็จ: {e}")
        raise

def init_fund_metrics_table(engine):
    try:
        create_table_sql = text("""
            CREATE TABLE IF NOT EXISTS stg_fund_metrics (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(20) NOT NULL,
                asset_type VARCHAR(20) NOT NULL,
                source VARCHAR(50) NOT NULL,
                metric_type VARCHAR(50) NOT NULL,
                metric_name VARCHAR(255) NOT NULL,
                column_name VARCHAR(100),
                value_raw TEXT,
                value_num DECIMAL(20, 6),
                as_of_date DATE,
                row_hash VARCHAR(64),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uq_stg_fund_metrics_key UNIQUE (
                    ticker, asset_type, source, metric_type, metric_name, column_name, as_of_date
                )
            );
        """)
        with engine.connect() as conn:
            conn.execute(create_table_sql)
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_stg_metrics_ticker ON stg_fund_metrics(ticker);"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_stg_metrics_type ON stg_fund_metrics(metric_type);"))
    except Exception as e:
        print(f"❌ สร้างตาราง stg_fund_metrics ไม่สำเร็จ: {e}")
        raise

# ----------------------------------------------------------------------
# HELPER FUNCTIONS
# ----------------------------------------------------------------------

def get_active_tickers(source_name: str, asset_type: Optional[str] = None) -> List[Dict]:
    engine = get_db_engine()
    source_map = {
        "ft": "Financial Times", "financial times": "Financial Times",
        "yf": "Yahoo Finance", "yahoo finance": "Yahoo Finance",
        "sa": "Stock Analysis", "stock analysis": "Stock Analysis"
    }
    clean_source = source_map.get(source_name.lower(), source_name)
    sql_query = """
        SELECT ticker, asset_type, name FROM stg_security_master 
        WHERE source = :source AND status = 'active'
    """
    params = {"source": clean_source}
    if asset_type:
        sql_query += " AND asset_type = :asset_type"
        params["asset_type"] = asset_type.lower()
    sql = text(sql_query)
    try:
        with engine.connect() as conn:
            result = conn.execute(sql, params)
            tickers = [{"ticker": row.ticker, "asset_type": row.asset_type, "name": row.name} for row in result]
            print(f"📋 ดึงข้อมูลจาก DB สำเร็จ: {len(tickers)} ตัว (Source: {clean_source})")
            return tickers
    except Exception as e:
        print(f"❌ เกิดข้อผิดพลาดตอนดึงรายชื่อหุ้น: {e}")
        return []

def upsert_method(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = pg_insert(table.table).values(data)
    
    constraint_map = {
        'stg_security_master': 'uq_stg_master_key',
        'stg_price_history': 'uq_stg_price_key',
        'stg_daily_nav': 'uq_stg_daily_nav_key',
        'stg_dividend_history': 'uq_stg_dividend_key',
        'stg_allocations': 'uq_stg_allocations_key',
        'stg_fund_info': 'uq_stg_fund_info_key',
        'stg_fund_fees': 'uq_stg_fund_fees_key',
        'stg_fund_risk': 'uq_stg_fund_risk_key',
        'stg_fund_policy': 'uq_stg_fund_policy_key',
        'stg_fund_holdings': 'uq_stg_holdings_key',
        'stg_fund_metrics': 'uq_stg_fund_metrics_key',
    }
    
    table_name = table.table.name
    constraint = constraint_map.get(table_name)

    if constraint:
        set_ = {c.key: c for c in stmt.excluded if c.key not in ['id', 'updated_at']}
        where_clause = None
        if "row_hash" in table.table.c:
            where_clause = table.table.c.row_hash.is_distinct_from(stmt.excluded.row_hash)
        stmt = stmt.on_conflict_do_update(constraint=constraint, set_=set_, where=where_clause)
    
    result = conn.execute(stmt)
    return result.rowcount

def insert_dataframe(df: pd.DataFrame, table_name: str):
    if df.empty:
        print(f"⚠️  ไม่มีข้อมูลใน DataFrame ข้ามการบันทึก '{table_name}'")
        return
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            df.to_sql(name=table_name, con=conn, if_exists='append', index=False, method=upsert_method, chunksize=1000)
    except Exception as e:
        print(f"❌ บันทึกข้อมูลลงตาราง '{table_name}' ล้มเหลว: {e}")

# ----------------------------------------------------------------------
# MAIN EXECUTION
# ----------------------------------------------------------------------
if __name__ == "__main__":
    if test_connection():
        print("🚀 กำลังตรวจสอบและสร้างตารางใน Database...")
        engine = get_db_engine()
        init_master_table(engine)
        init_price_history_table(engine)
        init_daily_nav_table(engine)
        init_dividend_history_table(engine)
        init_allocations_table(engine)
        init_fund_info_table(engine)
        init_fund_fees_table(engine)
        init_fund_risk_table(engine)
        init_fund_policy_table(engine)
        init_fund_holdings_table(engine) 
        print("✨ ฐานข้อมูลพร้อมใช้งานแล้ว!")
