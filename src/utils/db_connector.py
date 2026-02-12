import os
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.mysql import insert as mysql_insert

BASE_DIR = Path(__file__).resolve().parent.parent.parent
ENV_PATH = BASE_DIR / ".env"

if ENV_PATH.exists():
    load_dotenv(dotenv_path=ENV_PATH)
else:
    print(f"Warning: .env file not found at {ENV_PATH}")


def get_db_url() -> str:
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "3306")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    dbname = os.getenv("DB_NAME")

    if not all([user, password, dbname]):
        raise ValueError("Missing DB config: DB_USER, DB_PASSWORD, DB_NAME")

    safe_password = quote_plus(password)
    return f"mysql+pymysql://{user}:{safe_password}@{host}:{port}/{dbname}?charset=utf8mb4"


def get_db_engine():
    try:
        db_url = get_db_url()
        return create_engine(db_url, pool_pre_ping=True)
    except Exception as e:
        print(f"Failed to create DB engine: {e}")
        raise


def get_db_connection():
    return get_db_engine()


def test_connection() -> bool:
    try:
        engine = get_db_engine()
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            print(f"Connected. Test query result: {result.scalar()}")
        return True
    except Exception as e:
        print(f"Connection failed: {e}")
        return False


def _create_index_if_missing(engine, table_name: str, index_name: str, columns: str):
    check_sql = text(
        """
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = :table_name
          AND index_name = :index_name
        LIMIT 1
        """
    )
    with engine.begin() as conn:
        exists = conn.execute(check_sql, {"table_name": table_name, "index_name": index_name}).scalar()
        if not exists:
            conn.execute(text(f"CREATE INDEX {index_name} ON {table_name} ({columns})"))


def init_master_table(engine):
    create_table_sql = text(
        """
        CREATE TABLE IF NOT EXISTS stg_security_master (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            ticker VARCHAR(50) NOT NULL,
            asset_type VARCHAR(50) NOT NULL,
            source VARCHAR(50) NOT NULL,
            name TEXT,
            status VARCHAR(20) DEFAULT 'active',
            row_hash VARCHAR(255),
            first_seen DATE,
            last_seen DATE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_stg_master_key (ticker, asset_type, source)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    with engine.begin() as conn:
        conn.execute(create_table_sql)
    _create_index_if_missing(engine, "stg_security_master", "idx_stg_master_ticker", "ticker")


def init_price_history_table(engine):
    create_table_sql = text(
        """
        CREATE TABLE IF NOT EXISTS stg_price_history (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            ticker VARCHAR(50) NOT NULL,
            asset_type VARCHAR(50) NOT NULL,
            source VARCHAR(50) NOT NULL,
            date DATE NOT NULL,
            open DECIMAL(18,4),
            high DECIMAL(18,4),
            low DECIMAL(18,4),
            close DECIMAL(18,4),
            adj_close DECIMAL(18,4),
            volume BIGINT,
            name TEXT,
            status VARCHAR(20) DEFAULT 'active',
            row_hash VARCHAR(255),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_stg_price_key (ticker, asset_type, source, date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    with engine.begin() as conn:
        conn.execute(create_table_sql)
    _create_index_if_missing(engine, "stg_price_history", "idx_stg_price_ticker", "ticker")


def init_daily_nav_table(engine):
    create_table_sql = text(
        """
        CREATE TABLE IF NOT EXISTS stg_daily_nav (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            ticker VARCHAR(50) NOT NULL,
            asset_type VARCHAR(50) NOT NULL,
            source VARCHAR(50) NOT NULL,
            nav_price DECIMAL(18,4),
            currency VARCHAR(10),
            as_of_date DATE NOT NULL,
            scrape_date DATE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_stg_daily_nav_key (ticker, asset_type, source, as_of_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    with engine.begin() as conn:
        conn.execute(create_table_sql)
    _create_index_if_missing(engine, "stg_daily_nav", "idx_stg_nav_ticker", "ticker")


def init_dividend_history_table(engine):
    create_table_sql = text(
        """
        CREATE TABLE IF NOT EXISTS stg_dividend_history (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            ticker VARCHAR(50),
            asset_type VARCHAR(50),
            source VARCHAR(50),
            ex_date DATE,
            payment_date DATE,
            amount DECIMAL(18,6),
            currency VARCHAR(10),
            type VARCHAR(20) DEFAULT 'Cash',
            row_hash VARCHAR(255),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_stg_dividend_key (ticker, asset_type, source, ex_date, payment_date, amount, type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    with engine.begin() as conn:
        conn.execute(create_table_sql)
    _create_index_if_missing(engine, "stg_dividend_history", "idx_stg_div_ticker", "ticker")


def init_allocations_table(engine):
    create_table_sql = text(
        """
        CREATE TABLE IF NOT EXISTS stg_allocations (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            ticker VARCHAR(20) NOT NULL,
            asset_type VARCHAR(20) NOT NULL,
            source VARCHAR(50) NOT NULL,
            allocation_type VARCHAR(50) NOT NULL,
            item_name VARCHAR(100) NOT NULL,
            value_net DECIMAL(10,4),
            value_category_avg DECIMAL(10,4),
            value_long DECIMAL(10,4),
            value_short DECIMAL(10,4),
            as_of_date DATE,
            row_hash VARCHAR(64),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_stg_allocations_key (ticker, asset_type, source, allocation_type, item_name, as_of_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    with engine.begin() as conn:
        conn.execute(create_table_sql)
    _create_index_if_missing(engine, "stg_allocations", "idx_stg_alloc_ticker", "ticker")


def init_fund_info_table(engine):
    create_table_sql = text(
        """
        CREATE TABLE IF NOT EXISTS stg_fund_info (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
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
            region VARCHAR(100),
            country VARCHAR(100),
            leverage VARCHAR(20),
            options VARCHAR(20),
            shares_out DECIMAL(20,2),
            market_cap_size VARCHAR(50),
            investment_style VARCHAR(50),
            row_hash VARCHAR(64),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_stg_fund_info_key (ticker, asset_type, source)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    with engine.begin() as conn:
        conn.execute(create_table_sql)


def init_fund_fees_table(engine):
    create_table_sql = text(
        """
        CREATE TABLE IF NOT EXISTS stg_fund_fees (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            ticker VARCHAR(20) NOT NULL,
            asset_type VARCHAR(20) NOT NULL,
            source VARCHAR(50) NOT NULL,
            expense_ratio DECIMAL(5,4),
            initial_charge DECIMAL(5,4),
            exit_charge DECIMAL(5,4),
            assets_aum DECIMAL(20,2),
            top_10_hold_pct DECIMAL(5,2),
            holdings_count INT,
            holdings_turnover DECIMAL(5,2),
            row_hash VARCHAR(64),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_stg_fund_fees_key (ticker, asset_type, source)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    with engine.begin() as conn:
        conn.execute(create_table_sql)


def init_fund_risk_table(engine):
    create_table_sql = text(
        """
        CREATE TABLE IF NOT EXISTS stg_fund_risk (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            ticker VARCHAR(20) NOT NULL,
            asset_type VARCHAR(20) NOT NULL,
            source VARCHAR(50) NOT NULL,
            sharpe_ratio_1y DECIMAL(5,2), sharpe_ratio_3y DECIMAL(5,2), sharpe_ratio_5y DECIMAL(5,2), sharpe_ratio_10y DECIMAL(5,2),
            beta_1y DECIMAL(5,2), beta_3y DECIMAL(5,2), beta_5y DECIMAL(5,2), beta_10y DECIMAL(5,2),
            alpha_1y DECIMAL(5,2), alpha_3y DECIMAL(5,2), alpha_5y DECIMAL(5,2), alpha_10y DECIMAL(5,2),
            standard_dev_1y DECIMAL(5,2), standard_dev_3y DECIMAL(5,2), standard_dev_5y DECIMAL(5,2), standard_dev_10y DECIMAL(5,2),
            r_squared_1y DECIMAL(5,2), r_squared_3y DECIMAL(5,2), r_squared_5y DECIMAL(5,2), r_squared_10y DECIMAL(5,2),
            rsi_daily DECIMAL(5,2), moving_avg_200 DECIMAL(10,2), morningstar_rating INT,
            lipper_total_return_3y INT, lipper_total_return_5y INT, lipper_total_return_10y INT, lipper_total_return_overall INT,
            lipper_consistent_return_3y INT, lipper_consistent_return_5y INT, lipper_consistent_return_10y INT, lipper_consistent_return_overall INT,
            lipper_preservation_3y INT, lipper_preservation_5y INT, lipper_preservation_10y INT, lipper_preservation_overall INT,
            lipper_expense_3y INT, lipper_expense_5y INT, lipper_expense_10y INT, lipper_expense_overall INT,
            row_hash VARCHAR(64),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_stg_fund_risk_key (ticker, asset_type, source)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    with engine.begin() as conn:
        conn.execute(create_table_sql)


def init_fund_policy_table(engine):
    create_table_sql = text(
        """
        CREATE TABLE IF NOT EXISTS stg_fund_policy (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            ticker VARCHAR(20) NOT NULL,
            asset_type VARCHAR(20) NOT NULL,
            source VARCHAR(50) NOT NULL,
            dividend_yield DECIMAL(5,2),
            dividend_growth_1y DECIMAL(5,2),
            dividend_growth_3y DECIMAL(5,2),
            dividend_growth_5y DECIMAL(5,2),
            dividend_growth_10y DECIMAL(5,2),
            dividend_consecutive_years INT,
            payout_ratio DECIMAL(5,2),
            total_return_ytd DECIMAL(5,2),
            total_return_1y DECIMAL(5,2),
            pe_ratio DECIMAL(5,2),
            row_hash VARCHAR(64),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_stg_fund_policy_key (ticker, asset_type, source)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    with engine.begin() as conn:
        conn.execute(create_table_sql)


def init_fund_holdings_table(engine):
    create_table_sql = text(
        """
        CREATE TABLE IF NOT EXISTS stg_fund_holdings (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            ticker VARCHAR(20) NOT NULL,
            asset_type VARCHAR(20) NOT NULL,
            source VARCHAR(50) NOT NULL,
            holding_ticker VARCHAR(20),
            holding_name VARCHAR(255) NOT NULL,
            holding_percentage DECIMAL(10,4),
            shares_held DECIMAL(20,2),
            market_value DECIMAL(20,2),
            sector VARCHAR(100),
            country VARCHAR(100),
            as_of_date DATE,
            row_hash VARCHAR(64),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_stg_holdings_key (ticker, asset_type, source, holding_name, as_of_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    with engine.begin() as conn:
        conn.execute(create_table_sql)
    _create_index_if_missing(engine, "stg_fund_holdings", "idx_stg_hold_ticker", "ticker")
    _create_index_if_missing(engine, "stg_fund_holdings", "idx_stg_hold_name", "holding_name")


def get_active_tickers(source_name: str, asset_type: Optional[str] = None) -> List[Dict]:
    engine = get_db_engine()
    source_map = {
        "ft": "Financial Times",
        "financial times": "Financial Times",
        "yf": "Yahoo Finance",
        "yahoo finance": "Yahoo Finance",
        "sa": "Stock Analysis",
        "stock analysis": "Stock Analysis",
    }
    clean_source = source_map.get(source_name.lower(), source_name)

    sql_query = """
        SELECT ticker, asset_type, name
        FROM stg_security_master
        WHERE source = :source
          AND status = 'active'
    """
    params = {"source": clean_source}

    if asset_type:
        sql_query += " AND asset_type = :asset_type"
        params["asset_type"] = asset_type.lower()

    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql_query), params)
            return [
                {"ticker": row.ticker, "asset_type": row.asset_type, "name": row.name}
                for row in result
            ]
    except Exception as e:
        print(f"Failed to fetch active tickers: {e}")
        return []


def upsert_method(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    if not data:
        return 0

    stmt = mysql_insert(table.table).values(data)
    update_columns = {
        col.name: stmt.inserted[col.name]
        for col in table.table.columns
        if col.name != "id"
    }

    result = conn.execute(stmt.on_duplicate_key_update(**update_columns))
    return result.rowcount


def insert_dataframe(df: pd.DataFrame, table_name: str):
    if df.empty:
        print(f"Empty dataframe. Skip table '{table_name}'")
        return

    engine = get_db_engine()
    with engine.begin() as conn:
        df.to_sql(
            name=table_name,
            con=conn,
            if_exists="append",
            index=False,
            method=upsert_method,
            chunksize=1000,
        )


if __name__ == "__main__":
    if test_connection():
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
        print("Database bootstrap completed.")
