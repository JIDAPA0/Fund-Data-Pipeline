import sys
from pathlib import Path

from sqlalchemy import text

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.utils.db_connector import get_db_engine

# ==========================================
# 1. DB CONFIGURATION
# ==========================================

TABLE_SPECS = {
    "stg_security_master": ("uq_stg_master_key", ["ticker", "asset_type", "source"]),
    "stg_price_history": ("uq_stg_price_key", ["ticker", "asset_type", "source", "date"]),
    "stg_daily_nav": ("uq_stg_daily_nav_key", ["ticker", "asset_type", "source", "as_of_date"]),
    "stg_dividend_history": (None, []),
    "stg_allocations": (
        "uq_stg_allocations_key",
        ["ticker", "asset_type", "source", "allocation_type", "item_name", "as_of_date"],
    ),
    "stg_fund_info": ("uq_stg_fund_info_key", ["ticker", "asset_type", "source"]),
    "stg_fund_fees": ("uq_stg_fund_fees_key", ["ticker", "asset_type", "source"]),
    "stg_fund_risk": ("uq_stg_fund_risk_key", ["ticker", "asset_type", "source"]),
    "stg_fund_policy": ("uq_stg_fund_policy_key", ["ticker", "asset_type", "source"]),
    "stg_fund_holdings": (
        "uq_stg_holdings_key",
        ["ticker", "asset_type", "source", "holding_name", "as_of_date"],
    ),
}


def column_exists(conn, table: str, column: str) -> bool:
    query = text(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = :table
          AND column_name = :column
        """
    )
    return conn.execute(query, {"table": table, "column": column}).fetchone() is not None


def constraint_exists(conn, name: str) -> bool:
    query = text(
        """
        SELECT 1
        FROM pg_constraint
        WHERE conname = :name
        """
    )
    return conn.execute(query, {"name": name}).fetchone() is not None


def get_pk_columns(conn, table: str) -> list[str]:
    query = text(
        """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
        WHERE c.relname = :table
          AND i.indisprimary = true
        ORDER BY a.attnum
        """
    )
    rows = conn.execute(query, {"table": table}).fetchall()
    return [r[0] for r in rows]


def get_pk_name(conn, table: str) -> str | None:
    query = text(
        """
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = to_regclass(:table)
          AND contype = 'p'
        """
    )
    row = conn.execute(query, {"table": table}).fetchone()
    return row[0] if row else None


def ensure_id_primary_key(conn, table: str, unique_name: str | None, unique_cols: list[str]) -> None:
    if not column_exists(conn, table, "id"):
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN id SERIAL"))
        conn.execute(text(f"UPDATE {table} SET id = DEFAULT WHERE id IS NULL"))
        conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN id SET NOT NULL"))
        conn.execute(
            text(
                """
                SELECT setval(
                    pg_get_serial_sequence(:table, 'id'),
                    COALESCE((SELECT MAX(id) FROM {table}), 1),
                    true
                )
                """.format(table=table)
            ),
            {"table": table},
        )

    pk_columns = get_pk_columns(conn, table)
    if pk_columns != ["id"]:
        pk_name = get_pk_name(conn, table)
        if pk_name:
            conn.execute(text(f"ALTER TABLE {table} DROP CONSTRAINT {pk_name}"))
        conn.execute(text(f"ALTER TABLE {table} ADD CONSTRAINT {table}_pkey PRIMARY KEY (id)"))

    if unique_name and unique_cols:
        if not constraint_exists(conn, unique_name):
            cols = ", ".join(unique_cols)
            conn.execute(text(f"ALTER TABLE {table} ADD CONSTRAINT {unique_name} UNIQUE ({cols})"))


def main() -> None:
    engine = get_db_engine()
    updated = []
    skipped = []

    with engine.begin() as conn:
        for table, (unique_name, unique_cols) in TABLE_SPECS.items():
            if not column_exists(conn, table, "id"):
                ensure_id_primary_key(conn, table, unique_name, unique_cols)
                updated.append(table)
            else:
                ensure_id_primary_key(conn, table, unique_name, unique_cols)
                skipped.append(table)

    print("DB schema check completed.")
    print(f"Updated tables: {updated}")
    print(f"Verified tables: {skipped}")


if __name__ == "__main__":
    main()
