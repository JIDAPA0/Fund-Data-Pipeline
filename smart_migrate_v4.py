#!/usr/bin/env python3
"""
Smart migration from fund_thai (raw CSV import) to Model V4 tables in public.

Features:
- Inspect sample rows to verify data meaning
- Print mapping plan with sample values
- --dry-run prints plan only
- Without --dry-run, inserts data with type coercion
"""

from __future__ import annotations

import argparse
import os
import re
from typing import Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


SOURCE_SCHEMA_DEFAULT = "fund_thai"
TARGET_SCHEMA_DEFAULT = "public"


def load_env() -> None:
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        load_dotenv(dotenv_path=env_path)


def normalize(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", name.lower())


def build_engine(host: str, port: str, user: str, password: str, dbname: str):
    safe_password = quote_plus(password or "")
    url = f"postgresql://{user}:{safe_password}@{host}:{port}/{dbname}"
    return create_engine(url)


def fetch_table_columns(
    engine,
    schema: str,
    table: str,
) -> List[Dict[str, str]]:
    query = text(
        """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table
        ORDER BY ordinal_position
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"schema": schema, "table": table}).fetchall()
    return [
        {
            "name": row[0],
            "data_type": row[1],
            "is_nullable": row[2],
            "column_default": row[3],
        }
        for row in rows
    ]


def table_exists(engine, schema: str, table: str) -> bool:
    query = text(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = :schema AND table_name = :table
        """
    )
    with engine.connect() as conn:
        return conn.execute(query, {"schema": schema, "table": table}).first() is not None


def table_row_count(engine, schema: str, table: str) -> int:
    query = text(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
    with engine.connect() as conn:
        row = conn.execute(query).fetchone()
    return int(row[0]) if row else 0


def fetch_sample_rows(
    engine,
    schema: str,
    table: str,
    limit: int,
) -> pd.DataFrame:
    query = text(f'SELECT * FROM "{schema}"."{table}" LIMIT {limit}')
    return pd.read_sql_query(query, engine)


def sample_values(df: pd.DataFrame, max_values: int = 3) -> Dict[str, str]:
    samples: Dict[str, str] = {}
    for col in df.columns:
        series = df[col].dropna()
        if series.empty:
            samples[col] = "NULL"
            continue
        uniques = series.unique()
        head_vals = uniques[:max_values]
        if len(head_vals) == 1:
            samples[col] = repr(head_vals[0])
        else:
            samples[col] = ", ".join(repr(v) for v in head_vals)
    return samples


def apply_filters(df: pd.DataFrame, filters: Optional[Dict[str, Iterable[str]]]) -> pd.DataFrame:
    if not filters:
        return df
    filtered = df
    for col, values in filters.items():
        if col not in filtered.columns:
            return filtered.iloc[0:0]
        if isinstance(values, (list, tuple, set)):
            filtered = filtered[filtered[col].isin(values)]
        else:
            filtered = filtered[filtered[col] == values]
    return filtered


def resolve_target_column(
    canonical: str,
    actual_cols: List[str],
) -> Optional[str]:
    if not actual_cols:
        return canonical
    canonical_norm = normalize(canonical)
    for col in actual_cols:
        if normalize(col) == canonical_norm:
            return col
    return None


def build_mapping_plan(
    source_cols: List[str],
    target_cols: List[str],
    column_map: Dict[str, str],
    defaults: Dict[str, str],
) -> Tuple[Dict[str, Optional[str]], Dict[str, str], Dict[str, str]]:
    mapping: Dict[str, Optional[str]] = {}
    reasons: Dict[str, str] = {}
    mapped_targets = set()

    for src in source_cols:
        canonical = column_map.get(src)
        if not canonical:
            mapping[src] = None
            reasons[src] = "unmapped"
            continue
        resolved = resolve_target_column(canonical, target_cols)
        if resolved is None:
            mapping[src] = None
            reasons[src] = "target_missing"
            continue
        mapping[src] = resolved
        reasons[src] = "mapped"
        mapped_targets.add(resolved)

    resolved_defaults = {}
    for canonical, value in defaults.items():
        resolved = resolve_target_column(canonical, target_cols)
        if resolved is None:
            continue
        if resolved in mapped_targets:
            continue
        resolved_defaults[resolved] = value

    return mapping, reasons, resolved_defaults


def describe_mapping_plan(
    source_table: str,
    target_table: str,
    mapping: Dict[str, Optional[str]],
    reasons: Dict[str, str],
    samples: Dict[str, str],
    defaults: Dict[str, str],
    filter_desc: Optional[str] = None,
) -> None:
    header = f"[Mapping Plan] {source_table} -> {target_table}"
    if filter_desc:
        header += f" [{filter_desc}]"
    print(f"\n{header}")
    for src, target in mapping.items():
        sample = samples.get(src, "NULL")
        if target:
            reason = reasons.get(src, "mapped")
            note = f" ({reason})" if reason != "mapped" else ""
            print(f"- {src} (sample: {sample}) -> {target}{note}")
        else:
            reason = reasons.get(src, "unmapped")
            note = f" ({reason})" if reason != "unmapped" else ""
            print(f"- {src} (sample: {sample}) -> Unmapped{note}")
    if defaults:
        print("[Defaults]")
        for key, value in defaults.items():
            print(f"- {key} = {repr(value)}")


def describe_update_plan(
    title: str,
    lines: List[Dict[str, str]],
    samples: Dict[str, str],
) -> None:
    print(f"\n[Mapping Plan] {title}")
    for line in lines:
        src = line["source"]
        tgt = line["target"]
        note = line.get("note")
        sample = samples.get(src, "NULL")
        suffix = f" ({note})" if note else ""
        print(f"- {src} (sample: {sample}) -> {tgt}{suffix}")


def coerce_boolean(series: pd.Series) -> pd.Series:
    mapping = {
        "true": True,
        "false": False,
        "1": True,
        "0": False,
        "yes": True,
        "no": False,
        "y": True,
        "n": False,
    }
    return series.map(
        lambda v: mapping.get(str(v).strip().lower()) if pd.notna(v) else None
    )


def coerce_column(df: pd.DataFrame, col: str, data_type: str) -> None:
    if col not in df.columns:
        return
    lower = data_type.lower()
    if "date" == lower:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    elif "timestamp" in lower:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    elif any(t in lower for t in ("numeric", "decimal", "double", "real")):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    elif "int" in lower:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    elif "bool" in lower:
        df[col] = coerce_boolean(df[col])


def coerce_types(df: pd.DataFrame, target_cols_info: List[Dict[str, str]]) -> pd.DataFrame:
    for col_info in target_cols_info:
        coerce_column(df, col_info["name"], col_info["data_type"])
    return df


def maybe_fill_dividend_policy(df: pd.DataFrame, target_cols_info: List[Dict[str, str]]) -> None:
    dividend_col = None
    data_type = None
    for col_info in target_cols_info:
        if normalize(col_info["name"]) == "dividendpolicy":
            dividend_col = col_info["name"]
            data_type = col_info["data_type"].lower()
            break
    if not dividend_col or dividend_col not in df.columns:
        return
    if data_type and "bool" in data_type:
        df[dividend_col] = coerce_boolean(df[dividend_col])
        return
    dividend_yes = {"true", "1", "yes", "y", "\u0e08\u0e48\u0e32\u0e22"}
    dividend_no = {"false", "0", "no", "n", "\u0e44\u0e21\u0e48\u0e08\u0e48\u0e32\u0e22"}
    df[dividend_col] = df[dividend_col].map(
        lambda v: "Dividend"
        if str(v).strip().lower().replace(" ", "") in dividend_yes
        else "No Dividend"
        if pd.notna(v)
        else None
    )


def fund_id_strategy(target_cols_info: List[Dict[str, str]]) -> str:
    for col in target_cols_info:
        if normalize(col["name"]) == "fundid":
            default = col.get("column_default") or ""
            if default and "nextval" in default:
                return "auto"
            if col["is_nullable"].lower() == "yes":
                return "nullable"
            return "required"
    return "missing"


def fetch_fund_id_map(
    engine,
    schema: str,
    fund_master_table: str,
    fund_code_column: str,
    fund_id_column: str,
) -> Dict[str, str]:
    query = text(
        f'SELECT "{fund_code_column}", "{fund_id_column}" FROM "{schema}"."{fund_master_table}"'
    )
    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()
    return {row[0]: row[1] for row in rows}


def select_latest_risk(df: pd.DataFrame) -> pd.DataFrame:
    if "as_of_date" not in df.columns:
        return df
    df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce")
    df = df.sort_values("as_of_date")
    return df.drop_duplicates(subset=["fund_code"], keep="last")


def postprocess_sector_country(df: pd.DataFrame) -> pd.DataFrame:
    if "type" not in df.columns or "sector_name" not in df.columns:
        return df
    mask = df["type"].astype(str).str.lower().eq("country_alloc")
    df.loc[mask, "sector_name"] = "Country: " + df.loc[mask, "sector_name"].astype(str)
    return df


def migrate_table(
    engine,
    source_schema: str,
    target_schema: str,
    source_table: str,
    target_table: str,
    mapping: Dict[str, Optional[str]],
    defaults: Dict[str, str],
    target_cols_info: List[Dict[str, str]],
    chunksize: int,
    dry_run: bool,
    fund_id_lookup: Optional[Dict[str, str]] = None,
    fund_id_column: Optional[str] = None,
    fund_code_source: Optional[str] = None,
    fund_id_from_source: Optional[str] = None,
    filters: Optional[Dict[str, Iterable[str]]] = None,
    preprocess_fn: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
    postprocess_fn: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
    required_cols: Optional[List[str]] = None,
    dedupe_cols: Optional[List[str]] = None,
    extra_select_cols: Optional[List[str]] = None,
    read_all: bool = False,
) -> int:
    if dry_run:
        return 0

    mapped_cols = {src: tgt for src, tgt in mapping.items() if tgt}
    select_cols = set(mapped_cols.keys())
    if extra_select_cols:
        select_cols.update(extra_select_cols)
    if filters:
        select_cols.update(filters.keys())
    if fund_code_source:
        select_cols.add(fund_code_source)

    select_list = ", ".join(f'"{c}"' for c in sorted(select_cols))
    query = f'SELECT {select_list} FROM "{source_schema}"."{source_table}"'

    def process_frame(df: pd.DataFrame) -> int:
        df = apply_filters(df, filters)
        if preprocess_fn:
            df = preprocess_fn(df)
        if df.empty:
            return 0
        df = df.copy()

        if fund_code_source and fund_code_source in df.columns:
            df["_fund_code_lookup"] = df[fund_code_source]

        df = df.rename(columns=mapped_cols)

        if fund_id_lookup and fund_id_column and fund_code_source:
            if "_fund_code_lookup" in df.columns:
                df[fund_id_column] = df["_fund_code_lookup"].map(fund_id_lookup)

        if fund_id_from_source and fund_id_column:
            source_after = mapped_cols.get(fund_id_from_source, fund_id_from_source)
            if source_after in df.columns:
                df[fund_id_column] = df[source_after]

        for col, value in defaults.items():
            if col not in df.columns:
                df[col] = value

        if postprocess_fn:
            df = postprocess_fn(df)

        if required_cols:
            df = df.dropna(subset=required_cols)
        if dedupe_cols:
            df = df.drop_duplicates(subset=dedupe_cols)

        insert_cols = [col["name"] for col in target_cols_info if col["name"] in df.columns]
        df = df[insert_cols]

        maybe_fill_dividend_policy(df, target_cols_info)
        df = coerce_types(df, target_cols_info)

        df.to_sql(
            target_table,
            con=engine,
            schema=target_schema,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )
        return len(df)

    total_rows = 0
    if read_all:
        df_all = pd.read_sql_query(query, engine)
        total_rows = process_frame(df_all)
        return total_rows

    for chunk in pd.read_sql_query(query, engine, chunksize=chunksize):
        total_rows += process_frame(chunk)
    return total_rows


def update_fund_master_isin(
    engine,
    source_schema: str,
    target_schema: str,
    dry_run: bool,
) -> int:
    if dry_run:
        return 0
    query = text(
        f"""
        WITH src AS (
            SELECT DISTINCT ON (code) fund_code, code
            FROM "{source_schema}"."funds_codes"
            WHERE type ILIKE 'isin' AND code IS NOT NULL
            ORDER BY code, fund_code
        )
        UPDATE "{target_schema}"."fund_master" fm
        SET isin_code = COALESCE(fm.isin_code, src.code)
        FROM src
        WHERE fm.fund_code = src.fund_code
          AND (fm.isin_code IS NULL OR fm.isin_code = '')
          AND NOT EXISTS (
              SELECT 1
              FROM "{target_schema}"."fund_master" fm2
              WHERE fm2.isin_code = src.code
                AND fm2.fund_code <> src.fund_code
          )
        """
    )
    with engine.begin() as conn:
        result = conn.execute(query)
    return result.rowcount or 0


def update_fund_master_fees(
    engine,
    source_schema: str,
    target_schema: str,
    dry_run: bool,
) -> int:
    if dry_run:
        return 0
    query = text(
        f"""
        UPDATE "{target_schema}"."fund_master" fm
        SET total_expense_ratio = COALESCE(fm.total_expense_ratio, f.ter_actual, f.ter_max),
            management_fee = COALESCE(fm.management_fee, f.management_actual, f.management_max)
        FROM "{source_schema}"."funds_fee" f
        WHERE fm.fund_code = f.fund_code
        """
    )
    with engine.begin() as conn:
        result = conn.execute(query)
    return result.rowcount or 0


def build_security_master(
    engine,
    source_schema: str,
    target_schema: str,
    dry_run: bool,
) -> int:
    if dry_run:
        return 0
    query = text(
        f"""
        SELECT DISTINCT name, type, source
        FROM "{source_schema}"."funds_holding"
        WHERE name IS NOT NULL
        """
    )
    df = pd.read_sql_query(query, engine)
    if df.empty:
        return 0

    df = df.rename(
        columns={
            "name": "security_name",
            "type": "security_type",
            "source": "data_source",
        }
    )
    df["security_name"] = df["security_name"].astype(str).str.strip()
    df = df[df["security_name"] != ""]
    df["data_source"] = df["data_source"].fillna("Thai_Web")
    df["security_type"] = df["security_type"].fillna("holding")
    df = df.drop_duplicates(subset=["security_name", "data_source"])

    existing = pd.read_sql_query(
        f'SELECT security_name, data_source FROM "{target_schema}"."security_master"',
        engine,
    )
    if not existing.empty:
        df = df.merge(
            existing,
            on=["security_name", "data_source"],
            how="left",
            indicator=True,
        )
        df = df[df["_merge"] == "left_only"].drop(columns=["_merge"])

    if df.empty:
        return 0

    df.to_sql(
        "security_master",
        con=engine,
        schema=target_schema,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )
    return len(df)


def fetch_security_id_map(
    engine,
    target_schema: str,
) -> Dict[Tuple[str, str], int]:
    query = text(
        f'SELECT security_id, security_name, data_source FROM "{target_schema}"."security_master"'
    )
    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()
    return {(row[1], row[2] or "Thai_Web"): row[0] for row in rows}


def migrate_holdings(
    engine,
    source_schema: str,
    target_schema: str,
    chunksize: int,
    dry_run: bool,
    fund_id_lookup: Dict[str, str],
    security_id_lookup: Dict[Tuple[str, str], int],
) -> int:
    if dry_run:
        return 0
    query = text(
        f"""
        SELECT fund_code, name, source, percent, as_of_date
        FROM "{source_schema}"."funds_holding"
        """
    )
    total_rows = 0
    for chunk in pd.read_sql_query(query, engine, chunksize=chunksize):
        chunk["parent_fund_id"] = chunk["fund_code"].map(fund_id_lookup)
        chunk["data_source"] = chunk["source"].fillna("Thai_Web")
        chunk["_sec_key"] = list(zip(chunk["name"], chunk["data_source"]))
        chunk["held_security_id"] = chunk["_sec_key"].map(security_id_lookup)
        chunk["held_fund_id"] = None
        chunk["report_date"] = pd.to_datetime(chunk["as_of_date"], errors="coerce").dt.date
        chunk["percentage"] = pd.to_numeric(chunk["percent"], errors="coerce")
        df = chunk[
            ["parent_fund_id", "held_security_id", "held_fund_id", "report_date", "percentage"]
        ]
        df = df.dropna(subset=["parent_fund_id", "held_security_id"])
        df = df.drop_duplicates(subset=["parent_fund_id", "held_security_id", "report_date"])

        df.to_sql(
            "fund_holdings",
            con=engine,
            schema=target_schema,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )
        total_rows += len(df)
    return total_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smart migrate fund_thai to Model V4.")
    parser.add_argument("--source-schema", default=SOURCE_SCHEMA_DEFAULT)
    parser.add_argument("--target-schema", default=TARGET_SCHEMA_DEFAULT)
    parser.add_argument("--host", default=os.getenv("DB_HOST", "localhost"))
    parser.add_argument("--port", default=os.getenv("DB_PORT", "5433"))
    parser.add_argument("--user", default=os.getenv("DB_USER", "admin"))
    parser.add_argument("--password", default=os.getenv("DB_PASSWORD", "password"))
    parser.add_argument("--db", default=os.getenv("DB_NAME", "funds_db"))
    parser.add_argument("--sample-rows", type=int, default=5)
    parser.add_argument("--chunksize", type=int, default=50000)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    load_env()
    args = parse_args()
    engine = build_engine(args.host, args.port, args.user, args.password, args.db)

    migration_tasks = [
        {
            "name": "fund_master",
            "source_table": "funds_master_info",
            "target_table": "fund_master",
            "column_map": {
                "fund_code": "fund_code",
                "full_name_th": "fund_name_th",
                "full_name_en": "fund_name_en",
                "amc": "asset_management_co",
                "category": "fund_type",
                "risk_level": "risk_level",
                "is_dividend": "dividend_policy",
                "inception_date": "inception_date",
                "currency": "base_currency",
                "country": "domicile",
                "source_url": "factsheet_url",
            },
            "defaults": {
                "data_source": "Thai_Web",
                "base_currency": "THB",
                "domicile": "Thailand",
            },
            "required": ["fund_code"],
            "dedupe_cols": ["fund_code"],
        },
        {
            "name": "fund_performance",
            "source_table": "funds_daily",
            "target_table": "fund_performance",
            "column_map": {
                "fund_code": "fund_id",
                "nav_date": "nav_date",
                "nav_value": "nav_value",
            },
            "defaults": {},
            "required": ["fund_id", "nav_date"],
            "dedupe_cols": ["fund_id", "nav_date"],
        },
        {
            "name": "fund_asset_allocation",
            "source_table": "funds_allocations",
            "target_table": "fund_asset_allocation",
            "column_map": {
                "fund_code": "fund_id",
                "name": "asset_type",
                "percent": "percentage",
                "as_of_date": "updated_date",
            },
            "defaults": {},
            "filters": {"type": ["asset_alloc"]},
            "filter_desc": "type IN ('asset_alloc')",
            "required": ["fund_id", "asset_type"],
            "dedupe_cols": ["fund_id", "asset_type"],
            "extra_select_cols": ["type"],
        },
        {
            "name": "fund_sector_breakdown",
            "source_table": "funds_allocations",
            "target_table": "fund_sector_breakdown",
            "column_map": {
                "fund_code": "fund_id",
                "name": "sector_name",
                "percent": "percentage",
                "as_of_date": "updated_date",
            },
            "defaults": {},
            "filters": {"type": ["sector_alloc", "country_alloc"]},
            "filter_desc": "type IN ('sector_alloc','country_alloc')",
            "required": ["fund_id", "sector_name"],
            "dedupe_cols": ["fund_id", "sector_name"],
            "extra_select_cols": ["type"],
            "postprocess_fn": postprocess_sector_country,
        },
        {
            "name": "fund_risk",
            "source_table": "funds_statistics",
            "target_table": "fund_risk",
            "column_map": {
                "fund_code": "fund_id",
                "sharpe_ratio": "sharpe",
                "max_drawdown": "max_drawdown",
                "tracking_error": "tracking_error",
            },
            "defaults": {},
            "required": ["fund_id"],
            "dedupe_cols": ["fund_id"],
            "extra_select_cols": ["as_of_date"],
            "preprocess_fn": select_latest_risk,
            "read_all": True,
        },
    ]

    source_tables = {
        task["source_table"] for task in migration_tasks
    } | {"funds_codes", "funds_fee", "funds_holding"}

    sample_cache: Dict[str, pd.DataFrame] = {}
    print("=== Step 1: Inspect & Analyze ===")
    for source_table in sorted(source_tables):
        if not table_exists(engine, args.source_schema, source_table):
            print(f"[Skip] Source table not found: {args.source_schema}.{source_table}")
            continue
        sample_df = fetch_sample_rows(engine, args.source_schema, source_table, args.sample_rows)
        sample_cache[source_table] = sample_df
        print(f"\n[{source_table}] sample rows (first {args.sample_rows}):")
        if sample_df.empty:
            print("(no rows)")
        else:
            print(sample_df.head(args.sample_rows).to_string(index=False))

    plan_entries = []
    for task in migration_tasks:
        source_table = task["source_table"]
        if source_table not in sample_cache:
            continue

        sample_df = sample_cache[source_table]
        filtered_sample = apply_filters(sample_df.copy(), task.get("filters"))
        if filtered_sample.empty and task.get("filters"):
            print(
                f"\n[{source_table}] no rows match filter: {task.get('filter_desc')}"
            )

        if filtered_sample.empty:
            source_cols = [col["name"] for col in fetch_table_columns(engine, args.source_schema, source_table)]
        else:
            source_cols = list(filtered_sample.columns)

        target_cols_info = []
        target_cols = []
        if table_exists(engine, args.target_schema, task["target_table"]):
            target_cols_info = fetch_table_columns(engine, args.target_schema, task["target_table"])
            target_cols = [col["name"] for col in target_cols_info]
        else:
            print(
                f"[Warn] Target table not found: {args.target_schema}.{task['target_table']}."
            )

        mapping, reasons, defaults = build_mapping_plan(
            source_cols=source_cols,
            target_cols=target_cols,
            column_map=task["column_map"],
            defaults=task.get("defaults", {}),
        )
        plan_entries.append(
            {
                "task": task,
                "mapping": mapping,
                "reasons": reasons,
                "defaults": defaults,
                "target_cols_info": target_cols_info,
                "samples": sample_values(filtered_sample) if not filtered_sample.empty else {},
            }
        )

    print("\n=== Step 3: Mapping Plan ===")
    for entry in plan_entries:
        task = entry["task"]
        describe_mapping_plan(
            source_table=task["source_table"],
            target_table=task["target_table"],
            mapping=entry["mapping"],
            reasons=entry["reasons"],
            samples=entry["samples"],
            defaults=entry["defaults"],
            filter_desc=task.get("filter_desc"),
        )

    if "funds_codes" in sample_cache:
        describe_update_plan(
            "funds_codes -> fund_master (ISIN update)",
            [
                {"source": "fund_code", "target": "fund_master.fund_code"},
                {"source": "code", "target": "fund_master.isin_code", "note": "type=ISIN"},
            ],
            sample_values(sample_cache["funds_codes"]),
        )

    if "funds_fee" in sample_cache:
        describe_update_plan(
            "funds_fee -> fund_master (fees update)",
            [
                {"source": "fund_code", "target": "fund_master.fund_code"},
                {
                    "source": "ter_actual",
                    "target": "fund_master.total_expense_ratio",
                    "note": "fallback ter_max",
                },
                {
                    "source": "management_actual",
                    "target": "fund_master.management_fee",
                    "note": "fallback management_max",
                },
            ],
            sample_values(sample_cache["funds_fee"]),
        )

    if "funds_holding" in sample_cache:
        describe_update_plan(
            "funds_holding -> security_master",
            [
                {"source": "name", "target": "security_master.security_name"},
                {"source": "type", "target": "security_master.security_type"},
                {"source": "source", "target": "security_master.data_source"},
            ],
            sample_values(sample_cache["funds_holding"]),
        )
        describe_update_plan(
            "funds_holding -> fund_holdings",
            [
                {
                    "source": "fund_code",
                    "target": "fund_holdings.parent_fund_id",
                    "note": "via fund_master lookup",
                },
                {
                    "source": "name",
                    "target": "fund_holdings.held_security_id",
                    "note": "via security_master lookup",
                },
                {"source": "percent", "target": "fund_holdings.percentage"},
                {"source": "as_of_date", "target": "fund_holdings.report_date"},
            ],
            sample_values(sample_cache["funds_holding"]),
        )

    if args.dry_run:
        print("\nDry run complete. No data inserted.")
        return 0

    print("\n=== Step 4: Execute Migration ===")
    for task in migration_tasks:
        if not table_exists(engine, args.target_schema, task["target_table"]):
            print(f"Target table missing: {args.target_schema}.{task['target_table']}")
            return 1

    master_entry = next(
        (entry for entry in plan_entries if entry["task"]["name"] == "fund_master"),
        None,
    )
    if not master_entry:
        print("Missing mapping for fund_master. Abort.")
        return 1

    fund_master_cols_info = master_entry["target_cols_info"]
    fund_id_mode = fund_id_strategy(fund_master_cols_info)
    fund_id_column_name = resolve_target_column(
        "fund_id", [c["name"] for c in fund_master_cols_info]
    )

    master_row_count = table_row_count(
        engine, args.target_schema, master_entry["task"]["target_table"]
    )
    if master_row_count > 0:
        print(
            f"Skip insert into {master_entry['task']['target_table']} "
            f"(already has {master_row_count} rows)"
        )
    else:
        inserted_master = migrate_table(
            engine=engine,
            source_schema=args.source_schema,
            target_schema=args.target_schema,
            source_table="funds_master_info",
            target_table=master_entry["task"]["target_table"],
            mapping=master_entry["mapping"],
            defaults=master_entry["defaults"],
            target_cols_info=fund_master_cols_info,
            chunksize=args.chunksize,
            dry_run=False,
            fund_id_from_source="fund_code" if fund_id_mode == "required" else None,
            fund_id_column=fund_id_column_name,
            required_cols=master_entry["task"].get("required"),
            dedupe_cols=master_entry["task"].get("dedupe_cols"),
        )
        print(
            f"Inserted {inserted_master} rows into {master_entry['task']['target_table']}"
        )

    updated_isin = update_fund_master_isin(
        engine, args.source_schema, args.target_schema, dry_run=False
    )
    print(f"Updated ISIN codes: {updated_isin} rows")

    updated_fees = update_fund_master_fees(
        engine, args.source_schema, args.target_schema, dry_run=False
    )
    print(f"Updated fees: {updated_fees} rows")

    fund_id_lookup = None
    fund_id_column = resolve_target_column(
        "fund_id", [c["name"] for c in fund_master_cols_info]
    )
    fund_code_column = resolve_target_column(
        "fund_code", [c["name"] for c in fund_master_cols_info]
    )

    if (
        fund_id_mode in ("auto", "required")
        and fund_id_column
        and fund_code_column
        and table_exists(engine, args.target_schema, master_entry["task"]["target_table"])
    ):
        fund_id_lookup = fetch_fund_id_map(
            engine,
            args.target_schema,
            master_entry["task"]["target_table"],
            fund_code_column,
            fund_id_column,
        )

    for entry in plan_entries:
        task = entry["task"]
        if task["name"] == "fund_master":
            continue

        target_cols_info = entry["target_cols_info"]
        target_cols = [col["name"] for col in target_cols_info]
        resolved_fund_id = resolve_target_column("fund_id", target_cols)
        fund_code_source = "fund_code" if "fund_code" in task["column_map"] else None

        target_count = table_row_count(engine, args.target_schema, task["target_table"])
        if target_count > 0:
            print(
                f"Skip insert into {task['target_table']} (already has {target_count} rows)"
            )
            continue

        inserted = migrate_table(
            engine=engine,
            source_schema=args.source_schema,
            target_schema=args.target_schema,
            source_table=task["source_table"],
            target_table=task["target_table"],
            mapping=entry["mapping"],
            defaults=entry["defaults"],
            target_cols_info=target_cols_info,
            chunksize=args.chunksize,
            dry_run=False,
            fund_id_lookup=fund_id_lookup,
            fund_id_column=resolved_fund_id,
            fund_code_source=fund_code_source,
            fund_id_from_source="fund_code" if resolved_fund_id and not fund_id_lookup else None,
            filters=task.get("filters"),
            preprocess_fn=task.get("preprocess_fn"),
            postprocess_fn=task.get("postprocess_fn"),
            required_cols=task.get("required"),
            dedupe_cols=task.get("dedupe_cols"),
            extra_select_cols=task.get("extra_select_cols"),
            read_all=task.get("read_all", False),
        )
        print(f"Inserted {inserted} rows into {task['target_table']}")

    if not table_exists(engine, args.target_schema, "security_master"):
        print(f"Target table missing: {args.target_schema}.security_master")
        return 1

    inserted_security = build_security_master(
        engine, args.source_schema, args.target_schema, dry_run=False
    )
    print(f"Inserted {inserted_security} rows into security_master")

    security_id_lookup = fetch_security_id_map(engine, args.target_schema)
    if fund_id_lookup is None:
        print("Missing fund_id lookup; cannot load holdings.")
        return 1

    holdings_count = table_row_count(engine, args.target_schema, "fund_holdings")
    if holdings_count > 0:
        print(f"Skip insert into fund_holdings (already has {holdings_count} rows)")
        return 0

    inserted_holdings = migrate_holdings(
        engine=engine,
        source_schema=args.source_schema,
        target_schema=args.target_schema,
        chunksize=args.chunksize,
        dry_run=False,
        fund_id_lookup=fund_id_lookup,
        security_id_lookup=security_id_lookup,
    )
    print(f"Inserted {inserted_holdings} rows into fund_holdings")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
