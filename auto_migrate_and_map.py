#!/usr/bin/env python3
"""
Auto-migrate and map data from a source schema (fund_thai) to a target schema (public).

Mapping logic overview:
1) Discovery:
   - Read table/column metadata for both schemas from information_schema.
   - Only BASE TABLEs are considered.
2) Table matching:
   - Normalize table names (lowercase, alnum only) and score similarity.
   - Add a column-overlap score based on best column name matches.
   - Optional table name hints boost matches (still auto-scored).
3) Column matching:
   - Normalize column names and score matches using:
     a) Exact normalized match (highest confidence)
     b) Synonym hints (semantic match)
     c) Substring match
     d) Fuzzy similarity (difflib ratio)
   - One-to-one mapping: a target column is mapped at most once.
   - Non-exact matches are reported with reasons.
4) Defaults:
   - If a target column is missing in the source mapping, apply defaults
     for known business fields (e.g., Base_Currency -> "THB",
     Data_Source -> "Thai_Web").
5) Insert:
   - Pull source data in chunks, rename columns, apply defaults,
     coerce types based on target data types, and insert into target.
6) Report:
   - Print rows inserted, sample rows, and unmapped source columns.
"""

from __future__ import annotations

import argparse
import difflib
import os
import re
from urllib.parse import quote_plus
from typing import Dict, Iterable, List, Tuple

import pandas as pd
from sqlalchemy import create_engine, text


SOURCE_SCHEMA_DEFAULT = "fund_thai"
TARGET_SCHEMA_DEFAULT = "public"

# Table name hints (semantic intent) to improve auto-matching.
TABLE_HINTS = {
    "funds_master_info": ["fund_master", "fund_master_info", "funds_master"],
    "funds_daily": ["fund_performance", "fund_daily", "fund_nav"],
    "funds_statistics": ["fund_risk", "fund_statistics"],
    "funds_holding": ["fund_holdings", "fund_holding"],
    "funds_allocations": ["fund_asset_allocation", "fund_sector_breakdown", "fund_allocation"],
    "funds_fee": ["fund_master", "fund_fees", "fund_fee"],
    "funds_codes": ["security_master", "fund_master", "fund_codes"],
}

# Column synonym hints for semantic matching.
# Examples (non-exact mapping rationale):
# - amc -> asset_management_co: "amc" is commonly used for asset management company
# - nav_value -> nav_value/nav_price: NAV value is commonly stored as NAV price/value
SYNONYM_HINTS = {
    "amc": ["asset_management_co", "asset_management_company", "management_company"],
    "nav_value": ["nav_value", "nav_price", "nav"],
    "nav_date": ["nav_date", "as_of_date", "date"],
    "fund_code": ["fund_code", "fund_id", "fundid"],
    "isin": ["isin_code", "isin"],
    "cusip": ["cusip_number", "cusip"],
    "management_fee": ["management_fee", "management_actual", "management_max"],
    "total_expense_ratio": ["total_expense_ratio", "expense_ratio"],
    "risk_level": ["risk_level", "risk_rating"],
    "base_currency": ["base_currency", "currency", "ccy"],
    "data_source": ["data_source", "source", "source_name"],
}

# Defaults for missing target columns (normalized target column name -> value)
DEFAULT_VALUES = {
    "basecurrency": "THB",
    "currency": "THB",
    "datasource": "Thai_Web",
    "country": "Thailand",
    "countrycode": "TH",
    "domicile": "Thailand",
}


def normalize_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", name.lower())


def build_engine(host: str, port: str, user: str, password: str, dbname: str):
    safe_user = quote_plus(user)
    safe_password = quote_plus(password)
    url = f"postgresql://{safe_user}:{safe_password}@{host}:{port}/{dbname}"
    return create_engine(url)


def fetch_tables(engine, schema: str) -> Dict[str, Dict[str, str]]:
    query = text(
        """
        SELECT c.table_name, c.column_name, c.data_type
        FROM information_schema.columns c
        JOIN information_schema.tables t
          ON c.table_schema = t.table_schema
         AND c.table_name = t.table_name
        WHERE c.table_schema = :schema
          AND t.table_type = 'BASE TABLE'
        ORDER BY c.table_name, c.ordinal_position
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"schema": schema}).fetchall()
    tables: Dict[str, Dict[str, str]] = {}
    for table_name, column_name, data_type in rows:
        tables.setdefault(table_name, {})[column_name] = data_type
    return tables


def table_name_score(source: str, target: str) -> float:
    src_norm = normalize_name(source)
    tgt_norm = normalize_name(target)
    return difflib.SequenceMatcher(None, src_norm, tgt_norm).ratio()


def column_similarity_score(
    source_cols: Iterable[str],
    target_cols: Iterable[str],
) -> float:
    source_cols_list = list(source_cols)
    target_norms = [normalize_name(c) for c in target_cols]
    matches = 0
    for col in source_cols_list:
        src_norm = normalize_name(col)
        best = 0.0
        for tgt_norm in target_norms:
            score = difflib.SequenceMatcher(None, src_norm, tgt_norm).ratio()
            best = max(best, score)
        if best >= 0.85:
            matches += 1
    if not source_cols_list:
        return 0.0
    return matches / max(len(source_cols_list), 1)


def pick_target_table(
    source_table: str,
    source_cols: List[str],
    target_tables: Dict[str, Dict[str, str]],
) -> Tuple[str | None, float]:
    best_table = None
    best_score = 0.0
    hint_targets = [normalize_name(t) for t in TABLE_HINTS.get(source_table, [])]
    for target_table, target_cols in target_tables.items():
        name_score = table_name_score(source_table, target_table)
        col_score = column_similarity_score(source_cols, list(target_cols.keys()))
        score = (0.4 * name_score) + (0.6 * col_score)
        if normalize_name(target_table) in hint_targets:
            score += 0.2
        if score > best_score:
            best_score = score
            best_table = target_table
    if best_score < 0.3:
        return None, best_score
    return best_table, best_score


def build_synonym_map() -> Dict[str, List[str]]:
    mapped = {}
    for key, values in SYNONYM_HINTS.items():
        mapped[normalize_name(key)] = [normalize_name(v) for v in values]
    return mapped


def match_columns(
    source_cols: List[str],
    target_cols: List[str],
) -> Tuple[Dict[str, str], Dict[str, str], List[str]]:
    target_norm_to_name = {}
    for col in target_cols:
        target_norm_to_name.setdefault(normalize_name(col), []).append(col)

    synonym_map = build_synonym_map()
    used_targets = set()
    mapping = {}
    reasons = {}
    unmapped = []

    for src in source_cols:
        src_norm = normalize_name(src)
        best_target = None
        best_score = 0.0
        best_reason = "unmapped"

        # Exact match
        if src_norm in target_norm_to_name:
            for candidate in target_norm_to_name[src_norm]:
                if candidate not in used_targets:
                    best_target = candidate
                    best_score = 1.0
                    best_reason = "exact"
                    break

        # Synonym match
        if not best_target and src_norm in synonym_map:
            for synonym in synonym_map[src_norm]:
                if synonym in target_norm_to_name:
                    for candidate in target_norm_to_name[synonym]:
                        if candidate not in used_targets:
                            best_target = candidate
                            best_score = 0.95
                            best_reason = "synonym"
                            break
                if best_target:
                    break

        # Substring and fuzzy
        if not best_target:
            for tgt in target_cols:
                if tgt in used_targets:
                    continue
                tgt_norm = normalize_name(tgt)
                if src_norm in tgt_norm or tgt_norm in src_norm:
                    score = 0.85
                    if score > best_score:
                        best_score = score
                        best_target = tgt
                        best_reason = "substring"
                else:
                    score = difflib.SequenceMatcher(None, src_norm, tgt_norm).ratio()
                    if score > best_score:
                        best_score = score
                        best_target = tgt
                        best_reason = "fuzzy"

        if best_target and best_score >= 0.75:
            mapping[src] = best_target
            used_targets.add(best_target)
            reasons[src] = best_reason
        else:
            unmapped.append(src)

    return mapping, reasons, unmapped


def coerce_types(df: pd.DataFrame, target_types: Dict[str, str]) -> pd.DataFrame:
    for col, dtype in target_types.items():
        if col not in df.columns:
            continue
        if dtype in {"integer", "bigint", "numeric", "double precision", "real", "decimal"}:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif dtype == "date":
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        elif dtype.startswith("timestamp"):
            df[col] = pd.to_datetime(df[col], errors="coerce")
        elif dtype == "boolean":
            df[col] = df[col].map(
                lambda v: None
                if pd.isna(v)
                else str(v).strip().lower() in {"true", "1", "yes"}
            )
    return df


def build_select_query(schema: str, table: str, columns: List[str]) -> str:
    quoted_cols = [f'"{c}"' for c in columns]
    return f'SELECT {", ".join(quoted_cols)} FROM "{schema}"."{table}"'


def migrate_table(
    engine,
    source_schema: str,
    source_table: str,
    target_schema: str,
    target_table: str,
    mapping: Dict[str, str],
    reasons: Dict[str, str],
    target_types: Dict[str, str],
    defaults: Dict[str, str],
    chunksize: int,
    dry_run: bool,
) -> int:
    source_cols = list(mapping.keys())
    select_query = build_select_query(source_schema, source_table, source_cols)
    total_rows = 0

    if dry_run:
        return 0

    for chunk in pd.read_sql_query(select_query, engine, chunksize=chunksize):
        chunk = chunk.rename(columns=mapping)
        for col, value in defaults.items():
            if col in target_types and col not in chunk.columns:
                chunk[col] = value
        chunk = coerce_types(chunk, target_types)
        chunk.to_sql(
            target_table,
            con=engine,
            schema=target_schema,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )
        total_rows += len(chunk)

    return total_rows


def sample_rows(engine, schema: str, table: str, limit: int = 5) -> pd.DataFrame:
    query = text(f'SELECT * FROM "{schema}"."{table}" LIMIT {limit}')
    return pd.read_sql_query(query, engine)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Auto migrate fund_thai to public.")
    parser.add_argument("--source-schema", default=SOURCE_SCHEMA_DEFAULT)
    parser.add_argument("--target-schema", default=TARGET_SCHEMA_DEFAULT)
    parser.add_argument("--host", default=os.getenv("DB_HOST", "localhost"))
    parser.add_argument("--port", default=os.getenv("DB_PORT", "5433"))
    parser.add_argument("--user", default=os.getenv("DB_USER", "admin"))
    parser.add_argument("--password", default=os.getenv("DB_PASSWORD", "password"))
    parser.add_argument("--db", default=os.getenv("DB_NAME", "funds_db"))
    parser.add_argument("--chunksize", type=int, default=50000)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    engine = build_engine(args.host, args.port, args.user, args.password, args.db)

    source_tables = fetch_tables(engine, args.source_schema)
    target_tables = fetch_tables(engine, args.target_schema)

    if not source_tables:
        print("No source tables found.")
        return 1
    if not target_tables:
        print("No target tables found.")
        return 1

    report = []
    unmapped_columns_all = {}
    successes = []
    failures = []
    skipped = []

    print("--- Migration Report ---")
    for source_table, source_cols in source_tables.items():
        target_table, score = pick_target_table(source_table, list(source_cols.keys()), target_tables)
        if not target_table:
            print(f"Skip {source_table}: no confident target match (score={score:.2f})")
            skipped.append((source_table, "no_target_match"))
            continue

        target_cols = list(target_tables[target_table].keys())
        mapping, reasons, unmapped = match_columns(list(source_cols.keys()), target_cols)
        if not mapping:
            print(f"Skip {source_table} -> {target_table}: no column mapping found.")
            skipped.append((source_table, "no_column_mapping"))
            continue

        defaults = {
            col: DEFAULT_VALUES.get(normalize_name(col), None)
            for col in target_cols
            if normalize_name(col) in DEFAULT_VALUES
        }
        defaults = {k: v for k, v in defaults.items() if v is not None}

        try:
            rows = migrate_table(
                engine,
                args.source_schema,
                source_table,
                args.target_schema,
                target_table,
                mapping,
                reasons,
                target_tables[target_table],
                defaults,
                args.chunksize,
                args.dry_run,
            )
        except Exception as exc:  # noqa: BLE001 - report and continue
            print(f"FAILED {source_table} -> {target_table}: {exc}")
            failures.append((source_table, target_table, str(exc)))
            continue

        print(f"Table: {source_table} -> {target_table} (score={score:.2f})")
        for src, tgt in mapping.items():
            reason = reasons.get(src, "exact")
            note = "" if reason == "exact" else f" ({reason})"
            print(f"  - {src} -> {tgt}{note}")
        print(f"  Rows inserted: {rows}")
        successes.append((source_table, target_table, rows))

        if unmapped:
            unmapped_columns_all[source_table] = unmapped

        if not args.dry_run:
            sample = sample_rows(engine, args.target_schema, target_table)
            if not sample.empty:
                print(sample.head(5).to_string(index=False))
            else:
                print("  Sample: (no rows)")

        report.append((source_table, target_table, rows))

    if unmapped_columns_all:
        print("\n[Unmapped Columns]")
        for table, cols in unmapped_columns_all.items():
            print(f"- {table}: {', '.join(cols)}")

    if successes or failures or skipped:
        print("\n[Summary]")
        for source_table, target_table, rows in successes:
            print(f"OK {source_table} -> {target_table}: {rows} rows")
        for source_table, target_table, error in failures:
            print(f"FAIL {source_table} -> {target_table}: {error}")
        for source_table, reason in skipped:
            print(f"SKIP {source_table}: skipped ({reason})")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
