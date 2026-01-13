#!/usr/bin/env python3
import argparse
import os
import sys
from datetime import time as dt_time
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.types import Boolean, Date, DateTime, Float, Text


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DATA_DIR = BASE_DIR / "data_thai"
DEFAULT_SCHEMA = "fund_thai"

DEFAULT_FILES = [
    "funds_master_info.csv",
    "funds_daily.csv",
    "funds_allocations.csv",
    "funds_holding.csv",
    "funds_fee.csv",
    "funds_codes.csv",
    "funds_statistics.csv",
]

TEXT_HINTS = ("code", "id", "isin", "cusip", "ticker")


def load_env() -> None:
    env_path = BASE_DIR / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)


def read_csv_with_fallback(path: Path, **kwargs) -> pd.DataFrame:
    encodings = ("utf-8-sig", "utf-8", "cp874", "tis-620")
    last_error = None
    for enc in encodings:
        try:
            return pd.read_csv(path, encoding=enc, **kwargs)
        except UnicodeDecodeError as exc:
            last_error = exc
    raise last_error


def infer_column_type(name: str, series: pd.Series) -> object:
    values = series.dropna()
    if values.empty:
        return Text()

    lower_name = name.lower()
    if any(hint in lower_name for hint in TEXT_HINTS):
        return Text()

    numeric = pd.to_numeric(values, errors="coerce")
    if numeric.notna().mean() >= 0.9:
        return Float()

    datetime_vals = pd.to_datetime(values, errors="coerce", infer_datetime_format=True)
    if datetime_vals.notna().mean() >= 0.9:
        if (datetime_vals.dt.time == dt_time(0, 0)).all():
            return Date()
        return DateTime()

    bool_map = {"true", "false", "0", "1", "yes", "no"}
    if values.astype(str).str.strip().str.lower().isin(bool_map).all():
        return Boolean()

    return Text()


def infer_types(sample: pd.DataFrame) -> dict:
    return {col: infer_column_type(col, sample[col]) for col in sample.columns}


def coerce_boolean(series: pd.Series) -> pd.Series:
    mapping = {
        "true": True,
        "false": False,
        "1": True,
        "0": False,
        "yes": True,
        "no": False,
    }
    return series.map(
        lambda v: mapping.get(str(v).strip().lower()) if pd.notna(v) else None
    )


def coerce_chunk(df: pd.DataFrame, type_map: dict) -> pd.DataFrame:
    for col, col_type in type_map.items():
        if col not in df.columns:
            continue
        if isinstance(col_type, Float):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif isinstance(col_type, Date):
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        elif isinstance(col_type, DateTime):
            df[col] = pd.to_datetime(df[col], errors="coerce")
        elif isinstance(col_type, Boolean):
            df[col] = coerce_boolean(df[col])
    return df


def build_engine(host: str, port: str, user: str, password: str, dbname: str, schema: str):
    safe_password = quote_plus(password) if password else ""
    url = f"postgresql://{user}:{safe_password}@{host}:{port}/{dbname}"
    connect_args = {
        "client_encoding": "utf8",
        "options": f"-c search_path={schema}",
    }
    return create_engine(url, connect_args=connect_args)


def import_file(
    engine,
    schema: str,
    path: Path,
    sample_rows: int,
    chunk_rows: int,
) -> int:
    sample = read_csv_with_fallback(
        path,
        dtype=str,
        nrows=sample_rows,
        keep_default_na=True,
        na_values=["", "NA", "NaN", "nan", "null", "None"],
        low_memory=False,
    )
    if sample.empty and not sample.columns.any():
        return 0

    type_map = infer_types(sample)
    table_name = path.stem

    sample.head(0).to_sql(
        table_name,
        con=engine,
        schema=schema,
        if_exists="replace",
        index=False,
        dtype=type_map,
    )

    total_rows = 0
    for chunk in read_csv_with_fallback(
        path,
        dtype=str,
        chunksize=chunk_rows,
        keep_default_na=True,
        na_values=["", "NA", "NaN", "nan", "null", "None"],
        low_memory=False,
    ):
        chunk = coerce_chunk(chunk, type_map)
        chunk.to_sql(
            table_name,
            con=engine,
            schema=schema,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )
        total_rows += len(chunk)

    print(f"✅ Imported {total_rows} rows into {schema}.{table_name}")
    return total_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import Thai fund CSVs into Postgres.")
    parser.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR))
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument("--files", nargs="*", default=DEFAULT_FILES)
    parser.add_argument("--sample-rows", type=int, default=10000)
    parser.add_argument("--chunk-rows", type=int, default=50000)
    parser.add_argument("--host", default=os.getenv("DB_HOST", "localhost"))
    parser.add_argument("--port", default=os.getenv("DB_PORT", "5433"))
    parser.add_argument("--user", default=os.getenv("DB_USER", "admin"))
    parser.add_argument("--password", default=os.getenv("DB_PASSWORD", "password"))
    parser.add_argument("--db", default=os.getenv("DB_NAME", "funds_db"))
    return parser.parse_args()


def main() -> int:
    load_env()
    args = parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        print(f"❌ Data directory not found: {data_dir}")
        return 1

    missing = [f for f in args.files if not (data_dir / f).exists()]
    if missing:
        print("❌ Missing files:")
        for name in missing:
            print(f"  - {name}")
        return 1

    engine = build_engine(args.host, args.port, args.user, args.password, args.db, args.schema)
    try:
        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{args.schema}"'))
    except Exception as exc:
        print(f"❌ Failed to create schema '{args.schema}': {exc}")
        return 1

    total_all = 0
    for file_name in args.files:
        path = data_dir / file_name
        try:
            total_all += import_file(
                engine,
                args.schema,
                path,
                sample_rows=args.sample_rows,
                chunk_rows=args.chunk_rows,
            )
        except Exception as exc:
            print(f"❌ Failed to import {file_name}: {exc}")
            return 1

    print(f"✅ Done. Total rows imported: {total_all}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
