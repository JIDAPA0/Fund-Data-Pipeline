## Fund ETL Pipeline

Extracts fund data (lists, performance, static details, holdings) and syncs into PostgreSQL with hash-based upsert.

### Project Layout
- `src/01_master_list_acquisition` – scrapers for master list (FT/YF/SA).
- `src/02_daily_performance` – daily NAV/price/dividend scrapers.
- `src/03_master_detail_static` – static detail scrapers (info/fees/risk/policy).
- `src/04_holdings_acquisition` – holdings/allocations scrapers.
- `src/05_db_synchronization` – cleaners/validators/hashers/loaders + orchestrators and `main_pipeline.py`.
- `validation_output` – raw scrape outputs consumed by cleaners.
- `data/03_staging` & `data/04_hashed` – cleaned/hashed staging outputs before DB load.

### Row Hash Logic
- Each loader uses `row_hash` to skip identical rows: `upsert_method` updates only when `row_hash` differs (`IS DISTINCT FROM`).
- Hash is generated from all data columns per row in the hasher steps.

### Running the pipeline
1. Install dependencies: `pip install -r requirements.txt`
2. Ensure `.env` has DB credentials (see `.env.example`).
3. Run end-to-end from repo root:  
   `PYTHONPATH=. python src/05_db_synchronization/main_pipeline.py`

### Docker
- Build & run: `docker-compose up --build`
- Container entrypoint installs `cron_schedule` and starts cron (runs on schedule).

### Cron
- `cron_schedule` runs the full pipeline Mon–Fri 06:00 Asia/Bangkok (container TZ already set).

### Data flow (modules, flat file layout)
1. Master sync: scrapers → validator/remediator → loader.
2. Performance sync: scrapers → cleaners/validators → hashed → DB loaders. Staging files live flat under `data/03_staging` (e.g., `merged_daily_nav.csv`, `validated_daily_nav.csv`, `price_history/<source>/*.csv`, `dividend_history/<source>/*.csv`). Hashed outputs live under `data/04_hashed/price_history` and `data/04_hashed/dividend_history` without date folders.
3. Detail sync: reads `validation_output/*/03_Detail_Static/*fund_*.csv` → staging `data/03_static_details` → hashed `data/04_hashed/static_details` → loads `stg_fund_info/fees/risk/policy`.
4. Holdings sync: reads `validation_output/Financial_Times/04_Holdings/*` → staging `data/03_staging/holdings` → hashed `data/04_hashed/holdings` → loads `stg_fund_holdings` and `stg_allocations`.

### Export/Import schema
- Export schema (no data) to `database/schema_dump.sql`: `chmod +x database/export_schema.sh && ./database/export_schema.sh`
- Import on another machine: `psql -h <host> -U <user> -d <db> -f database/schema_dump.sql`
