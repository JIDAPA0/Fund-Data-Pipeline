## Fund ETL Pipeline

Fund data pipeline for master list, daily performance, static detail, and holdings. Data is synchronized to **MySQL** with hash-based upsert.

### Project Layout
- `src/01_master_list_acquisition`: scrapers for master list (FT/YF/SA)
- `src/02_daily_performance`: NAV/price/dividend scrapers
- `src/03_master_detail_static`: static detail scrapers (info/fees/risk/policy)
- `src/04_holdings_acquisition`: holdings/allocations scrapers
- `src/05_db_synchronization`: cleaners/validators/hashers/loaders + orchestrators and `main_pipeline.py`

### Row Hash Logic
- Loaders upsert with unique keys.
- `row_hash` is used to detect meaningful row changes and keep synchronization idempotent.

### Environment
1. Copy env file: `cp .env.example .env`
2. Configure DB and credentials in `.env`
3. For Docker Compose usage, keep `DB_HOST=db`

### Run with Python
- Install dependencies: `pip install -r requirements.txt`
- Run all modules: `PYTHONPATH=. python src/05_db_synchronization/main_pipeline.py`
- Run one module:
  - `PYTHONPATH=. python src/05_db_synchronization/main_pipeline.py --module master`
  - `PYTHONPATH=. python src/05_db_synchronization/main_pipeline.py --module performance`
  - `PYTHONPATH=. python src/05_db_synchronization/main_pipeline.py --module detail`
  - `PYTHONPATH=. python src/05_db_synchronization/main_pipeline.py --module holdings`

### Prefect (แทน Cron)
ใช้ Prefect schedule แทน `cron_schedule` เดิม

1. Start Prefect server (local):
   - `prefect server start`
2. Register and serve scheduled run (Mon-Fri 06:00 Asia/Bangkok):
   - `PYTHONPATH=. python src/05_db_synchronization/main_pipeline.py --serve --cron "0 6 * * 1-5" --timezone "Asia/Bangkok"`
3. Serve specific module schedule:
   - `PYTHONPATH=. python src/05_db_synchronization/main_pipeline.py --module master --serve --cron "0 6 * * 1-5" --timezone "Asia/Bangkok"`

### Docker
- Build and run: `docker-compose up --build`
- Services:
  - `db`: MySQL 8.4
  - `scraper`: ETL app container

### Notes
- Legacy cron file (`cron_schedule`) is no longer the recommended scheduler.
- If migrating from PostgreSQL, migrate schema/data before first MySQL run.
