-- Model V4 schema (normalized fund tables)

CREATE TABLE IF NOT EXISTS public.fund_master (
    fund_id BIGSERIAL PRIMARY KEY,
    fund_code VARCHAR(50) UNIQUE,
    isin_code VARCHAR(20) UNIQUE,
    fund_name_th TEXT,
    fund_name_en TEXT,
    bloomberg_ticker VARCHAR(50),
    fund_type TEXT,
    domicile TEXT,
    asset_management_co TEXT,
    asset_class TEXT,
    geography_focus TEXT,
    strategy TEXT,
    base_currency VARCHAR(10),
    risk_level NUMERIC(5, 2),
    dividend_policy TEXT,
    inception_date DATE,
    total_expense_ratio NUMERIC(8, 4),
    management_fee NUMERIC(8, 4),
    factsheet_url TEXT,
    data_source VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS public.fund_risk (
    fund_id BIGINT PRIMARY KEY,
    std_dev NUMERIC(10, 4),
    sharpe NUMERIC(10, 4),
    max_drawdown NUMERIC(10, 4),
    tracking_error NUMERIC(10, 4),
    CONSTRAINT fk_fund_risk_fund
        FOREIGN KEY (fund_id) REFERENCES public.fund_master (fund_id)
);

CREATE TABLE IF NOT EXISTS public.fund_performance (
    fund_id BIGINT NOT NULL,
    nav_date DATE NOT NULL,
    nav_value NUMERIC(18, 6),
    dividend_value NUMERIC(18, 6),
    PRIMARY KEY (fund_id, nav_date),
    CONSTRAINT fk_fund_performance_fund
        FOREIGN KEY (fund_id) REFERENCES public.fund_master (fund_id)
);

CREATE TABLE IF NOT EXISTS public.security_master (
    security_id BIGSERIAL PRIMARY KEY,
    isin_code VARCHAR(20) UNIQUE,
    ticker VARCHAR(50),
    security_name TEXT,
    security_type TEXT,
    sector TEXT,
    industry TEXT,
    country_code VARCHAR(10),
    market_cap_class TEXT,
    exchange TEXT,
    currency VARCHAR(10),
    data_source VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS public.fund_holdings (
    holding_id BIGSERIAL PRIMARY KEY,
    parent_fund_id BIGINT NOT NULL,
    held_security_id BIGINT,
    held_fund_id BIGINT,
    report_date DATE,
    percentage NUMERIC(10, 4),
    CONSTRAINT fk_hold_parent_fund
        FOREIGN KEY (parent_fund_id) REFERENCES public.fund_master (fund_id),
    CONSTRAINT fk_hold_security
        FOREIGN KEY (held_security_id) REFERENCES public.security_master (security_id),
    CONSTRAINT fk_hold_fund
        FOREIGN KEY (held_fund_id) REFERENCES public.fund_master (fund_id)
);

CREATE TABLE IF NOT EXISTS public.fund_sector_breakdown (
    fund_id BIGINT NOT NULL,
    sector_name TEXT NOT NULL,
    percentage NUMERIC(10, 4),
    updated_date DATE,
    PRIMARY KEY (fund_id, sector_name),
    CONSTRAINT fk_sector_fund
        FOREIGN KEY (fund_id) REFERENCES public.fund_master (fund_id)
);

CREATE TABLE IF NOT EXISTS public.fund_asset_allocation (
    fund_id BIGINT NOT NULL,
    asset_type TEXT NOT NULL,
    percentage NUMERIC(10, 4),
    long_short TEXT,
    updated_date DATE,
    PRIMARY KEY (fund_id, asset_type),
    CONSTRAINT fk_asset_alloc_fund
        FOREIGN KEY (fund_id) REFERENCES public.fund_master (fund_id)
);

CREATE INDEX IF NOT EXISTS idx_fund_master_code ON public.fund_master (fund_code);
CREATE INDEX IF NOT EXISTS idx_fund_master_isin ON public.fund_master (isin_code);
CREATE INDEX IF NOT EXISTS idx_fund_performance_fund ON public.fund_performance (fund_id);
CREATE INDEX IF NOT EXISTS idx_fund_risk_fund ON public.fund_risk (fund_id);
CREATE INDEX IF NOT EXISTS idx_fund_holdings_parent ON public.fund_holdings (parent_fund_id);
CREATE INDEX IF NOT EXISTS idx_fund_holdings_security ON public.fund_holdings (held_security_id);
CREATE INDEX IF NOT EXISTS idx_security_master_ticker ON public.security_master (ticker);
