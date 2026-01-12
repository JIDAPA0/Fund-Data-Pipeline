--
-- PostgreSQL database dump
--

\restrict vy6hYJtKyBty0OYNd03NUOHvA40bghZwtOgoKiUPcNmQhPuRqqZUepP8wErNWsg

-- Dumped from database version 14.20 (Homebrew)
-- Dumped by pg_dump version 14.20 (Homebrew)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: stg_allocations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stg_allocations (
    id integer NOT NULL,
    ticker character varying(20) NOT NULL,
    asset_type character varying(20) NOT NULL,
    source character varying(50) NOT NULL,
    allocation_type character varying(50) NOT NULL,
    item_name character varying(100) NOT NULL,
    value_net numeric(10,4),
    value_category_avg numeric(10,4),
    value_long numeric(10,4),
    value_short numeric(10,4),
    as_of_date date,
    row_hash character varying(64),
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: stg_allocations_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.stg_allocations_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stg_allocations_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.stg_allocations_id_seq OWNED BY public.stg_allocations.id;


--
-- Name: stg_daily_nav; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stg_daily_nav (
    id integer NOT NULL,
    ticker character varying(50) NOT NULL,
    asset_type character varying(50) NOT NULL,
    source character varying(50) NOT NULL,
    nav_price numeric(18,4),
    currency character varying(10),
    as_of_date date NOT NULL,
    scrape_date date,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: stg_daily_nav_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.stg_daily_nav_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stg_daily_nav_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.stg_daily_nav_id_seq OWNED BY public.stg_daily_nav.id;


--
-- Name: stg_dividend_history; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stg_dividend_history (
    id integer NOT NULL,
    ticker character varying(50),
    asset_type character varying(50),
    source character varying(50),
    ex_date date,
    payment_date date,
    amount numeric(18,6),
    currency character varying(10),
    type character varying(20) DEFAULT 'Cash'::character varying,
    row_hash character varying(255),
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: stg_dividend_history_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.stg_dividend_history_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stg_dividend_history_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.stg_dividend_history_id_seq OWNED BY public.stg_dividend_history.id;


--
-- Name: stg_fund_fees; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stg_fund_fees (
    ticker character varying(20) NOT NULL,
    asset_type character varying(20) NOT NULL,
    source character varying(50) NOT NULL,
    expense_ratio numeric(5,4),
    initial_charge numeric(5,4),
    exit_charge numeric(5,4),
    assets_aum numeric(20,2),
    top_10_hold_pct numeric(5,2),
    holdings_count integer,
    holdings_turnover numeric(5,2),
    row_hash character varying(64),
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    id integer NOT NULL
);


--
-- Name: stg_fund_fees_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.stg_fund_fees_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stg_fund_fees_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.stg_fund_fees_id_seq OWNED BY public.stg_fund_fees.id;


--
-- Name: stg_fund_holdings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stg_fund_holdings (
    id integer NOT NULL,
    ticker character varying(20) NOT NULL,
    asset_type character varying(20) NOT NULL,
    source character varying(50) NOT NULL,
    holding_ticker character varying(20),
    holding_name character varying(255) NOT NULL,
    holding_percentage numeric(10,4),
    shares_held numeric(20,2),
    market_value numeric(20,2),
    sector character varying(100),
    country character varying(100),
    as_of_date date,
    row_hash character varying(64),
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: stg_fund_holdings_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.stg_fund_holdings_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stg_fund_holdings_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.stg_fund_holdings_id_seq OWNED BY public.stg_fund_holdings.id;


--
-- Name: stg_fund_info; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stg_fund_info (
    ticker character varying(20) NOT NULL,
    asset_type character varying(20) NOT NULL,
    source character varying(50) NOT NULL,
    name character varying(255),
    isin_number character varying(20),
    cusip_number character varying(20),
    issuer text,
    category text,
    index_benchmark character varying(255),
    inception_date date,
    exchange text,
    region text,
    country text,
    leverage character varying(20),
    options character varying(20),
    shares_out numeric(20,2),
    market_cap_size character varying(50),
    investment_style character varying(50),
    row_hash character varying(64),
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    id integer NOT NULL
);


--
-- Name: stg_fund_info_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.stg_fund_info_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stg_fund_info_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.stg_fund_info_id_seq OWNED BY public.stg_fund_info.id;


--
-- Name: stg_fund_metrics; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stg_fund_metrics (
    id integer NOT NULL,
    ticker character varying(20) NOT NULL,
    asset_type character varying(20) NOT NULL,
    source character varying(50) NOT NULL,
    metric_type character varying(50) NOT NULL,
    metric_name character varying(255) NOT NULL,
    column_name character varying(100),
    value_raw text,
    value_num numeric(20,6),
    as_of_date date,
    row_hash character varying(64),
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: stg_fund_metrics_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.stg_fund_metrics_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stg_fund_metrics_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.stg_fund_metrics_id_seq OWNED BY public.stg_fund_metrics.id;


--
-- Name: stg_fund_policy; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stg_fund_policy (
    ticker character varying(20) NOT NULL,
    asset_type character varying(20) NOT NULL,
    source character varying(50) NOT NULL,
    dividend_yield numeric(5,2),
    dividend_growth_1y numeric(5,2),
    dividend_growth_3y numeric(5,2),
    dividend_growth_5y numeric(5,2),
    dividend_growth_10y numeric(5,2),
    dividend_consecutive_years integer,
    payout_ratio numeric(5,2),
    total_return_ytd numeric(5,2),
    total_return_1y numeric(5,2),
    pe_ratio numeric(5,2),
    row_hash character varying(64),
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    id integer NOT NULL
);


--
-- Name: stg_fund_policy_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.stg_fund_policy_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stg_fund_policy_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.stg_fund_policy_id_seq OWNED BY public.stg_fund_policy.id;


--
-- Name: stg_fund_risk; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stg_fund_risk (
    ticker character varying(20) NOT NULL,
    asset_type character varying(20) NOT NULL,
    source character varying(50) NOT NULL,
    sharpe_ratio_1y numeric(5,2),
    sharpe_ratio_3y numeric(5,2),
    sharpe_ratio_5y numeric(5,2),
    sharpe_ratio_10y numeric(5,2),
    beta_1y numeric(5,2),
    beta_3y numeric(5,2),
    beta_5y numeric(5,2),
    beta_10y numeric(5,2),
    alpha_1y numeric(5,2),
    alpha_3y numeric(5,2),
    alpha_5y numeric(5,2),
    alpha_10y numeric(5,2),
    standard_dev_1y numeric(5,2),
    standard_dev_3y numeric(5,2),
    standard_dev_5y numeric(5,2),
    standard_dev_10y numeric(5,2),
    r_squared_1y numeric(5,2),
    r_squared_3y numeric(5,2),
    r_squared_5y numeric(5,2),
    r_squared_10y numeric(5,2),
    rsi_daily numeric(5,2),
    moving_avg_200 numeric(10,2),
    morningstar_rating integer,
    lipper_total_return_3y integer,
    lipper_total_return_5y integer,
    lipper_total_return_10y integer,
    lipper_total_return_overall integer,
    lipper_consistent_return_3y integer,
    lipper_consistent_return_5y integer,
    lipper_consistent_return_10y integer,
    lipper_consistent_return_overall integer,
    lipper_preservation_3y integer,
    lipper_preservation_5y integer,
    lipper_preservation_10y integer,
    lipper_preservation_overall integer,
    lipper_expense_3y integer,
    lipper_expense_5y integer,
    lipper_expense_10y integer,
    lipper_expense_overall integer,
    row_hash character varying(64),
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    id integer NOT NULL
);


--
-- Name: stg_fund_risk_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.stg_fund_risk_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stg_fund_risk_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.stg_fund_risk_id_seq OWNED BY public.stg_fund_risk.id;


--
-- Name: stg_price_history; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stg_price_history (
    id integer NOT NULL,
    ticker character varying(50) NOT NULL,
    asset_type character varying(50) NOT NULL,
    source character varying(50) NOT NULL,
    date date NOT NULL,
    open numeric(18,4),
    high numeric(18,4),
    low numeric(18,4),
    close numeric(18,4),
    adj_close numeric(18,4),
    volume bigint,
    name text,
    status character varying(20) DEFAULT 'active'::character varying,
    row_hash character varying(255),
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: stg_price_history_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.stg_price_history_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stg_price_history_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.stg_price_history_id_seq OWNED BY public.stg_price_history.id;


--
-- Name: stg_security_master; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stg_security_master (
    id integer NOT NULL,
    ticker character varying(50) NOT NULL,
    asset_type character varying(50) NOT NULL,
    source character varying(50) NOT NULL,
    name text,
    status character varying(20) DEFAULT 'active'::character varying,
    row_hash character varying(255),
    first_seen date,
    last_seen date,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: stg_security_master_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.stg_security_master_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stg_security_master_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.stg_security_master_id_seq OWNED BY public.stg_security_master.id;


--
-- Name: stg_allocations id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_allocations ALTER COLUMN id SET DEFAULT nextval('public.stg_allocations_id_seq'::regclass);


--
-- Name: stg_daily_nav id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_daily_nav ALTER COLUMN id SET DEFAULT nextval('public.stg_daily_nav_id_seq'::regclass);


--
-- Name: stg_dividend_history id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_dividend_history ALTER COLUMN id SET DEFAULT nextval('public.stg_dividend_history_id_seq'::regclass);


--
-- Name: stg_fund_fees id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_fund_fees ALTER COLUMN id SET DEFAULT nextval('public.stg_fund_fees_id_seq'::regclass);


--
-- Name: stg_fund_holdings id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_fund_holdings ALTER COLUMN id SET DEFAULT nextval('public.stg_fund_holdings_id_seq'::regclass);


--
-- Name: stg_fund_info id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_fund_info ALTER COLUMN id SET DEFAULT nextval('public.stg_fund_info_id_seq'::regclass);


--
-- Name: stg_fund_metrics id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_fund_metrics ALTER COLUMN id SET DEFAULT nextval('public.stg_fund_metrics_id_seq'::regclass);


--
-- Name: stg_fund_policy id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_fund_policy ALTER COLUMN id SET DEFAULT nextval('public.stg_fund_policy_id_seq'::regclass);


--
-- Name: stg_fund_risk id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_fund_risk ALTER COLUMN id SET DEFAULT nextval('public.stg_fund_risk_id_seq'::regclass);


--
-- Name: stg_price_history id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_price_history ALTER COLUMN id SET DEFAULT nextval('public.stg_price_history_id_seq'::regclass);


--
-- Name: stg_security_master id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_security_master ALTER COLUMN id SET DEFAULT nextval('public.stg_security_master_id_seq'::regclass);


--
-- Name: stg_allocations stg_allocations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_allocations
    ADD CONSTRAINT stg_allocations_pkey PRIMARY KEY (id);


--
-- Name: stg_daily_nav stg_daily_nav_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_daily_nav
    ADD CONSTRAINT stg_daily_nav_pkey PRIMARY KEY (id);


--
-- Name: stg_dividend_history stg_dividend_history_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_dividend_history
    ADD CONSTRAINT stg_dividend_history_pkey PRIMARY KEY (id);


--
-- Name: stg_fund_fees stg_fund_fees_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_fund_fees
    ADD CONSTRAINT stg_fund_fees_pkey PRIMARY KEY (id);


--
-- Name: stg_fund_holdings stg_fund_holdings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_fund_holdings
    ADD CONSTRAINT stg_fund_holdings_pkey PRIMARY KEY (id);


--
-- Name: stg_fund_info stg_fund_info_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_fund_info
    ADD CONSTRAINT stg_fund_info_pkey PRIMARY KEY (id);


--
-- Name: stg_fund_metrics stg_fund_metrics_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_fund_metrics
    ADD CONSTRAINT stg_fund_metrics_pkey PRIMARY KEY (id);


--
-- Name: stg_fund_policy stg_fund_policy_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_fund_policy
    ADD CONSTRAINT stg_fund_policy_pkey PRIMARY KEY (id);


--
-- Name: stg_fund_risk stg_fund_risk_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_fund_risk
    ADD CONSTRAINT stg_fund_risk_pkey PRIMARY KEY (id);


--
-- Name: stg_price_history stg_price_history_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_price_history
    ADD CONSTRAINT stg_price_history_pkey PRIMARY KEY (id);


--
-- Name: stg_security_master stg_security_master_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_security_master
    ADD CONSTRAINT stg_security_master_pkey PRIMARY KEY (id);


--
-- Name: stg_allocations uq_stg_allocations_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_allocations
    ADD CONSTRAINT uq_stg_allocations_key UNIQUE (ticker, asset_type, source, allocation_type, item_name, as_of_date);


--
-- Name: stg_daily_nav uq_stg_daily_nav_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_daily_nav
    ADD CONSTRAINT uq_stg_daily_nav_key UNIQUE (ticker, asset_type, source, as_of_date);


--
-- Name: stg_dividend_history uq_stg_dividend_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_dividend_history
    ADD CONSTRAINT uq_stg_dividend_key UNIQUE (ticker, asset_type, source, ex_date, payment_date, amount, type);


--
-- Name: stg_fund_fees uq_stg_fund_fees_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_fund_fees
    ADD CONSTRAINT uq_stg_fund_fees_key UNIQUE (ticker, asset_type, source);


--
-- Name: stg_fund_info uq_stg_fund_info_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_fund_info
    ADD CONSTRAINT uq_stg_fund_info_key UNIQUE (ticker, asset_type, source);


--
-- Name: stg_fund_metrics uq_stg_fund_metrics_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_fund_metrics
    ADD CONSTRAINT uq_stg_fund_metrics_key UNIQUE (ticker, asset_type, source, metric_type, metric_name, column_name, as_of_date);


--
-- Name: stg_fund_policy uq_stg_fund_policy_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_fund_policy
    ADD CONSTRAINT uq_stg_fund_policy_key UNIQUE (ticker, asset_type, source);


--
-- Name: stg_fund_risk uq_stg_fund_risk_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_fund_risk
    ADD CONSTRAINT uq_stg_fund_risk_key UNIQUE (ticker, asset_type, source);


--
-- Name: stg_fund_holdings uq_stg_holdings_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_fund_holdings
    ADD CONSTRAINT uq_stg_holdings_key UNIQUE (ticker, asset_type, source, holding_name, as_of_date);


--
-- Name: stg_security_master uq_stg_master_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_security_master
    ADD CONSTRAINT uq_stg_master_key UNIQUE (ticker, asset_type, source);


--
-- Name: stg_price_history uq_stg_price_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_price_history
    ADD CONSTRAINT uq_stg_price_key UNIQUE (ticker, asset_type, source, date);


--
-- Name: idx_stg_alloc_ticker; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stg_alloc_ticker ON public.stg_allocations USING btree (ticker);


--
-- Name: idx_stg_alloc_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stg_alloc_type ON public.stg_allocations USING btree (allocation_type);


--
-- Name: idx_stg_div_ticker; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stg_div_ticker ON public.stg_dividend_history USING btree (ticker);


--
-- Name: idx_stg_ffees_ticker; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stg_ffees_ticker ON public.stg_fund_fees USING btree (ticker);


--
-- Name: idx_stg_finfo_ticker; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stg_finfo_ticker ON public.stg_fund_info USING btree (ticker);


--
-- Name: idx_stg_fpolicy_ticker; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stg_fpolicy_ticker ON public.stg_fund_policy USING btree (ticker);


--
-- Name: idx_stg_hold_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stg_hold_name ON public.stg_fund_holdings USING btree (holding_name);


--
-- Name: idx_stg_hold_ticker; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stg_hold_ticker ON public.stg_fund_holdings USING btree (ticker);


--
-- Name: idx_stg_master_ticker; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stg_master_ticker ON public.stg_security_master USING btree (ticker);


--
-- Name: idx_stg_metrics_ticker; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stg_metrics_ticker ON public.stg_fund_metrics USING btree (ticker);


--
-- Name: idx_stg_metrics_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stg_metrics_type ON public.stg_fund_metrics USING btree (metric_type);


--
-- Name: idx_stg_nav_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stg_nav_date ON public.stg_daily_nav USING btree (as_of_date);


--
-- Name: idx_stg_nav_ticker; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stg_nav_ticker ON public.stg_daily_nav USING btree (ticker);


--
-- Name: idx_stg_price_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stg_price_date ON public.stg_price_history USING btree (date);


--
-- Name: idx_stg_price_hash; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stg_price_hash ON public.stg_price_history USING btree (row_hash);


--
-- Name: idx_stg_price_ticker; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stg_price_ticker ON public.stg_price_history USING btree (ticker);


--
-- PostgreSQL database dump complete
--

\unrestrict vy6hYJtKyBty0OYNd03NUOHvA40bghZwtOgoKiUPcNmQhPuRqqZUepP8wErNWsg

