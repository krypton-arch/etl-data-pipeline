-- Normalized 3NF PostgreSQL Warehouse Schema for Multi-Source ETL Pipeline

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id VARCHAR(32) PRIMARY KEY,
    full_name VARCHAR(150) NOT NULL,
    email_masked VARCHAR(255) NOT NULL,
    email_hash CHAR(64) NOT NULL,
    phone_masked VARCHAR(32) NOT NULL,
    phone_hash CHAR(64) NOT NULL,
    city VARCHAR(100) NOT NULL DEFAULT 'Unknown',
    signup_date TIMESTAMPTZ NOT NULL,
    kyc_status VARCHAR(32) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fact_orders (
    order_id VARCHAR(32) PRIMARY KEY,
    customer_id VARCHAR(32) NOT NULL REFERENCES dim_customers(customer_id)
        ON UPDATE CASCADE ON DELETE RESTRICT,
    order_timestamp TIMESTAMPTZ NOT NULL,
    amount NUMERIC(12, 2) NOT NULL CHECK (amount > 0),
    currency VARCHAR(10) NOT NULL,
    payment_method VARCHAR(50) NOT NULL,
    shipping_country VARCHAR(64) NOT NULL,
    shipping_city VARCHAR(100) NOT NULL,
    shipping_postal_code VARCHAR(20) NOT NULL,
    item_count INTEGER NOT NULL DEFAULT 0 CHECK (item_count >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fact_order_items (
    order_item_id VARCHAR(64) PRIMARY KEY,
    order_id VARCHAR(32) NOT NULL REFERENCES fact_orders(order_id)
        ON UPDATE CASCADE ON DELETE CASCADE,
    line_number INTEGER NOT NULL CHECK (line_number > 0),
    sku VARCHAR(64) NOT NULL,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(100) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    line_total NUMERIC(12, 2) NOT NULL CHECK (line_total >= 0),
    CONSTRAINT uq_fact_order_items_order_line UNIQUE (order_id, line_number)
);

CREATE TABLE IF NOT EXISTS etl_batch_runs (
    batch_id VARCHAR(64) PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    duration_seconds NUMERIC(10, 3) NOT NULL,
    status VARCHAR(32) NOT NULL,
    total_records_read INTEGER NOT NULL,
    valid_records INTEGER NOT NULL,
    quarantined_records INTEGER NOT NULL,
    duplicates_removed INTEGER NOT NULL,
    postgres_rows_loaded INTEGER NOT NULL,
    mongo_docs_upserted INTEGER NOT NULL,
    dq_report_path TEXT
);

-- Analytical and Lookup Indexes
CREATE INDEX IF NOT EXISTS idx_dim_customers_email_hash ON dim_customers(email_hash);
CREATE INDEX IF NOT EXISTS idx_dim_customers_phone_hash ON dim_customers(phone_hash);
CREATE INDEX IF NOT EXISTS idx_dim_customers_kyc_status ON dim_customers(kyc_status);
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer_id ON fact_orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_timestamp ON fact_orders(order_timestamp);
CREATE INDEX IF NOT EXISTS idx_fact_orders_currency ON fact_orders(currency);
CREATE INDEX IF NOT EXISTS idx_fact_order_items_order_id ON fact_order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_fact_order_items_sku ON fact_order_items(sku);
CREATE INDEX IF NOT EXISTS idx_etl_batch_runs_started_at ON etl_batch_runs(started_at DESC);
