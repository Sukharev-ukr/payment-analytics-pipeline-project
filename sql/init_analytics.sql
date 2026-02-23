-- Analytics layer: transformed tables populated by PySpark batch jobs, consumed by Grafana.

CREATE SCHEMA IF NOT EXISTS analytics;

-- How much each merchant is owed after captures, refunds, chargebacks.
-- This is basically the core PSP calculation.
CREATE TABLE analytics.merchant_daily_settlement (
    settlement_date     DATE            NOT NULL,
    merchant_id         VARCHAR(50)     NOT NULL,
    merchant_name       VARCHAR(200),
    merchant_country    VARCHAR(5),
    total_authorized    DECIMAL(15,2)   DEFAULT 0,
    total_captured      DECIMAL(15,2)   DEFAULT 0,
    total_refunded      DECIMAL(15,2)   DEFAULT 0,
    total_chargebacks   DECIMAL(15,2)   DEFAULT 0,
    net_settlement_eur  DECIMAL(15,2)   DEFAULT 0,
    transaction_count   INTEGER         DEFAULT 0,
    chargeback_count    INTEGER         DEFAULT 0,
    chargeback_ratio    DECIMAL(5,4),
    currency            VARCHAR(3)      DEFAULT 'EUR',
    computed_at         TIMESTAMP       DEFAULT NOW(),
    PRIMARY KEY (settlement_date, merchant_id)
);

-- Auth success rates and volumes per payment method + issuer country.
-- Low auth rates = lost revenue, so this is a key metric for checkout optimization.
CREATE TABLE analytics.payment_method_performance (
    report_date             DATE            NOT NULL,
    payment_method          VARCHAR(50)     NOT NULL,
    issuer_country          VARCHAR(5)      NOT NULL,
    total_authorizations    INTEGER         DEFAULT 0,
    successful_authorizations INTEGER       DEFAULT 0,
    authorization_rate      DECIMAL(5,4),
    avg_amount_eur          DECIMAL(15,2),
    total_volume_eur        DECIMAL(15,2),
    is_3ds_rate             DECIMAL(5,4),
    computed_at             TIMESTAMP       DEFAULT NOW(),
    PRIMARY KEY (report_date, payment_method, issuer_country)
);

-- Merchants exceeding Visa/Mastercard chargeback thresholds.
-- Compliance-critical: card schemes fine PSPs for high chargeback ratios.
CREATE TABLE analytics.chargeback_alerts (
    alert_date          DATE            NOT NULL,
    merchant_id         VARCHAR(50)     NOT NULL,
    merchant_name       VARCHAR(200),
    chargeback_count    INTEGER,
    transaction_count   INTEGER,
    chargeback_ratio    DECIMAL(5,4),
    threshold_exceeded  VARCHAR(20),
    visa_threshold      DECIMAL(5,4)    DEFAULT 0.009,
    mc_threshold        DECIMAL(5,4)    DEFAULT 0.010,
    severity            VARCHAR(10),
    computed_at         TIMESTAMP       DEFAULT NOW(),
    PRIMARY KEY (alert_date, merchant_id)
);

-- Tracks txns through lifecycle stages (auth -> capture -> settlement).
-- Helps merchants see where they lose conversions.
CREATE TABLE analytics.conversion_funnel (
    report_date                 DATE            NOT NULL,
    merchant_id                 VARCHAR(50)     NOT NULL,
    authorized_count            INTEGER         DEFAULT 0,
    captured_count              INTEGER         DEFAULT 0,
    settled_count               INTEGER         DEFAULT 0,
    refunded_count              INTEGER         DEFAULT 0,
    auth_to_capture_rate        DECIMAL(5,4),
    capture_to_settle_rate      DECIMAL(5,4),
    refund_rate                 DECIMAL(5,4),
    avg_auth_to_capture_hours   DECIMAL(10,2),
    computed_at                 TIMESTAMP       DEFAULT NOW(),
    PRIMARY KEY (report_date, merchant_id)
);

-- Quality check results log
CREATE TABLE analytics.data_quality_log (
    check_id            SERIAL          PRIMARY KEY,
    check_timestamp     TIMESTAMP       DEFAULT NOW(),
    table_name          VARCHAR(100),
    check_name          VARCHAR(100),
    check_type          VARCHAR(50),
    status              VARCHAR(10),
    records_checked     INTEGER,
    records_failed      INTEGER,
    failure_percentage  DECIMAL(5,2),
    details             TEXT,
    severity            VARCHAR(10)
);
