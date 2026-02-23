-- Raw layer: payment events as ingested from Kafka, minimal transformation applied.

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE raw.payment_events (
    event_id                VARCHAR(50)     PRIMARY KEY,
    psp_reference           VARCHAR(50)     NOT NULL,
    merchant_id             VARCHAR(50)     NOT NULL,
    merchant_name           VARCHAR(200),
    merchant_country        VARCHAR(5),
    merchant_category_code  VARCHAR(10),
    event_type              VARCHAR(30)     NOT NULL,
    event_timestamp         TIMESTAMP       NOT NULL,
    amount                  DECIMAL(15,2)   NOT NULL,
    currency                VARCHAR(3)      NOT NULL,
    original_amount         DECIMAL(15,2),
    original_currency       VARCHAR(3),
    payment_method          VARCHAR(50),
    payment_method_variant  VARCHAR(50),
    card_bin                VARCHAR(6),
    card_last_four          VARCHAR(4),
    issuer_country          VARCHAR(5),
    shopper_interaction     VARCHAR(30),
    shopper_reference       VARCHAR(100),
    acquirer_response_code  VARCHAR(10),
    is_3ds_authenticated    BOOLEAN,
    risk_score              INTEGER,
    amount_eur              DECIMAL(15,2),
    ingested_at             TIMESTAMP       DEFAULT NOW()
);

-- indexes for the most common query patterns in downstream transforms
CREATE INDEX idx_events_psp_ref     ON raw.payment_events(psp_reference);
CREATE INDEX idx_events_merchant    ON raw.payment_events(merchant_id);
CREATE INDEX idx_events_timestamp   ON raw.payment_events(event_timestamp);
CREATE INDEX idx_events_type        ON raw.payment_events(event_type);

-- events that failed validation, kept for debugging
CREATE TABLE raw.rejected_events (
    rejection_id            SERIAL          PRIMARY KEY,
    event_id                VARCHAR(50),
    raw_payload             TEXT            NOT NULL,
    rejection_reason        VARCHAR(500)    NOT NULL,
    rejected_at             TIMESTAMP       DEFAULT NOW()
);
