-- The PRIMARY KEY on transaction_id is what makes the audit consumer idempotent.
CREATE TABLE IF NOT EXISTS audit_log (
    transaction_id VARCHAR(64)  PRIMARY KEY,
    card_id        VARCHAR(64)  NOT NULL,
    merchant       VARCHAR(128),
    country        VARCHAR(8),
    amount_cents   BIGINT       NOT NULL,
    currency       VARCHAR(8)   NOT NULL,
    event_ts       BIGINT       NOT NULL
);
