CREATE TABLE IF NOT EXISTS scored_transactions (
    txn_id          TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL,
    amount          NUMERIC(12,2),
    merchant        TEXT,
    city            TEXT,
    device_id       TEXT,
    event_ts        TIMESTAMPTZ,
    processed_ts    TIMESTAMPTZ,
    latency_ms      INT,
    risk_score      INT,
    decision        TEXT,          -- ALLOW / REVIEW / BLOCK
    reasons         JSONB,         -- explainable reason codes
    true_label      BOOLEAN,       -- simulator ground truth (only for evaluation)
    analyst_verdict TEXT           -- reserved for the feedback-loop feature
);

CREATE INDEX IF NOT EXISTS idx_scored_decision ON scored_transactions (decision);
CREATE INDEX IF NOT EXISTS idx_scored_event_ts ON scored_transactions (event_ts);

CREATE OR REPLACE VIEW alerts AS
SELECT * FROM scored_transactions WHERE decision <> 'ALLOW';
