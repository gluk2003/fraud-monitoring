CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
CREATE TABLE scores (
    transaction_id TEXT PRIMARY KEY,
    transaction_time TIMESTAMPTZ NOT NULL,
    score DOUBLE PRECISION NOT NULL CHECK (score BETWEEN 0 AND 1),
    fraud_flag BOOLEAN NOT NULL,
    model_version TEXT NOT NULL,
    us_state TEXT NOT NULL,
    merch TEXT NOT NULL,
    cat_id TEXT NOT NULL,
    amount DOUBLE PRECISION NOT NULL CHECK (amount >= 0),
    scored_at TIMESTAMPTZ NOT NULL,
    written_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX scores_scored_at ON scores (scored_at DESC);
CREATE INDEX scores_state_merch_time ON scores (us_state, merch, scored_at DESC);
CREATE USER grafana_reader WITH PASSWORD 'grafana_reader';
GRANT CONNECT ON DATABASE fraud TO grafana_reader;
GRANT USAGE ON SCHEMA public TO grafana_reader;
GRANT SELECT ON scores TO grafana_reader;
