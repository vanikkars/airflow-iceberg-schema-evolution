CREATE TABLE IF NOT EXISTS audit_log_dml (
    event_id        SERIAL PRIMARY KEY,
    audit_operation TEXT,
    audit_timestamp TIMESTAMPTZ,
    tbl_schema      TEXT,
    tbl_name        TEXT,
    raw_data        TEXT
);