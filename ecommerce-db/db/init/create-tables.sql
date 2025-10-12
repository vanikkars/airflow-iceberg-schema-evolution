
CREATE TABLE IF NOT EXISTS audit_log_dml (
    audit_event_id  SERIAL PRIMARY KEY,
    audit_operation TEXT,
    audit_timestamp TIMESTAMP WITH TIME ZONE,
    tbl_schema      TEXT,
    tbl_name        TEXT,
    raw_data        TEXT
);

CREATE INDEX IF NOT EXISTS idx_audit_log_dml_event_id ON audit_log_dml(audit_event_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_dml_audit_timestamp ON audit_log_dml(audit_timestamp);

