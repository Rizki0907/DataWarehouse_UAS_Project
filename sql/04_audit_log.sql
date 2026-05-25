-- SECTION 5: AUDIT LOGGING
-- This table tracks the execution performance of our Airflow DAG.

CREATE TABLE IF NOT EXISTS fact_etl_audit (
    audit_key             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id                VARCHAR(255) NOT NULL,
    execution_date_utc    TIMESTAMPTZ NOT NULL,
    status                VARCHAR(50) NOT NULL,
    duration_seconds      NUMERIC(10,2),
    rows_extracted_ohlcv  INTEGER,
    rows_extracted_fng    INTEGER,
    rows_transformed      INTEGER,
    error_message         TEXT,
    created_at            TIMESTAMPTZ DEFAULT NOW()
);

-- Buat index pada run_id dan status untuk mempermudah pencarian log
CREATE INDEX IF NOT EXISTS idx_fact_etl_audit_run_id ON fact_etl_audit(run_id);
CREATE INDEX IF NOT EXISTS idx_fact_etl_audit_status ON fact_etl_audit(status);
