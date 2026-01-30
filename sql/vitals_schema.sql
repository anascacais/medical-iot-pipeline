

CREATE TABLE IF NOT EXISTS `{{PROJECT_ID}}.{{DATASET}}.{{TABLE}}` (
    ts_smp TIMESTAMP,
    ts_ing TIMESTAMP NOT NULL,
    sensor_id STRING NOT NULL,
    modality STRING NOT NULL,
    value FLOAT64,
    flag_type_code INT64 NOT NULL
);