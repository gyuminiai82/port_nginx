CREATE TABLE IF NOT EXISTS sensor_data (
    time TIMESTAMPTZ NOT NULL,
    payload JSONB
);

-- TimescaleDB 하이퍼테이블 생성 (1일 단위 파티셔닝)
SELECT create_hypertable('sensor_data', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
