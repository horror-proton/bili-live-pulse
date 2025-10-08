\conninfo
\c test
DROP TABLE IF EXISTS danmaku;
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE TABLE danmaku (
    "time"      TIMESTAMPTZ  NOT NULL,
    id_str      TEXT         NOT NULL,
    roomid      INTEGER      NOT NULL,
    "text"      TEXT
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'time',
    tsdb.segmentby = 'roomid',
    tsdb.orderby = 'time DESC'
);

-- using first 4 chars is enough to prevent collisions
CREATE UNIQUE INDEX idx_id_str_time ON danmaku(LEFT(id_str, 4), time);
CREATE INDEX idx_roomid ON danmaku(roomid);
CREATE INDEX idx_text ON danmaku(LEFT(text, 1)); 
