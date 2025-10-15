\conninfo
\set ECHO all
\set ON_ERROR_STOP on
\timing
-- DROP TABLE IF EXISTS danmaku;
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE TABLE IF NOT EXISTS danmaku (
    "time"      TIMESTAMPTZ  NOT NULL,
    id_str      TEXT         NOT NULL,
    room_id      INTEGER      NOT NULL,
    "text"      TEXT
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'time',
    tsdb.segmentby = 'room_id',
    tsdb.orderby = 'time DESC'
);

-- using first 4 chars is enough to prevent collisions
CREATE UNIQUE   INDEX IF NOT EXISTS idx_danmaku_id_str_time ON danmaku(LEFT(id_str, 4), time);
CREATE          INDEX IF NOT EXISTS idx_danmaku_room_id ON danmaku(room_id);
CREATE          INDEX IF NOT EXISTS idx_danmaku_text ON danmaku(LEFT(text, 1)); 

-- online_rank_count
CREATE TABLE IF NOT EXISTS online_rank_count (
    "time"      TIMESTAMPTZ  NOT NULL,
    room_id     INTEGER      NOT NULL,
    "count"     INTEGER      NOT NULL
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'time',
    tsdb.segmentby = 'room_id',
    tsdb.orderby = 'time DESC'
);

CREATE INDEX IF NOT EXISTS idx_online_rank_count_room_id ON online_rank_count(room_id);

-- watched
CREATE TABLE IF NOT EXISTS watched (
    "time"      TIMESTAMPTZ  NOT NULL,
    room_id     INTEGER      NOT NULL,
    num         INTEGER      NOT NULL
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'time',
    tsdb.segmentby = 'room_id',
    tsdb.orderby = 'time DESC'
);

CREATE INDEX IF NOT EXISTS idx_watched_room_id ON watched(room_id);

-- live_status
CREATE TABLE IF NOT EXISTS live_status (
    "time"      TIMESTAMPTZ  NOT NULL,
    room_id     INTEGER      NOT NULL,
    status      SMALLINT      NOT NULL
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'time',
    tsdb.segmentby = 'room_id',
    tsdb.orderby = 'time DESC'
);

CREATE INDEX IF NOT EXISTS idx_live_status_room_id ON watched(room_id);

--
SELECT * FROM timescaledb_information.hypertable_columnstore_settings;
