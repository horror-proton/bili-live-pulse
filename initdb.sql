\conninfo
\set ECHO all
\set ON_ERROR_STOP on
\timing
-- DROP TABLE IF EXISTS danmaku;
CREATE EXTENSION IF NOT EXISTS timescaledb;


-- room_key_cache
CREATE TABLE IF NOT EXISTS room_key_cache (
    room_id     INTEGER     UNIQUE NOT NULL,
    room_key    TEXT        NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_room_key_cache_room_id ON room_key_cache(room_id);
CREATE INDEX IF NOT EXISTS idx_room_key_cache_updated_at ON room_key_cache(updated_at);

--

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
CREATE UNIQUE   INDEX IF NOT EXISTS idx_danmaku_id_str_time ON danmaku(id_str, time);
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

CREATE INDEX IF NOT EXISTS idx_live_status_room_id ON live_status(room_id);

-- like_info
CREATE TABLE IF NOT EXISTS like_info (
    "time"      TIMESTAMPTZ  NOT NULL,
    room_id     INTEGER      NOT NULL,
    click_count INTEGER      NOT NULL
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'time',
    tsdb.segmentby = 'room_id',
    tsdb.orderby = 'time DESC'
);

CREATE INDEX IF NOT EXISTS idx_like_info_room_id ON like_info(room_id);

--
CREATE TABLE IF NOT EXISTS room_info (
    "time"              TIMESTAMPTZ     NOT NULL,
    room_id             INTEGER         NOT NULL,
    area_id             INTEGER         NOT NULL,
    area_name           TEXT            NOT NULL,
    parent_area_id      INTEGER         NOT NULL,
    parent_area_name    TEXT            NOT NULL,
    title               TEXT            NOT NULL,
    live_id_str         TEXT,
    live_time           TIMESTAMPTZ
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'time',
    tsdb.segmentby = 'room_id',
    tsdb.orderby = 'time DESC'
);

CREATE INDEX IF NOT EXISTS idx_room_info_room_id ON room_info(room_id);

--

CREATE TABLE IF NOT EXISTS guard (
    "time"      TIMESTAMPTZ NOT NULL,
    room_id     INTEGER     NOT NULL,
    guard_level SMALLINT    NOT NULL,
    num         INTEGER     NOT NULL,
    "uid"       BIGINT      NOT NULL,
    "username" TEXT        NOT NULL
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'time',
    tsdb.segmentby = 'room_id',
    tsdb.orderby = 'time DESC'
);

CREATE INDEX IF NOT EXISTS idx_guard_room_id ON guard(room_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_guard_time_uid ON guard(time, "uid");

--

CREATE TABLE IF NOT EXISTS live_meta (
    "live_id_str"   TEXT        UNIQUE NOT NULL,
    "room_id"       INTEGER     NOT NULL,
    "live_time"     TIMESTAMPTZ NULL,
    "live_platform" TEXT        NULL,
    "end_time_est" TIMESTAMPTZ NULL,
    "end_time_before" TIMESTAMPTZ NULL
);

COMMENT ON COLUMN live_meta.end_time_before IS 'The first time we observed the stream ended.';

CREATE INDEX IF NOT EXISTS idx_live_meta_room_id ON live_meta(room_id);

--

CREATE TABLE IF NOT EXISTS real_time_message (
    "time"      TIMESTAMPTZ NOT NULL,
    room_id     INTEGER     NOT NULL,
    fans        INTEGER     NOT NULL,
    fans_club   INTEGER     NOT NULL
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'time',
    tsdb.segmentby = 'room_id',
    tsdb.orderby = 'time DESC'
);

CREATE INDEX IF NOT EXISTS idx_real_time_message_room_id ON real_time_message(room_id);

--
SELECT * FROM timescaledb_information.hypertable_columnstore_settings;
