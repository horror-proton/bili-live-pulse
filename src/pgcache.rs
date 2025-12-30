use sqlx::PgPool;
use sqlx::query;

/*
// TODO: generic cache implementation
struct PgCache {
    pool: PgPool,
    table_name: String,
}

impl PgCache {
    pub async fn new(pool: PgPool, table_name: &str) -> Self {
        PgCache {
            pool,
            table_name: table_name.to_string(),
        }
    }
}
*/

pub struct RoomKeyCache {
    pool: PgPool,
}

impl RoomKeyCache {
    pub fn new(pool: PgPool) -> Self {
        RoomKeyCache { pool }
    }

    // get latest room_key by room_id if any
    pub async fn get(&self, room_id: i32) -> sqlx::Result<Option<String>> {
        query!(
            r#"
            SELECT room_key FROM room_key_cache WHERE room_id = $1 ORDER BY updated_at DESC LIMIT 1
            "#,
            room_id
        )
        .fetch_optional(&self.pool)
        .await
        .map(|row| row.map(|r| r.room_key))
    }

    pub async fn try_store(&self, room_id: i32, room_key: &str) -> sqlx::Result<bool> {
        query!(
            r#"
            INSERT INTO room_key_cache (room_id, room_key, updated_at) VALUES ($1, $2, NOW())
            ON CONFLICT (room_id) DO
            UPDATE SET room_key = EXCLUDED.room_key, updated_at = NOW() WHERE room_key_cache.updated_at < NOW()
            "#,
            room_id,
            room_key,
        )
        .execute(&self.pool)
        .await
        .map(|r| r.rows_affected() > 0)
    }

    pub async fn invalidate(&self, room_id: i32, room_key: &str) -> sqlx::Result<bool> {
        query!(
            r#"
            DELETE FROM room_key_cache WHERE room_id = $1 AND room_key = $2
            "#,
            room_id,
            room_key,
        )
        .execute(&self.pool)
        .await
        .map(|r| r.rows_affected() > 0)
    }
}
