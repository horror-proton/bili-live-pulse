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
    instance_id: String,
}

impl RoomKeyCache {
    pub fn new(pool: PgPool, instance_id: &str) -> Self {
        RoomKeyCache {
            pool,
            instance_id: instance_id.to_string(),
        }
    }

    pub async fn try_get(&self, room_id: i32) -> sqlx::Result<Option<RoomKeyLease>> {
        let record = query!(
            r#"
            UPDATE room_key_cache SET instance_id = $2, lease_until = NOW() + INTERVAL '30 seconds'
            WHERE id = (
                SELECT id FROM room_key_cache
                WHERE room_id = $1 AND (lease_until IS NULL OR lease_until < NOW()) AND expired = FALSE
                LIMIT 1
            )
            RETURNING id, room_key
            "#,
            room_id,
            self.instance_id
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(record.map(|r| RoomKeyLease {
            id: r.id,
            key: r.room_key,
            pool: self.pool.clone(),
        }))
    }

    pub async fn insert_and_get(&self, room_id: i32, room_key: &str) -> sqlx::Result<RoomKeyLease> {
        let record = query!(
            r#"
            INSERT INTO room_key_cache (room_id, room_key, instance_id, lease_until)
            VALUES ($1, $2, $3, NOW() + INTERVAL '30 seconds')
            RETURNING id, room_key
            "#,
            room_id,
            room_key,
            self.instance_id
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(RoomKeyLease {
            id: record.id,
            key: record.room_key,
            pool: self.pool.clone(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct RoomKeyLease {
    id: i32,
    key: String,

    pool: PgPool,
}

impl RoomKeyLease {
    pub fn key(&self) -> &str {
        &self.key
    }

    pub async fn renew(&self) -> sqlx::Result<bool> {
        query!(
            "UPDATE room_key_cache SET lease_until = NOW() + INTERVAL '30 seconds' WHERE id = $1",
            self.id
        )
        .execute(&self.pool)
        .await
        .map(|r| r.rows_affected() > 0)
    }

    pub async fn release(&self) -> sqlx::Result<bool> {
        query!(
            "UPDATE room_key_cache SET lease_until = NULL WHERE id = $1",
            self.id
        )
        .execute(&self.pool)
        .await
        .map(|r| r.rows_affected() > 0)
    }

    pub async fn invalidate(&self) -> sqlx::Result<()> {
        query!(
            "UPDATE room_key_cache SET expired = TRUE WHERE id = $1",
            self.id
        )
        .execute(&self.pool)
        .await
        .map(|_| ())
    }
}
