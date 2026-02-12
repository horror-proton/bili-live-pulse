use log::{debug, error, info, trace, warn};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

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

    registory: Arc<Mutex<HashMap<i32, () /*Weak<RoomKeyLease>*/>>>,
    drop_sender: tokio::sync::mpsc::UnboundedSender<i32>,
    _drop_handler: Option<tokio::task::JoinHandle<()>>,
    _renew_handler: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Debug, Clone)]
pub struct RoomKeyLease {
    pool: PgPool, // TODO: remove this

    id: i32,
    key: String,
    drop_sender: tokio::sync::mpsc::UnboundedSender<i32>,
}

impl RoomKeyCache {
    pub fn new(pool: PgPool, instance_id: &str) -> Self {
        let (drop_sender, mut drop_receiver) = tokio::sync::mpsc::unbounded_channel::<i32>();

        let registory = Arc::new(Mutex::new(HashMap::new()));

        let registry_clone = registory.clone();
        let pool_clone = pool.clone();

        let drop_handler = tokio::spawn(async move {
            while let Some(record_id) = drop_receiver.recv().await {
                let mut reg = registry_clone.lock().await;
                reg.remove(&record_id);

                let _ = query!(
                    "UPDATE room_key_cache SET lease_until = NULL WHERE id = $1",
                    record_id
                )
                .execute(&pool_clone)
                .await
                .map_err(|e| {
                    warn!("Failed to release lease for record {}: {:?}", record_id, e);
                });
            }
        });

        let registry_clone = registory.clone();
        let pool_clone = pool.clone();

        let renew_handler = tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;

                let reg = registry_clone.lock().await;
                let record_ids: Vec<i32> = reg.keys().cloned().collect();

                let _ = query!(
                    "UPDATE room_key_cache SET lease_until = NOW() + INTERVAL '30 seconds' WHERE id = ANY($1)",
                    &record_ids
                )
                .execute(&pool_clone)
                .await
                .map_err(|e| {
                    warn!("Failed to renew leases: {:?}", e);
                    panic!("Failed to renew leases: {:?}", e);
                });
            }
        });

        let factory = RoomKeyCache {
            pool,
            instance_id: instance_id.to_string(),
            registory,
            drop_sender,
            _drop_handler: Some(drop_handler),
            _renew_handler: Some(renew_handler),
        };

        factory
    }

    pub async fn try_get(&self, room_id: i32) -> sqlx::Result<Option<RoomKeyLease>> {
        let record = query!(
            r#"
            UPDATE room_key_cache SET instance_id = $2, lease_until = NOW() + INTERVAL '30 seconds'
            WHERE id = (
                SELECT id FROM room_key_cache
                WHERE room_id = $1
                AND (lease_until IS NULL OR lease_until < NOW())
                AND expired = FALSE
                LIMIT 1
            )
            RETURNING id, room_key
            "#,
            room_id,
            self.instance_id
        )
        .fetch_optional(&self.pool)
        .await?;

        let record = match record {
            Some(r) => r,
            None => return Ok(None),
        };

        self.registory.lock().await.insert(record.id, ());
        Ok(Some(RoomKeyLease {
            pool: self.pool.clone(),
            id: record.id,
            key: record.room_key,
            drop_sender: self.drop_sender.clone(),
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

        self.registory.lock().await.insert(record.id, ());
        Ok(RoomKeyLease {
            pool: self.pool.clone(),
            id: record.id,
            key: record.room_key,
            drop_sender: self.drop_sender.clone(),
        })
    }
}

impl RoomKeyLease {
    pub fn key(&self) -> &str {
        &self.key
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

impl Drop for RoomKeyLease {
    fn drop(&mut self) {
        self.drop_sender.send(self.id).unwrap_or_else(|e| {
            warn!("Failed to send drop signal for record {}: {:?}", self.id, e);
            // TODO:
        });
    }
}

mod tests {
    use super::*;

    #[sqlx::test]
    async fn test_room_key_cache(pool: PgPool) -> sqlx::Result<()> {
        query!(
            r#"
CREATE TABLE IF NOT EXISTS room_key_cache (
    id          SERIAL      PRIMARY KEY,
    room_id     INTEGER     NOT NULL,
    room_key    TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expired     BOOLEAN     NOT NULL DEFAULT FALSE,
    instance_id TEXT        NULL,
    lease_until TIMESTAMPTZ NULL
);
            "#
        )
        .execute(&pool)
        .await?;

        let cache = RoomKeyCache::new(pool, "instance_1");

        // Insert a new key
        let lease = cache.insert_and_get(1, "key1").await?;
        assert_eq!(lease.key(), "key1");

        // Try to get the same key (should fail because it's leased)
        let lease2 = cache.try_get(1).await?;
        assert!(lease2.is_none());

        // Drop the lease and try again
        drop(lease);
        tokio::time::sleep(std::time::Duration::from_secs(1)).await; // Wait for the drop handler

        let lease3 = cache.try_get(1).await?;
        assert!(lease3.is_some());
        assert_eq!(lease3.unwrap().key(), "key1");

        Ok(())
    }
}
