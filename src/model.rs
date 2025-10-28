use anyhow::Context;
use anyhow::Result;
use sqlx::postgres::PgQueryResult;
use sqlx::query::Query;
use sqlx::{Execute, PgPool, query};

type PgQuery<'q> = Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>;

pub trait FromMsg {
    fn from_msg(room_id: u32, m: &serde_json::Value) -> Result<Self>
    where
        Self: Sized;
}

pub trait Insertable {
    const LOCK_TABLE: bool = false;
    const TABLE_NAME: &'static str = "";
    fn get_room_id(&self) -> u32 {
        0
    }
    fn build_query(&self) -> PgQuery<'_>;
}

pub struct OnlineCount {
    room_id: i32,
    count: i32,
}

impl FromMsg for OnlineCount {
    fn from_msg(room_id: u32, m: &serde_json::Value) -> Result<Self> {
        let room_id = room_id as i32;
        let count = m
            .get("data")
            .context("Missing data field")?
            .get("count")
            .context("Missing count field")?
            .as_i64()
            .context("not i64")? as i32;
        Ok(OnlineCount { room_id, count })
    }
}

impl Insertable for OnlineCount {
    fn build_query(&self) -> PgQuery<'_> {
        query!(
            r#"
            INSERT INTO online_rank_count (time, room_id, count)
            SELECT NOW(), $1, $2
            WHERE
                pg_try_advisory_xact_lock(hashtext('online_rank_count'), $1)
            AND NOT COALESCE((
                SELECT count = $2
                FROM online_rank_count
                WHERE room_id = $1
                AND time > NOW() - INTERVAL '5 minutes'
                ORDER BY time DESC
                LIMIT 1
            ), FALSE)
            "#,
            self.room_id,
            self.count
        )
    }
}

pub struct LikeInfo {
    room_id: i32,
    click_count: i32,
}

impl FromMsg for LikeInfo {
    fn from_msg(room_id: u32, m: &serde_json::Value) -> Result<Self>
    where
        Self: Sized,
    {
        let room_id = room_id as i32;
        let click_count = m
            .get("data")
            .context("Missing data field")?
            .get("click_count")
            .context("Missing click_count field")?
            .as_i64()
            .context("not i64")? as i32;

        Ok(LikeInfo {
            room_id,
            click_count,
        })
    }
}

impl Insertable for LikeInfo {
    fn build_query(&self) -> PgQuery<'_> {
        query!(
            r#"
            INSERT INTO like_info (time, room_id, click_count)
            SELECT NOW(), $1, $2
            WHERE
                pg_try_advisory_xact_lock(hashtext('like_info'), $1)
            AND NOT COALESCE((
                SELECT click_count = $2
                FROM like_info
                WHERE room_id = $1
                AND time > NOW() - INTERVAL '5 minutes'
                ORDER BY time DESC
                LIMIT 1
            ), FALSE)
            "#,
            self.room_id,
            self.click_count
        )
    }
}

pub struct Watched {
    room_id: i32,
    num: i32,
}

impl FromMsg for Watched {
    fn from_msg(room_id: u32, m: &serde_json::Value) -> Result<Self>
    where
        Self: Sized,
    {
        let room_id = room_id as i32;
        let num = m
            .get("data")
            .context("Missing data field")?
            .get("num")
            .context("Missing num field")?
            .as_i64()
            .context("not i64")? as i32;

        Ok(Watched { room_id, num })
    }
}

impl Insertable for Watched {
    fn build_query(&self) -> PgQuery<'_> {
        query!(
            r#"
            INSERT INTO watched (time, room_id, num)
            SELECT NOW(), $1, $2
            WHERE
                pg_try_advisory_xact_lock(hashtext('watched'), $1)
            AND NOT COALESCE((
                SELECT num = $2
                FROM watched
                WHERE room_id = $1
                AND time > NOW() - INTERVAL '5 minutes'
                ORDER BY time DESC
                LIMIT 1
            ), FALSE)
            "#,
            self.room_id,
            self.num
        )
    }
}

async fn begin_roomid_lock(
    pool: &PgPool,
    table: &str,
    room_id: u32,
) -> std::result::Result<sqlx::Transaction<'static, sqlx::Postgres>, sqlx::Error> {
    let lock_stmt = query!(
        "SELECT pg_advisory_xact_lock(hashtext($1), $2)",
        table,
        room_id as i64
    );
    pool.begin_with(lock_stmt.sql()).await
}

pub async fn insert_struct<T>(pool: &PgPool, data: &T) -> sqlx::Result<PgQueryResult>
where
    T: Insertable,
{
    if T::LOCK_TABLE {
        let mut tx = begin_roomid_lock(pool, T::TABLE_NAME, data.get_room_id()).await?;
        let res = data.build_query().execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(res)
    } else {
        let q = data.build_query();
        q.execute(pool).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::{PgPool, PgTransaction};
    use tokio::sync::OnceCell;

    static DB: OnceCell<PgPool> = OnceCell::const_new();

    async fn get_db_pool() -> &'static PgPool {
        DB.get_or_init(|| async {
            let database_url = std::env::var("DATABASE_URL")
                .unwrap_or("postgres://user:password@localhost/testdb".to_string());
            PgPool::connect(&database_url)
                .await
                .expect("Failed to connect to database")
        })
        .await
    }

    async fn get_tx() -> sqlx::Transaction<'static, sqlx::Postgres> {
        let pool = get_db_pool().await;
        pool.begin().await.expect("Failed to begin transaction")
    }

    fn remove_lock(sql: &str) -> String {
        let re = regex::Regex::new(r"pg_try_advisory_xact_lock\(hashtext\(.*?\), .*?\)").unwrap();
        re.replace_all(sql, "TRUE").to_string()
    }

    async fn test_insertable<T>(data: &T) -> Result<()>
    where
        T: Insertable,
    {
        {
            let mut tx = get_tx().await;

            let res = data.build_query().execute(&mut *tx).await?;
            assert_eq!(res.rows_affected(), 1);

            // blocked by lock
            let res = data.build_query().execute(&mut *tx).await?;
            assert_eq!(res.rows_affected(), 0);

            tx.rollback().await?;
        }

        {
            let mut tx = get_tx().await;
            let mut q = data.build_query();

            let args = q.take_arguments().unwrap().unwrap();
            let new_sql = remove_lock(q.sql());
            let res = sqlx::query_with(&new_sql, args).execute(&mut *tx).await?;
            assert_eq!(res.rows_affected(), 1);

            // blocked by recent duplicate
            let res = data.build_query().execute(&mut *tx).await?;
            assert_eq!(res.rows_affected(), 0);

            tx.rollback().await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_insert_online_count() -> Result<()> {
        let msg = serde_json::json!(
          {"cmd":"ONLINE_RANK_COUNT","data":{"count":1254,"count_text":"1254","online_count":1254,"online_count_text":"1254"}}
        );
        let data = OnlineCount::from_msg(12345, &msg)?;

        test_insertable(&data).await
    }

    #[tokio::test]
    async fn test_insert_like_info() -> Result<()> {
        let msg = serde_json::json!(
            {"cmd":"LIKE_INFO_V3_UPDATE","data":{"click_count":5661}}
        );
        let data = LikeInfo::from_msg(12345, &msg)?;
        test_insertable(&data).await
    }
}
