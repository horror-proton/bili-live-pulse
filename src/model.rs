use anyhow::Context;
use anyhow::Result;
use serde::Deserialize;
use sqlx::PgTransaction;
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

#[derive(Deserialize)]
pub struct RoomInfo {
    #[serde(skip)]
    room_id: i32,

    area_id: i32,
    area_name: String,
    parent_area_id: i32,
    parent_area_name: String,
    pub live_status: Option<i16>,
    title: String,
}

impl FromMsg for RoomInfo {
    fn from_msg(room_id: u32, m: &serde_json::Value) -> Result<Self>
    where
        Self: Sized,
    {
        let room_id = room_id as i32;
        let data = m.get("data").context("Missing data field")?;
        let result = RoomInfo::deserialize(data)?;
        Ok(RoomInfo { room_id, ..result })
    }
}

impl Insertable for RoomInfo {
    fn build_query(&self) -> PgQuery<'_> {
        // do not count on evaluation order of AND, use CTE here
        // https://www.postgresql.org/docs/18/sql-expressions.html#SYNTAX-EXPRESS-EVAL
        query!(
            r#"
            WITH lock AS MATERIALIZED (SELECT pg_try_advisory_xact_lock(hashtext('room_info'), $1) AS got)
            INSERT INTO room_info (time, room_id, area_id, area_name, parent_area_id, parent_area_name, title)
            SELECT NOW(), $1, $2, $3, $4, $5, $6
            WHERE
                (SELECT got FROM lock)
            AND NOT COALESCE((
                SELECT area_id = $2
                AND area_name = $3
                AND parent_area_id = $4
                AND parent_area_name = $5
                AND title = $6
                FROM room_info
                WHERE room_id = $1
                AND time > NOW() - INTERVAL '5 minutes'
                ORDER BY time DESC
                LIMIT 1
            ), FALSE)
            "#,
            self.room_id,
            self.area_id,
            self.area_name,
            self.parent_area_id,
            self.parent_area_name,
            self.title
        )
    }
}

impl RoomInfo {
    pub fn from_api_result(room_id: u32, rsp: &serde_json::Value) -> Result<RoomInfo> {
        let room_id = room_id as i32;
        let data = rsp.get("data").context("Missing data field")?;
        let result = Self::deserialize(data)?;
        Ok(Self { room_id, ..result })
    }
}

#[derive(Deserialize)]
pub struct OnlineCount {
    #[serde(skip)]
    room_id: i32,
    count: i32,
}

impl FromMsg for OnlineCount {
    fn from_msg(room_id: u32, m: &serde_json::Value) -> Result<Self> {
        let room_id = room_id as i32;
        let data = m.get("data").context("Missing data field")?;
        let result = Self::deserialize(data)?;
        Ok(Self { room_id, ..result })
    }
}

impl Insertable for OnlineCount {
    fn build_query(&self) -> PgQuery<'_> {
        query!(
            r#"
            WITH lock AS MATERIALIZED (SELECT pg_try_advisory_xact_lock(hashtext('online_rank_count'), $1) AS got)
            INSERT INTO online_rank_count (time, room_id, count)
            SELECT NOW(), $1, $2
            WHERE
                (SELECT got FROM lock)
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

#[derive(Deserialize)]
pub struct LikeInfo {
    #[serde(skip)]
    room_id: i32,
    click_count: i32,
}

impl FromMsg for LikeInfo {
    fn from_msg(room_id: u32, m: &serde_json::Value) -> Result<Self>
    where
        Self: Sized,
    {
        let room_id = room_id as i32;
        let data = m.get("data").context("Missing data field")?;
        let result = Self::deserialize(data)?;
        Ok(Self { room_id, ..result })
    }
}

impl Insertable for LikeInfo {
    fn build_query(&self) -> PgQuery<'_> {
        query!(
            r#"
            WITH lock AS MATERIALIZED (SELECT pg_try_advisory_xact_lock(hashtext('like_info'), $1) AS got)
            INSERT INTO like_info (time, room_id, click_count)
            SELECT NOW(), $1, $2
            WHERE
                (SELECT got FROM lock)
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

#[derive(Deserialize)]
pub struct Watched {
    #[serde(skip)]
    room_id: i32,
    num: i32,
}

impl FromMsg for Watched {
    fn from_msg(room_id: u32, m: &serde_json::Value) -> Result<Self>
    where
        Self: Sized,
    {
        let room_id = room_id as i32;
        let data = m.get("data").context("Missing data field")?;
        let result = Self::deserialize(data)?;
        Ok(Self { room_id, ..result })
    }
}

impl Insertable for Watched {
    fn build_query(&self) -> PgQuery<'_> {
        query!(
            r#"
            WITH lock AS MATERIALIZED (SELECT pg_try_advisory_xact_lock(hashtext('watched'), $1) AS got)
            INSERT INTO watched (time, room_id, num)
            SELECT NOW(), $1, $2
            WHERE
                (SELECT got FROM lock)
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

#[derive(Deserialize)]
pub struct Guard {
    #[serde(skip)]
    room_id: i32,

    start_time: i64,
    guard_level: i16,
    num: i32,
    uid: i64,
    username: String,
}

impl FromMsg for Guard {
    fn from_msg(room_id: u32, m: &serde_json::Value) -> Result<Self>
    where
        Self: Sized,
    {
        let room_id = room_id as i32;
        let data = m.get("data").context("Missing data field")?;
        let result = Self::deserialize(data)?;
        Ok(Self { room_id, ..result })
    }
}

impl Insertable for Guard {
    fn build_query(&self) -> PgQuery<'_> {
        query!(
            r#"
            INSERT INTO guard (time, room_id, guard_level, num, uid, username)
            VALUES (TO_TIMESTAMP($1), $2, $3, $4, $5, $6)
            ON CONFLICT (time, uid) DO NOTHING
            "#,
            self.start_time as f64,
            self.room_id,
            self.guard_level,
            self.num,
            self.uid,
            self.username,
        )
    }
}

async fn begin_roomid_lock(
    pool: &PgPool,
    table: &str,
    room_id: u32,
) -> std::result::Result<PgTransaction<'static>, sqlx::Error> {
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
    use serde_json::json;
    use serial_test::serial;
    use sqlx::{PgPool, PgTransaction};

    async fn test_pool() -> PgPool {
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or("postgres://postgres@localhost/test".to_string());
        PgPool::connect(&database_url)
            .await
            .expect("Failed to connect to database")
    }

    async fn get_tx(pool: &PgPool) -> PgTransaction<'static> {
        // let pool = get_db_pool().await;
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
        let pool = test_pool().await;

        let mut tx = get_tx(&pool).await;

        let res = data.build_query().execute(&mut *tx).await?;
        assert_eq!(res.rows_affected(), 1);

        {
            let mut tx2 = get_tx(&pool).await;
            let res = data.build_query().execute(&mut *tx2).await?;
            assert_eq!(res.rows_affected(), 0, "Should be blocked by lock");
            tx2.rollback().await?;
        }

        let res = data.build_query().execute(&mut *tx).await?;
        assert_eq!(
            res.rows_affected(),
            0,
            "Should be blocked by recent duplicate"
        );

        tx.rollback().await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_insert_online_count() -> Result<()> {
        let msg = serde_json::json!(
          {"cmd":"ONLINE_RANK_COUNT","data":{"count":1254,"count_text":"1254","online_count":1254,"online_count_text":"1254"}}
        );
        let data = OnlineCount::from_msg(12345, &msg)?;

        test_insertable(&data).await
    }

    #[tokio::test]
    #[serial]
    async fn test_insert_like_info() -> Result<()> {
        let msg = serde_json::json!(
            {"cmd":"LIKE_INFO_V3_UPDATE","data":{"click_count":5661}}
        );
        let data = LikeInfo::from_msg(12345, &msg)?;
        test_insertable(&data).await
    }

    #[tokio::test]
    #[serial]
    async fn test_insert_room_info() -> Result<()> {
        let msg = serde_json::json!(
            {"cmd":"ROOM_CHANGE","data":{"area_id":216,"area_name":"我的世界","live_key":"637964572313074796","parent_area_id":6,"parent_area_name":"单机游戏","sub_session_key":"637964572313074796sub_time:1760781222","title":"游戏超链接！~我的世界~"}}
        );
        let data = RoomInfo::from_msg(12345, &msg)?;
        test_insertable(&data).await
    }

    #[tokio::test]
    #[serial]
    async fn test_api_room_info() -> Result<()> {
        let ret = json!({
          "code": 0,
          "msg": "ok",
          "message": "ok",
          "data": {
            "uid": 702013828,
            "room_id": 31255806,
            "short_id": 0,
            "attention": 15212,
            "online": 2556,
            "is_portrait": false,
            "description": "直播全球地震信息、海啸信息、突发火山信息。直播间内容未经允许禁止二次利用！",
            "live_status": 1,
            "area_id": 701,
            "parent_area_id": 11,
            "parent_area_name": "知识",
            "old_area_id": 6,
            "background": "https://i0.hdslb.com/bfs/live/f3c1e1e22dfb1942bd88c33f1aa174efe7a38dfd.jpg",
            "title": "全球地震预警-信息/海啸信息/EEW",
            "user_cover": "https://i0.hdslb.com/bfs/live/new_room_cover/7ada6fa2338213fda7f82e9a3b4247264390de5c.jpg",
            "keyframe": "https://i0.hdslb.com/bfs/live-key-frame/keyframe10301901000031255806jy8toe.jpg",
            "is_strict_room": false,
            "live_time": "2025-10-30 18:43:53",
            "tags": "EEW,地震预警,地震信息,地震,自然灾害,灾害,Earthquake,中国,日本,地理",
            "is_anchor": 0,
            "room_silent_type": "",
            "room_silent_level": 0,
            "room_silent_second": 0,
            "area_name": "科技·科学",
          }
        });
        let room_info = RoomInfo::from_api_result(12345, &ret)?;
        assert_eq!(room_info.room_id, 12345);
        assert_eq!(room_info.area_id, 701);
        assert_eq!(room_info.area_name, "科技·科学");
        assert_eq!(room_info.parent_area_id, 11);
        assert_eq!(room_info.parent_area_name, "知识");
        assert_eq!(room_info.live_status, Some(1));
        assert_eq!(room_info.title, "全球地震预警-信息/海啸信息/EEW");
        test_insertable(&room_info).await
    }

    #[tokio::test]
    #[serial]
    async fn test_insert_watched() -> Result<()> {
        let ret = json!({"cmd":"WATCHED_CHANGE","data":{"num":608,"text_large":"608人看过","text_small":"608"}});
        let data = Watched::from_msg(12345, &ret)?;
        assert_eq!(data.num, 608);
        test_insertable(&data).await
    }

    #[tokio::test]
    #[serial]
    async fn test_insert_guard() -> Result<()> {
        let ret = json!({
          "cmd": "GUARD_BUY",
          "data": {
            "uid": 14225357,
            "username": "妙妙喵喵妙妙喵O_O",
            "guard_level": 3,
            "num": 1,
            "price": 198000,
            "gift_id": 10003,
            "gift_name": "舰长",
            "start_time": 1677069316,
            "end_time": 1677069316
          }
        });
        let data = Guard::from_msg(12345, &ret)?;

        let pool = test_pool().await;

        let mut tx = get_tx(&pool).await;
        let res = data.build_query().execute(&mut *tx).await?;
        assert_eq!(res.rows_affected(), 1);
        let res = data.build_query().execute(&mut *tx).await?;

        assert_eq!(res.rows_affected(), 0, "Should be blocked by conflict");
        tx.rollback().await?;
        Ok(())
    }
}
