use anyhow::Context;
use anyhow::Result;
use serde::Deserialize;
use sqlx::PgTransaction;
use sqlx::postgres::PgQueryResult;
use sqlx::query::Query;
use sqlx::types::chrono::{DateTime, FixedOffset, NaiveDateTime, TimeZone};
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

#[derive(Deserialize, Debug)]
pub struct RoomInfo {
    #[serde(skip)]
    room_id: i32,

    area_id: i32,
    area_name: String,
    parent_area_id: i32,
    parent_area_name: String,
    pub live_status: Option<i16>,
    title: String,
    pub up_session: Option<String>,

    #[serde(default)]
    #[serde(deserialize_with = "deserialize_sqlx_datetime")]
    live_time: Option<DateTime<FixedOffset>>,
}

static TIMEZONE: FixedOffset = FixedOffset::east_opt(8 * 3600).unwrap();

fn deserialize_sqlx_datetime<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<DateTime<FixedOffset>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = match Option::<String>::deserialize(deserializer)? {
        Some(v) => v,
        None => return Ok(None),
    };

    if s == "0000-00-00 00:00:00" {
        return Ok(None);
    }

    let ndt =
        NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S").map_err(serde::de::Error::custom)?;
    let dt = TIMEZONE.from_local_datetime(&ndt).single().ok_or_else(|| {
        serde::de::Error::custom("Failed to convert to DateTime with FixedOffset")
    })?;
    Ok(Some(dt))
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
            INSERT INTO room_info (time, room_id, area_id, area_name, parent_area_id, parent_area_name, title, live_id_str, live_time)
            SELECT NOW(), $1, $2, $3, $4, $5, $6, $7, $8
            WHERE
                (SELECT got FROM lock)
            AND NOT COALESCE((
                SELECT area_id = $2
                AND parent_area_id = $4
                AND title = $6
                AND live_id_str IS NOT DISTINCT FROM $7
                FROM room_info
                WHERE room_id = $1
                AND time > NOW() - INTERVAL '60 minutes'
                ORDER BY time DESC
                LIMIT 1
            ), FALSE)
            "#,
            self.room_id,
            self.area_id,
            self.area_name,
            self.parent_area_id,
            self.parent_area_name,
            self.title,
            self.up_session,
            self.live_time
        )
    }
}

impl RoomInfo {
    pub fn from_api_result(room_id: u32, rsp: &serde_json::Value) -> Result<Self> {
        let room_id = room_id as i32;
        let data = rsp
            .get("data")
            .context(format!("Missing data field in {}", rsp.to_string()))?;
        let result = Self::deserialize(data)
            .context(format!("Failed to deserialize from {}", rsp.to_string()))?;
        Ok(Self { room_id, ..result })
    }

    fn generate_live_meta(&self) -> Option<LiveMeta> {
        let live_key = self.up_session.as_ref()?;
        if live_key.is_empty() {
            return None;
        }
        return Some(LiveMeta {
            roomid: self.room_id,
            live_key: live_key.clone(),
            live_platform: None,
            live_time: self.live_time.map(|dt| dt.timestamp() as i64),
        });
    }

    pub async fn update_live_meta(&self, pool: &PgPool) -> sqlx::Result<()> {
        let lm = self.generate_live_meta();

        // set end_time_before for previous live sessions if any
        store_live_meta_end_time(self.room_id, lm.as_ref().map(|v| v.live_key.clone()))
            .execute(pool)
            .await?;

        if let Some(live_meta) = lm {
            insert_struct(pool, &live_meta).await?;
        }
        Ok(())
    }
}

pub fn store_live_meta_end_time(room_id: i32, key_not_eq: Option<String>) -> PgQuery<'static> {
    query!(
        r#"
        UPDATE live_meta
        SET end_time_before = NOW()
        WHERE room_id = $1 AND (live_id_str <> $2 OR $2 IS NULL)
        AND end_time_before IS NULL AND end_time_est IS NULL
        "#,
        room_id,
        key_not_eq,
    )
}

#[derive(Deserialize, Debug)]
pub struct LiveMeta {
    roomid: i32,
    pub live_key: String,
    live_platform: Option<String>,
    live_time: Option<i64>,
}

impl FromMsg for LiveMeta {
    fn from_msg(room_id: u32, m: &serde_json::Value) -> Result<Self>
    where
        Self: Sized,
    {
        let room_id = room_id as i32;
        let data = m;
        let result = Self::deserialize(data)?;
        Ok(Self {
            roomid: room_id,
            ..result
        })
    }
}

impl Insertable for LiveMeta {
    fn build_query(&self) -> PgQuery<'_> {
        if self.live_key.is_empty() || self.live_key == "0" {
            // TODO: add test for Live started: {"cmd":"LIVE","live_key":"0","live_model":0,"live_platform":"pc_link","roomid":24872476,"sub_session_key":"","voice_background":""}
            println!("Empty live_key, skip inserting live_meta {:?}", self);
            return query!("");
        }
        query!(
            r#"
            INSERT INTO live_meta (live_id_str, room_id, live_time, live_platform)
            VALUES ($1, $2, TO_TIMESTAMP($3), $4)
            ON CONFLICT (live_id_str)
            DO UPDATE SET
                live_time = EXCLUDED.live_time,
                live_platform = EXCLUDED.live_platform
            WHERE (live_meta.live_time IS NULL AND EXCLUDED.live_time IS NOT NULL)
            OR (live_meta.live_platform IS NULL AND EXCLUDED.live_platform IS NOT NULL)
            "#,
            self.live_key,
            self.roomid,
            self.live_time.map(|t| t as f64),
            self.live_platform,
        )
    }
}

#[derive(Deserialize)]
pub struct LiveMetaEnd {
    #[serde(skip)]
    room_id: i32,
    send_time: i64,
}

impl FromMsg for LiveMetaEnd {
    fn from_msg(room_id: u32, m: &serde_json::Value) -> Result<Self>
    where
        Self: Sized,
    {
        let room_id = room_id as i32;
        let data = m;
        let result = Self::deserialize(data)?;
        Ok(Self { room_id, ..result })
    }
}

impl LiveMetaEnd {
    pub fn store_end_time_est(&self, live_id_str: &str) -> PgQuery<'_> {
        query!(
            r#"
            UPDATE live_meta
            SET end_time_est = TO_TIMESTAMP($2)
            WHERE live_id_str = $1
            AND end_time_est IS NULL
            "#,
            live_id_str,
            self.send_time as f64 / 1000.0,
        )
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

#[derive(Deserialize)]
pub struct RealTimeMessage {
    #[serde(skip)]
    room_id: i32,

    fans: i32,
    fans_club: i32,
}

impl FromMsg for RealTimeMessage {
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

impl Insertable for RealTimeMessage {
    fn build_query(&self) -> PgQuery<'_> {
        query!(
            r#"
            WITH lock AS MATERIALIZED (SELECT pg_try_advisory_xact_lock(hashtext('watched'), $1) AS got)
            INSERT INTO real_time_message (time, room_id, fans, fans_club)
            SELECT NOW(), $1, $2, $3
            WHERE
                (SELECT got FROM lock)
            AND NOT COALESCE((
                SELECT fans = $2
                AND fans_club = $3
                FROM real_time_message
                WHERE room_id = $1
                AND time > NOW() - INTERVAL '5 minutes'
                ORDER BY time DESC
                LIMIT 1
            ), FALSE)
            "#,
            self.room_id,
            self.fans,
            self.fans_club
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
        let mut ret = json!({
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
        assert_eq!(
            room_info.live_time.unwrap(),
            TIMEZONE
                .with_ymd_and_hms(2025, 10, 30, 18, 43, 53)
                .single()
                .unwrap()
        );
        assert_eq!(room_info.title, "全球地震预警-信息/海啸信息/EEW");
        test_insertable(&room_info).await?;

        ret["data"]["live_time"] = serde_json::Value::String("0000-00-00 00:00:00".to_string());
        let room_info = RoomInfo::from_api_result(12345, &ret)?;
        assert!(room_info.live_time.is_none());
        test_insertable(&room_info).await?;
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_insert_live_start_1() -> Result<()> {
        let ret = json!(
        {
            "cmd":"LIVE",
            "live_key":"650619521652046956",
            "live_model":0,
            "live_platform":"pc",
            "live_time":1765368103,
            "roomid":12345,
            "special_types":[50],
            "sub_session_key":"650619521652046956sub_time:1765368103",
            "voice_background":""
        });
        let data = LiveMeta::from_msg(12345, &ret)?;
        assert_eq!(data.live_key, "650619521652046956");
        assert_eq!(data.live_platform, Some(String::from("pc")));
        assert_eq!(data.live_time, Some(1765368103));

        let pool = test_pool().await;
        let mut tx = get_tx(&pool).await;
        let res = data.build_query().execute(&mut *tx).await?;
        assert_eq!(res.rows_affected(), 1);
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_insert_live_start_2() -> Result<()> {
        let ret = json!(
        {
            "cmd":"LIVE",
            "live_key":"650619521652046956",
            "live_model":0,
            "live_platform":"pc",
            "roomid":81004,
            "sub_session_key":"650619521652046956sub_time:1765368103",
            "voice_background":""
        });
        let data = LiveMeta::from_msg(12345, &ret)?;
        assert_eq!(data.live_key, "650619521652046956");
        assert_eq!(data.live_platform, Some(String::from("pc")));
        assert_eq!(data.live_time, None);

        let pool = test_pool().await;
        let mut tx = get_tx(&pool).await;
        let res = data.build_query().execute(&mut *tx).await?;
        assert_eq!(res.rows_affected(), 1);
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_insert_live_meta_from_room_info() -> Result<()> {
        let pool = test_pool().await;
        let mut tx = get_tx(&pool).await;

        let mut ri = RoomInfo {
            room_id: 12345,
            area_id: 0,
            area_name: String::new(),
            parent_area_id: 0,
            parent_area_name: String::new(),
            live_status: Some(1),
            title: String::new(),
            up_session: Some(String::from("11111111")),
            live_time: Some(
                TIMEZONE
                    .with_ymd_and_hms(2025, 10, 30, 18, 43, 53)
                    .single()
                    .unwrap(),
            ),
        };

        // store new live meta
        ri.generate_live_meta()
            .unwrap()
            .build_query()
            .execute(&mut *tx)
            .await?;
        query!(
            "SELECT * FROM live_meta WHERE room_id = 12345 AND live_id_str = '11111111' LIMIT 1"
        )
        .fetch_one(&mut *tx)
        .await?;

        ri.live_status = Some(0);
        store_live_meta_end_time(ri.room_id, None)
            .execute(&mut *tx)
            .await?;
        query!("SELECT end_time_before FROM live_meta WHERE live_id_str = '11111111' LIMIT 1")
            .fetch_one(&mut *tx)
            .await?
            .end_time_before
            .context("end_time_before should be set")?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_insert_live_end() -> Result<()> {
        let pool = test_pool().await;
        let mut tx = get_tx(&pool).await;

        let ret = json!({"cmd":"PREPARING","msg_id":"75389731379118592:1000:1000","p_is_ack":true,"p_msg_type":1,"roomid":"81004","send_time":1760451130537_i64});
        let data = LiveMetaEnd::from_msg(12345, &ret)?;
        assert_eq!(data.send_time, 1760451130537_i64);

        query!("INSERT INTO live_meta (live_id_str, room_id, live_time) VALUES ('test_live_end', 12345, TO_TIMESTAMP(1760450000)) ON CONFLICT DO NOTHING")
            .execute(&mut *tx)
            .await?;

        data.store_end_time_est("test_live_end")
            .execute(&mut *tx)
            .await?;

        query!("SELECT end_time_est FROM live_meta WHERE live_id_str = 'test_live_end' LIMIT 1")
            .fetch_one(&mut *tx)
            .await?
            .end_time_est
            .context("end_time_est should be set")?;

        Ok(())
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

    #[tokio::test]
    #[serial]
    async fn test_insert_real_time_message() -> Result<()> {
        let ret = json!({"cmd":"ROOM_REAL_TIME_MESSAGE_UPDATE","data":{"fans":691456,"fans_club":3806,"red_notice":-1,"roomid":12345}});
        let data = RealTimeMessage::from_msg(12345, &ret)?;
        assert_eq!(data.fans, 691456);
        assert_eq!(data.fans_club, 3806);
        test_insertable(&data).await?;
        Ok(())
    }
}
