use super::model;
use super::utils::backoff;
use super::utils::backoff::RateLimiter;
use super::wbi;
use anyhow::{Context, Result};
use log::{debug, error, info, trace, warn};

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum DanmuInfoResult {
    Success { data: DanmuInfoResultData },
    Error { code: i32, message: String },
}

#[derive(serde::Deserialize)]
struct DanmuInfoResultData {
    token: String,
}

pub async fn fetch_room_key(
    rate_limiter: &RateLimiter,
    room_id: u32,
    wbi_keys: Option<(String, String)>,
) -> Result<String> {
    struct Args {
        room_id: u32,
        wbi_keys: Option<(String, String)>,
    }
    type Error = backoff::RetryOrError<Args, anyhow::Error>;

    rate_limiter
        .call_with_retry(
            Args { room_id, wbi_keys },
            async |args| -> std::result::Result<String, Error> {
                match fetch_room_key_impl(args.room_id, args.wbi_keys.clone()).await {
                    Ok(DanmuInfoResult::Success { data }) => Ok(data.token),
                    Ok(DanmuInfoResult::Error { code, message }) => {
                        if code == -352 {
                            warn!(
                                "Rate limited when fetching room key for room {}: {}",
                                room_id, message
                            );
                            Err(Error::Retry(args))
                        } else {
                            Err(Error::Error(anyhow::anyhow!(
                                "Error fetching room key for room {}: {} ({})",
                                room_id,
                                message,
                                code
                            )))
                        }
                    }
                    Err(e) => Err(Error::Error(e)),
                }
            },
        )
        .await
}

async fn fetch_room_key_impl(
    roomid: u32,
    wbi_keys: Option<(String, String)>,
) -> Result<DanmuInfoResult> {
    let keys = match wbi_keys {
        Some(k) => k,
        None => wbi::get_wbi_keys().await?,
    };

    let params = wbi::encode_wbi(
        vec![
            ("id", roomid.to_string()),
            ("type", "0".to_string()),
            ("web_location", "444.8".to_string()),
        ],
        keys,
    );

    let url = format!(
        "https://api.live.bilibili.com/xlive/web-room/v1/index/getDanmuInfo?{}",
        params
    );

    use reqwest::header::USER_AGENT;

    info!("Fetching room key from URL: {}", url);
    let resp = reqwest::Client::new()
        .get(&url)
        .header(USER_AGENT, "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
        .send()
        .await?.bytes().await?;
    let res = serde_json::from_slice::<DanmuInfoResult>(&resp).context(format!(
        "Failed to parse getDanmuInfo response: {}",
        String::from_utf8_lossy(&resp)
    ))?;
    // TODO: handle rate limit response: {"code":-352,"message":"-352","ttl":1}

    Ok(res)
}

pub async fn fetch_live_status(room_id: u32) -> Result<model::RoomInfo> {
    let cli = reqwest::Client::new();
    let resp = cli
        .get("https://api.live.bilibili.com/room/v1/Room/get_info")
        .query(&[("room_id", room_id)])
        .send()
        .await?
        .bytes() // .json()
        .await?;

    let resp = serde_json::from_slice::<serde_json::Value>(&resp).context(format!(
        "Missing data field in API response: {}",
        String::from_utf8_lossy(&resp)
    ))?;

    model::RoomInfo::from_api_result(room_id, &resp).context(format!(
        "Failed to parse info of room {}: {} ",
        room_id, resp
    ))
}

pub async fn fetch_buvidv3() -> Result<String> {
    #[derive(serde::Deserialize)]
    struct RespData {
        b_3: String,
    }

    #[derive(serde::Deserialize)]
    struct Resp {
        data: RespData,
    }

    let resp = reqwest::Client::new()
        .get("https://api.bilibili.com/x/frontend/finger/spi")
        .send()
        .await?
        .json::<Resp>()
        .await?;
    Ok(resp.data.b_3)
}
