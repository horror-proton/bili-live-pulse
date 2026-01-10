use anyhow::Result;
use log::{debug, error, info, trace, warn};
use serde_json::Value;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::{msg::LiveMessage, token_bucket};

use super::msg;

struct ConnectionReplica {
    cancel_token: CancellationToken,
    handle: Option<tokio::task::JoinHandle<(msg::MsgConnection, anyhow::Result<()>)>>,
}

impl ConnectionReplica {
    pub async fn new(
        mut connection: msg::MsgConnection,
        key: msg::RoomKeyLease,
        message_tx: broadcast::Sender<LiveMessage>,
    ) -> Self {
        let cancel_token = CancellationToken::new();
        let ct_clone = cancel_token.clone();
        let handle = tokio::spawn(async move {
            let res = connection.start(ct_clone, message_tx, key).await;
            (connection, res)
        });

        Self {
            cancel_token,
            handle: Some(handle),
        }
    }

    pub async fn join(&mut self) -> Result<(msg::MsgConnection, anyhow::Result<()>)> {
        self.cancel();
        let res = self.handle.take().unwrap().await?;
        Ok(res)
    }

    pub fn cancel(&self) {
        self.cancel_token.cancel();
    }
}

impl Drop for ConnectionReplica {
    fn drop(&mut self) {
        if self.handle.is_some() {
            self.cancel();
        }
    }
}

// TODO: which to return, connection or channel?
pub async fn ensure_connection(
    room_id: u32,
    key: &msg::RoomKeyLease,
    buvid: &str,
    token_bucket: &token_bucket::TokenBucket,
    mut existing: Option<msg::MsgConnection>,
) -> Result<msg::MsgConnection> {
    loop {
        let lhs_c = match existing {
            Some(conn) => conn,
            None => msg::MsgConnection::new(room_id, key.key(), buvid).await?,
        };

        let rhs_c = msg::MsgConnection::new(room_id, key.key(), buvid).await?;

        let (lhs_tx, lhs_rx) = broadcast::channel::<LiveMessage>(1);
        let (rhs_tx, rhs_rx) = broadcast::channel::<LiveMessage>(1);

        let mut lhs = ConnectionReplica::new(lhs_c, key.clone(), lhs_tx).await;
        let mut rhs = ConnectionReplica::new(rhs_c, key.clone(), rhs_tx).await;

        token_bucket.consume_one().await;
        info!(room_id; "Comparing connections");
        let mut cmp = Comparator2::new(lhs_rx, rhs_rx).with_filter(|m: &LiveMessage| match m {
            LiveMessage::HeartbeatReply(_) => false,
            LiveMessage::Message(Value::Object(obj)) => {
                if let Some(Value::String(s)) = obj.get("cmd") {
                    if s == "LOG_IN_NOTICE" {
                        return false;
                    }
                }
                true
            }
            _ => true,
        });
        let (l, r) = cmp.record().await;
        info!(room_id; "Comparison results: lhs={} rhs={}", l, r);

        if l >= 0.79 && r >= 0.79 {
            let (conn, _) = lhs.join().await?;
            return Ok(conn);
        }

        let (lhs_msgs, rhs_msgs) = cmp.dump();
        warn!(room_id; "Inconsistent connections detected: lhs={} ({}) rhs={} ({})", l, lhs_msgs.len(), r, rhs_msgs.len());

        if l < r {
            // lhs has more messages not in rhs, keep lhs
            debug!(room_id; "Keeping LHS connection");
            let (conn, _) = lhs.join().await?;
            existing = Some(conn);
        } else {
            debug!(room_id; "Keeping RHS connection");
            let (conn, _) = rhs.join().await?;
            existing = Some(conn);
        }
    }
}

struct Comparator2<T>
where
    T: std::fmt::Debug,
{
    lhs: ReceiverInfo<T>,
    rhs: ReceiverInfo<T>,
}

struct ReceiverInfo<T>
where
    T: std::fmt::Debug,
{
    receiver: broadcast::Receiver<T>,
    buffer: Vec<T>,
    filter: Option<fn(&T) -> bool>,
}

impl<T> ReceiverInfo<T>
where
    T: std::fmt::Debug + Clone,
{
    pub async fn recv(&mut self) -> () {
        if let Ok(m) = self.receiver.recv().await {
            if let Some(filter) = self.filter {
                if !filter(&m) {
                    return;
                }
            }
            debug!("Received message: {:?}", m);
            self.buffer.push(m);
        }
    }

    pub async fn clear(&mut self) {
        self.receiver = self.receiver.resubscribe();
    }
}

impl<T> Comparator2<T>
where
    T: Eq + std::fmt::Debug + Clone,
{
    pub fn new(lhs: broadcast::Receiver<T>, rhs: broadcast::Receiver<T>) -> Self {
        Self {
            lhs: ReceiverInfo {
                receiver: lhs,
                buffer: Vec::new(),
                filter: None,
            },
            rhs: ReceiverInfo {
                receiver: rhs,
                buffer: Vec::new(),
                filter: None,
            },
        }
    }

    pub fn with_filter(mut self, filter: fn(&T) -> bool) -> Self {
        self.lhs.filter = Some(filter);
        self.rhs.filter = Some(filter);
        self
    }

    pub fn dump(self) -> (Vec<T>, Vec<T>) {
        (self.lhs.buffer, self.rhs.buffer)
    }

    // TODO: return receivers instead of using refs
    pub async fn record(&mut self) -> (f64, f64) {
        let max_length = 20;

        use tokio::time;

        self.lhs.clear().await;
        self.rhs.clear().await;

        let timeout = time::Instant::now() + time::Duration::from_secs(60 * 5);
        let mut print_interval = time::interval(time::Duration::from_secs(30));
        loop {
            tokio::select! {
                _ = self.lhs.recv() => {
                    if self.lhs.buffer.len() >= max_length {
                        break;
                    }
                }
                _ = self.rhs.recv() => {
                    if self.rhs.buffer.len() >= max_length {
                        break;
                    }
                }
                _ = time::sleep_until(timeout) => {
                    debug!("Timeout reached while recording messages");
                    break;
                }
                _ = print_interval.tick() => {
                    debug!("Recording messages: lhs={} rhs={}", self.lhs.buffer.len(), self.rhs.buffer.len());
                }
            }
        }

        let lcs_length = longest_common_subsequence(&self.lhs.buffer, &self.rhs.buffer);

        if self.lhs.buffer.is_empty() && self.rhs.buffer.is_empty() {
            return (0., 0.);
        }

        if self.lhs.buffer.is_empty() {
            return (1.0, 0.0);
        }

        if self.rhs.buffer.is_empty() {
            return (0.0, 1.0);
        }

        (
            lcs_length as f64 / self.lhs.buffer.len() as f64,
            lcs_length as f64 / self.rhs.buffer.len() as f64,
        )
    }
}

fn longest_common_subsequence<T: Eq>(lhs: &Vec<T>, rhs: &Vec<T>) -> usize {
    if lhs.len() < rhs.len() {
        return longest_common_subsequence(rhs, lhs);
    }

    let mut prev = vec![0; rhs.len() + 1];
    let mut curr = vec![0; rhs.len() + 1];

    for i in 1..=lhs.len() {
        for j in 1..=rhs.len() {
            if lhs[i - 1] == rhs[j - 1] {
                curr[j] = prev[j - 1] + 1;
            } else {
                curr[j] = curr[j - 1].max(prev[j]);
            }
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[rhs.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lcs() {
        let seq1 = vec![1, 2, 3, 4, 1];
        let seq2 = vec![3, 4, 1, 2, 1];
        let lcs_length = longest_common_subsequence(&seq1, &seq2);
        assert_eq!(lcs_length, 3); // The LCS is [3, 4, 1]
    }
}
