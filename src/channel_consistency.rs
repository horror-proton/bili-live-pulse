use anyhow::Result;
use log::{debug, error, info, trace, warn};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::msg;

struct ConnectionReplica {
    cancel_token: CancellationToken,
    handle: Option<tokio::task::JoinHandle<(msg::MsgConnection, anyhow::Result<()>)>>,
}

impl ConnectionReplica {
    pub async fn new(
        mut connection: msg::MsgConnection,
        key: msg::RoomKeyLease,
        message_tx: mpsc::Sender<msg::LiveMessage>,
    ) -> Self {
        let cancel_token = CancellationToken::new();
        let ct_clone = cancel_token.clone();
        let handle = tokio::spawn(async move {
            let res = connection.start(ct_clone, message_tx, key).await;
            (connection, res)
        });

        debug!("Spawned connection replica task");
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
        debug!("Cancelling connection replica");
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
    mut existing: Option<msg::MsgConnection>,
) -> Result<msg::MsgConnection> {
    loop {
        let lhs_c = match existing {
            Some(conn) => conn,
            None => msg::MsgConnection::new(room_id, key.key()).await?,
        };

        let rhs_c = msg::MsgConnection::new(room_id, key.key()).await?;

        let (lhs_tx, mut lhs_rx) = mpsc::channel::<msg::LiveMessage>(5);
        let (rhs_tx, mut rhs_rx) = mpsc::channel::<msg::LiveMessage>(5);

        let mut lhs = ConnectionReplica::new(lhs_c, key.clone(), lhs_tx).await;
        let mut rhs = ConnectionReplica::new(rhs_c, key.clone(), rhs_tx).await;

        info!(room_id; "Comparing connections");
        let (l, r) = Comparator2::new(&mut lhs_rx, &mut rhs_rx).record().await;

        if l <= 0.25 && r <= 0.25 {
            let (conn, _) = lhs.join().await?;
            return Ok(conn);
        }

        warn!(room_id; "Inconsistent connections detected: lhs={} rhs={}", l, r);

        if l > r {
            let (conn, _) = rhs.join().await?;
            existing = Some(conn);
        } else {
            let (conn, _) = lhs.join().await?;
            existing = Some(conn);
        }
    }
}

struct Comparator2<'a, T>
where
    T: std::fmt::Debug,
{
    lhs: ReceiverInfo<'a, T>,
    rhs: ReceiverInfo<'a, T>,
}

struct ReceiverInfo<'a, T>
where
    T: std::fmt::Debug,
{
    receiver: &'a mut mpsc::Receiver<T>,
    buffer: Vec<T>,
}

impl<T> ReceiverInfo<'_, T>
where
    T: std::fmt::Debug,
{
    pub async fn recv(&mut self) -> Option<()> {
        self.receiver.recv().await.map(|m| {
            debug!("Received message {:?}", m);
            self.buffer.push(m);
        })
    }
}

impl<'a, T> Comparator2<'a, T>
where
    T: Eq + std::fmt::Debug,
{
    pub fn new(lhs: &'a mut mpsc::Receiver<T>, rhs: &'a mut mpsc::Receiver<T>) -> Self {
        Self {
            lhs: ReceiverInfo {
                receiver: lhs,
                buffer: Vec::new(),
            },
            rhs: ReceiverInfo {
                receiver: rhs,
                buffer: Vec::new(),
            },
        }
    }

    // TODO: return receivers instead of using refs
    pub async fn record(&mut self) -> (f64, f64) {
        let max_length = 20;

        use tokio::time;

        let timeout = time::Instant::now() + time::Duration::from_secs(30);
        loop {
            tokio::select! {
                _ = self.lhs.recv() => {
                    debug!("LHS length {}", self.lhs.buffer.len());
                    if self.lhs.buffer.len() >= max_length {
                        break;
                    }
                }
                _ = self.rhs.recv() => {
                    debug!("RHS length {}", self.rhs.buffer.len());
                    if self.rhs.buffer.len() >= max_length {
                        break;
                    }
                }
                _ = time::sleep_until(timeout) => {
                    debug!("Timeout reached while recording messages");
                    break;
                }
            }
        }

        let lcs_length = longest_common_subsequence(&self.lhs.buffer, &self.rhs.buffer);

        let lhs_exclusive = self.lhs.buffer.len() - lcs_length;
        let rhs_exclusive = self.rhs.buffer.len() - lcs_length;

        if self.lhs.buffer.is_empty() && self.rhs.buffer.is_empty() {
            return (1.0, 1.0);
        }

        if self.lhs.buffer.is_empty() {
            return (0.0, 1.0);
        }

        if self.rhs.buffer.is_empty() {
            return (1.0, 0.0);
        }

        (
            lhs_exclusive as f64 / self.lhs.buffer.len() as f64,
            rhs_exclusive as f64 / self.rhs.buffer.len() as f64,
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
