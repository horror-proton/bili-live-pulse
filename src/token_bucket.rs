use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::sync::Notify;
use tokio::time;

pub struct TokenBucket {
    capacity: u32,
    tokens: AtomicU32,
    refill_rate: u32, // tokens per second
    last_refill: time::Instant,
    notify: Notify,
}

impl TokenBucket {
    pub fn new(capacity: u32, refill_rate: u32) -> Arc<Self> {
        let result = Arc::new(TokenBucket {
            capacity,
            tokens: AtomicU32::new(capacity),
            refill_rate,
            last_refill: time::Instant::now(),
            notify: Notify::new(),
        });

        tokio::spawn({
            let bucket = Arc::clone(&result);
            async move {
                let mut interval = time::interval(time::Duration::from_secs(1) / refill_rate);
                loop {
                    interval.tick().await;
                    bucket.try_refill_one();
                }
            }
        });

        result
    }

    fn try_refill_one(&self) {
        if self
            .tokens
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                Some(std::cmp::min(current + 1, self.capacity))
            })
            .is_ok()
        {
            self.notify.notify_one();
        };
    }

    pub fn try_consume_one(&self) -> bool {
        self.tokens
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                if current > 0 { Some(current - 1) } else { None }
            })
            .is_ok()
    }

    pub async fn consume_one(&self) {
        loop {
            if self.try_consume_one() {
                return;
            }
            self.notify.notified().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_token_bucket_refill() {
        let bucket = TokenBucket::new(5, 2);
        for _ in 0..5 {
            assert!(bucket.try_consume_one());
        }
        assert!(!bucket.try_consume_one());
        sleep(Duration::from_secs(1)).await;
        assert!(bucket.try_consume_one());
    }

    #[tokio::test]
    async fn test_token_bucket_blocking_consume() {
        let bucket = TokenBucket::new(1, 1);
        assert!(bucket.try_consume_one());
        assert!(!bucket.try_consume_one());
        let now = time::Instant::now();
        bucket.consume_one().await;
        bucket.consume_one().await;
        assert!(now.elapsed() >= Duration::from_secs(1));
    }
}
