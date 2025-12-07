use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::time;

pub struct TokenBucket {
    capacity: u32,
    tokens: AtomicU32,
    refill_rate: u32, // tokens per second
    last_refill: time::Instant,
}

impl TokenBucket {
    pub fn new(capacity: u32, refill_rate: u32) -> Arc<Self> {
        let result = Arc::new(TokenBucket {
            capacity,
            tokens: AtomicU32::new(capacity),
            refill_rate,
            last_refill: time::Instant::now(),
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
        self.tokens
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                Some(std::cmp::min(current + 1, self.capacity))
            })
            .ok();
    }

    pub fn try_consume_one(&self) -> bool {
        self.tokens
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                if current > 0 { Some(current - 1) } else { None }
            })
            .is_ok()
    }
}
