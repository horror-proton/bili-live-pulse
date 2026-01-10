use std::sync::Arc;
use tokio::sync::Mutex;

use tokio::time::Duration;
use tokio::time::Instant;

pub struct RateLimiter {
    base_delay: Duration,
    last_limit: Arc<Mutex<Option<Instant>>>, // shared state
                                             // TODO: last_pending
}

pub enum RetryOrError<S, E> {
    Retry(S),
    Error(E),
}

impl RateLimiter {
    pub fn default() -> Self {
        Self {
            base_delay: Duration::from_millis(500),
            last_limit: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn call_with_retry<C, F, Fut, T, E>(
        &self,
        mut ctx: C,
        mut attempt: F,
    ) -> std::result::Result<T, E>
    where
        F: FnMut(C) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, RetryOrError<C, E>>>,
    {
        let mut retries = 0;
        loop {
            if let Some(last) = *self.last_limit.lock().await {
                let last = last;
                let delay = self.base_delay * (2u32.pow(retries.min(10)));
                if Instant::now() < last + delay {
                    tokio::time::sleep(delay / 10).await;
                    continue;
                }
            }

            match attempt(ctx).await {
                Ok(result) => {
                    self.last_limit.lock().await.take();
                    return Ok(result);
                }
                Err(RetryOrError::Retry(c)) => {
                    retries += 1;
                    *self.last_limit.lock().await = Some(Instant::now());
                    ctx = c;
                    continue;
                }
                Err(RetryOrError::Error(e)) => return Err(e),
            }
        }
    }
}
