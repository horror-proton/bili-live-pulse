use log::{debug, error, info, warn};
use reqwest::Client;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use tokio::time::Duration;
use tokio_util::{future::FutureExt, sync::CancellationToken};

/// Represents a room's status from an instance's perspective.
#[derive(Debug, Clone, Deserialize)]
pub struct RoomStatus {
    pub room_id: u32,
    pub live_status: i16,
    pub connection_ready: bool,
}

/// Represents an instance in the cluster.
#[derive(Debug, Clone)]
pub struct Instance {
    pub id: String,
    pub base_url: String,
}

/// Service discovery trait - to be implemented for production use.
pub trait ServiceDiscovery: Send + Sync {
    /// Get all known instances.
    fn discover_instances(&self) -> Vec<Instance>;
}

/// Result of comparing captures from multiple instances.
#[derive(Debug, Clone)]
pub enum ComparisonResult {
    /// All instances are consistent and ready.
    Ready,
    /// Instances are inconsistent. The contained instance IDs are candidates for restart,
    /// ordered from worst to best (first = most divergent).
    Inconsistent { worst_instances: Vec<String> },
}

/// Comparison algorithm trait - to be implemented for production use.
pub trait ComparisonAlgorithm: Send + Sync {
    /// Compare captured messages from multiple instances.
    ///
    /// Returns a ComparisonResult indicating whether instances are consistent,
    /// or which instance(s) should be restarted if inconsistent.
    ///
    /// # Arguments
    /// * `room_id` - The room being compared
    /// * `captures` - A slice of (instance_id, messages) tuples from each instance
    fn compare(&self, room_id: u32, captures: &[(String, Vec<String>)]) -> ComparisonResult;
}

/// Coordinator that polls instances and manages room connection readiness.
pub struct Coordinator<SD: ServiceDiscovery, CA: ComparisonAlgorithm> {
    service_discovery: SD,
    comparison_algorithm: CA,
    http_client: Client,
    poll_interval: Duration,
}

impl<SD: ServiceDiscovery, CA: ComparisonAlgorithm> Coordinator<SD, CA> {
    pub fn new(service_discovery: SD, comparison_algorithm: CA) -> Self {
        Self {
            service_discovery,
            comparison_algorithm,
            http_client: Client::new(),
            poll_interval: Duration::from_secs(600),
        }
    }

    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Run the coordinator loop.
    pub async fn run(&self) -> anyhow::Result<()> {
        info!("Coordinator started");

        let mut interval = tokio::time::interval(self.poll_interval);

        loop {
            interval.tick().await;

            if let Err(e) = self.poll_and_reconcile().await {
                error!("Error in poll and reconcile: {:#}", e);
            }
        }
    }

    /// Poll all instances and reconcile room states.
    async fn poll_and_reconcile(&self) -> anyhow::Result<()> {
        let instances = self.service_discovery.discover_instances();

        if instances.is_empty() {
            debug!("No instances discovered");
            return Ok(());
        }

        info!("Discovered {} instances", instances.len());

        // Get all rooms from all instances
        let mut room_status_by_instance: HashMap<String, Vec<RoomStatus>> = HashMap::new();

        for instance in &instances {
            match self.get_instance_rooms(instance).await {
                Ok(rooms) => {
                    room_status_by_instance.insert(instance.id.clone(), rooms);
                }
                Err(e) => {
                    warn!("Failed to get rooms from instance {}: {:#}", instance.id, e);
                }
            }
        }

        // Find rooms that are not ready
        let not_ready_rooms = self.find_not_ready_rooms(&room_status_by_instance);

        if not_ready_rooms.is_empty() {
            debug!("All rooms are ready");
            return Ok(());
        }

        info!("Found {} not-ready rooms", not_ready_rooms.len());

        // For each not-ready room, capture and compare.
        //
        // A single room capture can take up to ~5 minutes (see `/api/rooms/{room_id}/capture`),
        // so processing rooms sequentially does not scale. Limit concurrent comparisons to keep
        // per-instance load bounded while still making progress across many rooms.
        const MAX_CONCURRENT_ROOM_COMPARISONS: usize = 20;
        info!(
            "Reconciling not-ready rooms with concurrency={}",
            MAX_CONCURRENT_ROOM_COMPARISONS
        );

        use futures_util::stream::{self, StreamExt};

        let instances_ref = &instances;
        stream::iter(not_ready_rooms.into_iter())
            .map(|room_id| async move {
                let res = self.handle_not_ready_room(instances_ref, room_id).await;
                (room_id, res)
            })
            .buffer_unordered(MAX_CONCURRENT_ROOM_COMPARISONS)
            .for_each(|(room_id, res)| async move {
                if let Err(e) = res {
                    error!("Error handling not-ready room {}: {:#}", room_id, e);
                }
            })
            .await;

        Ok(())
    }

    /// Get rooms from a single instance.
    async fn get_instance_rooms(&self, instance: &Instance) -> anyhow::Result<Vec<RoomStatus>> {
        let url = format!("{}/api/rooms", instance.base_url);

        let response = self
            .http_client
            .get(&url)
            .timeout(Duration::from_secs(5))
            .send()
            .await?;

        if !response.status().is_success() {
            anyhow::bail!("Instance returned status {}", response.status());
        }

        let rooms: Vec<RoomStatus> = response.json().await?;
        Ok(rooms)
    }

    /// Find rooms that are not ready across instances.
    fn find_not_ready_rooms(
        &self,
        room_status_by_instance: &HashMap<String, Vec<RoomStatus>>,
    ) -> HashSet<u32> {
        let mut not_ready = HashSet::new();

        for (_instance_id, rooms) in room_status_by_instance {
            for room in rooms {
                if !room.connection_ready {
                    not_ready.insert(room.room_id);
                }
            }
        }

        not_ready
    }

    /// Handle a not-ready room by capturing and comparing.
    async fn handle_not_ready_room(
        &self,
        instances: &[Instance],
        room_id: u32,
    ) -> anyhow::Result<()> {
        info!("Handling not-ready room {}", room_id);

        let captures = self
            .capture_from_multiple_instances_concurrent(instances, room_id)
            .await?;

        if captures.len() < 2 {
            warn!(room_id; "less than 2 instances, skipping");
            return Ok(());
        }

        // Compare captures
        let result = self.comparison_algorithm.compare(room_id, &captures);

        match result {
            ComparisonResult::Ready => {
                info!("Room {} comparison passed, marking as ready", room_id);
                // Mark the room of all the instances ready
                for (instance_id, _) in captures {
                    if let Some(instance) = instances.iter().find(|i| i.id == instance_id) {
                        if let Err(e) = self.mark_room_ready(instance, room_id).await {
                            error!(
                                "Failed to mark room {} as ready on {}: {:#}",
                                room_id, instance.id, e
                            );
                        }
                    }
                }
            }
            ComparisonResult::Inconsistent { worst_instances } => {
                if let Some(w) = worst_instances.first() {
                    warn!(room_id; "Comparison failed, restarting the worst instance: {:?}", w);
                    // Restart only the worst instance
                    if let Some(instance) = instances.iter().find(|i| &i.id == w) {
                        if let Err(e) = self.restart_room(instance, room_id).await {
                            error!(
                                "Failed to restart room {} on instance {}: {:#}",
                                room_id, instance.id, e
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Capture messages from multiple instances concurrently (at almost the exact time).
    async fn capture_from_multiple_instances_concurrent(
        &self,
        instances: &[Instance],
        room_id: u32,
    ) -> anyhow::Result<Vec<(String, Vec<String>)>> {
        use futures_util::future::join_all;

        // Create capture futures for all instances
        let cancel_token = CancellationToken::new();
        let capture_futures = instances.iter().map(|instance| async {
            let cancel_token = cancel_token.clone();
            let start = tokio::time::Instant::now();
            let result = self
                .capture_room_messages(instance, room_id, &cancel_token)
                .await;
            let duration = start.elapsed();

            cancel_token.cancel(); // cancel captures of other instances

            match result {
                Ok(ref msgs) => info!(
                    room_id; "{}: captured {} messages in {:?}",
                    instance.id,
                    msgs.len(),
                    duration
                ),
                Err(ref e) => warn!(
                    room_id; "{}: capture failed after {:?}: {:#}",
                    instance.id, duration, e
                ),
            }
            (instance.id.clone(), result)
        });

        // Execute all captures concurrently
        let results = join_all(capture_futures).await;

        // Filter successful captures
        let captures = results
            .into_iter()
            .filter_map(|(id, result)| result.ok().map(|msgs| (id, msgs)))
            .collect();

        Ok(captures)
    }

    /// Capture messages from a room on an instance.
    async fn capture_room_messages(
        &self,
        instance: &Instance,
        room_id: u32,
        cancel: &CancellationToken,
    ) -> anyhow::Result<Vec<String>> {
        let url = format!("{}/api/rooms/{}/sse", instance.base_url, room_id);

        use eventsource_stream::Eventsource;
        use futures_util::StreamExt;

        let mut event_stream = reqwest::Client::new()
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .bytes_stream()
            .eventsource();

        let mut messages = Vec::new();

        while let Some(Some(event)) = event_stream.next().with_cancellation_token(cancel).await {
            match event {
                Ok(ev) => {
                    if ev.event == "message" {
                        messages.push(ev.data);
                        if messages.len() >= 20 {
                            break;
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "Error reading SSE from instance {} for room {}: {:#}",
                        instance.id, room_id, e
                    );
                    break;
                }
            }
        }
        Ok(messages)
    }

    /// Mark a room as ready on an instance.
    async fn mark_room_ready(&self, instance: &Instance, room_id: u32) -> anyhow::Result<()> {
        let url = format!(
            "{}/api/rooms/{}/connection-ready",
            instance.base_url, room_id
        );

        let response = self
            .http_client
            .put(&url)
            .timeout(Duration::from_secs(5))
            .send()
            .await?;

        if !response.status().is_success() {
            anyhow::bail!("Mark ready returned status {}", response.status());
        }

        debug!(
            "Marked room {} as ready on instance {}",
            room_id, instance.id
        );
        Ok(())
    }

    /// Restart a room on an instance.
    async fn restart_room(&self, instance: &Instance, room_id: u32) -> anyhow::Result<()> {
        let url = format!("{}/api/rooms/{}/restart", instance.base_url, room_id);

        let response = self
            .http_client
            .post(&url)
            .timeout(Duration::from_secs(5))
            .send()
            .await?;

        if !response.status().is_success() {
            anyhow::bail!("Restart returned status {}", response.status());
        }

        debug!("Restarted room {} on instance {}", room_id, instance.id);
        Ok(())
    }
}

/// Stub service discovery for testing/development.
#[derive(Debug, Clone)]
pub struct StubServiceDiscovery {
    instances: Vec<Instance>,
}

impl StubServiceDiscovery {
    pub fn new(instances: Vec<Instance>) -> Self {
        Self { instances }
    }
}

impl ServiceDiscovery for StubServiceDiscovery {
    fn discover_instances(&self) -> Vec<Instance> {
        self.instances.clone()
    }
}

/// Stub comparison algorithm for testing/development.
#[derive(Debug, Clone, Default)]
pub struct StubComparisonAlgorithm;

fn lcs_len<T: Eq>(a: &[T], b: &[T]) -> usize {
    if a.is_empty() || b.is_empty() {
        return 0;
    }

    let m = b.len();
    let mut prev = vec![0usize; m + 1];
    let mut curr = vec![0usize; m + 1];

    for ai in a {
        for (j, bj) in b.iter().enumerate() {
            let j1 = j + 1;
            if ai == bj {
                curr[j1] = prev[j] + 1;
            } else {
                curr[j1] = prev[j1].max(curr[j]);
            }
        }

        std::mem::swap(&mut prev, &mut curr);
        curr.fill(0);
    }

    prev[m]
}

fn lcs_distance<T: Eq>(a: &[T], b: &[T]) -> usize {
    let lcs = lcs_len(a, b);
    a.len() + b.len() - 2 * lcs
}

impl ComparisonAlgorithm for StubComparisonAlgorithm {
    fn compare(&self, room_id: u32, captures: &[(String, Vec<String>)]) -> ComparisonResult {
        if captures.len() < 2 {
            return ComparisonResult::Inconsistent {
                worst_instances: captures.iter().map(|(id, _)| id.clone()).collect(),
            };
        }

        for (i, (_, msgs)) in captures.iter().enumerate() {
            if msgs.len() < 3 {
                return ComparisonResult::Inconsistent {
                    worst_instances: vec![captures[i].0.clone()],
                };
            }
        }

        const MAX_ALLOWED_MISSING: usize = 1;
        let max_allowed_distance = 2 * MAX_ALLOWED_MISSING;

        let n = captures.len();
        let mut distances = vec![vec![0usize; n]; n];
        let mut total_distance = vec![0usize; n];

        for i in 0..n {
            for j in (i + 1)..n {
                let dist = lcs_distance(&captures[i].1, &captures[j].1);
                distances[i][j] = dist;
                distances[j][i] = dist;
                total_distance[i] += dist;
                total_distance[j] += dist;
            }
        }

        let reference_idx = (0..n)
            .min_by_key(|&i| (total_distance[i], &captures[i].0))
            .unwrap_or(0);

        let mut outliers: Vec<(usize, usize)> = (0..n)
            .filter_map(|i| {
                if i == reference_idx {
                    return None;
                }
                let dist = distances[reference_idx][i];
                (dist > max_allowed_distance).then_some((i, dist))
            })
            .collect();

        if outliers.is_empty() {
            return ComparisonResult::Ready;
        }

        if n == 2 {
            let mut idx = vec![0, 1];
            idx.sort_by(|l, r| {
                captures[*l]
                    .1
                    .len()
                    .cmp(&captures[*r].1.len())
                    .then_with(|| total_distance[*r].cmp(&total_distance[*l]))
            });

            return ComparisonResult::Inconsistent {
                worst_instances: idx.into_iter().map(|i| captures[i].0.clone()).collect(),
            };
        }

        outliers.sort_by(|(i1, d1), (i2, d2)| {
            d2.cmp(d1)
                .then_with(|| total_distance[*i2].cmp(&total_distance[*i1]))
                .then_with(|| captures[*i1].0.cmp(&captures[*i2].0))
        });

        ComparisonResult::Inconsistent {
            worst_instances: outliers
                .into_iter()
                .map(|(i, _)| captures[i].0.clone())
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stub_comparison_identical() {
        let algo = StubComparisonAlgorithm;
        let captures = vec![
            (
                "instance-1".to_string(),
                vec!["a".to_string(), "b".to_string(), "c".to_string()],
            ),
            (
                "instance-2".to_string(),
                vec!["a".to_string(), "b".to_string(), "c".to_string()],
            ),
        ];
        assert!(matches!(
            algo.compare(123, &captures),
            ComparisonResult::Ready
        ));
    }

    #[test]
    fn test_stub_comparison_too_few() {
        let algo = StubComparisonAlgorithm;
        let captures = vec![
            ("instance-1".to_string(), vec!["a".to_string()]),
            (
                "instance-2".to_string(),
                vec!["a".to_string(), "b".to_string(), "c".to_string()],
            ),
        ];
        let result = algo.compare(123, &captures);
        if let ComparisonResult::Inconsistent { worst_instances } = result {
            assert_eq!(worst_instances[0], "instance-1".to_string(),);
        } else {
            panic!("expected inconsistent");
        }
    }

    #[test]
    fn test_stub_comparison_small_shift_passes() {
        let algo = StubComparisonAlgorithm;
        let captures = vec![
            (
                "instance-1".to_string(),
                vec![
                    "2".to_string(),
                    "3".to_string(),
                    "4".to_string(),
                    "5".to_string(),
                    "6".to_string(),
                    "7".to_string(),
                    "8".to_string(),
                ],
            ),
            (
                "instance-2".to_string(),
                vec![
                    "3".to_string(),
                    "4".to_string(),
                    "5".to_string(),
                    "6".to_string(),
                    "7".to_string(),
                    "8".to_string(),
                    "9".to_string(),
                ],
            ),
        ];
        assert!(matches!(
            algo.compare(123, &captures),
            ComparisonResult::Ready
        ));
    }

    #[test]
    fn test_stub_comparison_two_instances_inconsistent_restarts_less() {
        let algo = StubComparisonAlgorithm;
        let captures = vec![
            (
                "instance-1".to_string(),
                vec![
                    "1".to_string(),
                    "2".to_string(),
                    "3".to_string(),
                    "4".to_string(),
                    "5".to_string(),
                    "6".to_string(),
                    "7".to_string(),
                ],
            ),
            (
                "instance-2".to_string(),
                vec![
                    "6".to_string(),
                    "7".to_string(),
                    "8".to_string(),
                    "9".to_string(),
                    "10".to_string(),
                ],
            ),
        ];
        let result = algo.compare(123, &captures);
        if let ComparisonResult::Inconsistent { worst_instances } = result {
            assert_eq!(worst_instances[0], "instance-2".to_string());
        } else {
            panic!("expected inconsistent");
        }
    }

    #[test]
    fn test_stub_comparison_outlier_is_worst_with_three_instances() {
        let algo = StubComparisonAlgorithm;
        let captures = vec![
            (
                "instance-1".to_string(),
                vec![
                    "1".to_string(),
                    "2".to_string(),
                    "3".to_string(),
                    "4".to_string(),
                    "5".to_string(),
                    "6".to_string(),
                    "7".to_string(),
                ],
            ),
            (
                "instance-2".to_string(),
                vec![
                    "2".to_string(),
                    "3".to_string(),
                    "4".to_string(),
                    "5".to_string(),
                    "6".to_string(),
                    "7".to_string(),
                    "8".to_string(),
                ],
            ),
            (
                "instance-3".to_string(),
                vec![
                    "9".to_string(),
                    "10".to_string(),
                    "11".to_string(),
                    "12".to_string(),
                    "13".to_string(),
                    "14".to_string(),
                    "15".to_string(),
                ],
            ),
        ];
        let result = algo.compare(123, &captures);
        if let ComparisonResult::Inconsistent { worst_instances } = result {
            assert_eq!(worst_instances, vec!["instance-3".to_string()]);
        } else {
            panic!("expected inconsistent");
        }
    }
}
