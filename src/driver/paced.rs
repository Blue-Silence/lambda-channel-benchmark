#![allow(dead_code)]

use std::future::Future;
use std::time::Duration;

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use tokio::time::{sleep, Instant};

use crate::driver::latency::LatencySummary;

pub(crate) type PacedTask = BoxFuture<'static, Result<(), String>>;

#[derive(Clone, Debug)]
pub(crate) struct PacedTaskRunConfig {
    pub(crate) target_ops_per_s: f64,
    pub(crate) max_in_flight: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PacedTaskRunReport {
    pub(crate) target_ops_per_s: f64,
    pub(crate) achieved_ops_per_s: f64,
    pub(crate) successful_ops_per_s: f64,
    pub(crate) total_tasks: usize,
    pub(crate) completed_tasks: usize,
    pub(crate) failed_tasks: usize,
    pub(crate) wall_time_ms: f64,
    pub(crate) offered_latency: LatencySummary,
    pub(crate) service_latency: LatencySummary,
    pub(crate) schedule_lag: LatencySummary,
    pub(crate) samples: Vec<PacedTaskSample>,
    pub(crate) failures: Vec<PacedTaskFailure>,
}

impl PacedTaskRunReport {
    pub(crate) fn achieved_ratio(&self) -> f64 {
        rate_ratio(self.achieved_ops_per_s, self.target_ops_per_s)
    }

    pub(crate) fn successful_ratio(&self) -> f64 {
        rate_ratio(self.successful_ops_per_s, self.target_ops_per_s)
    }

    pub(crate) fn has_failures(&self) -> bool {
        self.failed_tasks > 0 || !self.failures.is_empty()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PacedTaskSample {
    pub(crate) index: usize,
    pub(crate) ok: bool,
    pub(crate) offered_latency_ms: f64,
    pub(crate) service_latency_ms: f64,
    pub(crate) schedule_lag_ms: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PacedTaskFailure {
    pub(crate) index: usize,
    pub(crate) message: String,
}

struct TaskExecution {
    index: usize,
    scheduled_at: Instant,
    started_at: Instant,
    completed_at: Instant,
    result: Result<(), String>,
}

pub(crate) fn boxed_task<F>(future: F) -> PacedTask
where
    F: Future<Output = Result<(), String>> + Send + 'static,
{
    Box::pin(future)
}

pub(crate) async fn run_paced_tasks(
    tasks: Vec<PacedTask>,
    config: PacedTaskRunConfig,
) -> Result<PacedTaskRunReport, String> {
    validate_config(&config)?;

    let total_tasks = tasks.len();
    let wall_start = Instant::now();
    let period = Duration::from_secs_f64(1.0 / config.target_ops_per_s);
    let mut join_set = JoinSet::new();
    let mut executions = Vec::with_capacity(total_tasks);

    for (index, task) in tasks.into_iter().enumerate() {
        while join_set.len() >= config.max_in_flight {
            collect_next_execution(&mut join_set, &mut executions).await?;
        }

        let scheduled_at = wall_start + period.mul_f64(index as f64);
        let now = Instant::now();
        if scheduled_at > now {
            sleep(scheduled_at - now).await;
        }

        join_set.spawn(async move {
            let started_at = Instant::now();
            let result = task.await;
            let completed_at = Instant::now();
            TaskExecution {
                index,
                scheduled_at,
                started_at,
                completed_at,
                result,
            }
        });
    }

    while !join_set.is_empty() {
        collect_next_execution(&mut join_set, &mut executions).await?;
    }

    let wall_time_ms = duration_ms(duration_since(Instant::now(), wall_start));
    Ok(build_report(
        config.target_ops_per_s,
        total_tasks,
        wall_time_ms,
        executions,
    ))
}

fn validate_config(config: &PacedTaskRunConfig) -> Result<(), String> {
    if !config.target_ops_per_s.is_finite() || config.target_ops_per_s <= 0.0 {
        return Err("target_ops_per_s must be a finite positive number".to_string());
    }
    if config.max_in_flight == 0 {
        return Err("max_in_flight must be greater than zero".to_string());
    }
    Ok(())
}

async fn collect_next_execution(
    join_set: &mut JoinSet<TaskExecution>,
    executions: &mut Vec<TaskExecution>,
) -> Result<(), String> {
    let Some(next) = join_set.join_next().await else {
        return Ok(());
    };
    let execution = next.map_err(|err| format!("paced task join failed: {err}"))?;
    executions.push(execution);
    Ok(())
}

fn build_report(
    target_ops_per_s: f64,
    total_tasks: usize,
    wall_time_ms: f64,
    mut executions: Vec<TaskExecution>,
) -> PacedTaskRunReport {
    executions.sort_by_key(|execution| execution.index);

    let mut offered_latencies = Vec::with_capacity(executions.len());
    let mut service_latencies = Vec::with_capacity(executions.len());
    let mut schedule_lags = Vec::with_capacity(executions.len());
    let mut samples = Vec::with_capacity(executions.len());
    let mut failures = Vec::new();

    for execution in executions {
        let offered_latency_ms = duration_ms(duration_since(
            execution.completed_at,
            execution.scheduled_at,
        ));
        let service_latency_ms =
            duration_ms(duration_since(execution.completed_at, execution.started_at));
        let schedule_lag_ms =
            duration_ms(duration_since(execution.started_at, execution.scheduled_at));
        let ok = execution.result.is_ok();

        offered_latencies.push(offered_latency_ms);
        service_latencies.push(service_latency_ms);
        schedule_lags.push(schedule_lag_ms);
        if let Err(message) = execution.result {
            failures.push(PacedTaskFailure {
                index: execution.index,
                message,
            });
        }
        samples.push(PacedTaskSample {
            index: execution.index,
            ok,
            offered_latency_ms,
            service_latency_ms,
            schedule_lag_ms,
        });
    }

    let completed_tasks = samples.len();
    let failed_tasks = failures.len();
    let elapsed_s = wall_time_ms / 1000.0;
    let achieved_ops_per_s = rate_per_second(completed_tasks, elapsed_s);
    let successful_ops_per_s = rate_per_second(completed_tasks - failed_tasks, elapsed_s);

    PacedTaskRunReport {
        target_ops_per_s,
        achieved_ops_per_s,
        successful_ops_per_s,
        total_tasks,
        completed_tasks,
        failed_tasks,
        wall_time_ms,
        offered_latency: LatencySummary::from_samples(&offered_latencies),
        service_latency: LatencySummary::from_samples(&service_latencies),
        schedule_lag: LatencySummary::from_samples(&schedule_lags),
        samples,
        failures,
    }
}

fn rate_per_second(count: usize, elapsed_s: f64) -> f64 {
    if elapsed_s <= 0.0 {
        return 0.0;
    }
    count as f64 / elapsed_s
}

fn rate_ratio(actual: f64, target: f64) -> f64 {
    if !actual.is_finite() || !target.is_finite() || target <= 0.0 {
        return 0.0;
    }
    actual / target
}

fn duration_since(later: Instant, earlier: Instant) -> Duration {
    later.checked_duration_since(earlier).unwrap_or_default()
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::time::sleep;

    use super::{boxed_task, run_paced_tasks, PacedTaskRunConfig};

    #[tokio::test]
    async fn rejects_invalid_config() {
        let err = run_paced_tasks(
            Vec::new(),
            PacedTaskRunConfig {
                target_ops_per_s: 0.0,
                max_in_flight: 1,
            },
        )
        .await
        .unwrap_err();

        assert!(err.contains("target_ops_per_s"));
    }

    #[tokio::test]
    async fn reports_successes_and_failures() {
        let tasks = vec![
            boxed_task(async { Ok(()) }),
            boxed_task(async { Err("boom".to_string()) }),
            boxed_task(async { Ok(()) }),
        ];

        let report = run_paced_tasks(
            tasks,
            PacedTaskRunConfig {
                target_ops_per_s: 10_000.0,
                max_in_flight: 2,
            },
        )
        .await
        .unwrap();

        assert_eq!(report.total_tasks, 3);
        assert_eq!(report.completed_tasks, 3);
        assert_eq!(report.failed_tasks, 1);
        assert_eq!(report.failures[0].index, 1);
        assert_eq!(report.offered_latency.count, 3);
        assert!(report.achieved_ops_per_s > 0.0);
    }

    #[tokio::test]
    async fn caps_max_in_flight_tasks() {
        let current = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();

        for _ in 0..5 {
            let current = Arc::clone(&current);
            let max_seen = Arc::clone(&max_seen);
            tasks.push(boxed_task(async move {
                let now = current.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(now, Ordering::SeqCst);
                sleep(Duration::from_millis(20)).await;
                current.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            }));
        }

        let report = run_paced_tasks(
            tasks,
            PacedTaskRunConfig {
                target_ops_per_s: 1_000_000.0,
                max_in_flight: 2,
            },
        )
        .await
        .unwrap();

        assert_eq!(report.completed_tasks, 5);
        assert!(max_seen.load(Ordering::SeqCst) <= 2);
    }
}
