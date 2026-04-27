#![allow(dead_code)]

use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use futures::{future::BoxFuture, FutureExt};
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};

use crate::driver::latency::LatencySummary;

pub(crate) type PacedTask = BoxFuture<'static, Result<(), String>>;

#[derive(Clone, Debug)]
pub(crate) struct PacedTaskRunConfig {
    pub(crate) target_ops_per_s: f64,
    pub(crate) max_in_flight: usize,
    pub(crate) pacer_core_id: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PacedTaskRunReport {
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
pub struct PacedTaskSample {
    pub(crate) index: usize,
    pub(crate) ok: bool,
    pub(crate) offered_latency_ms: f64,
    pub(crate) service_latency_ms: f64,
    pub(crate) schedule_lag_ms: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PacedTaskFailure {
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
    if total_tasks == 0 {
        let wall_start = Instant::now();
        let wall_time_ms = duration_ms(duration_since(Instant::now(), wall_start));
        return Ok(build_report(
            config.target_ops_per_s,
            total_tasks,
            wall_time_ms,
            Vec::new(),
        ));
    }

    let period = Duration::from_secs_f64(1.0 / config.target_ops_per_s);
    let runtime = Handle::current();
    let available_slots = Arc::new(AtomicUsize::new(config.max_in_flight));
    let max_in_flight = config.max_in_flight;
    let (execution_tx, mut execution_rx) = mpsc::channel(total_tasks);
    let (start_tx, start_rx) = oneshot::channel();
    let mut executions = Vec::with_capacity(total_tasks);

    let pacer_core_id = config
        .pacer_core_id
        .or_else(pacer_core_id_from_env)
        .or_else(default_pacer_core_id);
    let pacer = thread::Builder::new()
        .name("lc-bench-pacer".to_string())
        .spawn(move || {
            if let Some(core_id) = pacer_core_id {
                if let Err(err) = pin_current_thread_to_core(core_id) {
                    eprintln!("failed to pin pacer thread to core {core_id}: {err}");
                }
            }

            let wall_start = Instant::now();
            let _ = start_tx.send(wall_start);
            let mut next_send_at = wall_start;

            for (index, task) in tasks.into_iter().enumerate() {
                let scheduled_at = next_send_at;
                next_send_at += period;
                let execution_tx = execution_tx.clone();
                let available_slots_for_task = Arc::clone(&available_slots);

                wait_until_send_time(scheduled_at, &available_slots);
                let previous = available_slots.fetch_sub(1, Ordering::Relaxed);
                debug_assert!(previous > 0, "pacer acquired capacity below zero");

                runtime.spawn(async move {
                    let started_at = Instant::now();
                    let result = AssertUnwindSafe(task).catch_unwind().await;
                    let result = match result {
                        Ok(result) => result,
                        Err(payload) => {
                            Err(format!("paced task panicked: {}", panic_message(payload)))
                        }
                    };
                    let completed_at = Instant::now();
                    let previous = available_slots_for_task.fetch_add(1, Ordering::Relaxed);
                    debug_assert!(
                        previous < max_in_flight,
                        "pacer released more capacity than max_in_flight"
                    );
                    let _ = execution_tx
                        .send(TaskExecution {
                            index,
                            scheduled_at,
                            started_at,
                            completed_at,
                            result,
                        })
                        .await;
                });
            }
        })
        .map_err(|err| format!("failed to spawn pacer thread: {err}"))?;

    let wall_start = start_rx
        .await
        .map_err(|_| "pacer thread exited before reporting start time".to_string())?;

    while executions.len() < total_tasks {
        let Some(execution) = execution_rx.recv().await else {
            break;
        };
        executions.push(execution);
    }

    pacer
        .join()
        .map_err(|payload| format!("pacer thread panicked: {}", panic_message(payload)))?;

    if executions.len() != total_tasks {
        return Err(format!(
            "paced run collected {} of {} task results",
            executions.len(),
            total_tasks
        ));
    }

    let wall_end = executions
        .iter()
        .map(|execution| execution.completed_at)
        .max()
        .unwrap_or(wall_start);
    let wall_time_ms = duration_ms(duration_since(wall_end, wall_start));
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

fn wait_until_send_time(scheduled_at: Instant, available_slots: &AtomicUsize) {
    while available_slots.load(Ordering::Relaxed) == 0 {
        std::hint::spin_loop();
    }
    while Instant::now() < scheduled_at {
        std::hint::spin_loop();
    }
}

fn pacer_core_id_from_env() -> Option<usize> {
    std::env::var("LC_BENCH_PACER_CORE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
}

#[cfg(target_os = "linux")]
fn default_pacer_core_id() -> Option<usize> {
    let cpulist = std::fs::read_to_string("/sys/devices/system/node/node0/cpulist").ok()?;
    highest_cpu_in_cpulist(cpulist.trim())
}

#[cfg(not(target_os = "linux"))]
fn default_pacer_core_id() -> Option<usize> {
    None
}

fn highest_cpu_in_cpulist(cpulist: &str) -> Option<usize> {
    cpulist
        .split(',')
        .filter_map(|part| {
            let part = part.trim();
            if part.is_empty() {
                return None;
            }
            if let Some((_, end)) = part.split_once('-') {
                end.trim().parse::<usize>().ok()
            } else {
                part.parse::<usize>().ok()
            }
        })
        .max()
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        return (*message).to_string();
    }
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    "unknown panic payload".to_string()
}

#[cfg(target_os = "linux")]
fn pin_current_thread_to_core(core_id: usize) -> Result<(), String> {
    if core_id >= libc::CPU_SETSIZE as usize {
        return Err(format!(
            "core id {core_id} is outside CPU_SETSIZE={}",
            libc::CPU_SETSIZE
        ));
    }

    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(core_id, &mut set);
        let rc = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
        if rc == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error().to_string())
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn pin_current_thread_to_core(_core_id: usize) -> Result<(), String> {
    Err("thread affinity is only implemented on Linux".to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::time::sleep;

    use super::{boxed_task, highest_cpu_in_cpulist, run_paced_tasks, PacedTaskRunConfig};

    #[tokio::test]
    async fn rejects_invalid_config() {
        let err = run_paced_tasks(
            Vec::new(),
            PacedTaskRunConfig {
                target_ops_per_s: 0.0,
                max_in_flight: 1,
                pacer_core_id: None,
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
                pacer_core_id: None,
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
                pacer_core_id: None,
            },
        )
        .await
        .unwrap();

        assert_eq!(report.completed_tasks, 5);
        assert!(max_seen.load(Ordering::SeqCst) <= 2);
    }

    #[test]
    fn parses_highest_cpu_from_cpulist() {
        assert_eq!(highest_cpu_in_cpulist("0-3,8-11"), Some(11));
        assert_eq!(highest_cpu_in_cpulist("2,4,6-7"), Some(7));
        assert_eq!(highest_cpu_in_cpulist(""), None);
    }
}
