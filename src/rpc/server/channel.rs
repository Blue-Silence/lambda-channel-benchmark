use std::collections::VecDeque;
use std::error::Error;
use std::fmt::Debug;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use lambda_channel::blob_store_impl::native_store_handle::BlobGetOptions;
use lambda_channel::elem_ptrs::{BlobElemPtr, LocalFileElemPtr};
use lambda_channel::metadata_blob_channel::receiver::Popped;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio::task::JoinSet;

use crate::driver::latency::LatencySummary;
use crate::driver::paced::{boxed_task, run_paced_tasks, PacedTask, PacedTaskRunConfig};
use crate::rpc::protocol::{
    AcceptedResponse, ChannelReceiverResult, ChannelSendResult, RequestResult,
    StartChannelReceiverRequest, StartPacedChannelSendRequest,
};
use crate::rpc::server::state::{
    create_request_on_node, current_expr, current_expr_mut, fail_request_on_node,
    finish_request_on_node, mark_request_ready_on_node, put_artifact, put_metric,
    request_cancel_flag_on_node, NodeRuntimeState,
};

const HEADER_LEN: usize = 32;
const HEADER_MAGIC: &[u8; 4] = b"LCCH";
const HEADER_MARKER: u32 = 0xC0DECafe;
const MAX_POLL_PERIOD_MS: u64 = 500;
const MAX_RETAINED_FAILURES: usize = 64;

pub(crate) async fn submit_paced_channel_send_on_node(
    instance_id: &str,
    runtime: Arc<Mutex<NodeRuntimeState>>,
    request: StartPacedChannelSendRequest,
) -> AcceptedResponse {
    let accepted =
        create_request_on_node(instance_id, &runtime, &request.run_id, "channel-send").await;
    let Some(req_id) = accepted.req_id.clone() else {
        return accepted;
    };

    tokio::spawn(async move {
        let run_id = request.run_id.clone();
        let result = paced_channel_send_on_node(&runtime, request).await;
        match result {
            Ok(result) => {
                if let Err(err) = finish_request_on_node(
                    &runtime,
                    &run_id,
                    &req_id,
                    RequestResult::ChannelSend(result),
                )
                .await
                {
                    eprintln!("failed to finish channel send request {req_id}: {err}");
                }
            }
            Err(message) => {
                if let Err(err) = fail_request_on_node(&runtime, &run_id, &req_id, message).await {
                    eprintln!("failed to fail channel send request {req_id}: {err}");
                }
            }
        }
    });

    accepted
}

pub(crate) async fn submit_channel_receiver_on_node(
    instance_id: &str,
    runtime: Arc<Mutex<NodeRuntimeState>>,
    request: StartChannelReceiverRequest,
) -> AcceptedResponse {
    let accepted =
        create_request_on_node(instance_id, &runtime, &request.run_id, "channel-receiver").await;
    let Some(req_id) = accepted.req_id.clone() else {
        return accepted;
    };
    let cancel_flag = match request_cancel_flag_on_node(&runtime, &request.run_id, &req_id).await {
        Ok(flag) => flag,
        Err(err) => {
            eprintln!("failed to get cancel flag for channel receiver request {req_id}: {err}");
            return accepted;
        }
    };

    tokio::spawn(async move {
        let run_id = request.run_id.clone();
        let result = channel_receiver_on_node(&runtime, &req_id, request, cancel_flag).await;
        match result {
            Ok(result) => {
                if let Err(err) = finish_request_on_node(
                    &runtime,
                    &run_id,
                    &req_id,
                    RequestResult::ChannelReceiver(result),
                )
                .await
                {
                    eprintln!("failed to finish channel receiver request {req_id}: {err}");
                }
            }
            Err(message) => {
                if let Err(err) = fail_request_on_node(&runtime, &run_id, &req_id, message).await {
                    eprintln!("failed to fail channel receiver request {req_id}: {err}");
                }
            }
        }
    });

    accepted
}

async fn paced_channel_send_on_node(
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    request: StartPacedChannelSendRequest,
) -> Result<ChannelSendResult, String> {
    if request.count == 0 {
        return Err("channel send count must be greater than zero".to_string());
    }
    if request.object_size_bytes < HEADER_LEN as u64 {
        return Err(format!(
            "channel payload object_size_bytes must be at least {HEADER_LEN}"
        ));
    }
    if !request.target_ops_per_s.is_finite() || request.target_ops_per_s <= 0.0 {
        return Err("target_ops_per_s must be a finite positive number".to_string());
    }
    if request.max_in_flight == 0 {
        return Err("max_in_flight must be greater than zero".to_string());
    }

    let (run_dir, sender) = {
        let runtime = runtime.lock().await;
        let current = current_expr(&runtime, &request.run_id)?;
        (
            current.run_dir.clone(),
            current
                .sender
                .as_ref()
                .ok_or_else(|| "channel send requires initialized sender state".to_string())?
                .handle
                .clone(),
        )
    };
    let payload_dir = run_dir
        .join("channel-payloads")
        .join(unique_batch_id("send"));
    tokio::fs::create_dir_all(&payload_dir)
        .await
        .map_err(|err| {
            format!(
                "failed to create channel payload dir {}: {err}",
                payload_dir.display()
            )
        })?;

    eprintln!(
        "channel sender prestage start: run_id={} count={} object_size_bytes={}",
        request.run_id, request.count, request.object_size_bytes
    );
    let prestage_started = Instant::now();
    let payload_paths =
        prestage_payloads(&payload_dir, request.count, request.object_size_bytes).await?;
    let prestage_payload_ms = prestage_started.elapsed().as_secs_f64() * 1000.0;
    eprintln!(
        "channel sender prestage done: run_id={} payload_count={} elapsed_ms={:.3}",
        request.run_id,
        payload_paths.len(),
        prestage_payload_ms
    );

    let tasks = build_send_tasks(
        sender.clone(),
        payload_paths,
        request.object_size_bytes,
        &request.run_id,
    );
    eprintln!(
        "channel sender paced start: run_id={} task_count={} target_ops_per_s={} max_in_flight={}",
        request.run_id,
        tasks.len(),
        request.target_ops_per_s,
        request.max_in_flight
    );
    let paced = run_paced_tasks(
        tasks,
        PacedTaskRunConfig {
            target_ops_per_s: request.target_ops_per_s,
            max_in_flight: request.max_in_flight,
            pacer_core_id: None,
        },
    )
    .await?;
    eprintln!(
        "channel sender paced done: run_id={} completed={} failed={} wall_time_ms={:.3}",
        request.run_id, paced.completed_tasks, paced.failed_tasks, paced.wall_time_ms
    );
    let mut failure_messages = paced
        .failures
        .iter()
        .take(MAX_RETAINED_FAILURES)
        .map(|failure| format!("index={}: {}", failure.index, failure.message))
        .collect::<Vec<_>>();
    let close_started = Instant::now();
    eprintln!("channel sender close start: run_id={}", request.run_id);
    let close_result = sender.close().await;
    let close_elapsed_ms = close_started.elapsed().as_secs_f64() * 1000.0;
    match close_result {
        Ok(_) => {
            eprintln!(
                "channel sender close done: run_id={} elapsed_ms={:.3}",
                request.run_id, close_elapsed_ms
            );
        }
        Err(err) => {
            let detail = error_detail(&err);
            eprintln!(
                "channel sender close failed: run_id={} detail={}",
                request.run_id, detail
            );
            if failure_messages.len() < MAX_RETAINED_FAILURES {
                failure_messages.push(format!("close failed: {detail}"));
            }
        }
    }
    let sent_count = paced.completed_tasks.saturating_sub(paced.failed_tasks);
    let total_bytes = request.object_size_bytes.saturating_mul(sent_count as u64);

    let payload_dir_string = path_to_string(&payload_dir);
    let mut runtime = runtime.lock().await;
    let current = current_expr_mut(&mut runtime, &request.run_id)?;
    put_artifact(
        current,
        "channel_payload_dir",
        "channel_payload_dir",
        serde_json::Value::String(payload_dir_string.clone()),
    );
    put_metric(
        current,
        "channel_send_total_bytes",
        total_bytes as f64,
        "bytes",
    );
    current.phase = "channel_send_finished".to_string();
    runtime.generation += 1;

    Ok(ChannelSendResult {
        sent_count,
        total_bytes,
        close_elapsed_ms,
        prestage_payload_ms,
        payload_strategy: request.payload_strategy,
        payload_dir: payload_dir_string,
        paced,
        failure_messages,
    })
}

async fn channel_receiver_on_node(
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    req_id: &str,
    request: StartChannelReceiverRequest,
    cancel_flag: Arc<AtomicBool>,
) -> Result<ChannelReceiverResult, String> {
    if !request.poll_target_ops_per_s.is_finite() || request.poll_target_ops_per_s <= 0.0 {
        return Err("poll_target_ops_per_s must be a finite positive number".to_string());
    }
    if request.materialize_concurrency == 0 {
        return Err("materialize_concurrency must be greater than zero".to_string());
    }
    if request.poll_concurrency == 0 {
        return Err("poll_concurrency must be greater than zero".to_string());
    }

    let (run_dir, receiver) = {
        let runtime = runtime.lock().await;
        let current = current_expr(&runtime, &request.run_id)?;
        (
            current.run_dir.clone(),
            current
                .receiver
                .as_ref()
                .ok_or_else(|| "channel receiver requires initialized receiver state".to_string())?
                .handle
                .clone(),
        )
    };
    let output_dir = run_dir
        .join("channel-materialized")
        .join(sanitize_path_part(&request.output_subdir))
        .join(unique_batch_id("recv"));
    tokio::fs::create_dir_all(&output_dir)
        .await
        .map_err(|err| {
            format!(
                "failed to create channel materialize dir {}: {err}",
                output_dir.display()
            )
        })?;

    mark_request_ready_on_node(runtime, &request.run_id, req_id).await?;

    let started_unix_ns = unix_now_ns();
    let started_at = Instant::now();
    let poll_period = Duration::from_secs_f64(1.0 / request.poll_target_ops_per_s)
        .min(Duration::from_millis(MAX_POLL_PERIOD_MS));
    let mut next_poll_at = tokio::time::Instant::now();
    let deadline = request
        .max_runtime_ms
        .map(|ms| tokio::time::Instant::now() + Duration::from_millis(ms));
    let eof_seen = Arc::new(AtomicBool::new(false));
    let mut delivered_count = 0usize;
    let mut delivered_bytes = 0u64;
    let mut empty_polls = 0u64;
    let mut transient_poll_errors = 0u64;
    let mut timed_out = false;
    let mut cancelled = false;
    let mut eof_seq = None;
    let mut eof_elapsed_ms = None;
    let mut pop_tasks = JoinSet::new();
    let mut pending_materialize = VecDeque::new();
    let mut materialize_tasks = JoinSet::new();
    let mut delivery_samples = Vec::new();
    let mut materialize_samples = Vec::new();
    let mut failure_messages = Vec::new();

    while !eof_seen.load(Ordering::Relaxed) {
        drain_ready_receiver_pop_results(
            &mut pop_tasks,
            &mut pending_materialize,
            &mut empty_polls,
            &mut transient_poll_errors,
            &mut eof_seq,
            &mut eof_elapsed_ms,
            &eof_seen,
        )?;
        drain_ready_receiver_materialize_results(
            &mut materialize_tasks,
            &mut delivered_count,
            &mut delivered_bytes,
            &mut delivery_samples,
            &mut materialize_samples,
            &mut failure_messages,
        )?;
        spawn_pending_materialize_tasks(
            &mut pending_materialize,
            &mut materialize_tasks,
            request.materialize_concurrency,
            &output_dir,
        );
        if cancel_flag.load(Ordering::Relaxed) {
            cancelled = true;
            if failure_messages.len() < MAX_RETAINED_FAILURES {
                failure_messages.push("receiver cancelled before EOF".to_string());
            }
            break;
        }
        if deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
            timed_out = true;
            if failure_messages.len() < MAX_RETAINED_FAILURES {
                failure_messages.push("receiver timed out before EOF".to_string());
            }
            break;
        }
        while pop_tasks.len() >= request.poll_concurrency {
            collect_receiver_pop_result(
                &mut pop_tasks,
                &mut pending_materialize,
                &mut empty_polls,
                &mut transient_poll_errors,
                &mut eof_seq,
                &mut eof_elapsed_ms,
                &eof_seen,
            )
            .await?;
            drain_ready_receiver_materialize_results(
                &mut materialize_tasks,
                &mut delivered_count,
                &mut delivered_bytes,
                &mut delivery_samples,
                &mut materialize_samples,
                &mut failure_messages,
            )?;
            spawn_pending_materialize_tasks(
                &mut pending_materialize,
                &mut materialize_tasks,
                request.materialize_concurrency,
                &output_dir,
            );
        }
        tokio::time::sleep_until(next_poll_at).await;
        next_poll_at += poll_period;
        if cancel_flag.load(Ordering::Relaxed) {
            cancelled = true;
            if failure_messages.len() < MAX_RETAINED_FAILURES {
                failure_messages.push("receiver cancelled before EOF".to_string());
            }
            break;
        }
        if eof_seen.load(Ordering::Relaxed) {
            break;
        }
        let receiver = receiver.clone();
        let eof_seen = eof_seen.clone();
        let cancel_flag = cancel_flag.clone();
        pop_tasks.spawn(async move {
            receiver_pop_task(receiver, started_at, eof_seen, cancel_flag).await
        });
    }

    while !pop_tasks.is_empty() || !pending_materialize.is_empty() || !materialize_tasks.is_empty()
    {
        drain_ready_receiver_pop_results(
            &mut pop_tasks,
            &mut pending_materialize,
            &mut empty_polls,
            &mut transient_poll_errors,
            &mut eof_seq,
            &mut eof_elapsed_ms,
            &eof_seen,
        )?;
        drain_ready_receiver_materialize_results(
            &mut materialize_tasks,
            &mut delivered_count,
            &mut delivered_bytes,
            &mut delivery_samples,
            &mut materialize_samples,
            &mut failure_messages,
        )?;
        spawn_pending_materialize_tasks(
            &mut pending_materialize,
            &mut materialize_tasks,
            request.materialize_concurrency,
            &output_dir,
        );
        if !pop_tasks.is_empty() {
            collect_receiver_pop_result(
                &mut pop_tasks,
                &mut pending_materialize,
                &mut empty_polls,
                &mut transient_poll_errors,
                &mut eof_seq,
                &mut eof_elapsed_ms,
                &eof_seen,
            )
            .await?;
        } else if !materialize_tasks.is_empty() {
            collect_receiver_materialize_result(
                &mut materialize_tasks,
                &mut delivered_count,
                &mut delivered_bytes,
                &mut delivery_samples,
                &mut materialize_samples,
                &mut failure_messages,
            )
            .await?;
        }
    }

    receiver
        .close()
        .await
        .map_err(|err| format!("failed to close receiver: {err}"))?;
    let elapsed_ms = started_at.elapsed().as_secs_f64() * 1000.0;
    let finished_unix_ns = unix_now_ns();
    let output_dir_string = path_to_string(&output_dir);

    let mut runtime = runtime.lock().await;
    let current = current_expr_mut(&mut runtime, &request.run_id)?;
    put_artifact(
        current,
        "channel_materialized_dir",
        "channel_materialized_dir",
        serde_json::Value::String(output_dir_string.clone()),
    );
    put_metric(
        current,
        "channel_receiver_delivered_count",
        delivered_count as f64,
        "ops",
    );
    current.phase = "channel_receiver_finished".to_string();
    runtime.generation += 1;

    Ok(ChannelReceiverResult {
        delivered_count,
        delivered_bytes,
        empty_polls,
        transient_poll_errors,
        timed_out,
        cancelled,
        eof_seq,
        eof_elapsed_ms,
        elapsed_ms,
        started_unix_ns,
        finished_unix_ns,
        materialized_dir: output_dir_string,
        poll_target_ops_per_s: request.poll_target_ops_per_s,
        poll_concurrency: request.poll_concurrency,
        materialize_concurrency: request.materialize_concurrency,
        delivery_latency: LatencySummary::from_samples(&delivery_samples),
        materialize_latency: LatencySummary::from_samples(&materialize_samples),
        delivery_latency_samples_ms: delivery_samples,
        materialize_latency_samples_ms: materialize_samples,
        failure_messages,
    })
}

enum ReceiverPopTaskResult {
    Empty,
    TransientError(String),
    Eof { seq: i64, elapsed_ms: f64 },
    Elem { seq: i64, ptr: BlobElemPtr },
}

enum ReceiverMaterializeTaskResult {
    Delivered {
        bytes: u64,
        delivery_ms: f64,
        materialize_ms: f64,
    },
    Failure(String),
}

async fn receiver_pop_task(
    receiver: lambda_channel::metadata_blob_channel::receiver::AsyncMetadataBlobRecv,
    started_at: Instant,
    eof_seen: Arc<AtomicBool>,
    cancel_flag: Arc<AtomicBool>,
) -> Result<ReceiverPopTaskResult, String> {
    if eof_seen.load(Ordering::Relaxed) || cancel_flag.load(Ordering::Relaxed) {
        return Ok(ReceiverPopTaskResult::Empty);
    }
    match receiver.try_pop().await {
        Ok(None) => Ok(ReceiverPopTaskResult::Empty),
        Ok(Some(Popped::Eof(seq))) => {
            eof_seen.store(true, Ordering::Relaxed);
            Ok(ReceiverPopTaskResult::Eof {
                seq,
                elapsed_ms: started_at.elapsed().as_secs_f64() * 1000.0,
            })
        }
        Ok(Some(Popped::Elem { seq, ptr })) => Ok(ReceiverPopTaskResult::Elem { seq, ptr }),
        Err(err) if is_transient_try_pop_error(&err) => Ok(ReceiverPopTaskResult::TransientError(
            format!("transient receiver try_pop failed: {err}"),
        )),
        Err(err) => Err(format!("receiver try_pop failed: {err}")),
    }
}

async fn receiver_materialize_task(
    output_dir: std::path::PathBuf,
    seq: i64,
    mut ptr: BlobElemPtr,
) -> Result<ReceiverMaterializeTaskResult, String> {
    let dst = output_dir.join(format!("materialized_{seq:08}.bin"));
    let dst_string = path_to_string(&dst);
    let materialize_started = Instant::now();
    let path = match ptr
        .get_with_options(
            Some(dst_string.as_str()),
            BlobGetOptions { prefer_link: true },
        )
        .await
    {
        Ok(path) => path,
        Err(err) => {
            return Ok(ReceiverMaterializeTaskResult::Failure(format!(
                "materialize seq={seq} failed: {err}"
            )));
        }
    };
    let materialize_ms = materialize_started.elapsed().as_secs_f64() * 1000.0;
    let (_payload_index, send_unix_ns, object_size_bytes) = match read_payload_header(&path).await {
        Ok(header) => header,
        Err(err) => return Ok(ReceiverMaterializeTaskResult::Failure(err)),
    };
    let now_ns = unix_now_ns();
    let delivery_ms = now_ns.saturating_sub(send_unix_ns).min(u64::MAX / 2) as f64 / 1_000_000.0;
    Ok(ReceiverMaterializeTaskResult::Delivered {
        bytes: object_size_bytes,
        delivery_ms,
        materialize_ms,
    })
}

#[allow(clippy::too_many_arguments)]
async fn collect_receiver_pop_result(
    pop_tasks: &mut JoinSet<Result<ReceiverPopTaskResult, String>>,
    pending_materialize: &mut VecDeque<(i64, BlobElemPtr)>,
    empty_polls: &mut u64,
    transient_poll_errors: &mut u64,
    eof_seq: &mut Option<i64>,
    eof_elapsed_ms: &mut Option<f64>,
    eof_seen: &AtomicBool,
) -> Result<(), String> {
    let Some(result) = pop_tasks.join_next().await else {
        return Ok(());
    };
    handle_receiver_pop_result(
        result,
        pending_materialize,
        empty_polls,
        transient_poll_errors,
        eof_seq,
        eof_elapsed_ms,
        eof_seen,
    )
}

#[allow(clippy::too_many_arguments)]
fn drain_ready_receiver_pop_results(
    pop_tasks: &mut JoinSet<Result<ReceiverPopTaskResult, String>>,
    pending_materialize: &mut VecDeque<(i64, BlobElemPtr)>,
    empty_polls: &mut u64,
    transient_poll_errors: &mut u64,
    eof_seq: &mut Option<i64>,
    eof_elapsed_ms: &mut Option<f64>,
    eof_seen: &AtomicBool,
) -> Result<(), String> {
    while let Some(result) = pop_tasks.try_join_next() {
        handle_receiver_pop_result(
            result,
            pending_materialize,
            empty_polls,
            transient_poll_errors,
            eof_seq,
            eof_elapsed_ms,
            eof_seen,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn handle_receiver_pop_result(
    result: Result<Result<ReceiverPopTaskResult, String>, tokio::task::JoinError>,
    pending_materialize: &mut VecDeque<(i64, BlobElemPtr)>,
    empty_polls: &mut u64,
    transient_poll_errors: &mut u64,
    eof_seq: &mut Option<i64>,
    eof_elapsed_ms: &mut Option<f64>,
    eof_seen: &AtomicBool,
) -> Result<(), String> {
    match result.map_err(|err| format!("receiver pop task join failed: {err}"))? {
        Ok(ReceiverPopTaskResult::Empty) => {
            *empty_polls += 1;
        }
        Ok(ReceiverPopTaskResult::TransientError(_err)) => {
            *transient_poll_errors += 1;
        }
        Ok(ReceiverPopTaskResult::Eof { seq, elapsed_ms }) => {
            eof_seen.store(true, Ordering::Relaxed);
            if eof_seq.is_none() {
                *eof_seq = Some(seq);
                *eof_elapsed_ms = Some(elapsed_ms);
            }
        }
        Ok(ReceiverPopTaskResult::Elem { seq, ptr }) => {
            pending_materialize.push_back((seq, ptr));
        }
        Err(err) => return Err(err),
    }
    Ok(())
}

fn spawn_pending_materialize_tasks(
    pending_materialize: &mut VecDeque<(i64, BlobElemPtr)>,
    materialize_tasks: &mut JoinSet<Result<ReceiverMaterializeTaskResult, String>>,
    materialize_concurrency: usize,
    output_dir: &Path,
) {
    while materialize_tasks.len() < materialize_concurrency {
        let Some((seq, ptr)) = pending_materialize.pop_front() else {
            break;
        };
        let output_dir = output_dir.to_path_buf();
        materialize_tasks
            .spawn(async move { receiver_materialize_task(output_dir, seq, ptr).await });
    }
}

async fn collect_receiver_materialize_result(
    tasks: &mut JoinSet<Result<ReceiverMaterializeTaskResult, String>>,
    delivered_count: &mut usize,
    delivered_bytes: &mut u64,
    delivery_samples: &mut Vec<f64>,
    materialize_samples: &mut Vec<f64>,
    failure_messages: &mut Vec<String>,
) -> Result<(), String> {
    let Some(result) = tasks.join_next().await else {
        return Ok(());
    };
    handle_receiver_materialize_result(
        result,
        delivered_count,
        delivered_bytes,
        delivery_samples,
        materialize_samples,
        failure_messages,
    )
}

fn drain_ready_receiver_materialize_results(
    tasks: &mut JoinSet<Result<ReceiverMaterializeTaskResult, String>>,
    delivered_count: &mut usize,
    delivered_bytes: &mut u64,
    delivery_samples: &mut Vec<f64>,
    materialize_samples: &mut Vec<f64>,
    failure_messages: &mut Vec<String>,
) -> Result<(), String> {
    while let Some(result) = tasks.try_join_next() {
        handle_receiver_materialize_result(
            result,
            delivered_count,
            delivered_bytes,
            delivery_samples,
            materialize_samples,
            failure_messages,
        )?;
    }
    Ok(())
}

fn handle_receiver_materialize_result(
    result: Result<Result<ReceiverMaterializeTaskResult, String>, tokio::task::JoinError>,
    delivered_count: &mut usize,
    delivered_bytes: &mut u64,
    delivery_samples: &mut Vec<f64>,
    materialize_samples: &mut Vec<f64>,
    failure_messages: &mut Vec<String>,
) -> Result<(), String> {
    match result.map_err(|err| format!("materialize task join failed: {err}"))? {
        Ok(ReceiverMaterializeTaskResult::Delivered {
            bytes,
            delivery_ms,
            materialize_ms,
        }) => {
            *delivered_count += 1;
            *delivered_bytes = (*delivered_bytes).saturating_add(bytes);
            delivery_samples.push(delivery_ms);
            materialize_samples.push(materialize_ms);
        }
        Ok(ReceiverMaterializeTaskResult::Failure(err)) => {
            if failure_messages.len() < MAX_RETAINED_FAILURES {
                failure_messages.push(err);
            }
        }
        Err(err) => return Err(err),
    }
    Ok(())
}

fn is_transient_try_pop_error(err: &impl std::fmt::Display) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    message.contains("throttling")
        || message.contains("throughput")
        || message.contains("requestlimit")
        || message.contains("request limit")
        || message.contains("timeout")
        || message.contains("temporar")
        || message.contains("failed to list elements")
        || message.contains("failed to mark element consumed")
}

async fn prestage_payloads(
    payload_dir: &Path,
    count: usize,
    object_size_bytes: u64,
) -> Result<Vec<String>, String> {
    let mut paths = Vec::with_capacity(count);
    for index in 0..count {
        let path = payload_dir.join(format!("payload_{index:08}.bin"));
        let mut file = tokio::fs::File::create(&path)
            .await
            .map_err(|err| format!("failed to create payload {}: {err}", path.display()))?;
        file.write_all(&[0u8; HEADER_LEN])
            .await
            .map_err(|err| format!("failed to reserve header in {}: {err}", path.display()))?;
        file.set_len(object_size_bytes)
            .await
            .map_err(|err| format!("failed to size payload {}: {err}", path.display()))?;
        paths.push(path_to_string(&path));
    }
    Ok(paths)
}

fn build_send_tasks(
    sender: lambda_channel::metadata_blob_channel::sender::AsyncMetadataBlobSender,
    payload_paths: Vec<String>,
    object_size_bytes: u64,
    run_id: &str,
) -> Vec<PacedTask> {
    let run_hash = stable_run_hash(run_id);
    payload_paths
        .into_iter()
        .enumerate()
        .map(|(index, path)| {
            let sender = sender.clone();
            boxed_task(async move {
                let send_unix_ns = unix_now_ns();
                patch_payload_header(
                    &path,
                    index as i64,
                    send_unix_ns,
                    object_size_bytes,
                    run_hash,
                )
                .await?;
                let ptr = LocalFileElemPtr::new(path)
                    .map_err(|err| format!("failed to create local file element ptr: {err}"))?;
                sender.push(Box::new(ptr)).await.map(|_| ()).map_err(|err| {
                    eprintln!(
                        "channel push failed: task={} detail={}",
                        index,
                        error_detail(&err)
                    );
                    format!("channel push failed for task={index}: {err}")
                })
            })
        })
        .collect()
}

fn error_detail(err: &(impl Error + Debug + 'static)) -> String {
    let mut parts = vec![err.to_string(), format!("{err:?}")];
    let mut source = err.source();
    while let Some(inner) = source {
        parts.push(inner.to_string());
        source = inner.source();
    }
    parts.dedup();
    parts.join(" | caused by: ")
}

async fn patch_payload_header(
    path: &str,
    seq: i64,
    send_unix_ns: u64,
    object_size_bytes: u64,
    run_hash: u32,
) -> Result<(), String> {
    let mut header = [0u8; HEADER_LEN];
    header[0..4].copy_from_slice(HEADER_MAGIC);
    header[4..12].copy_from_slice(&seq.to_le_bytes());
    header[12..20].copy_from_slice(&send_unix_ns.to_le_bytes());
    header[20..28].copy_from_slice(&object_size_bytes.to_le_bytes());
    header[28..32].copy_from_slice(&(HEADER_MARKER ^ run_hash).to_le_bytes());
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .await
        .map_err(|err| format!("failed to open payload {path} for header patch: {err}"))?;
    file.seek(std::io::SeekFrom::Start(0))
        .await
        .map_err(|err| format!("failed to seek payload {path}: {err}"))?;
    file.write_all(&header)
        .await
        .map_err(|err| format!("failed to write payload header {path}: {err}"))?;
    file.flush()
        .await
        .map_err(|err| format!("failed to flush payload header {path}: {err}"))?;
    Ok(())
}

async fn read_payload_header(path: &str) -> Result<(i64, u64, u64), String> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|err| format!("failed to open materialized payload {path}: {err}"))?;
    let mut bytes = [0u8; HEADER_LEN];
    file.read_exact(&mut bytes)
        .await
        .map_err(|err| format!("failed to read materialized payload header {path}: {err}"))?;
    if &bytes[0..4] != HEADER_MAGIC {
        return Err(format!("materialized payload {path} has invalid magic"));
    }
    let seq = i64::from_le_bytes(bytes[4..12].try_into().expect("slice length checked"));
    let send_unix_ns = u64::from_le_bytes(bytes[12..20].try_into().expect("slice length checked"));
    let object_size_bytes =
        u64::from_le_bytes(bytes[20..28].try_into().expect("slice length checked"));
    Ok((seq, send_unix_ns, object_size_bytes))
}

fn stable_run_hash(run_id: &str) -> u32 {
    let mut hash = 2166136261u32;
    for byte in run_id.bytes() {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(16777619);
    }
    hash
}

fn unique_batch_id(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    format!("{prefix}-{nanos}")
}

fn unix_now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn sanitize_path_part(input: &str) -> String {
    let mut out = String::with_capacity(input.len().max(1));
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
            out.push(ch);
        } else {
            out.push('-');
        }
    }
    if out.is_empty() {
        "receiver".to_string()
    } else {
        out
    }
}
