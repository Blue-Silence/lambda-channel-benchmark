use std::sync::Arc;
use std::time::{Duration, Instant};

use tarpc::context;
use tokio::sync::Mutex;
use tokio::time::sleep;

use crate::config::{InstanceConfig, InstancesConfig};
use crate::rpc::client::{connect_node, ensure_finished_result};
use crate::rpc::protocol::{
    AcceptedResponse, BeginExprRequest, CancelRequestRequest, ChannelReceiverResult,
    ChannelReceiverSamplesChunkRequest, ChannelSendResult, InitReceiverRequest, InitSenderRequest,
    PollRequestRequest, PollRequestResponse, RequestResult, ResetExprRequest,
    StartChannelReceiverRequest, StartPacedChannelSendRequest,
};
use crate::rpc::server::channel::{
    submit_channel_receiver_on_node, submit_paced_channel_send_on_node,
};
use crate::rpc::server::state::{
    begin_expr_on_node, cancel_request_on_node, channel_receiver_samples_chunk_on_node,
    init_receiver_on_node, init_sender_on_node, poll_request_on_node, reset_expr_on_node,
    response_to_result, NodeRuntimeState,
};

const REQUEST_POLL_INTERVAL: Duration = Duration::from_millis(500);
const RESULT_POLL_INTERVAL: Duration = Duration::from_secs(5);
const RECEIVER_SAMPLE_CHUNK_SIZE: usize = 16 * 1024;

pub(super) async fn begin_on_target(
    instances: &InstancesConfig,
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    target_id: &str,
    run_id: &str,
    force_reset: bool,
) -> Result<(), String> {
    if target_id == instance.id {
        begin_expr_on_node(
            instance,
            runtime,
            BeginExprRequest {
                run_id: run_id.to_string(),
                force_reset,
            },
        )
        .await
    } else {
        let target = instances
            .find_instance(target_id)
            .ok_or_else(|| format!("unknown target instance id: {target_id}"))?;
        let response = connect_node(target)
            .await?
            .begin_expr(
                context::current(),
                BeginExprRequest {
                    run_id: run_id.to_string(),
                    force_reset,
                },
            )
            .await
            .map_err(|err| format!("begin_expr RPC failed for {}: {err}", target.id))?;
        response_to_result(response)
    }
}

pub(super) async fn reset_on_target(
    instances: &InstancesConfig,
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    target_id: &str,
    run_id: &str,
    force_reset: bool,
) -> Result<(), String> {
    if target_id == instance.id {
        reset_expr_on_node(
            runtime,
            ResetExprRequest {
                run_id: run_id.to_string(),
                force_reset,
            },
        )
        .await
    } else {
        let target = instances
            .find_instance(target_id)
            .ok_or_else(|| format!("unknown target instance id: {target_id}"))?;
        let response = connect_node(target)
            .await?
            .reset_expr(
                context::current(),
                ResetExprRequest {
                    run_id: run_id.to_string(),
                    force_reset,
                },
            )
            .await
            .map_err(|err| format!("reset_expr RPC failed for {}: {err}", target.id))?;
        response_to_result(response)
    }
}

pub(super) async fn init_sender_on_target(
    instances: &InstancesConfig,
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    target_id: &str,
    request: InitSenderRequest,
) -> Result<(), String> {
    if target_id == instance.id {
        init_sender_on_node(instance, runtime, request).await
    } else {
        let target = instances
            .find_instance(target_id)
            .ok_or_else(|| format!("unknown target instance id: {target_id}"))?;
        let response = connect_node(target)
            .await?
            .init_sender(context::current(), request)
            .await
            .map_err(|err| format!("init_sender RPC failed for {}: {err}", target.id))?;
        response_to_result(response)
    }
}

pub(super) async fn init_receiver_on_target(
    instances: &InstancesConfig,
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    target_id: &str,
    request: InitReceiverRequest,
) -> Result<(), String> {
    if target_id == instance.id {
        init_receiver_on_node(instance, runtime, request).await
    } else {
        let target = instances
            .find_instance(target_id)
            .ok_or_else(|| format!("unknown target instance id: {target_id}"))?;
        let response = connect_node(target)
            .await?
            .init_receiver(context::current(), request)
            .await
            .map_err(|err| format!("init_receiver RPC failed for {}: {err}", target.id))?;
        response_to_result(response)
    }
}

pub(super) async fn start_receiver_on_target(
    instances: &InstancesConfig,
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    target_id: &str,
    request: StartChannelReceiverRequest,
) -> Result<AcceptedResponse, String> {
    if target_id == instance.id {
        Ok(submit_channel_receiver_on_node(&instance.id, runtime.clone(), request).await)
    } else {
        let target = instances
            .find_instance(target_id)
            .ok_or_else(|| format!("unknown target instance id: {target_id}"))?;
        connect_node(target)
            .await?
            .start_channel_receiver(context::current(), request)
            .await
            .map_err(|err| format!("start_channel_receiver RPC failed for {}: {err}", target.id))
    }
}

pub(super) async fn wait_receiver_ready_on_target(
    instances: &InstancesConfig,
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    target_id: &str,
    run_id: &str,
    barrier_timeout_ms: u64,
    accepted: &AcceptedResponse,
) -> Result<(), String> {
    let req_id = req_id(target_id, accepted)?;
    let deadline = Instant::now() + Duration::from_millis(barrier_timeout_ms);
    loop {
        let response = poll_request_on_target(
            instances,
            instance,
            runtime,
            target_id,
            PollRequestRequest {
                run_id: run_id.to_string(),
                req_id: req_id.clone(),
                include_result: false,
            },
        )
        .await?;
        if response.ready {
            return Ok(());
        }
        ensure_finished_result(
            target_id,
            &req_id,
            response.status,
            response.result,
            response.message,
        )?;
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for receiver readiness target {target_id} req_id={req_id}"
            ));
        }
        sleep(REQUEST_POLL_INTERVAL).await;
    }
}

pub(super) async fn finish_receiver_on_target(
    instances: &InstancesConfig,
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    target_id: &str,
    run_id: &str,
    barrier_timeout_ms: u64,
    accepted: AcceptedResponse,
) -> Result<ChannelReceiverResult, String> {
    let req_id = req_id(target_id, &accepted)?;
    let result = wait_for_request_result_on_target(
        instances,
        instance,
        runtime,
        target_id,
        run_id,
        barrier_timeout_ms,
        accepted,
    )
    .await?;
    match result {
        RequestResult::ChannelReceiver(mut result) => {
            let (delivery, materialize) = fetch_receiver_samples_on_target(
                instances, instance, runtime, target_id, run_id, &req_id,
            )
            .await?;
            result.delivery_latency_samples_ms = delivery;
            result.materialize_latency_samples_ms = materialize;
            Ok(result)
        }
        other => Err(format!(
            "target {target_id} returned unexpected result for channel receiver: {other:?}"
        )),
    }
}

pub(super) async fn cancel_receiver_on_target(
    instances: &InstancesConfig,
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    target_id: &str,
    run_id: &str,
    accepted: &AcceptedResponse,
) -> Result<(), String> {
    let req_id = req_id(target_id, accepted)?;
    let request = CancelRequestRequest {
        run_id: run_id.to_string(),
        req_id,
    };
    if target_id == instance.id {
        cancel_request_on_node(runtime, request).await
    } else {
        let target = instances
            .find_instance(target_id)
            .ok_or_else(|| format!("unknown target instance id: {target_id}"))?;
        let response = connect_node(target)
            .await?
            .cancel_request(context::current(), request)
            .await
            .map_err(|err| format!("cancel_request RPC failed for {}: {err}", target.id))?;
        response_to_result(response)
    }
}

async fn fetch_receiver_samples_on_target(
    instances: &InstancesConfig,
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    target_id: &str,
    run_id: &str,
    req_id: &str,
) -> Result<(Vec<f64>, Vec<f64>), String> {
    let mut offset = 0usize;
    let mut delivery = Vec::new();
    let mut materialize = Vec::new();
    loop {
        let response = channel_receiver_samples_chunk_on_target(
            instances,
            instance,
            runtime,
            target_id,
            ChannelReceiverSamplesChunkRequest {
                run_id: run_id.to_string(),
                req_id: req_id.to_string(),
                offset,
                limit: RECEIVER_SAMPLE_CHUNK_SIZE,
            },
        )
        .await?;
        if !response.ok {
            return Err(format!(
                "target {target_id} rejected channel receiver samples chunk: {}",
                response.message
            ));
        }
        if !matches!(
            response.status,
            crate::rpc::protocol::RequestStatus::Finished
        ) {
            return Err(format!(
                "target {target_id} channel receiver samples are not ready: {:?}: {}",
                response.status, response.message
            ));
        }
        delivery.extend(response.delivery_latency_samples_ms);
        materialize.extend(response.materialize_latency_samples_ms);
        if !response.has_more {
            if delivery.len() != response.delivery_count {
                return Err(format!(
                    "target {target_id} returned {} delivery samples, expected {}",
                    delivery.len(),
                    response.delivery_count
                ));
            }
            if materialize.len() != response.materialize_count {
                return Err(format!(
                    "target {target_id} returned {} materialize samples, expected {}",
                    materialize.len(),
                    response.materialize_count
                ));
            }
            return Ok((delivery, materialize));
        }
        offset = offset.saturating_add(RECEIVER_SAMPLE_CHUNK_SIZE);
    }
}

pub(super) async fn start_send_on_target(
    instances: &InstancesConfig,
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    target_id: &str,
    barrier_timeout_ms: u64,
    request: StartPacedChannelSendRequest,
) -> Result<ChannelSendResult, String> {
    let run_id = request.run_id.clone();
    let accepted = if target_id == instance.id {
        submit_paced_channel_send_on_node(&instance.id, runtime.clone(), request).await
    } else {
        let target = instances
            .find_instance(target_id)
            .ok_or_else(|| format!("unknown target instance id: {target_id}"))?;
        connect_node(target)
            .await?
            .start_paced_channel_send(context::current(), request)
            .await
            .map_err(|err| {
                format!(
                    "start_paced_channel_send RPC failed for {}: {err}",
                    target.id
                )
            })?
    };
    let result = wait_for_request_result_on_target(
        instances,
        instance,
        runtime,
        target_id,
        &run_id,
        barrier_timeout_ms,
        accepted,
    )
    .await?;
    match result {
        RequestResult::ChannelSend(result) => Ok(result),
        other => Err(format!(
            "target {target_id} returned unexpected result for channel send: {other:?}"
        )),
    }
}

async fn wait_for_request_result_on_target(
    instances: &InstancesConfig,
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    target_id: &str,
    run_id: &str,
    barrier_timeout_ms: u64,
    accepted: AcceptedResponse,
) -> Result<RequestResult, String> {
    if !accepted.ok {
        return Err(format!(
            "target {target_id} rejected request: {}",
            accepted.message
        ));
    }
    let req_id = req_id(target_id, &accepted)?;
    let deadline = Instant::now() + Duration::from_millis(barrier_timeout_ms);
    loop {
        let response = match poll_request_on_target(
            instances,
            instance,
            runtime,
            target_id,
            PollRequestRequest {
                run_id: run_id.to_string(),
                req_id: req_id.clone(),
                include_result: true,
            },
        )
        .await
        {
            Ok(response) => response,
            Err(err) if is_transient_poll_error(&err) && Instant::now() < deadline => {
                eprintln!(
                    "transient poll_request failure for target {target_id} req_id={req_id}: {err}; retrying"
                );
                sleep(RESULT_POLL_INTERVAL).await;
                continue;
            }
            Err(err) => return Err(err),
        };
        if let Some(result) = ensure_finished_result(
            target_id,
            &req_id,
            response.status,
            response.result,
            response.message,
        )? {
            return Ok(result);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for target {target_id} req_id={req_id} after {barrier_timeout_ms} ms"
            ));
        }
        sleep(RESULT_POLL_INTERVAL).await;
    }
}

async fn poll_request_on_target(
    instances: &InstancesConfig,
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    target_id: &str,
    request: PollRequestRequest,
) -> Result<PollRequestResponse, String> {
    if target_id == instance.id {
        Ok(poll_request_on_node(&instance.id, runtime, request).await)
    } else {
        let target = instances
            .find_instance(target_id)
            .ok_or_else(|| format!("unknown target instance id: {target_id}"))?;
        connect_node(target)
            .await?
            .poll_request(context::current(), request)
            .await
            .map_err(|err| format!("poll_request RPC failed for {}: {err}", target.id))
    }
}

async fn channel_receiver_samples_chunk_on_target(
    instances: &InstancesConfig,
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    target_id: &str,
    request: ChannelReceiverSamplesChunkRequest,
) -> Result<crate::rpc::protocol::ChannelReceiverSamplesChunkResponse, String> {
    if target_id == instance.id {
        Ok(channel_receiver_samples_chunk_on_node(&instance.id, runtime, request).await)
    } else {
        let target = instances
            .find_instance(target_id)
            .ok_or_else(|| format!("unknown target instance id: {target_id}"))?;
        connect_node(target)
            .await?
            .get_channel_receiver_samples_chunk(context::current(), request)
            .await
            .map_err(|err| {
                format!(
                    "get_channel_receiver_samples_chunk RPC failed for {}: {err}",
                    target.id
                )
            })
    }
}

fn req_id(target_id: &str, accepted: &AcceptedResponse) -> Result<String, String> {
    if !accepted.ok {
        return Err(format!(
            "target {target_id} rejected request: {}",
            accepted.message
        ));
    }
    accepted
        .req_id
        .clone()
        .ok_or_else(|| format!("target {target_id} accepted request without req_id"))
}

fn is_transient_poll_error(err: &str) -> bool {
    let err = err.to_ascii_lowercase();
    err.contains("deadline")
        || err.contains("timed out")
        || err.contains("timeout")
        || err.contains("connection reset")
        || err.contains("broken pipe")
        || err.contains("transport")
}
