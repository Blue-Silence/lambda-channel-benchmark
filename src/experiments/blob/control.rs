use std::sync::Arc;
use std::time::{Duration, Instant};

use tarpc::context;
use tokio::sync::Mutex;
use tokio::time::sleep;

use crate::config::{ExperimentSpec, InstanceConfig, InstancesConfig};
use crate::rpc::client::{connect_node, ensure_finished_result};
use crate::rpc::protocol::{
    AcceptedResponse, BeginExprRequest, BlobGetResult, BlobPutResult, GetBlobBatchRequest,
    InitBlobStoreRequest, PacedBlobGetRequest, PacedBlobGetResult, PollRequestRequest,
    PollRequestResponse, PrepareBlobGetAppendRequest, PrepareBlobGetBeginRequest,
    PrepareBlobGetFinishRequest, PutBlobBatchRequest, RequestResult, ResetExprRequest,
    RunBlobGetRequest, RunBlobGetResponse, StartPreparedBlobGetRequest,
};
use crate::rpc::server::blob::{
    init_blob_store_on_node, submit_blob_get_batch_on_node, submit_blob_put_batch_on_node,
    submit_paced_blob_get_on_node, submit_prepared_blob_get_on_node,
};
use crate::rpc::server::state::{
    begin_expr_on_node, poll_request_on_node, reset_expr_on_node, response_to_result,
    NodeRuntimeState,
};

pub(crate) struct BlobGetOutcome {
    pub(crate) prepared_count: usize,
    pub(crate) materialized_count: usize,
    pub(crate) total_bytes: u64,
    pub(crate) peer_put_elapsed_ms: f64,
    pub(crate) local_get_elapsed_ms: f64,
}

pub(crate) async fn run_blob_get_on_node(
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    instances: &InstancesConfig,
    request: RunBlobGetRequest,
) -> Result<BlobGetOutcome, String> {
    if request.peer_instance_id.trim().is_empty() {
        return Err("peer_instance_id must not be empty".to_string());
    }
    let peer = instances
        .find_instance(&request.peer_instance_id)
        .ok_or_else(|| format!("unknown peer instance id: {}", request.peer_instance_id))?
        .clone();

    begin_on_target(
        instances,
        instance,
        runtime,
        &peer.id,
        &request.run_id,
        request.force_reset,
    )
    .await?;
    init_blob_store_on_target(
        instances,
        instance,
        runtime,
        &peer.id,
        &request.run_id,
        &request.blob_store_backend,
    )
    .await?;
    let peer_put = put_blob_batch_on_target(
        instances,
        instance,
        runtime,
        &peer.id,
        request.barrier_timeout_ms,
        PutBlobBatchRequest {
            run_id: request.run_id.clone(),
            count: request.count,
            object_size_bytes: request.object_size_bytes,
        },
    )
    .await?;

    begin_expr_on_node(
        instance,
        runtime,
        BeginExprRequest {
            run_id: request.run_id.clone(),
            force_reset: request.force_reset && peer.id != instance.id,
        },
    )
    .await?;
    init_blob_store_on_node(
        instance,
        runtime,
        InitBlobStoreRequest {
            run_id: request.run_id.clone(),
            backend: request.blob_store_backend.clone(),
            root_dir: None,
            force_reinit: false,
            experiment: None,
            resource_id: None,
            create_remote_resources: false,
        },
    )
    .await?;
    let get = get_blob_batch_on_target(
        instances,
        instance,
        runtime,
        &instance.id,
        request.barrier_timeout_ms,
        GetBlobBatchRequest {
            run_id: request.run_id.clone(),
            refs: peer_put.refs.clone(),
        },
    )
    .await?;

    if request.cleanup {
        reset_on_target(
            instances,
            instance,
            runtime,
            &peer.id,
            &request.run_id,
            true,
        )
        .await?;
        reset_expr_on_node(
            runtime,
            ResetExprRequest {
                run_id: request.run_id,
                force_reset: true,
            },
        )
        .await?;
    }

    Ok(BlobGetOutcome {
        prepared_count: peer_put.count,
        materialized_count: get.count,
        total_bytes: get.total_bytes,
        peer_put_elapsed_ms: peer_put.elapsed_ms,
        local_get_elapsed_ms: get.elapsed_ms,
    })
}

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

pub(super) async fn init_blob_store_on_target(
    instances: &InstancesConfig,
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    target_id: &str,
    run_id: &str,
    backend: &str,
) -> Result<(), String> {
    init_blob_store_on_target_with_request(
        instances,
        instance,
        runtime,
        target_id,
        InitBlobStoreRequest {
            run_id: run_id.to_string(),
            backend: backend.to_string(),
            root_dir: None,
            force_reinit: false,
            experiment: None,
            resource_id: None,
            create_remote_resources: false,
        },
    )
    .await
}

pub(super) async fn init_blob_store_on_target_with_request(
    instances: &InstancesConfig,
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    target_id: &str,
    request: InitBlobStoreRequest,
) -> Result<(), String> {
    if target_id == instance.id {
        init_blob_store_on_node(instance, runtime, request).await
    } else {
        let target = instances
            .find_instance(target_id)
            .ok_or_else(|| format!("unknown target instance id: {target_id}"))?;
        let response = connect_node(target)
            .await?
            .init_blob_store(context::current(), request)
            .await
            .map_err(|err| format!("init_blob_store RPC failed for {}: {err}", target.id))?;
        response_to_result(response)
    }
}

pub(super) async fn put_blob_batch_on_target(
    instances: &InstancesConfig,
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    target_id: &str,
    barrier_timeout_ms: u64,
    request: PutBlobBatchRequest,
) -> Result<BlobPutResult, String> {
    let run_id = request.run_id.clone();
    let accepted = if target_id == instance.id {
        submit_blob_put_batch_on_node(&instance.id, runtime.clone(), request).await
    } else {
        let target = instances
            .find_instance(target_id)
            .ok_or_else(|| format!("unknown target instance id: {target_id}"))?;
        connect_node(target)
            .await?
            .put_blob_batch(context::current(), request)
            .await
            .map_err(|err| format!("put_blob_batch RPC failed for {}: {err}", target.id))?
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
        RequestResult::BlobPut(result) => Ok(result),
        other => Err(format!(
            "target {target_id} returned unexpected result for blob put: {other:?}"
        )),
    }
}

pub(super) async fn get_blob_batch_on_target(
    instances: &InstancesConfig,
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    target_id: &str,
    barrier_timeout_ms: u64,
    request: GetBlobBatchRequest,
) -> Result<BlobGetResult, String> {
    let run_id = request.run_id.clone();
    let accepted = if target_id == instance.id {
        submit_blob_get_batch_on_node(&instance.id, runtime.clone(), request).await
    } else {
        let target = instances
            .find_instance(target_id)
            .ok_or_else(|| format!("unknown target instance id: {target_id}"))?;
        connect_node(target)
            .await?
            .get_blob_batch(context::current(), request)
            .await
            .map_err(|err| format!("get_blob_batch RPC failed for {}: {err}", target.id))?
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
        RequestResult::BlobGet(result) => Ok(result),
        other => Err(format!(
            "target {target_id} returned unexpected result for blob get: {other:?}"
        )),
    }
}

#[allow(dead_code)]
pub(super) async fn paced_blob_get_on_target(
    instances: &InstancesConfig,
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    target_id: &str,
    barrier_timeout_ms: u64,
    request: PacedBlobGetRequest,
) -> Result<PacedBlobGetResult, String> {
    let run_id = request.run_id.clone();
    let accepted = if target_id == instance.id {
        submit_paced_blob_get_on_node(&instance.id, runtime.clone(), request).await
    } else {
        let target = instances
            .find_instance(target_id)
            .ok_or_else(|| format!("unknown target instance id: {target_id}"))?;
        connect_node(target)
            .await?
            .get_blob_paced(context::current(), request)
            .await
            .map_err(|err| format!("get_blob_paced RPC failed for {}: {err}", target.id))?
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
        RequestResult::PacedBlobGet(result) => Ok(result),
        other => Err(format!(
            "target {target_id} returned unexpected result for paced blob get: {other:?}"
        )),
    }
}

pub(super) async fn prepare_blob_get_begin_on_target(
    instances: &InstancesConfig,
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    target_id: &str,
    request: PrepareBlobGetBeginRequest,
) -> Result<(), String> {
    if target_id == instance.id {
        crate::rpc::server::blob::prepare_blob_get_begin_on_node(runtime, request).await
    } else {
        let target = instances
            .find_instance(target_id)
            .ok_or_else(|| format!("unknown target instance id: {target_id}"))?;
        let response = connect_node(target)
            .await?
            .prepare_blob_get_begin(context::current(), request)
            .await
            .map_err(|err| format!("prepare_blob_get_begin RPC failed for {}: {err}", target.id))?;
        response_to_result(response)
    }
}

pub(super) async fn prepare_blob_get_append_on_target(
    instances: &InstancesConfig,
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    target_id: &str,
    request: PrepareBlobGetAppendRequest,
) -> Result<(), String> {
    if target_id == instance.id {
        crate::rpc::server::blob::prepare_blob_get_append_on_node(runtime, request).await
    } else {
        let target = instances
            .find_instance(target_id)
            .ok_or_else(|| format!("unknown target instance id: {target_id}"))?;
        let response = connect_node(target)
            .await?
            .prepare_blob_get_append(context::current(), request)
            .await
            .map_err(|err| {
                format!(
                    "prepare_blob_get_append RPC failed for {}: {err}",
                    target.id
                )
            })?;
        response_to_result(response)
    }
}

pub(super) async fn prepare_blob_get_finish_on_target(
    instances: &InstancesConfig,
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    target_id: &str,
    request: PrepareBlobGetFinishRequest,
) -> Result<(), String> {
    if target_id == instance.id {
        crate::rpc::server::blob::prepare_blob_get_finish_on_node(runtime, request).await
    } else {
        let target = instances
            .find_instance(target_id)
            .ok_or_else(|| format!("unknown target instance id: {target_id}"))?;
        let response = connect_node(target)
            .await?
            .prepare_blob_get_finish(context::current(), request)
            .await
            .map_err(|err| {
                format!(
                    "prepare_blob_get_finish RPC failed for {}: {err}",
                    target.id
                )
            })?;
        response_to_result(response)
    }
}

pub(super) async fn start_prepared_blob_get_on_target(
    instances: &InstancesConfig,
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    target_id: &str,
    barrier_timeout_ms: u64,
    request: StartPreparedBlobGetRequest,
) -> Result<PacedBlobGetResult, String> {
    let run_id = request.run_id.clone();
    let accepted = if target_id == instance.id {
        submit_prepared_blob_get_on_node(&instance.id, runtime.clone(), request).await
    } else {
        let target = instances
            .find_instance(target_id)
            .ok_or_else(|| format!("unknown target instance id: {target_id}"))?;
        connect_node(target)
            .await?
            .start_prepared_blob_get(context::current(), request)
            .await
            .map_err(|err| {
                format!(
                    "start_prepared_blob_get RPC failed for {}: {err}",
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
        RequestResult::PacedBlobGet(result) => Ok(result),
        other => Err(format!(
            "target {target_id} returned unexpected result for prepared paced blob get: {other:?}"
        )),
    }
}

pub(super) async fn run_blob_get_on_target(
    instances: &InstancesConfig,
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    target_id: &str,
    request: RunBlobGetRequest,
) -> Result<RunBlobGetResponse, String> {
    if target_id == instance.id {
        let outcome = run_blob_get_on_node(instance, runtime, instances, request.clone()).await?;
        Ok(RunBlobGetResponse {
            ok: true,
            coordinator_id: instance.id.clone(),
            peer_instance_id: request.peer_instance_id,
            run_id: request.run_id,
            prepared_count: outcome.prepared_count,
            materialized_count: outcome.materialized_count,
            total_bytes: outcome.total_bytes,
            peer_put_elapsed_ms: outcome.peer_put_elapsed_ms,
            local_get_elapsed_ms: outcome.local_get_elapsed_ms,
            message: "blob get completed".to_string(),
        })
    } else {
        let target = instances
            .find_instance(target_id)
            .ok_or_else(|| format!("unknown target instance id: {target_id}"))?;
        let response = connect_node(target)
            .await?
            .run_blob_get(context::current(), request)
            .await
            .map_err(|err| format!("run_blob_get RPC failed for {}: {err}", target.id))?;
        if response.ok {
            Ok(response)
        } else {
            Err(format!(
                "target {} rejected run_blob_get: {}",
                target.id, response.message
            ))
        }
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
    let req_id = accepted
        .req_id
        .ok_or_else(|| format!("target {target_id} accepted request without req_id"))?;
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
            },
        )
        .await?;
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
        sleep(Duration::from_millis(10)).await;
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
        let response = poll_request_on_node(&instance.id, runtime, request).await;
        if response.ok {
            Ok(response)
        } else {
            Err(response.message)
        }
    } else {
        let target = instances
            .find_instance(target_id)
            .ok_or_else(|| format!("unknown target instance id: {target_id}"))?;
        let response = connect_node(target)
            .await?
            .poll_request(context::current(), request)
            .await
            .map_err(|err| format!("poll_request RPC failed for {}: {err}", target.id))?;
        if response.ok {
            Ok(response)
        } else {
            Err(format!(
                "target {} rejected poll_request: {}",
                target.id, response.message
            ))
        }
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

pub(super) fn participant_by_label(experiment: &ExperimentSpec, label: &str) -> Option<String> {
    experiment
        .participants
        .iter()
        .find(|participant| participant.label.as_deref() == Some(label))
        .map(|participant| participant.instance_id.clone())
}

pub(super) fn first_participant_id(experiment: &ExperimentSpec) -> Option<String> {
    experiment
        .participants
        .first()
        .map(|participant| participant.instance_id.clone())
}
