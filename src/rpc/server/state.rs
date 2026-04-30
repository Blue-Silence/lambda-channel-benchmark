use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use lambda_channel::blob_store_impl::{BlobRefHandle, BlobStoreHandle};
use lambda_channel::metadata_blob_channel::receiver::{AsyncMetadataBlobRecv, ConsumeMode};
use lambda_channel::metadata_blob_channel::sender::AsyncMetadataBlobSender;
use lambda_channel::metadata_store_impl::MetadataStoreHandle;
use tokio::sync::Mutex;

use crate::blob_store_factory::{create_blob_store, unique_resource_id, BlobStoreCreateOptions};
use crate::config::{ExperimentSpec, InstanceConfig};
use crate::metadata_store_factory;
use crate::rpc::protocol::{
    AcceptedResponse, Artifact, BeginExprRequest, BlobPutRefsChunkRequest,
    BlobPutRefsChunkResponse, CancelRequestRequest, ChannelReceiverSamplesChunkRequest,
    ChannelReceiverSamplesChunkResponse, ExprActionResponse, InitMetadataStoreRequest,
    InitReceiverRequest, InitSenderRequest, MetricRecord, PollExprRequest, PollExprResponse,
    PollRequestRequest, PollRequestResponse, RequestResult, RequestStatus, RequestSummary,
    ResetExprRequest, ResourceSummary,
};

#[derive(Clone)]
pub(crate) struct BlobStoreResource {
    pub(crate) backend: String,
    pub(crate) root_dir: Option<String>,
    pub(crate) handle: BlobStoreHandle,
}

#[derive(Clone)]
pub(crate) struct MetadataStoreResource {
    pub(crate) backend: String,
    pub(crate) details: BTreeMap<String, String>,
    pub(crate) handle: MetadataStoreHandle,
}

#[derive(Clone)]
pub(crate) struct SenderResource {
    pub(crate) channel_id: String,
    pub(crate) handle: AsyncMetadataBlobSender,
    pub(crate) blob_store: BlobStoreResource,
    pub(crate) metadata_store: MetadataStoreResource,
}

#[derive(Clone)]
pub(crate) struct ReceiverResource {
    pub(crate) channel_id: String,
    pub(crate) consumer_id: String,
    pub(crate) handle: AsyncMetadataBlobRecv,
    pub(crate) blob_store: BlobStoreResource,
    pub(crate) metadata_store: MetadataStoreResource,
}

#[derive(Clone)]
pub(crate) struct ExprState {
    pub(crate) run_id: String,
    pub(crate) run_dir: PathBuf,
    pub(crate) phase: String,
    pub(crate) blob_store: Option<BlobStoreResource>,
    pub(crate) metadata_store: Option<MetadataStoreResource>,
    pub(crate) sender: Option<SenderResource>,
    pub(crate) receiver: Option<ReceiverResource>,
    pub(crate) paced_get_staging: BTreeMap<String, StagedPacedBlobGet>,
    pub(crate) paced_get_plans: BTreeMap<String, PreparedPacedBlobGet>,
    pub(crate) artifacts: BTreeMap<String, Artifact>,
    pub(crate) metrics: Vec<MetricRecord>,
}

#[derive(Clone)]
pub(crate) struct StagedPacedBlobGet {
    pub(crate) expected_ref_count: usize,
    pub(crate) target_ops_per_s: f64,
    pub(crate) max_in_flight: usize,
    pub(crate) refs: Vec<serde_json::Value>,
    pub(crate) next_chunk_index: u64,
}

#[derive(Clone)]
pub(crate) struct PreparedPacedBlobGet {
    pub(crate) target_ops_per_s: f64,
    pub(crate) max_in_flight: usize,
    pub(crate) refs: Vec<BlobRefHandle>,
    pub(crate) materialized_dir: PathBuf,
}

#[derive(Clone)]
pub(crate) struct RequestRecord {
    pub(crate) run_id: String,
    pub(crate) req_id: String,
    pub(crate) kind: String,
    pub(crate) status: RequestStatus,
    pub(crate) ready: bool,
    pub(crate) cancelled: Arc<AtomicBool>,
    pub(crate) result: Option<RequestResult>,
    pub(crate) error: Option<String>,
}

#[derive(Clone, Default)]
pub(crate) struct NodeRuntimeState {
    pub(crate) current: Option<ExprState>,
    pub(crate) requests: BTreeMap<(String, String), RequestRecord>,
    pub(crate) next_request_seq: u64,
    pub(crate) generation: u64,
}

impl NodeRuntimeState {
    pub(crate) fn node_status(&self) -> &str {
        self.current
            .as_ref()
            .map(|expr| expr.phase.as_str())
            .unwrap_or("idle")
    }
}

impl ExprState {
    fn resource_summary(&self) -> ResourceSummary {
        ResourceSummary {
            blob_store: self
                .blob_store
                .as_ref()
                .map(|resource| {
                    resource
                        .root_dir
                        .as_ref()
                        .map(|root| format!("{}:{root}", resource.backend))
                        .unwrap_or_else(|| resource.backend.clone())
                })
                .or_else(|| {
                    self.sender
                        .as_ref()
                        .map(|resource| format!("sender:{}", resource.blob_store.backend))
                })
                .or_else(|| {
                    self.receiver
                        .as_ref()
                        .map(|resource| format!("receiver:{}", resource.blob_store.backend))
                }),
            metadata_store: self
                .metadata_store
                .as_ref()
                .map(|resource| resource.backend.clone())
                .or_else(|| {
                    self.sender
                        .as_ref()
                        .map(|resource| format!("sender:{}", resource.metadata_store.backend))
                })
                .or_else(|| {
                    self.receiver
                        .as_ref()
                        .map(|resource| format!("receiver:{}", resource.metadata_store.backend))
                }),
            sender: self
                .sender
                .as_ref()
                .map(|resource| resource.channel_id.clone()),
            receiver: self
                .receiver
                .as_ref()
                .map(|resource| format!("{}:{}", resource.channel_id, resource.consumer_id)),
        }
    }
}

fn request_summaries_for_run(
    requests: &BTreeMap<(String, String), RequestRecord>,
    run_id: &str,
) -> Vec<RequestSummary> {
    requests
        .values()
        .filter(|request| request.run_id == run_id)
        .map(|request| RequestSummary {
            req_id: request.req_id.clone(),
            kind: request.kind.clone(),
            status: request.status.clone(),
            message: request
                .error
                .clone()
                .unwrap_or_else(|| format!("{:?}", request.status)),
        })
        .collect()
}

pub(crate) async fn begin_expr_on_node(
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    request: BeginExprRequest,
) -> Result<(), String> {
    if request.run_id.trim().is_empty() {
        return Err("run_id must not be empty".to_string());
    }

    let old = {
        let mut runtime = runtime.lock().await;
        if let Some(current) = runtime.current.as_ref() {
            if current.run_id == request.run_id && !request.force_reset {
                return Ok(());
            }
            if current.run_id != request.run_id && !request.force_reset {
                return Err(format!(
                    "node is running run_id={}; begin_expr requested run_id={}",
                    current.run_id, request.run_id
                ));
            }
        }
        runtime.current.take()
    };
    close_expr_state(old).await?;

    let run_dir = instance
        .work_dir
        .join("runs")
        .join(unique_resource_id(&request.run_id, &instance.id));
    tokio::fs::create_dir_all(&run_dir)
        .await
        .map_err(|err| format!("failed to create expr run dir {}: {err}", run_dir.display()))?;

    let mut runtime = runtime.lock().await;
    runtime.current = Some(ExprState {
        run_id: request.run_id,
        run_dir,
        phase: "started".to_string(),
        blob_store: None,
        metadata_store: None,
        sender: None,
        receiver: None,
        paced_get_staging: BTreeMap::new(),
        paced_get_plans: BTreeMap::new(),
        artifacts: BTreeMap::new(),
        metrics: Vec::new(),
    });
    runtime.generation += 1;
    Ok(())
}

pub(crate) async fn init_metadata_store_on_node(
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    request: InitMetadataStoreRequest,
) -> Result<(), String> {
    let created = metadata_store_factory::create_metadata_store(
        &InstanceConfig {
            id: "local".to_string(),
            rpc_addr: String::new(),
            rpc_listen_addr: None,
            p2p_advertise_endpoint: String::new(),
            work_dir: PathBuf::new(),
            capabilities: Vec::new(),
            labels: BTreeMap::new(),
        },
        None,
        &request.backend,
        &request.run_id,
        true,
    )
    .await?;

    let mut runtime = runtime.lock().await;
    let current = current_expr_mut(&mut runtime, &request.run_id)?;
    if current.metadata_store.is_some() && !request.force_reinit {
        return Ok(());
    }
    current.metadata_store = Some(MetadataStoreResource {
        backend: created.backend,
        details: created.details,
        handle: created.handle,
    });
    current.phase = "metadata_store_ready".to_string();
    runtime.generation += 1;
    Ok(())
}

pub(crate) async fn init_sender_on_node(
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    request: InitSenderRequest,
) -> Result<(), String> {
    if request.channel_id.trim().is_empty() {
        return Err("sender channel_id must not be empty".to_string());
    }
    {
        let runtime = runtime.lock().await;
        let current = current_expr(&runtime, &request.run_id)?;
        if current.sender.is_some() && !request.force_reinit {
            return Ok(());
        }
    }

    let blob_store = create_channel_blob_store(
        instance,
        runtime,
        &request.run_id,
        request.backend.as_deref(),
        request.experiment.as_ref(),
        request.resource_id.as_deref(),
        request.root_dir.clone(),
        request.create_remote_resources,
    )
    .await?;
    let metadata_store = create_channel_metadata_store(
        instance,
        &request.run_id,
        request.metadata_backend.as_deref(),
        request.experiment.as_ref(),
        request.resource_id.as_deref(),
        request.create_remote_resources,
    )
    .await?;
    let sender = AsyncMetadataBlobSender::new(
        request.channel_id.clone(),
        metadata_store.handle.clone(),
        blob_store.handle.clone(),
        request.reopen,
        request.recover,
    )
    .await
    .map_err(|err| format!("failed to initialize sender: {err}"))?;

    let mut runtime = runtime.lock().await;
    let current = current_expr_mut(&mut runtime, &request.run_id)?;
    let metadata_details = metadata_store.details.clone();
    let blob_details = BTreeMap::from([
        ("backend".to_string(), blob_store.backend.clone()),
        (
            "root_dir".to_string(),
            blob_store.root_dir.clone().unwrap_or_default(),
        ),
    ]);
    current.sender = Some(SenderResource {
        channel_id: request.channel_id,
        handle: sender,
        blob_store,
        metadata_store,
    });
    put_artifact(
        current,
        "sender_blob_store_details",
        "blob_store_details",
        serde_json::to_value(blob_details).unwrap_or(serde_json::Value::Null),
    );
    put_artifact(
        current,
        "sender_metadata_store_details",
        "metadata_store_details",
        serde_json::to_value(metadata_details).unwrap_or(serde_json::Value::Null),
    );
    current.phase = "sender_ready".to_string();
    runtime.generation += 1;
    Ok(())
}

pub(crate) async fn init_receiver_on_node(
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    request: InitReceiverRequest,
) -> Result<(), String> {
    if request.channel_id.trim().is_empty() {
        return Err("receiver channel_id must not be empty".to_string());
    }
    if request.consumer_id.trim().is_empty() {
        return Err("receiver consumer_id must not be empty".to_string());
    }
    let consume_mode = parse_consume_mode(&request.consume_mode)?;
    {
        let runtime = runtime.lock().await;
        let current = current_expr(&runtime, &request.run_id)?;
        if current.receiver.is_some() && !request.force_reinit {
            return Ok(());
        }
    }

    let blob_store = create_channel_blob_store(
        instance,
        runtime,
        &request.run_id,
        request.backend.as_deref(),
        request.experiment.as_ref(),
        request.resource_id.as_deref(),
        request.root_dir.clone(),
        request.create_remote_resources,
    )
    .await?;
    let metadata_store = create_channel_metadata_store(
        instance,
        &request.run_id,
        request.metadata_backend.as_deref(),
        request.experiment.as_ref(),
        request.resource_id.as_deref(),
        request.create_remote_resources,
    )
    .await?;
    let Some(receiver) = AsyncMetadataBlobRecv::new(
        request.channel_id.clone(),
        metadata_store.handle.clone(),
        blob_store.handle.clone(),
        request.consumer_id.clone(),
        request.start_seq,
        consume_mode,
        request.passive_mode,
    )
    .await
    .map_err(|err| format!("failed to initialize receiver: {err}"))?
    else {
        return Err(format!(
            "channel {} does not exist for receiver initialization",
            request.channel_id
        ));
    };

    let mut runtime = runtime.lock().await;
    let current = current_expr_mut(&mut runtime, &request.run_id)?;
    let metadata_details = metadata_store.details.clone();
    let blob_details = BTreeMap::from([
        ("backend".to_string(), blob_store.backend.clone()),
        (
            "root_dir".to_string(),
            blob_store.root_dir.clone().unwrap_or_default(),
        ),
    ]);
    current.receiver = Some(ReceiverResource {
        channel_id: request.channel_id,
        consumer_id: request.consumer_id,
        handle: receiver,
        blob_store,
        metadata_store,
    });
    put_artifact(
        current,
        "receiver_blob_store_details",
        "blob_store_details",
        serde_json::to_value(blob_details).unwrap_or(serde_json::Value::Null),
    );
    put_artifact(
        current,
        "receiver_metadata_store_details",
        "metadata_store_details",
        serde_json::to_value(metadata_details).unwrap_or(serde_json::Value::Null),
    );
    current.phase = "receiver_ready".to_string();
    runtime.generation += 1;
    Ok(())
}

async fn create_channel_metadata_store(
    instance: &InstanceConfig,
    run_id: &str,
    metadata_backend: Option<&str>,
    experiment: Option<&ExperimentSpec>,
    resource_id: Option<&str>,
    create_remote_resources: bool,
) -> Result<MetadataStoreResource, String> {
    let backend = metadata_backend
        .map(str::to_string)
        .or_else(|| experiment.map(|experiment| experiment.lambda_channel.metadata_backend.clone()))
        .unwrap_or_else(|| "inmemory".to_string());
    let resource_id = resource_id.unwrap_or(run_id);
    let created = metadata_store_factory::create_metadata_store(
        instance,
        experiment,
        &backend,
        resource_id,
        create_remote_resources,
    )
    .await?;
    Ok(MetadataStoreResource {
        backend: created.backend,
        details: created.details,
        handle: created.handle,
    })
}

async fn create_channel_blob_store(
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    run_id: &str,
    backend: Option<&str>,
    experiment: Option<&ExperimentSpec>,
    resource_id: Option<&str>,
    root_dir: Option<String>,
    create_remote_resources: bool,
) -> Result<BlobStoreResource, String> {
    let run_dir = {
        let runtime = runtime.lock().await;
        current_expr(&runtime, run_id)?.run_dir.clone()
    };
    let backend = backend
        .map(str::to_string)
        .or_else(|| experiment.map(|experiment| experiment.benchmark.backend.clone()))
        .ok_or_else(|| "channel endpoint init requires blob backend".to_string())?;
    let resource_id = resource_id.unwrap_or(run_id);
    let created = create_blob_store(BlobStoreCreateOptions {
        instance,
        experiment,
        run_dir: &run_dir,
        backend: &backend,
        resource_id,
        root_dir: root_dir.map(PathBuf::from),
        create_remote_resources,
    })
    .await?;
    Ok(BlobStoreResource {
        backend: created.backend,
        root_dir: created.root_dir,
        handle: created.handle,
    })
}

pub(crate) async fn poll_expr_on_node(
    instance_id: &str,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    request: PollExprRequest,
) -> PollExprResponse {
    let runtime = runtime.lock().await;
    let Some(current) = runtime.current.as_ref() else {
        return PollExprResponse {
            ok: false,
            instance_id: instance_id.to_string(),
            run_id: None,
            phase: "idle".to_string(),
            generation: runtime.generation,
            resources: ResourceSummary {
                blob_store: None,
                metadata_store: None,
                sender: None,
                receiver: None,
            },
            artifacts: Vec::new(),
            metrics: Vec::new(),
            requests: Vec::new(),
            message: "node has no active expr".to_string(),
        };
    };
    let ok = current.run_id == request.run_id;
    PollExprResponse {
        ok,
        instance_id: instance_id.to_string(),
        run_id: Some(current.run_id.clone()),
        phase: current.phase.clone(),
        generation: runtime.generation,
        resources: current.resource_summary(),
        artifacts: current.artifacts.values().cloned().collect(),
        metrics: current.metrics.clone(),
        requests: request_summaries_for_run(&runtime.requests, &current.run_id),
        message: if ok {
            "expr state snapshot".to_string()
        } else {
            format!(
                "node is running run_id={}; poll requested run_id={}",
                current.run_id, request.run_id
            )
        },
    }
}

pub(crate) async fn create_request_on_node(
    instance_id: &str,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    run_id: &str,
    kind: &str,
) -> AcceptedResponse {
    if run_id.trim().is_empty() {
        return AcceptedResponse {
            ok: false,
            instance_id: instance_id.to_string(),
            run_id: run_id.to_string(),
            req_id: None,
            message: "run_id must not be empty".to_string(),
        };
    }

    let mut runtime = runtime.lock().await;
    runtime.next_request_seq += 1;
    let req_id = format!("{kind}-{:06}", runtime.next_request_seq);
    runtime.requests.insert(
        (run_id.to_string(), req_id.clone()),
        RequestRecord {
            run_id: run_id.to_string(),
            req_id: req_id.clone(),
            kind: kind.to_string(),
            status: RequestStatus::Running,
            ready: false,
            cancelled: Arc::new(AtomicBool::new(false)),
            result: None,
            error: None,
        },
    );
    if let Some(current) = runtime
        .current
        .as_mut()
        .filter(|current| current.run_id == run_id)
    {
        current.phase = format!("request_running:{kind}");
    }
    runtime.generation += 1;

    AcceptedResponse {
        ok: true,
        instance_id: instance_id.to_string(),
        run_id: run_id.to_string(),
        req_id: Some(req_id),
        message: "request accepted".to_string(),
    }
}

pub(crate) async fn mark_request_ready_on_node(
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    run_id: &str,
    req_id: &str,
) -> Result<(), String> {
    let mut runtime = runtime.lock().await;
    let key = (run_id.to_string(), req_id.to_string());
    let kind = {
        let request = runtime
            .requests
            .get_mut(&key)
            .ok_or_else(|| format!("unknown req_id={req_id} for run_id={run_id}"))?;
        request.ready = true;
        request.kind.clone()
    };
    if let Some(current) = runtime
        .current
        .as_mut()
        .filter(|current| current.run_id == run_id)
    {
        current.phase = format!("request_ready:{kind}");
    }
    runtime.generation += 1;
    Ok(())
}

pub(crate) async fn request_cancel_flag_on_node(
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    run_id: &str,
    req_id: &str,
) -> Result<Arc<AtomicBool>, String> {
    let runtime = runtime.lock().await;
    runtime
        .requests
        .get(&(run_id.to_string(), req_id.to_string()))
        .map(|record| record.cancelled.clone())
        .ok_or_else(|| format!("unknown req_id={req_id} for run_id={run_id}"))
}

pub(crate) async fn cancel_request_on_node(
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    request: CancelRequestRequest,
) -> Result<(), String> {
    let mut runtime = runtime.lock().await;
    let key = (request.run_id.clone(), request.req_id.clone());
    let kind = {
        let record = runtime.requests.get_mut(&key).ok_or_else(|| {
            format!(
                "unknown req_id={} for run_id={}",
                request.req_id, request.run_id
            )
        })?;
        record.cancelled.store(true, Ordering::Relaxed);
        record.kind.clone()
    };
    if let Some(current) = runtime
        .current
        .as_mut()
        .filter(|current| current.run_id == request.run_id)
    {
        current.phase = format!("request_cancelled:{kind}");
    }
    runtime.generation += 1;
    Ok(())
}

pub(crate) async fn finish_request_on_node(
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    run_id: &str,
    req_id: &str,
    result: RequestResult,
) -> Result<(), String> {
    let mut runtime = runtime.lock().await;
    let key = (run_id.to_string(), req_id.to_string());
    let kind = {
        let request = runtime
            .requests
            .get_mut(&key)
            .ok_or_else(|| format!("unknown req_id={req_id} for run_id={run_id}"))?;
        request.status = RequestStatus::Finished;
        request.result = Some(result);
        request.error = None;
        request.kind.clone()
    };
    if let Some(current) = runtime
        .current
        .as_mut()
        .filter(|current| current.run_id == run_id)
    {
        current.phase = format!("request_finished:{kind}");
    }
    runtime.generation += 1;
    Ok(())
}

pub(crate) async fn fail_request_on_node(
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    run_id: &str,
    req_id: &str,
    error: String,
) -> Result<(), String> {
    let mut runtime = runtime.lock().await;
    let key = (run_id.to_string(), req_id.to_string());
    let kind = {
        let request = runtime
            .requests
            .get_mut(&key)
            .ok_or_else(|| format!("unknown req_id={req_id} for run_id={run_id}"))?;
        request.status = RequestStatus::Failed;
        request.result = None;
        request.error = Some(error);
        request.kind.clone()
    };
    if let Some(current) = runtime
        .current
        .as_mut()
        .filter(|current| current.run_id == run_id)
    {
        current.phase = format!("request_failed:{kind}");
    }
    runtime.generation += 1;
    Ok(())
}

pub(crate) async fn poll_request_on_node(
    instance_id: &str,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    request: PollRequestRequest,
) -> PollRequestResponse {
    let runtime = runtime.lock().await;
    let Some(record) = runtime
        .requests
        .get(&(request.run_id.clone(), request.req_id.clone()))
    else {
        return PollRequestResponse {
            ok: true,
            instance_id: instance_id.to_string(),
            run_id: Some(request.run_id),
            req_id: request.req_id,
            status: RequestStatus::Missing,
            ready: false,
            result: None,
            message: "unknown request id".to_string(),
        };
    };

    PollRequestResponse {
        ok: true,
        instance_id: instance_id.to_string(),
        run_id: Some(record.run_id.clone()),
        req_id: record.req_id.clone(),
        status: record.status.clone(),
        ready: record.ready,
        result: if request.include_result {
            record.result.as_ref().map(strip_large_request_result)
        } else {
            None
        },
        message: record
            .error
            .clone()
            .unwrap_or_else(|| format!("{} request is {:?}", record.kind, record.status)),
    }
}

fn strip_large_request_result(result: &RequestResult) -> RequestResult {
    match result {
        RequestResult::ChannelReceiver(result) => {
            let mut result = result.clone();
            result.delivery_latency_samples_ms.clear();
            result.materialize_latency_samples_ms.clear();
            RequestResult::ChannelReceiver(result)
        }
        other => other.clone(),
    }
}

pub(crate) async fn blob_put_refs_chunk_on_node(
    instance_id: &str,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    request: BlobPutRefsChunkRequest,
) -> BlobPutRefsChunkResponse {
    if request.limit == 0 {
        return BlobPutRefsChunkResponse {
            ok: false,
            instance_id: instance_id.to_string(),
            run_id: Some(request.run_id),
            req_id: request.req_id,
            status: RequestStatus::Missing,
            count: 0,
            total_bytes: 0,
            elapsed_ms: 0.0,
            offset: request.offset,
            refs: Vec::new(),
            has_more: false,
            message: "refs chunk limit must be greater than zero".to_string(),
        };
    }

    let runtime = runtime.lock().await;
    let Some(record) = runtime
        .requests
        .get(&(request.run_id.clone(), request.req_id.clone()))
    else {
        return BlobPutRefsChunkResponse {
            ok: true,
            instance_id: instance_id.to_string(),
            run_id: Some(request.run_id),
            req_id: request.req_id,
            status: RequestStatus::Missing,
            count: 0,
            total_bytes: 0,
            elapsed_ms: 0.0,
            offset: request.offset,
            refs: Vec::new(),
            has_more: false,
            message: "unknown request id".to_string(),
        };
    };

    match (&record.status, &record.result) {
        (RequestStatus::Finished, Some(RequestResult::BlobPut(result))) => {
            let offset = request.offset.min(result.refs.len());
            let end = offset.saturating_add(request.limit).min(result.refs.len());
            BlobPutRefsChunkResponse {
                ok: true,
                instance_id: instance_id.to_string(),
                run_id: Some(record.run_id.clone()),
                req_id: record.req_id.clone(),
                status: record.status.clone(),
                count: result.count,
                total_bytes: result.total_bytes,
                elapsed_ms: result.elapsed_ms,
                offset,
                refs: result.refs[offset..end].to_vec(),
                has_more: end < result.refs.len(),
                message: format!("blob put refs chunk {offset}..{end}/{}", result.refs.len()),
            }
        }
        (RequestStatus::Finished, Some(other)) => BlobPutRefsChunkResponse {
            ok: false,
            instance_id: instance_id.to_string(),
            run_id: Some(record.run_id.clone()),
            req_id: record.req_id.clone(),
            status: record.status.clone(),
            count: 0,
            total_bytes: 0,
            elapsed_ms: 0.0,
            offset: request.offset,
            refs: Vec::new(),
            has_more: false,
            message: format!("request result is not BlobPut: {other:?}"),
        },
        _ => BlobPutRefsChunkResponse {
            ok: true,
            instance_id: instance_id.to_string(),
            run_id: Some(record.run_id.clone()),
            req_id: record.req_id.clone(),
            status: record.status.clone(),
            count: 0,
            total_bytes: 0,
            elapsed_ms: 0.0,
            offset: request.offset,
            refs: Vec::new(),
            has_more: false,
            message: record
                .error
                .clone()
                .unwrap_or_else(|| format!("{} request is {:?}", record.kind, record.status)),
        },
    }
}

pub(crate) async fn channel_receiver_samples_chunk_on_node(
    instance_id: &str,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    request: ChannelReceiverSamplesChunkRequest,
) -> ChannelReceiverSamplesChunkResponse {
    if request.limit == 0 {
        return ChannelReceiverSamplesChunkResponse {
            ok: false,
            instance_id: instance_id.to_string(),
            run_id: Some(request.run_id),
            req_id: request.req_id,
            status: RequestStatus::Missing,
            offset: request.offset,
            delivery_count: 0,
            materialize_count: 0,
            delivery_latency_samples_ms: Vec::new(),
            materialize_latency_samples_ms: Vec::new(),
            has_more: false,
            message: "samples chunk limit must be greater than zero".to_string(),
        };
    }

    let runtime = runtime.lock().await;
    let Some(record) = runtime
        .requests
        .get(&(request.run_id.clone(), request.req_id.clone()))
    else {
        return ChannelReceiverSamplesChunkResponse {
            ok: true,
            instance_id: instance_id.to_string(),
            run_id: Some(request.run_id),
            req_id: request.req_id,
            status: RequestStatus::Missing,
            offset: request.offset,
            delivery_count: 0,
            materialize_count: 0,
            delivery_latency_samples_ms: Vec::new(),
            materialize_latency_samples_ms: Vec::new(),
            has_more: false,
            message: "unknown request id".to_string(),
        };
    };

    match (&record.status, &record.result) {
        (RequestStatus::Finished, Some(RequestResult::ChannelReceiver(result))) => {
            let delivery_offset = request.offset.min(result.delivery_latency_samples_ms.len());
            let delivery_end = delivery_offset
                .saturating_add(request.limit)
                .min(result.delivery_latency_samples_ms.len());
            let materialize_offset = request
                .offset
                .min(result.materialize_latency_samples_ms.len());
            let materialize_end = materialize_offset
                .saturating_add(request.limit)
                .min(result.materialize_latency_samples_ms.len());
            let has_more = delivery_end < result.delivery_latency_samples_ms.len()
                || materialize_end < result.materialize_latency_samples_ms.len();
            ChannelReceiverSamplesChunkResponse {
                ok: true,
                instance_id: instance_id.to_string(),
                run_id: Some(record.run_id.clone()),
                req_id: record.req_id.clone(),
                status: record.status.clone(),
                offset: request.offset,
                delivery_count: result.delivery_latency_samples_ms.len(),
                materialize_count: result.materialize_latency_samples_ms.len(),
                delivery_latency_samples_ms: result.delivery_latency_samples_ms
                    [delivery_offset..delivery_end]
                    .to_vec(),
                materialize_latency_samples_ms: result.materialize_latency_samples_ms
                    [materialize_offset..materialize_end]
                    .to_vec(),
                has_more,
                message: format!(
                    "channel receiver samples chunk offset={} limit={}",
                    request.offset, request.limit
                ),
            }
        }
        (RequestStatus::Finished, Some(other)) => ChannelReceiverSamplesChunkResponse {
            ok: false,
            instance_id: instance_id.to_string(),
            run_id: Some(record.run_id.clone()),
            req_id: record.req_id.clone(),
            status: record.status.clone(),
            offset: request.offset,
            delivery_count: 0,
            materialize_count: 0,
            delivery_latency_samples_ms: Vec::new(),
            materialize_latency_samples_ms: Vec::new(),
            has_more: false,
            message: format!("request result is not ChannelReceiver: {other:?}"),
        },
        _ => ChannelReceiverSamplesChunkResponse {
            ok: true,
            instance_id: instance_id.to_string(),
            run_id: Some(record.run_id.clone()),
            req_id: record.req_id.clone(),
            status: record.status.clone(),
            offset: request.offset,
            delivery_count: 0,
            materialize_count: 0,
            delivery_latency_samples_ms: Vec::new(),
            materialize_latency_samples_ms: Vec::new(),
            has_more: false,
            message: record
                .error
                .clone()
                .unwrap_or_else(|| format!("{} request is {:?}", record.kind, record.status)),
        },
    }
}

pub(crate) async fn reset_expr_on_node(
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    request: ResetExprRequest,
) -> Result<(), String> {
    let old = {
        let mut runtime = runtime.lock().await;
        let Some(current) = runtime.current.as_ref() else {
            return Ok(());
        };
        if current.run_id != request.run_id && !request.force_reset {
            return Err(format!(
                "node is running run_id={}; refused reset for run_id={}",
                current.run_id, request.run_id
            ));
        }
        runtime.current.take()
    };
    close_expr_state(old).await?;
    let mut runtime = runtime.lock().await;
    runtime.generation += 1;
    Ok(())
}

pub(crate) async fn action_response(
    instance_id: &str,
    result: Result<(), String>,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
) -> ExprActionResponse {
    let (ok, message) = match result {
        Ok(()) => (true, "ok".to_string()),
        Err(message) => (false, message),
    };
    let runtime = runtime.lock().await;
    ExprActionResponse {
        ok,
        instance_id: instance_id.to_string(),
        run_id: runtime.current.as_ref().map(|expr| expr.run_id.clone()),
        phase: runtime.node_status().to_string(),
        generation: runtime.generation,
        message,
    }
}

pub(crate) fn response_to_result(response: ExprActionResponse) -> Result<(), String> {
    if response.ok {
        Ok(())
    } else {
        Err(response.message)
    }
}

pub(crate) fn current_expr<'a>(
    runtime: &'a NodeRuntimeState,
    run_id: &str,
) -> Result<&'a ExprState, String> {
    let current = runtime
        .current
        .as_ref()
        .ok_or_else(|| "node has no active expr".to_string())?;
    if current.run_id != run_id {
        return Err(format!(
            "node is running run_id={}; request used run_id={run_id}",
            current.run_id
        ));
    }
    Ok(current)
}

pub(crate) fn current_expr_mut<'a>(
    runtime: &'a mut NodeRuntimeState,
    run_id: &str,
) -> Result<&'a mut ExprState, String> {
    let current = runtime
        .current
        .as_mut()
        .ok_or_else(|| "node has no active expr".to_string())?;
    if current.run_id != run_id {
        return Err(format!(
            "node is running run_id={}; request used run_id={run_id}",
            current.run_id
        ));
    }
    Ok(current)
}

pub(crate) fn put_artifact(
    current: &mut ExprState,
    key: impl Into<String>,
    kind: impl Into<String>,
    value: serde_json::Value,
) {
    let key = key.into();
    current.artifacts.insert(
        key.clone(),
        Artifact {
            key,
            kind: kind.into(),
            value,
        },
    );
}

pub(crate) fn put_metric(
    current: &mut ExprState,
    key: impl Into<String>,
    value: f64,
    unit: impl Into<String>,
) {
    let key = key.into();
    current.metrics.retain(|metric| metric.key != key);
    current.metrics.push(MetricRecord {
        key,
        value,
        unit: unit.into(),
    });
}

async fn close_expr_state(expr: Option<ExprState>) -> Result<(), String> {
    let Some(expr) = expr else {
        return Ok(());
    };
    let mut errors = Vec::new();
    if let Some(receiver) = expr.receiver {
        if let Err(err) = close_blob_store_resource(receiver.blob_store, &expr.run_id).await {
            errors.push(format!("failed to close receiver blob store: {err}"));
        }
    }
    if let Some(sender) = expr.sender {
        if let Err(err) = close_blob_store_resource(sender.blob_store, &expr.run_id).await {
            errors.push(format!("failed to close sender blob store: {err}"));
        }
    }
    if let Some(blob_store) = expr.blob_store {
        if let Err(err) = close_blob_store_resource(blob_store, &expr.run_id).await {
            errors.push(err);
        }
    }
    match tokio::fs::remove_dir_all(&expr.run_dir).await {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => errors.push(format!(
            "failed to remove expr run dir {}: {err}",
            expr.run_dir.display()
        )),
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

pub(crate) async fn close_blob_store_resource(
    blob_store: BlobStoreResource,
    run_id: &str,
) -> Result<(), String> {
    let mut errors = Vec::new();
    if let Err(err) = blob_store.handle.close().await {
        errors.push(format!(
            "failed to close blob store for run_id={run_id}: {err}"
        ));
    }
    if let Some(root_dir) = blob_store.root_dir {
        let path = PathBuf::from(root_dir);
        match tokio::fs::remove_dir_all(&path).await {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => errors.push(format!(
                "failed to remove blob store resource dir {}: {err}",
                path.display()
            )),
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

fn parse_consume_mode(value: &str) -> Result<ConsumeMode, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "competitive" => Ok(ConsumeMode::Competitive),
        "fanout" => Ok(ConsumeMode::Fanout),
        other => Err(format!("unsupported consume_mode: {other}")),
    }
}
