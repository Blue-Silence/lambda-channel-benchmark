use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use lambda_channel::blob_store_impl::{BlobRefHandle, BlobStoreHandle};
use lambda_channel::metadata_blob_channel::receiver::{AsyncMetadataBlobRecv, ConsumeMode};
use lambda_channel::metadata_blob_channel::sender::AsyncMetadataBlobSender;
use lambda_channel::metadata_store_impl::in_memory::AsyncInMemoryMetadataStore;
use lambda_channel::metadata_store_impl::MetadataStoreHandle;
use tokio::sync::Mutex;

use crate::blob_store_factory::unique_resource_id;
use crate::config::InstanceConfig;
use crate::rpc::protocol::{
    AcceptedResponse, Artifact, BeginExprRequest, ExprActionResponse, InitMetadataStoreRequest,
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
    pub(crate) handle: MetadataStoreHandle,
}

#[derive(Clone)]
pub(crate) struct SenderResource {
    pub(crate) channel_id: String,
    pub(crate) _handle: AsyncMetadataBlobSender,
}

#[derive(Clone)]
pub(crate) struct ReceiverResource {
    pub(crate) channel_id: String,
    pub(crate) consumer_id: String,
    pub(crate) _handle: AsyncMetadataBlobRecv,
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
            blob_store: self.blob_store.as_ref().map(|resource| {
                resource
                    .root_dir
                    .as_ref()
                    .map(|root| format!("{}:{root}", resource.backend))
                    .unwrap_or_else(|| resource.backend.clone())
            }),
            metadata_store: self
                .metadata_store
                .as_ref()
                .map(|resource| resource.backend.clone()),
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
    let backend = request.backend.trim().to_ascii_lowercase();
    let handle = match backend.as_str() {
        "inmemory" | "in-memory" => {
            Arc::new(AsyncInMemoryMetadataStore::default()) as MetadataStoreHandle
        }
        other => return Err(format!("unsupported metadata store backend: {other}")),
    };

    let mut runtime = runtime.lock().await;
    let current = current_expr_mut(&mut runtime, &request.run_id)?;
    if current.metadata_store.is_some() && !request.force_reinit {
        return Ok(());
    }
    current.metadata_store = Some(MetadataStoreResource { backend, handle });
    current.phase = "metadata_store_ready".to_string();
    runtime.generation += 1;
    Ok(())
}

pub(crate) async fn init_sender_on_node(
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    request: InitSenderRequest,
) -> Result<(), String> {
    if request.channel_id.trim().is_empty() {
        return Err("sender channel_id must not be empty".to_string());
    }
    let (metadata_store, blob_store) = {
        let runtime = runtime.lock().await;
        let current = current_expr(&runtime, &request.run_id)?;
        (
            current
                .metadata_store
                .as_ref()
                .ok_or_else(|| "init_sender requires metadata_store state".to_string())?
                .handle
                .clone(),
            current
                .blob_store
                .as_ref()
                .ok_or_else(|| "init_sender requires blob_store state".to_string())?
                .handle
                .clone(),
        )
    };
    let sender = AsyncMetadataBlobSender::new(
        request.channel_id.clone(),
        metadata_store,
        blob_store,
        request.reopen,
        request.recover,
    )
    .await
    .map_err(|err| format!("failed to initialize sender: {err}"))?;

    let mut runtime = runtime.lock().await;
    let current = current_expr_mut(&mut runtime, &request.run_id)?;
    if current.sender.is_some() && !request.force_reinit {
        return Ok(());
    }
    current.sender = Some(SenderResource {
        channel_id: request.channel_id,
        _handle: sender,
    });
    current.phase = "sender_ready".to_string();
    runtime.generation += 1;
    Ok(())
}

pub(crate) async fn init_receiver_on_node(
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
    let (metadata_store, blob_store) = {
        let runtime = runtime.lock().await;
        let current = current_expr(&runtime, &request.run_id)?;
        (
            current
                .metadata_store
                .as_ref()
                .ok_or_else(|| "init_receiver requires metadata_store state".to_string())?
                .handle
                .clone(),
            current
                .blob_store
                .as_ref()
                .ok_or_else(|| "init_receiver requires blob_store state".to_string())?
                .handle
                .clone(),
        )
    };
    let Some(receiver) = AsyncMetadataBlobRecv::new(
        request.channel_id.clone(),
        metadata_store,
        blob_store,
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
    if current.receiver.is_some() && !request.force_reinit {
        return Ok(());
    }
    current.receiver = Some(ReceiverResource {
        channel_id: request.channel_id,
        consumer_id: request.consumer_id,
        _handle: receiver,
    });
    current.phase = "receiver_ready".to_string();
    runtime.generation += 1;
    Ok(())
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
        result: record.result.clone(),
        message: record
            .error
            .clone()
            .unwrap_or_else(|| format!("{} request is {:?}", record.kind, record.status)),
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
