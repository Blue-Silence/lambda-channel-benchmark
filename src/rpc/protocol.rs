use serde::{Deserialize, Serialize};

use crate::config::ExperimentSpec;
use crate::driver::paced::PacedTaskRunReport;

#[tarpc::service]
pub trait NodeRpc {
    async fn health(request: HealthRequest) -> HealthResponse;
    async fn describe() -> NodeDescription;
    async fn begin_expr(request: BeginExprRequest) -> ExprActionResponse;
    async fn init_blob_store(request: InitBlobStoreRequest) -> ExprActionResponse;
    async fn init_metadata_store(request: InitMetadataStoreRequest) -> ExprActionResponse;
    async fn init_sender(request: InitSenderRequest) -> ExprActionResponse;
    async fn init_receiver(request: InitReceiverRequest) -> ExprActionResponse;
    async fn put_blob_batch(request: PutBlobBatchRequest) -> AcceptedResponse;
    async fn get_blob_batch(request: GetBlobBatchRequest) -> AcceptedResponse;
    async fn get_blob_paced(request: PacedBlobGetRequest) -> AcceptedResponse;
    async fn poll_expr(request: PollExprRequest) -> PollExprResponse;
    async fn poll_request(request: PollRequestRequest) -> PollRequestResponse;
    async fn reset_expr(request: ResetExprRequest) -> ExprActionResponse;
    async fn submit_experiment(request: RunExperimentRequest) -> AcceptedResponse;
    async fn run_blob_get(request: RunBlobGetRequest) -> RunBlobGetResponse;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HealthRequest {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub ok: bool,
    pub instance_id: String,
    pub node_status: String,
    pub current_run_id: Option<String>,
    pub generation: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeDescription {
    pub instance_id: String,
    pub rpc_addr: String,
    pub p2p_advertise_endpoint: String,
    pub work_dir: String,
    pub capabilities: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BeginExprRequest {
    pub run_id: String,
    pub force_reset: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InitBlobStoreRequest {
    pub run_id: String,
    pub backend: String,
    pub root_dir: Option<String>,
    pub force_reinit: bool,
    pub experiment: Option<ExperimentSpec>,
    pub resource_id: Option<String>,
    pub create_remote_resources: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InitMetadataStoreRequest {
    pub run_id: String,
    pub backend: String,
    pub force_reinit: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InitSenderRequest {
    pub run_id: String,
    pub channel_id: String,
    pub reopen: bool,
    pub recover: bool,
    pub force_reinit: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InitReceiverRequest {
    pub run_id: String,
    pub channel_id: String,
    pub consumer_id: String,
    pub start_seq: i64,
    pub consume_mode: String,
    pub passive_mode: bool,
    pub force_reinit: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExprActionResponse {
    pub ok: bool,
    pub instance_id: String,
    pub run_id: Option<String>,
    pub phase: String,
    pub generation: u64,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AcceptedResponse {
    pub ok: bool,
    pub instance_id: String,
    pub run_id: String,
    pub req_id: Option<String>,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PutBlobBatchRequest {
    pub run_id: String,
    pub count: usize,
    pub object_size_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlobPutResult {
    pub count: usize,
    pub total_bytes: u64,
    pub elapsed_ms: f64,
    pub refs: Vec<serde_json::Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetBlobBatchRequest {
    pub run_id: String,
    pub refs: Vec<serde_json::Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlobGetResult {
    pub count: usize,
    pub total_bytes: u64,
    pub elapsed_ms: f64,
    pub materialized_paths: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PacedBlobGetRequest {
    pub run_id: String,
    pub refs: Vec<serde_json::Value>,
    pub target_ops_per_s: f64,
    pub max_in_flight: usize,
    pub start_after_unix_ns: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PacedBlobGetResult {
    pub count: usize,
    pub total_bytes: u64,
    pub elapsed_ms: f64,
    pub materialized_dir: String,
    pub paced: PacedTaskRunReport,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PollRequestRequest {
    pub run_id: String,
    pub req_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PollRequestResponse {
    pub ok: bool,
    pub instance_id: String,
    pub run_id: Option<String>,
    pub req_id: String,
    pub status: RequestStatus,
    pub result: Option<RequestResult>,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RequestStatus {
    Missing,
    Running,
    Finished,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RequestResult {
    BlobPut(BlobPutResult),
    BlobGet(BlobGetResult),
    PacedBlobGet(PacedBlobGetResult),
    Experiment(ExperimentRunResult),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequestSummary {
    pub req_id: String,
    pub kind: String,
    pub status: RequestStatus,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PollExprRequest {
    pub run_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PollExprResponse {
    pub ok: bool,
    pub instance_id: String,
    pub run_id: Option<String>,
    pub phase: String,
    pub generation: u64,
    pub resources: ResourceSummary,
    pub artifacts: Vec<Artifact>,
    pub metrics: Vec<MetricRecord>,
    pub requests: Vec<RequestSummary>,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResetExprRequest {
    pub run_id: String,
    pub force_reset: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunExperimentRequest {
    pub experiment: ExperimentSpec,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExperimentRunResult {
    pub coordinator_id: String,
    pub run_id: String,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunBlobGetRequest {
    pub run_id: String,
    pub peer_instance_id: String,
    pub count: usize,
    pub object_size_bytes: u64,
    pub blob_store_backend: String,
    pub barrier_timeout_ms: u64,
    pub force_reset: bool,
    pub cleanup: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunBlobGetResponse {
    pub ok: bool,
    pub coordinator_id: String,
    pub peer_instance_id: String,
    pub run_id: String,
    pub prepared_count: usize,
    pub materialized_count: usize,
    pub total_bytes: u64,
    pub peer_put_elapsed_ms: f64,
    pub local_get_elapsed_ms: f64,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResourceSummary {
    pub blob_store: Option<String>,
    pub metadata_store: Option<String>,
    pub sender: Option<String>,
    pub receiver: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Artifact {
    pub key: String,
    pub kind: String,
    pub value: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricRecord {
    pub key: String,
    pub value: f64,
    pub unit: String,
}
