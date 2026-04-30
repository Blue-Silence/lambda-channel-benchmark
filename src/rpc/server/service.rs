use std::sync::Arc;
use tarpc::context;
use tokio::sync::Mutex;

use crate::config::{experiment_summary, InstanceConfig, InstancesConfig};
use crate::experiments::{run_blob_get_on_node, run_experiment_on_node};
use crate::rpc::protocol::{
    AcceptedResponse, BeginExprRequest, BlobPutRefsChunkRequest, BlobPutRefsChunkResponse,
    CancelRequestRequest, ChannelReceiverSamplesChunkRequest, ChannelReceiverSamplesChunkResponse,
    ExperimentRunResult, ExprActionResponse, GetBlobBatchRequest, HealthRequest, HealthResponse,
    InitBlobStoreRequest, InitMetadataStoreRequest, InitReceiverRequest, InitSenderRequest,
    NodeDescription, NodeRpc, PacedBlobGetRequest, PollExprRequest, PollExprResponse,
    PollRequestRequest, PollRequestResponse, PrepareBlobGetAppendRequest,
    PrepareBlobGetBeginRequest, PrepareBlobGetFinishRequest, PutBlobBatchRequest, RequestResult,
    ResetExprRequest, RunBlobGetRequest, RunBlobGetResponse, RunExperimentRequest,
    StartChannelReceiverRequest, StartPacedChannelSendRequest, StartPreparedBlobGetRequest,
};
use crate::rpc::server::blob::{
    init_blob_store_on_node, prepare_blob_get_append_on_node, prepare_blob_get_begin_on_node,
    prepare_blob_get_finish_on_node, submit_blob_get_batch_on_node, submit_blob_put_batch_on_node,
    submit_paced_blob_get_on_node, submit_prepared_blob_get_on_node,
};
use crate::rpc::server::channel::{
    submit_channel_receiver_on_node, submit_paced_channel_send_on_node,
};
use crate::rpc::server::state::{
    action_response, begin_expr_on_node, blob_put_refs_chunk_on_node, cancel_request_on_node,
    channel_receiver_samples_chunk_on_node, create_request_on_node, fail_request_on_node,
    finish_request_on_node, init_metadata_store_on_node, init_receiver_on_node,
    init_sender_on_node, poll_expr_on_node, poll_request_on_node, reset_expr_on_node,
    NodeRuntimeState,
};

#[derive(Clone)]
pub(crate) struct NodeRpcService {
    instances: Arc<InstancesConfig>,
    instance: InstanceConfig,
    runtime: Arc<Mutex<NodeRuntimeState>>,
}

impl NodeRpcService {
    pub(crate) fn new(instances: InstancesConfig, instance: InstanceConfig) -> Self {
        Self {
            instances: Arc::new(instances),
            instance,
            runtime: Arc::new(Mutex::new(NodeRuntimeState::default())),
        }
    }
}

impl NodeRpc for NodeRpcService {
    async fn health(self, _: context::Context, _request: HealthRequest) -> HealthResponse {
        let runtime = self.runtime.lock().await;
        HealthResponse {
            ok: true,
            instance_id: self.instance.id,
            node_status: runtime.node_status().to_string(),
            current_run_id: runtime.current.as_ref().map(|expr| expr.run_id.clone()),
            generation: runtime.generation,
        }
    }

    async fn describe(self, _: context::Context) -> NodeDescription {
        NodeDescription {
            instance_id: self.instance.id,
            rpc_addr: self.instance.rpc_addr,
            p2p_advertise_endpoint: self.instance.p2p_advertise_endpoint,
            work_dir: self.instance.work_dir.display().to_string(),
            capabilities: self.instance.capabilities,
        }
    }

    async fn begin_expr(
        self,
        _: context::Context,
        request: BeginExprRequest,
    ) -> ExprActionResponse {
        action_response(
            &self.instance.id,
            begin_expr_on_node(&self.instance, &self.runtime, request).await,
            &self.runtime,
        )
        .await
    }

    async fn init_blob_store(
        self,
        _: context::Context,
        request: InitBlobStoreRequest,
    ) -> ExprActionResponse {
        action_response(
            &self.instance.id,
            init_blob_store_on_node(&self.instance, &self.runtime, request).await,
            &self.runtime,
        )
        .await
    }

    async fn init_metadata_store(
        self,
        _: context::Context,
        request: InitMetadataStoreRequest,
    ) -> ExprActionResponse {
        action_response(
            &self.instance.id,
            init_metadata_store_on_node(&self.runtime, request).await,
            &self.runtime,
        )
        .await
    }

    async fn init_sender(
        self,
        _: context::Context,
        request: InitSenderRequest,
    ) -> ExprActionResponse {
        action_response(
            &self.instance.id,
            init_sender_on_node(&self.instance, &self.runtime, request).await,
            &self.runtime,
        )
        .await
    }

    async fn init_receiver(
        self,
        _: context::Context,
        request: InitReceiverRequest,
    ) -> ExprActionResponse {
        action_response(
            &self.instance.id,
            init_receiver_on_node(&self.instance, &self.runtime, request).await,
            &self.runtime,
        )
        .await
    }

    async fn start_channel_receiver(
        self,
        _: context::Context,
        request: StartChannelReceiverRequest,
    ) -> AcceptedResponse {
        submit_channel_receiver_on_node(&self.instance.id, self.runtime.clone(), request).await
    }

    async fn start_paced_channel_send(
        self,
        _: context::Context,
        request: StartPacedChannelSendRequest,
    ) -> AcceptedResponse {
        submit_paced_channel_send_on_node(&self.instance.id, self.runtime.clone(), request).await
    }

    async fn put_blob_batch(
        self,
        _: context::Context,
        request: PutBlobBatchRequest,
    ) -> AcceptedResponse {
        submit_blob_put_batch_on_node(&self.instance.id, self.runtime.clone(), request).await
    }

    async fn get_blob_batch(
        self,
        _: context::Context,
        request: GetBlobBatchRequest,
    ) -> AcceptedResponse {
        submit_blob_get_batch_on_node(&self.instance.id, self.runtime.clone(), request).await
    }

    async fn get_blob_paced(
        self,
        _: context::Context,
        request: PacedBlobGetRequest,
    ) -> AcceptedResponse {
        submit_paced_blob_get_on_node(&self.instance.id, self.runtime.clone(), request).await
    }

    async fn prepare_blob_get_begin(
        self,
        _: context::Context,
        request: PrepareBlobGetBeginRequest,
    ) -> ExprActionResponse {
        action_response(
            &self.instance.id,
            prepare_blob_get_begin_on_node(&self.runtime, request).await,
            &self.runtime,
        )
        .await
    }

    async fn prepare_blob_get_append(
        self,
        _: context::Context,
        request: PrepareBlobGetAppendRequest,
    ) -> ExprActionResponse {
        action_response(
            &self.instance.id,
            prepare_blob_get_append_on_node(&self.runtime, request).await,
            &self.runtime,
        )
        .await
    }

    async fn prepare_blob_get_finish(
        self,
        _: context::Context,
        request: PrepareBlobGetFinishRequest,
    ) -> ExprActionResponse {
        action_response(
            &self.instance.id,
            prepare_blob_get_finish_on_node(&self.runtime, request).await,
            &self.runtime,
        )
        .await
    }

    async fn start_prepared_blob_get(
        self,
        _: context::Context,
        request: StartPreparedBlobGetRequest,
    ) -> AcceptedResponse {
        submit_prepared_blob_get_on_node(&self.instance.id, self.runtime.clone(), request).await
    }

    async fn poll_expr(self, _: context::Context, request: PollExprRequest) -> PollExprResponse {
        poll_expr_on_node(&self.instance.id, &self.runtime, request).await
    }

    async fn poll_request(
        self,
        _: context::Context,
        request: PollRequestRequest,
    ) -> PollRequestResponse {
        poll_request_on_node(&self.instance.id, &self.runtime, request).await
    }

    async fn cancel_request(
        self,
        _: context::Context,
        request: CancelRequestRequest,
    ) -> ExprActionResponse {
        action_response(
            &self.instance.id,
            cancel_request_on_node(&self.runtime, request).await,
            &self.runtime,
        )
        .await
    }

    async fn get_blob_put_refs_chunk(
        self,
        _: context::Context,
        request: BlobPutRefsChunkRequest,
    ) -> BlobPutRefsChunkResponse {
        blob_put_refs_chunk_on_node(&self.instance.id, &self.runtime, request).await
    }

    async fn get_channel_receiver_samples_chunk(
        self,
        _: context::Context,
        request: ChannelReceiverSamplesChunkRequest,
    ) -> ChannelReceiverSamplesChunkResponse {
        channel_receiver_samples_chunk_on_node(&self.instance.id, &self.runtime, request).await
    }

    async fn reset_expr(
        self,
        _: context::Context,
        request: ResetExprRequest,
    ) -> ExprActionResponse {
        action_response(
            &self.instance.id,
            reset_expr_on_node(&self.runtime, request).await,
            &self.runtime,
        )
        .await
    }

    async fn submit_experiment(
        self,
        _: context::Context,
        request: RunExperimentRequest,
    ) -> AcceptedResponse {
        let coordinator_id = self.instance.id.clone();
        let run_id = request.experiment.run.run_id.clone();
        println!(
            "node instance={} accepted submit_experiment request for {}",
            coordinator_id,
            experiment_summary(&request.experiment)
        );

        let accepted =
            create_request_on_node(&coordinator_id, &self.runtime, &run_id, "experiment").await;
        let Some(req_id) = accepted.req_id.clone() else {
            return accepted;
        };

        let instance = self.instance.clone();
        let runtime = self.runtime.clone();
        let instances = self.instances.clone();
        tokio::spawn(async move {
            let result =
                run_experiment_on_node(&instance, &runtime, &instances, request.experiment).await;
            match result {
                Ok(message) => {
                    let result = RequestResult::Experiment(ExperimentRunResult {
                        coordinator_id: instance.id.clone(),
                        run_id: run_id.clone(),
                        message,
                    });
                    if let Err(err) =
                        finish_request_on_node(&runtime, &run_id, &req_id, result).await
                    {
                        eprintln!("failed to finish experiment request {req_id}: {err}");
                    }
                }
                Err(message) => {
                    if let Err(err) =
                        fail_request_on_node(&runtime, &run_id, &req_id, message).await
                    {
                        eprintln!("failed to fail experiment request {req_id}: {err}");
                    }
                }
            }
        });

        accepted
    }

    async fn run_blob_get(
        self,
        _: context::Context,
        request: RunBlobGetRequest,
    ) -> RunBlobGetResponse {
        match run_blob_get_on_node(
            &self.instance,
            &self.runtime,
            &self.instances,
            request.clone(),
        )
        .await
        {
            Ok(outcome) => RunBlobGetResponse {
                ok: true,
                coordinator_id: self.instance.id,
                peer_instance_id: request.peer_instance_id,
                run_id: request.run_id,
                prepared_count: outcome.prepared_count,
                materialized_count: outcome.materialized_count,
                total_bytes: outcome.total_bytes,
                peer_put_elapsed_ms: outcome.peer_put_elapsed_ms,
                local_get_elapsed_ms: outcome.local_get_elapsed_ms,
                message: "blob get completed".to_string(),
            },
            Err(message) => RunBlobGetResponse {
                ok: false,
                coordinator_id: self.instance.id,
                peer_instance_id: request.peer_instance_id,
                run_id: request.run_id,
                prepared_count: 0,
                materialized_count: 0,
                total_bytes: 0,
                peer_put_elapsed_ms: 0.0,
                local_get_elapsed_ms: 0.0,
                message,
            },
        }
    }
}
