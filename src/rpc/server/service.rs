use std::sync::Arc;
use tarpc::context;
use tokio::sync::Mutex;

use crate::config::{experiment_summary, InstanceConfig, InstancesConfig};
use crate::experiments::{run_blob_get_on_node, run_experiment_on_node};
use crate::rpc::protocol::{
    AcceptedResponse, BeginExprRequest, ExprActionResponse, GetBlobBatchRequest, HealthRequest,
    HealthResponse, InitBlobStoreRequest, InitMetadataStoreRequest, InitReceiverRequest,
    InitSenderRequest, NodeDescription, NodeRpc, PollExprRequest, PollExprResponse,
    PollRequestRequest, PollRequestResponse, PutBlobBatchRequest, ResetExprRequest,
    RunBlobGetRequest, RunBlobGetResponse, RunExperimentRequest, RunExperimentResponse,
};
use crate::rpc::server::blob::{
    init_blob_store_on_node, submit_blob_get_batch_on_node, submit_blob_put_batch_on_node,
};
use crate::rpc::server::state::{
    action_response, begin_expr_on_node, init_metadata_store_on_node, init_receiver_on_node,
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
            init_sender_on_node(&self.runtime, request).await,
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
            init_receiver_on_node(&self.runtime, request).await,
            &self.runtime,
        )
        .await
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

    async fn run_experiment(
        self,
        _: context::Context,
        request: RunExperimentRequest,
    ) -> RunExperimentResponse {
        let coordinator_id = self.instance.id.clone();
        let run_id = request.experiment.run.run_id.clone();
        println!(
            "node instance={} accepted run_experiment request for {}",
            coordinator_id,
            experiment_summary(&request.experiment)
        );

        match run_experiment_on_node(
            &self.instance,
            &self.runtime,
            &self.instances,
            request.experiment,
        )
        .await
        {
            Ok(message) => RunExperimentResponse {
                ok: true,
                coordinator_id,
                run_id,
                message,
            },
            Err(message) => RunExperimentResponse {
                ok: false,
                coordinator_id,
                run_id,
                message,
            },
        }
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
