use tarpc::client;
use tarpc::tokio_serde::formats::Json;

use crate::config::InstanceConfig;
use crate::rpc::address::parse_rpc_addr;
use crate::rpc::protocol::{NodeRpcClient, RequestResult, RequestStatus};

pub async fn connect_node(instance: &InstanceConfig) -> Result<NodeRpcClient, String> {
    connect_node_addr(&instance.rpc_addr).await
}

pub async fn connect_node_addr(rpc_addr: &str) -> Result<NodeRpcClient, String> {
    let addr = parse_rpc_addr(rpc_addr)?;
    let transport = tarpc::serde_transport::tcp::connect(addr, Json::default)
        .await
        .map_err(|err| format!("failed to connect to {rpc_addr}: {err}"))?;
    Ok(NodeRpcClient::new(client::Config::default(), transport).spawn())
}

pub(crate) fn ensure_finished_result(
    target_id: &str,
    req_id: &str,
    status: RequestStatus,
    result: Option<RequestResult>,
    message: String,
) -> Result<Option<RequestResult>, String> {
    match status {
        RequestStatus::Finished => Ok(Some(result.ok_or_else(|| {
            format!("target {target_id} finished req_id={req_id} without result")
        })?)),
        RequestStatus::Failed => Err(format!(
            "target {target_id} failed req_id={req_id}: {message}"
        )),
        RequestStatus::Missing => Err(format!("target {target_id} does not know req_id={req_id}")),
        RequestStatus::Running => Ok(None),
    }
}
