use std::future::Future;

use futures::StreamExt;
use tarpc::server::{self, Channel};
use tarpc::tokio_serde::formats::Json;

use crate::config::{instances_summary, InstanceConfig, InstancesConfig};
use crate::rpc::address::parse_rpc_addr;
use crate::rpc::protocol::NodeRpc;
use crate::rpc::server::service::NodeRpcService;

pub async fn serve_node(
    instances: InstancesConfig,
    instance: InstanceConfig,
) -> Result<(), String> {
    let listen_addr = instance
        .rpc_listen_addr
        .as_deref()
        .unwrap_or(&instance.rpc_addr);
    let addr = parse_rpc_addr(listen_addr)?;
    let summary = instances_summary(&instances);
    let service = NodeRpcService::new(instances, instance.clone());
    let mut listener = tarpc::serde_transport::tcp::listen(addr, Json::default)
        .await
        .map_err(|err| format!("failed to listen on {listen_addr}: {err}"))?;
    listener.config_mut().max_frame_length(usize::MAX);

    println!(
        "node RPC listening on {} advertise_rpc_addr={} ({})",
        listen_addr, instance.rpc_addr, summary
    );
    while let Some(next_transport) = listener.next().await {
        let transport = match next_transport {
            Ok(transport) => transport,
            Err(err) => {
                eprintln!("node listener accepted a bad transport: {err}");
                continue;
            }
        };
        let channel = server::BaseChannel::with_defaults(transport);
        let service = service.clone();
        tokio::spawn(channel.execute(service.serve()).for_each(spawn_response));
    }
    Ok(())
}

async fn spawn_response(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}
