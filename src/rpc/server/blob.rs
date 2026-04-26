use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use lambda_channel::blob_store_impl::local_file_blob_store::AsyncLocalFileBlobStore;
use lambda_channel::blob_store_impl::{BlobRefHandle, BlobStoreHandle};
use lambda_channel::common::{NativeMap, NativeValue};
use tokio::sync::Mutex;

use crate::config::InstanceConfig;
use crate::payload_file::{create_timestamped_payload_file, PayloadFileSpec};
use crate::rpc::protocol::{
    AcceptedResponse, BlobGetResult, BlobPutResult, GetBlobBatchRequest, InitBlobStoreRequest,
    PutBlobBatchRequest, RequestResult,
};
use crate::rpc::server::state::{
    create_request_on_node, current_expr, current_expr_mut, fail_request_on_node,
    finish_request_on_node, put_artifact, put_metric, BlobStoreResource, NodeRuntimeState,
};

pub(crate) async fn init_blob_store_on_node(
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    request: InitBlobStoreRequest,
) -> Result<(), String> {
    let (run_dir, old) = {
        let mut runtime = runtime.lock().await;
        let current = current_expr_mut(&mut runtime, &request.run_id)?;
        if current.blob_store.is_some() && !request.force_reinit {
            return Ok(());
        }
        (
            current.run_dir.clone(),
            current.blob_store.take().map(|store| store.handle),
        )
    };
    if let Some(old) = old {
        old.close()
            .await
            .map_err(|err| format!("failed to close previous blob store: {err}"))?;
    }

    let backend = request.backend.trim().to_ascii_lowercase();
    let (root_dir, handle): (Option<String>, BlobStoreHandle) = match backend.as_str() {
        "local-file" | "localfs" => {
            let root_dir = request
                .root_dir
                .map(PathBuf::from)
                .unwrap_or_else(|| run_dir.join("blob-store"));
            tokio::fs::create_dir_all(&root_dir).await.map_err(|err| {
                format!(
                    "failed to create blob store root {}: {err}",
                    root_dir.display()
                )
            })?;
            let store = AsyncLocalFileBlobStore::new(path_to_string(&root_dir))
                .await
                .map_err(|err| format!("failed to create local file blob store: {err}"))?;
            (
                Some(path_to_string(&root_dir)),
                Arc::new(store) as BlobStoreHandle,
            )
        }
        "p2p" => {
            return Err(format!(
                "p2p blob store init is not implemented yet for node {}; this is the next primitive to wire",
                instance.id
            ))
        }
        other => return Err(format!("unsupported blob store backend: {other}")),
    };

    let mut runtime = runtime.lock().await;
    let current = current_expr_mut(&mut runtime, &request.run_id)?;
    current.blob_store = Some(BlobStoreResource {
        backend,
        root_dir,
        handle,
    });
    current.phase = "blob_store_ready".to_string();
    runtime.generation += 1;
    Ok(())
}

pub(crate) async fn submit_blob_put_batch_on_node(
    instance_id: &str,
    runtime: Arc<Mutex<NodeRuntimeState>>,
    request: PutBlobBatchRequest,
) -> AcceptedResponse {
    let accepted = create_request_on_node(instance_id, &runtime, &request.run_id, "blob-put").await;
    let Some(req_id) = accepted.req_id.clone() else {
        return accepted;
    };

    tokio::spawn(async move {
        let run_id = request.run_id.clone();
        let started_at = Instant::now();
        let result = put_blob_batch_on_node(&runtime, request).await;
        match result {
            Ok((refs, total_bytes)) => {
                let result = RequestResult::BlobPut(BlobPutResult {
                    count: refs.len(),
                    total_bytes,
                    elapsed_ms: started_at.elapsed().as_secs_f64() * 1000.0,
                    refs,
                });
                if let Err(err) = finish_request_on_node(&runtime, &run_id, &req_id, result).await {
                    eprintln!("failed to finish blob put request {req_id}: {err}");
                }
            }
            Err(message) => {
                if let Err(err) = fail_request_on_node(&runtime, &run_id, &req_id, message).await {
                    eprintln!("failed to fail blob put request {req_id}: {err}");
                }
            }
        }
    });

    accepted
}

pub(crate) async fn submit_blob_get_batch_on_node(
    instance_id: &str,
    runtime: Arc<Mutex<NodeRuntimeState>>,
    request: GetBlobBatchRequest,
) -> AcceptedResponse {
    let accepted = create_request_on_node(instance_id, &runtime, &request.run_id, "blob-get").await;
    let Some(req_id) = accepted.req_id.clone() else {
        return accepted;
    };

    tokio::spawn(async move {
        let run_id = request.run_id.clone();
        let started_at = Instant::now();
        let result = get_blob_batch_on_node(&runtime, request).await;
        match result {
            Ok((materialized_paths, total_bytes)) => {
                let result = RequestResult::BlobGet(BlobGetResult {
                    count: materialized_paths.len(),
                    total_bytes,
                    elapsed_ms: started_at.elapsed().as_secs_f64() * 1000.0,
                    materialized_paths,
                });
                if let Err(err) = finish_request_on_node(&runtime, &run_id, &req_id, result).await {
                    eprintln!("failed to finish blob get request {req_id}: {err}");
                }
            }
            Err(message) => {
                if let Err(err) = fail_request_on_node(&runtime, &run_id, &req_id, message).await {
                    eprintln!("failed to fail blob get request {req_id}: {err}");
                }
            }
        }
    });

    accepted
}

pub(crate) async fn put_blob_batch_on_node(
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    request: PutBlobBatchRequest,
) -> Result<(Vec<serde_json::Value>, u64), String> {
    if request.count == 0 {
        return Err("blob batch count must be greater than zero".to_string());
    }
    if request.object_size_bytes == 0 {
        return Err("object_size_bytes must be greater than zero".to_string());
    }

    let (run_dir, store) = {
        let runtime = runtime.lock().await;
        let current = current_expr(&runtime, &request.run_id)?;
        (
            current.run_dir.clone(),
            current
                .blob_store
                .as_ref()
                .ok_or_else(|| "put_blob_batch requires initialized blob_store state".to_string())?
                .handle
                .clone(),
        )
    };
    let payload_dir = run_dir.join("payloads").join(unique_batch_id("put"));
    tokio::fs::create_dir_all(&payload_dir)
        .await
        .map_err(|err| {
            format!(
                "failed to create payload dir {}: {err}",
                payload_dir.display()
            )
        })?;

    let mut paths = Vec::with_capacity(request.count);
    for idx in 0..request.count {
        let path = payload_dir.join(format!("payload_{idx:06}.bin"));
        create_timestamped_payload_file(
            &path,
            PayloadFileSpec {
                run_id: &request.run_id,
                seed: 0,
                index: idx as u64,
                size_bytes: request.object_size_bytes,
            },
        )
        .await?;
        paths.push(path_to_string(&path));
    }

    let mut refs = Vec::with_capacity(paths.len());
    for path in &paths {
        refs.push(
            store
                .put_file(path)
                .await
                .map_err(|err| format!("failed to put payload {path}: {err}"))?,
        );
    }
    let total_bytes = sum_file_sizes(&paths).await?;
    let refs_json = blob_refs_to_json_values(&refs)?;

    let mut runtime = runtime.lock().await;
    let current = current_expr_mut(&mut runtime, &request.run_id)?;
    put_artifact(
        current,
        "payload_paths",
        "payload_paths",
        serde_json::to_value(&paths).unwrap_or(serde_json::Value::Array(Vec::new())),
    );
    put_artifact(
        current,
        "blob_refs",
        "blob_refs",
        serde_json::Value::Array(refs_json.clone()),
    );
    put_metric(current, "blob_put_total_bytes", total_bytes as f64, "bytes");
    current.phase = "blob_batch_ready".to_string();
    runtime.generation += 1;
    Ok((refs_json, total_bytes))
}

pub(crate) async fn get_blob_batch_on_node(
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    request: GetBlobBatchRequest,
) -> Result<(Vec<String>, u64), String> {
    let (run_dir, store) = {
        let runtime = runtime.lock().await;
        let current = current_expr(&runtime, &request.run_id)?;
        (
            current.run_dir.clone(),
            current
                .blob_store
                .as_ref()
                .ok_or_else(|| "get_blob_batch requires initialized blob_store state".to_string())?
                .handle
                .clone(),
        )
    };
    let output_dir = run_dir.join("materialized").join(unique_batch_id("get"));
    tokio::fs::create_dir_all(&output_dir)
        .await
        .map_err(|err| {
            format!(
                "failed to create materialize output dir {}: {err}",
                output_dir.display()
            )
        })?;

    let mut refs = Vec::with_capacity(request.refs.len());
    for value in &request.refs {
        let native_map = json_to_native_map(value)?;
        refs.push(
            store
                .blob_ref_kind()
                .try_parse(&native_map)
                .map_err(|err| format!("failed to parse blob ref for local blob store: {err}"))?,
        );
    }

    let mut paths = Vec::with_capacity(refs.len());
    for (idx, reference) in refs.iter().enumerate() {
        let dst = output_dir.join(format!("materialized_{idx:06}.bin"));
        paths.push(
            store
                .get_file(reference.as_ref(), &path_to_string(&dst), None)
                .await
                .map_err(|err| format!("failed to materialize blob {idx}: {err}"))?,
        );
    }
    let total_bytes = sum_file_sizes(&paths).await?;

    let mut runtime = runtime.lock().await;
    let current = current_expr_mut(&mut runtime, &request.run_id)?;
    put_artifact(
        current,
        "materialized_paths",
        "materialized_paths",
        serde_json::to_value(&paths).unwrap_or(serde_json::Value::Array(Vec::new())),
    );
    put_metric(current, "blob_get_total_bytes", total_bytes as f64, "bytes");
    current.phase = "blob_batch_materialized".to_string();
    runtime.generation += 1;
    Ok((paths, total_bytes))
}

fn blob_refs_to_json_values(refs: &[BlobRefHandle]) -> Result<Vec<serde_json::Value>, String> {
    refs.iter()
        .map(|reference| {
            reference
                .to_native_map()
                .map(native_map_to_json)
                .map_err(|err| format!("failed to serialize blob ref: {err}"))
        })
        .collect()
}

async fn sum_file_sizes(paths: &[String]) -> Result<u64, String> {
    let mut total = 0_u64;
    for path in paths {
        let metadata = tokio::fs::metadata(path)
            .await
            .map_err(|err| format!("failed to stat {path}: {err}"))?;
        total = total.saturating_add(metadata.len());
    }
    Ok(total)
}

fn unique_batch_id(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    format!("{prefix}-{nanos}")
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn native_map_to_json(map: NativeMap) -> serde_json::Value {
    serde_json::Value::Object(
        map.into_iter()
            .map(|(key, value)| (key, native_value_to_json(value)))
            .collect(),
    )
}

fn native_value_to_json(value: NativeValue) -> serde_json::Value {
    match value {
        NativeValue::Null => serde_json::Value::Null,
        NativeValue::Bool(value) => serde_json::Value::Bool(value),
        NativeValue::Int(value) => serde_json::Value::Number(value.into()),
        NativeValue::Float(value) => serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        NativeValue::String(value) => serde_json::Value::String(value),
        NativeValue::List(values) => {
            serde_json::Value::Array(values.into_iter().map(native_value_to_json).collect())
        }
        NativeValue::Dict(map) => native_map_to_json(map),
    }
}

fn json_to_native_map(value: &serde_json::Value) -> Result<NativeMap, String> {
    match value {
        serde_json::Value::Object(map) => map
            .iter()
            .map(|(key, value)| Ok((key.clone(), json_to_native_value(value)?)))
            .collect(),
        other => Err(format!("expected blob ref JSON object, got {other}")),
    }
}

fn json_to_native_value(value: &serde_json::Value) -> Result<NativeValue, String> {
    Ok(match value {
        serde_json::Value::Null => NativeValue::Null,
        serde_json::Value::Bool(value) => NativeValue::Bool(*value),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                NativeValue::Int(value)
            } else if let Some(value) = value.as_f64() {
                NativeValue::Float(value)
            } else {
                return Err(format!("unsupported JSON number in native value: {value}"));
            }
        }
        serde_json::Value::String(value) => NativeValue::String(value.clone()),
        serde_json::Value::Array(values) => NativeValue::List(
            values
                .iter()
                .map(json_to_native_value)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        serde_json::Value::Object(map) => NativeValue::Dict(
            map.iter()
                .map(|(key, value)| Ok((key.clone(), json_to_native_value(value)?)))
                .collect::<Result<NativeMap, String>>()?,
        ),
    })
}
