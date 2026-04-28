use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use lambda_channel::blob_store_impl::{BlobRefHandle, BlobStoreHandle};
use lambda_channel::common::{NativeMap, NativeValue};
use tokio::sync::Mutex;
use tokio::time::{sleep_until, Duration, Instant as TokioInstant};

use crate::blob_store_factory::{create_blob_store, BlobStoreCreateOptions};
use crate::config::InstanceConfig;
use crate::driver::paced::{boxed_task, run_paced_tasks, PacedTask, PacedTaskRunConfig};
use crate::payload_file::{create_timestamped_payload_file, PayloadFileSpec};
use crate::rpc::protocol::{
    AcceptedResponse, BlobGetResult, BlobPutResult, GetBlobBatchRequest, InitBlobStoreRequest,
    PacedBlobGetRequest, PacedBlobGetResult, PrepareBlobGetAppendRequest,
    PrepareBlobGetBeginRequest, PrepareBlobGetFinishRequest, PutBlobBatchRequest, RequestResult,
    StartPreparedBlobGetRequest,
};
use crate::rpc::server::state::{
    create_request_on_node, current_expr, current_expr_mut, fail_request_on_node,
    finish_request_on_node, put_artifact, put_metric, BlobStoreResource, NodeRuntimeState,
    PreparedPacedBlobGet, StagedPacedBlobGet,
};

const BLOB_PUT_MAX_ATTEMPTS: usize = 5;
const BLOB_PUT_RETRY_BASE_DELAY_MS: u64 = 50;

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
        (current.run_dir.clone(), current.blob_store.take())
    };
    if let Some(old) = old {
        crate::rpc::server::state::close_blob_store_resource(old, &request.run_id).await?;
    }

    let resource_id = request
        .resource_id
        .clone()
        .unwrap_or_else(|| request.run_id.clone());
    let created = create_blob_store(BlobStoreCreateOptions {
        instance,
        experiment: request.experiment.as_ref(),
        run_dir: &run_dir,
        backend: &request.backend,
        resource_id: &resource_id,
        root_dir: request.root_dir.map(PathBuf::from),
        create_remote_resources: request.create_remote_resources,
    })
    .await?;

    let mut runtime = runtime.lock().await;
    let current = current_expr_mut(&mut runtime, &request.run_id)?;
    current.blob_store = Some(BlobStoreResource {
        backend: created.backend,
        root_dir: created.root_dir,
        handle: created.handle,
    });
    put_artifact(
        current,
        "blob_store_details",
        "blob_store_details",
        serde_json::to_value(&created.details).unwrap_or(serde_json::Value::Null),
    );
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

pub(crate) async fn submit_paced_blob_get_on_node(
    instance_id: &str,
    runtime: Arc<Mutex<NodeRuntimeState>>,
    request: PacedBlobGetRequest,
) -> AcceptedResponse {
    let accepted =
        create_request_on_node(instance_id, &runtime, &request.run_id, "blob-get-paced").await;
    let Some(req_id) = accepted.req_id.clone() else {
        return accepted;
    };

    tokio::spawn(async move {
        let run_id = request.run_id.clone();
        let started_at = Instant::now();
        let result = paced_blob_get_on_node(&runtime, request).await;
        match result {
            Ok((materialized_dir, total_bytes, paced)) => {
                let result = RequestResult::PacedBlobGet(PacedBlobGetResult {
                    count: paced.completed_tasks,
                    total_bytes,
                    elapsed_ms: started_at.elapsed().as_secs_f64() * 1000.0,
                    materialized_dir,
                    paced,
                });
                if let Err(err) = finish_request_on_node(&runtime, &run_id, &req_id, result).await {
                    eprintln!("failed to finish paced blob get request {req_id}: {err}");
                }
            }
            Err(message) => {
                if let Err(err) = fail_request_on_node(&runtime, &run_id, &req_id, message).await {
                    eprintln!("failed to fail paced blob get request {req_id}: {err}");
                }
            }
        }
    });

    accepted
}

pub(crate) async fn prepare_blob_get_begin_on_node(
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    request: PrepareBlobGetBeginRequest,
) -> Result<(), String> {
    if request.plan_id.trim().is_empty() {
        return Err("plan_id must not be empty".to_string());
    }
    if request.expected_ref_count == 0 {
        return Err("expected_ref_count must be greater than zero".to_string());
    }
    if !request.target_ops_per_s.is_finite() || request.target_ops_per_s <= 0.0 {
        return Err("target_ops_per_s must be a finite positive number".to_string());
    }
    if request.max_in_flight == 0 {
        return Err("max_in_flight must be greater than zero".to_string());
    }

    let mut runtime = runtime.lock().await;
    let current = current_expr_mut(&mut runtime, &request.run_id)?;
    current.paced_get_staging.insert(
        request.plan_id,
        StagedPacedBlobGet {
            expected_ref_count: request.expected_ref_count,
            target_ops_per_s: request.target_ops_per_s,
            max_in_flight: request.max_in_flight,
            refs: Vec::with_capacity(request.expected_ref_count),
            next_chunk_index: 0,
        },
    );
    current.phase = "blob_paced_get_staging".to_string();
    runtime.generation += 1;
    Ok(())
}

pub(crate) async fn prepare_blob_get_append_on_node(
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    request: PrepareBlobGetAppendRequest,
) -> Result<(), String> {
    if request.refs.is_empty() {
        return Err("refs chunk must not be empty".to_string());
    }

    let mut runtime = runtime.lock().await;
    let current = current_expr_mut(&mut runtime, &request.run_id)?;
    let plan = current
        .paced_get_staging
        .get_mut(&request.plan_id)
        .ok_or_else(|| format!("unknown paced get staging plan_id={}", request.plan_id))?;
    if request.chunk_index != plan.next_chunk_index {
        return Err(format!(
            "unexpected refs chunk_index={} for plan_id={}, expected {}",
            request.chunk_index, request.plan_id, plan.next_chunk_index
        ));
    }
    if plan.refs.len().saturating_add(request.refs.len()) > plan.expected_ref_count {
        return Err(format!(
            "refs chunks for plan_id={} exceed expected_ref_count={}",
            request.plan_id, plan.expected_ref_count
        ));
    }
    plan.refs.extend(request.refs);
    plan.next_chunk_index += 1;
    current.phase = "blob_paced_get_staging".to_string();
    runtime.generation += 1;
    Ok(())
}

pub(crate) async fn prepare_blob_get_finish_on_node(
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    request: PrepareBlobGetFinishRequest,
) -> Result<(), String> {
    let (run_dir, store, staging) = {
        let mut runtime = runtime.lock().await;
        let current = current_expr_mut(&mut runtime, &request.run_id)?;
        let store = current
            .blob_store
            .as_ref()
            .ok_or_else(|| {
                "prepare_blob_get_finish requires initialized blob_store state".to_string()
            })?
            .handle
            .clone();
        let staging = current
            .paced_get_staging
            .remove(&request.plan_id)
            .ok_or_else(|| format!("unknown paced get staging plan_id={}", request.plan_id))?;
        (current.run_dir.clone(), store, staging)
    };
    if staging.refs.len() != staging.expected_ref_count {
        return Err(format!(
            "paced get plan_id={} received {} refs, expected {}",
            request.plan_id,
            staging.refs.len(),
            staging.expected_ref_count
        ));
    }

    let refs = parse_blob_refs(&store, &staging.refs)?;
    let output_dir = run_dir
        .join("materialized")
        .join(unique_batch_id("paced-get"));
    tokio::fs::create_dir_all(&output_dir)
        .await
        .map_err(|err| {
            format!(
                "failed to create materialize output dir {}: {err}",
                output_dir.display()
            )
        })?;

    let mut runtime = runtime.lock().await;
    let current = current_expr_mut(&mut runtime, &request.run_id)?;
    current.paced_get_plans.insert(
        request.plan_id,
        PreparedPacedBlobGet {
            target_ops_per_s: staging.target_ops_per_s,
            max_in_flight: staging.max_in_flight,
            refs,
            materialized_dir: output_dir,
        },
    );
    current.phase = "blob_paced_get_ready".to_string();
    runtime.generation += 1;
    Ok(())
}

pub(crate) async fn submit_prepared_blob_get_on_node(
    instance_id: &str,
    runtime: Arc<Mutex<NodeRuntimeState>>,
    request: StartPreparedBlobGetRequest,
) -> AcceptedResponse {
    let accepted =
        create_request_on_node(instance_id, &runtime, &request.run_id, "blob-get-prepared").await;
    let Some(req_id) = accepted.req_id.clone() else {
        return accepted;
    };

    tokio::spawn(async move {
        let run_id = request.run_id.clone();
        let started_at = Instant::now();
        let result = prepared_paced_blob_get_on_node(&runtime, request).await;
        match result {
            Ok((materialized_dir, total_bytes, paced)) => {
                let result = RequestResult::PacedBlobGet(PacedBlobGetResult {
                    count: paced.completed_tasks,
                    total_bytes,
                    elapsed_ms: started_at.elapsed().as_secs_f64() * 1000.0,
                    materialized_dir,
                    paced,
                });
                if let Err(err) = finish_request_on_node(&runtime, &run_id, &req_id, result).await {
                    eprintln!("failed to finish prepared blob get request {req_id}: {err}");
                }
            }
            Err(message) => {
                if let Err(err) = fail_request_on_node(&runtime, &run_id, &req_id, message).await {
                    eprintln!("failed to fail prepared blob get request {req_id}: {err}");
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
    if request.max_in_flight == 0 {
        return Err("put_blob_batch max_in_flight must be greater than zero".to_string());
    }

    let (run_dir, store_backend, store) = {
        let runtime = runtime.lock().await;
        let current = current_expr(&runtime, &request.run_id)?;
        (
            current.run_dir.clone(),
            current
                .blob_store
                .as_ref()
                .ok_or_else(|| "put_blob_batch requires initialized blob_store state".to_string())?
                .backend
                .clone(),
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

    let cleanup_payloads_after_put = store_backend != "local-file";
    let (refs, paths) = put_generated_files_concurrent(
        store,
        &payload_dir,
        &request.run_id,
        request.count,
        request.object_size_bytes,
        request.max_in_flight,
        cleanup_payloads_after_put,
    )
    .await?;
    let total_bytes = request
        .object_size_bytes
        .checked_mul(request.count as u64)
        .ok_or_else(|| "blob put total bytes overflowed u64".to_string())?;
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

async fn put_generated_files_concurrent(
    store: BlobStoreHandle,
    payload_dir: &Path,
    run_id: &str,
    count: usize,
    object_size_bytes: u64,
    max_in_flight: usize,
    cleanup_payloads_after_put: bool,
) -> Result<(Vec<BlobRefHandle>, Vec<String>), String> {
    let mut refs: Vec<Option<BlobRefHandle>> = vec![None; count];
    let mut paths: Vec<Option<String>> = vec![None; count];
    let mut tasks = tokio::task::JoinSet::new();
    let mut next_index = 0usize;

    while next_index < count || !tasks.is_empty() {
        while next_index < count && tasks.len() < max_in_flight {
            let index = next_index;
            let path = payload_dir.join(format!("payload_{index:06}.bin"));
            let path_string = path_to_string(&path);
            let run_id = run_id.to_string();
            let store = store.clone();
            tasks.spawn(async move {
                create_timestamped_payload_file(
                    &path,
                    PayloadFileSpec {
                        run_id: &run_id,
                        seed: 0,
                        index: index as u64,
                        size_bytes: object_size_bytes,
                    },
                )
                .await?;
                let reference = put_file_with_retry(&store, &path_string).await?;
                if cleanup_payloads_after_put {
                    tokio::fs::remove_file(&path).await.map_err(|err| {
                        format!(
                            "failed to remove staged payload {} after put: {err}",
                            path.display()
                        )
                    })?;
                }
                Ok::<(usize, String, BlobRefHandle), String>((index, path_string, reference))
            });
            next_index += 1;
        }

        let Some(result) = tasks.join_next().await else {
            continue;
        };
        let (index, path, reference) =
            result.map_err(|err| format!("blob put task failed: {err}"))??;
        paths[index] = Some(path);
        refs[index] = Some(reference);
    }

    let refs = refs
        .into_iter()
        .enumerate()
        .map(|(index, reference)| {
            reference.ok_or_else(|| format!("missing blob ref for payload index {index}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let paths = paths
        .into_iter()
        .enumerate()
        .map(|(index, path)| path.ok_or_else(|| format!("missing payload path for index {index}")))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((refs, paths))
}

async fn put_file_with_retry(
    store: &BlobStoreHandle,
    path_string: &str,
) -> Result<BlobRefHandle, String> {
    let mut last_error = String::new();
    for attempt in 1..=BLOB_PUT_MAX_ATTEMPTS {
        match store.put_file(path_string).await {
            Ok(reference) => return Ok(reference),
            Err(err) => {
                last_error = format!("{err}");
                if attempt < BLOB_PUT_MAX_ATTEMPTS {
                    let delay_ms =
                        BLOB_PUT_RETRY_BASE_DELAY_MS.saturating_mul(1_u64 << (attempt - 1));
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
            }
        }
    }
    Err(format!(
        "failed to put payload {path_string} after {BLOB_PUT_MAX_ATTEMPTS} attempts: {last_error}"
    ))
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

pub(crate) async fn paced_blob_get_on_node(
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    request: PacedBlobGetRequest,
) -> Result<(String, u64, crate::driver::paced::PacedTaskRunReport), String> {
    if request.refs.is_empty() {
        return Err("paced blob get requires at least one blob ref".to_string());
    }
    if !request.target_ops_per_s.is_finite() || request.target_ops_per_s <= 0.0 {
        return Err("target_ops_per_s must be a finite positive number".to_string());
    }
    if request.max_in_flight == 0 {
        return Err("max_in_flight must be greater than zero".to_string());
    }

    let (run_dir, store) = {
        let runtime = runtime.lock().await;
        let current = current_expr(&runtime, &request.run_id)?;
        (
            current.run_dir.clone(),
            current
                .blob_store
                .as_ref()
                .ok_or_else(|| "paced blob get requires initialized blob_store state".to_string())?
                .handle
                .clone(),
        )
    };
    let output_dir = run_dir
        .join("materialized")
        .join(unique_batch_id("paced-get"));
    tokio::fs::create_dir_all(&output_dir)
        .await
        .map_err(|err| {
            format!(
                "failed to create materialize output dir {}: {err}",
                output_dir.display()
            )
        })?;

    let refs = parse_blob_refs(&store, &request.refs)?;
    if let Some(start_after_unix_ns) = request.start_after_unix_ns {
        wait_until_unix_ns(start_after_unix_ns).await;
    }

    let tasks = build_get_tasks(store, refs, output_dir.clone());
    let paced = run_paced_tasks(
        tasks,
        PacedTaskRunConfig {
            target_ops_per_s: request.target_ops_per_s,
            max_in_flight: request.max_in_flight,
            pacer_core_id: None,
        },
    )
    .await?;
    let paths = materialized_paths(&output_dir, request.refs.len());
    let total_bytes = sum_existing_file_sizes(&paths).await?;

    let output_dir_string = path_to_string(&output_dir);
    let mut runtime = runtime.lock().await;
    let current = current_expr_mut(&mut runtime, &request.run_id)?;
    put_artifact(
        current,
        "paced_materialized_dir",
        "paced_materialized_dir",
        serde_json::Value::String(output_dir_string.clone()),
    );
    put_metric(current, "blob_get_total_bytes", total_bytes as f64, "bytes");
    current.phase = "blob_paced_get_finished".to_string();
    runtime.generation += 1;
    Ok((output_dir_string, total_bytes, paced))
}

pub(crate) async fn prepared_paced_blob_get_on_node(
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    request: StartPreparedBlobGetRequest,
) -> Result<(String, u64, crate::driver::paced::PacedTaskRunReport), String> {
    let plan = {
        let mut runtime = runtime.lock().await;
        let current = current_expr_mut(&mut runtime, &request.run_id)?;
        let plan = current
            .paced_get_plans
            .remove(&request.plan_id)
            .ok_or_else(|| format!("unknown prepared paced get plan_id={}", request.plan_id))?;
        current.phase = "blob_paced_get_armed".to_string();
        runtime.generation += 1;
        plan
    };

    if let Some(start_after_unix_ns) = request.start_after_unix_ns {
        wait_until_unix_ns(start_after_unix_ns).await;
    }

    let (store, output_dir) = {
        let runtime = runtime.lock().await;
        let current = current_expr(&runtime, &request.run_id)?;
        (
            current
                .blob_store
                .as_ref()
                .ok_or_else(|| {
                    "start_prepared_blob_get requires initialized blob_store state".to_string()
                })?
                .handle
                .clone(),
            plan.materialized_dir.clone(),
        )
    };

    let ref_count = plan.refs.len();
    let tasks = build_get_tasks(store, plan.refs, output_dir.clone());
    let paced = run_paced_tasks(
        tasks,
        PacedTaskRunConfig {
            target_ops_per_s: plan.target_ops_per_s,
            max_in_flight: plan.max_in_flight,
            pacer_core_id: None,
        },
    )
    .await?;
    let paths = materialized_paths(&output_dir, ref_count);
    let total_bytes = sum_existing_file_sizes(&paths).await?;

    let output_dir_string = path_to_string(&output_dir);
    let mut runtime = runtime.lock().await;
    let current = current_expr_mut(&mut runtime, &request.run_id)?;
    put_artifact(
        current,
        "paced_materialized_dir",
        "paced_materialized_dir",
        serde_json::Value::String(output_dir_string.clone()),
    );
    put_metric(current, "blob_get_total_bytes", total_bytes as f64, "bytes");
    current.phase = "blob_paced_get_finished".to_string();
    runtime.generation += 1;
    Ok((output_dir_string, total_bytes, paced))
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

fn parse_blob_refs(
    store: &BlobStoreHandle,
    values: &[serde_json::Value],
) -> Result<Vec<BlobRefHandle>, String> {
    values
        .iter()
        .map(|value| {
            let native_map = json_to_native_map(value)?;
            store
                .blob_ref_kind()
                .try_parse(&native_map)
                .map_err(|err| format!("failed to parse blob ref: {err}"))
        })
        .collect()
}

fn build_get_tasks(
    store: BlobStoreHandle,
    refs: Vec<BlobRefHandle>,
    output_dir: PathBuf,
) -> Vec<PacedTask> {
    refs.into_iter()
        .enumerate()
        .map(|(idx, reference)| {
            let store = store.clone();
            let dst = path_to_string(&output_dir.join(format!("materialized_{idx:08}.bin")));
            boxed_task(async move {
                store
                    .get_file(reference.as_ref(), &dst, None)
                    .await
                    .map(|_| ())
                    .map_err(|err| format!("paced get failed for index {idx}: {err}"))
            })
        })
        .collect()
}

fn materialized_paths(output_dir: &Path, count: usize) -> Vec<String> {
    (0..count)
        .map(|idx| path_to_string(&output_dir.join(format!("materialized_{idx:08}.bin"))))
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

async fn sum_existing_file_sizes(paths: &[String]) -> Result<u64, String> {
    let mut total = 0_u64;
    for path in paths {
        match tokio::fs::metadata(path).await {
            Ok(metadata) => {
                total = total.saturating_add(metadata.len());
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(format!("failed to stat {path}: {err}")),
        }
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

async fn wait_until_unix_ns(start_after_unix_ns: u64) {
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(u64::MAX as u128) as u64)
        .unwrap_or(0);
    if start_after_unix_ns <= now_ns {
        return;
    }
    let delta = Duration::from_nanos(start_after_unix_ns - now_ns);
    sleep_until(TokioInstant::now() + delta).await;
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
