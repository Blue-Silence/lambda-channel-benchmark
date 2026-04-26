use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::config::{ExperimentSpec, InstanceConfig};
use crate::driver::paced::{boxed_task, run_paced_tasks, PacedTask};

use super::common::{self, MetadataDatapointOutcome};
use super::MetadataWorkload;

pub(super) async fn run_datapoint(
    instance: &InstanceConfig,
    experiment: &ExperimentSpec,
    workload: MetadataWorkload,
    target_ops_per_s: f64,
) -> Result<MetadataDatapointOutcome, String> {
    let resource_id = common::unique_resource_id(experiment, instance);
    let channel_id = common::channel_id(experiment, &resource_id);
    let store = common::create_metadata_store(instance, experiment, &resource_id).await?;
    let result = execute(
        instance,
        experiment,
        workload,
        target_ops_per_s,
        &resource_id,
        &channel_id,
        &store,
    )
    .await;
    let cleanup_result = common::cleanup_resources(store.cleanup).await;

    match result {
        Ok(outcome) => {
            cleanup_result?;
            Ok(outcome)
        }
        Err(err) => Err(common::with_followup_errors(err, cleanup_result)),
    }
}

async fn execute(
    instance: &InstanceConfig,
    experiment: &ExperimentSpec,
    workload: MetadataWorkload,
    target_ops_per_s: f64,
    resource_id: &str,
    channel_id: &str,
    store: &common::MetadataStoreResource,
) -> Result<MetadataDatapointOutcome, String> {
    let measured_operations = common::measured_operations(experiment, target_ops_per_s)?;
    let measured_count = common::measured_count(experiment, target_ops_per_s)?;
    let warmup_count = common::warmup_count(experiment)?;
    let payload_size = common::payload_size(experiment)?;
    let scan_limit = common::env_usize_any(
        experiment,
        &["LC_BENCH_METADATA_SCAN_LIMIT", "METADATA_SCAN_LIMIT"],
    )?
    .unwrap_or(32)
    .max(1);
    let preload_count = warmup_count
        .checked_add(measured_count)
        .and_then(|count| count.checked_add(scan_limit))
        .ok_or_else(|| "metadata prefix scan preload count overflowed usize".to_string())?;

    common::create_channel(&store.handle, channel_id).await?;
    common::put_elem_range_concurrent(
        &store.handle,
        channel_id,
        0,
        preload_count,
        payload_size,
        experiment.run.seed,
        experiment.benchmark.concurrency,
    )
    .await?;

    for start_seq in 0..warmup_count {
        store
            .handle
            .list_elems(channel_id, common::i64_from_usize(start_seq)?, scan_limit)
            .await
            .map_err(|err| format!("metadata prefix-scan warmup failed seq={start_seq}: {err}"))?;
    }

    let listed_count = Arc::new(AtomicUsize::new(0));
    let tasks = build_scan_tasks(
        store.handle.clone(),
        channel_id.to_string(),
        warmup_count,
        measured_count,
        scan_limit,
        Arc::clone(&listed_count),
    )?;
    let paced = run_paced_tasks(tasks, common::run_config(experiment, target_ops_per_s)?).await?;
    let report = common::MetadataDatapointReport::new(
        instance,
        experiment,
        workload,
        resource_id.to_string(),
        channel_id.to_string(),
        "list_elems",
        measured_operations,
        store.details.clone(),
        paced.clone(),
        common::counter_map([
            ("scan_count", measured_count as u64),
            (
                "listed_elem_count",
                listed_count.load(Ordering::Relaxed) as u64,
            ),
        ]),
        Some(scan_limit),
        None,
    );

    Ok(MetadataDatapointOutcome { report, paced })
}

fn build_scan_tasks(
    store: lambda_channel::metadata_store_impl::MetadataStoreHandle,
    channel_id: String,
    start_seq: usize,
    count: usize,
    scan_limit: usize,
    listed_count: Arc<AtomicUsize>,
) -> Result<Vec<PacedTask>, String> {
    let mut tasks = Vec::with_capacity(count);
    for index in 0..count {
        let seq = start_seq
            .checked_add(index)
            .ok_or_else(|| "metadata prefix scan seq overflowed usize".to_string())?;
        let seq_i64 = common::i64_from_usize(seq)?;
        let store = store.clone();
        let channel_id = channel_id.clone();
        let listed_count = Arc::clone(&listed_count);
        tasks.push(boxed_task(async move {
            let elems = store
                .list_elems(&channel_id, seq_i64, scan_limit)
                .await
                .map_err(|err| format!("metadata prefix scan failed seq={seq_i64}: {err}"))?;
            listed_count.fetch_add(elems.len(), Ordering::Relaxed);
            Ok(())
        }));
    }
    Ok(tasks)
}
