use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use lambda_channel::metadata_store_impl::MarkConsumedResult;

use crate::config::{ExperimentSpec, InstanceConfig};
use crate::driver::paced::{boxed_task, run_paced_tasks, PacedTask};

use super::common::{self, MetadataDatapointOutcome};
use super::MetadataWorkload;

#[derive(Default)]
struct ClaimCounters {
    claimed: AtomicUsize,
    already_consumed: AtomicUsize,
    missing: AtomicUsize,
    eof: AtomicUsize,
}

pub(super) async fn run_datapoint(
    instance: &InstanceConfig,
    experiment: &ExperimentSpec,
    workload: MetadataWorkload,
    target_ops_per_s: f64,
) -> Result<MetadataDatapointOutcome, String> {
    let resource_id = common::unique_resource_id(experiment, instance);
    let channel_id = common::channel_id(experiment, &resource_id);
    let resource_dir = common::create_resource_dir(instance, &resource_id).await?;
    let mut store = match common::create_metadata_store(instance, experiment, &resource_id).await {
        Ok(store) => store,
        Err(err) => {
            let _ = common::cleanup_resources(vec![common::MetadataCleanupResource::LocalDir(
                resource_dir,
            )])
            .await;
            return Err(err);
        }
    };
    store
        .cleanup
        .push(common::MetadataCleanupResource::LocalDir(resource_dir));
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
    let claim_target_count = common::env_usize_any(
        experiment,
        &[
            "LC_BENCH_METADATA_CLAIM_TARGET_COUNT",
            "METADATA_CLAIM_TARGET_COUNT",
        ],
    )?
    .unwrap_or(measured_count)
    .max(1);
    let total_elem_count = warmup_count
        .checked_add(claim_target_count)
        .ok_or_else(|| "metadata claim preload count overflowed usize".to_string())?;

    common::create_channel(&store.handle, channel_id).await?;
    common::put_elem_range(
        &store.handle,
        channel_id,
        0,
        total_elem_count,
        payload_size,
        experiment.run.seed,
    )
    .await?;

    for seq in 0..warmup_count {
        store
            .handle
            .mark_consumed(channel_id, common::i64_from_usize(seq)?, "warmup")
            .await
            .map_err(|err| format!("metadata claim warmup failed seq={seq}: {err}"))?;
    }

    let counters = Arc::new(ClaimCounters::default());
    let tasks = build_claim_tasks(
        store.handle.clone(),
        channel_id.to_string(),
        warmup_count,
        measured_count,
        claim_target_count,
        instance.id.clone(),
        Arc::clone(&counters),
    )?;
    let paced = run_paced_tasks(tasks, common::run_config(experiment, target_ops_per_s)?).await?;
    let report = common::MetadataDatapointReport::new(
        instance,
        experiment,
        workload,
        resource_id.to_string(),
        channel_id.to_string(),
        "mark_consumed",
        measured_operations,
        store.details.clone(),
        paced.clone(),
        common::counter_map([
            ("claim_attempt_count", measured_count as u64),
            (
                "claimed_count",
                counters.claimed.load(Ordering::Relaxed) as u64,
            ),
            (
                "already_consumed_count",
                counters.already_consumed.load(Ordering::Relaxed) as u64,
            ),
            (
                "missing_count",
                counters.missing.load(Ordering::Relaxed) as u64,
            ),
            ("eof_count", counters.eof.load(Ordering::Relaxed) as u64),
        ]),
        None,
        Some(claim_target_count),
    );

    Ok(MetadataDatapointOutcome { report, paced })
}

fn build_claim_tasks(
    store: lambda_channel::metadata_store_impl::MetadataStoreHandle,
    channel_id: String,
    start_seq: usize,
    count: usize,
    claim_target_count: usize,
    instance_id: String,
    counters: Arc<ClaimCounters>,
) -> Result<Vec<PacedTask>, String> {
    let mut tasks = Vec::with_capacity(count);
    for index in 0..count {
        let seq = start_seq
            .checked_add(index % claim_target_count)
            .ok_or_else(|| "metadata claim seq overflowed usize".to_string())?;
        let seq_i64 = common::i64_from_usize(seq)?;
        let store = store.clone();
        let channel_id = channel_id.clone();
        let consumer_id = format!("{instance_id}-claim-{index}");
        let counters = Arc::clone(&counters);
        tasks.push(boxed_task(async move {
            let result = store
                .mark_consumed(&channel_id, seq_i64, &consumer_id)
                .await
                .map_err(|err| format!("metadata claim failed seq={seq_i64}: {err}"))?;
            match result {
                MarkConsumedResult::Claimed(_) => {
                    counters.claimed.fetch_add(1, Ordering::Relaxed);
                }
                MarkConsumedResult::AlreadyConsumed(_) => {
                    counters.already_consumed.fetch_add(1, Ordering::Relaxed);
                }
                MarkConsumedResult::Missing => {
                    counters.missing.fetch_add(1, Ordering::Relaxed);
                }
                MarkConsumedResult::Eof => {
                    counters.eof.fetch_add(1, Ordering::Relaxed);
                }
            }
            Ok(())
        }));
    }
    Ok(tasks)
}
