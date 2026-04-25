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
    let measured_count = common::measured_count(experiment)?;
    let warmup_count = common::warmup_count(experiment)?;
    let payload_size = common::payload_size(experiment)?;

    common::create_channel(&store.handle, channel_id).await?;
    common::put_elem_range(
        &store.handle,
        channel_id,
        0,
        warmup_count,
        payload_size,
        experiment.run.seed,
    )
    .await?;

    let tasks = build_append_tasks(
        store.handle.clone(),
        channel_id.to_string(),
        warmup_count,
        measured_count,
        payload_size,
        experiment.run.seed,
    )?;
    let paced = run_paced_tasks(tasks, common::run_config(experiment, target_ops_per_s)?).await?;
    let report = common::MetadataDatapointReport::new(
        instance,
        experiment,
        workload,
        resource_id.to_string(),
        channel_id.to_string(),
        "put_elem",
        store.details.clone(),
        paced.clone(),
        common::counter_map([("appended_count", measured_count as u64)]),
        None,
        None,
    );

    Ok(MetadataDatapointOutcome { report, paced })
}

fn build_append_tasks(
    store: lambda_channel::metadata_store_impl::MetadataStoreHandle,
    channel_id: String,
    start_seq: usize,
    count: usize,
    payload_size: usize,
    seed: u64,
) -> Result<Vec<PacedTask>, String> {
    let mut tasks = Vec::with_capacity(count);
    for index in 0..count {
        let seq = start_seq
            .checked_add(index)
            .ok_or_else(|| "metadata append seq overflowed usize".to_string())?;
        let seq_i64 = common::i64_from_usize(seq)?;
        let store = store.clone();
        let channel_id = channel_id.clone();
        tasks.push(boxed_task(async move {
            store
                .put_elem(common::new_elem(&channel_id, seq_i64, seed, payload_size))
                .await
                .map_err(|err| format!("metadata append failed seq={seq_i64}: {err}"))
        }));
    }
    Ok(tasks)
}
