# CloudLab Experiment Principles

This document records the baseline principles for CloudLab performance runs of
`lambda-channel-benchmark`. It is meant to keep experiment interpretation stable
before we start tuning individual workloads.

## Target Hardware

Primary CloudLab target:

- Cluster/node type: CloudLab `c6620`
- CPU: 28-core Intel Xeon Gold 5512U at 2.1 GHz
- Network: 25 GbE and 100 GbE
- Source: https://docs.cloudlab.us/hardware.html

Unless a run explicitly says otherwise, results should be interpreted as
single-node CloudLab results on this class of machine.

## Managed Services

CloudLab nodes run the benchmark daemon, but S3 and DynamoDB are AWS managed
services.

- Default AWS region: `us-west-2`
- Do not use MinIO or DynamoDB Local for formal CloudLab runs.
- Table and bucket creation/cleanup are experiment lifecycle work and should not
  be counted as measured datapath work.
- S3/DynamoDB cleanup is local GC work, not synchronous work inside a datapoint.
  Experiment code should defer AWS deletion and only clean local temporary
  files. Run prefix-scoped GC before and after AWS-backed sweeps. Empty S3
  buckets may be deleted before a run; non-empty S3 buckets should be force
  cleaned after a run with AWS CLI cleanup. If cleanup fails, preserve the CSV
  and rerun the GC helper by prefix.
- Every AWS resource created by the benchmark must use a stable experiment
  prefix so interrupted runs can be cleaned up safely.
- Do not treat `inmemory` metadata as a formal CloudLab metadata backend.

## Runtime Principles

The benchmark has several concurrency layers. These knobs are not
interchangeable.

### Main `lc-bench` Tokio Runtime

The main benchmark binary uses an explicit Tokio worker-thread count so runs are
easier to compare across machines and rebuilds.

Initial principle:

- Use `8` main Tokio worker threads for the benchmark harness unless the run is
  explicitly studying runtime thread count.
- Treat this as a process/runtime setting, not a P2P store setting.
- If runtime thread count is studied, sweep it separately from workload
  concurrency.

### P2P Internal Helper Workers

The P2P store has an internal helper concurrency knob exposed in the library as
`non_abortable_task_workers`, which becomes `parallel_op_max_workers` in Rust.
It is not an operating-system thread count.

It affects P2P internal helper work such as critical background work, close-time
chunk work, and persist helper batches. It does not directly control the main
Tokio runtime or chunk-server transfer threads.

Initial principle:

- Do not interpret this as the same thing as `native_worker_threads`.
- Set this high enough that required but non-latency-sensitive helper work does
  not become an artificial bottleneck for latency-sensitive foreground work.
- For microbenchmarks, use `16` uniformly. This keeps hidden helper concurrency
  bounded and makes latency results easier to interpret.
- Treat this as a queueing/backlog-avoidance setting, not a CPU thread budget.
- Record the value with every P2P result.
- If background persist, close-time convergence, or helper backlog becomes part
  of the measured path, run a separate sensitivity sweep such as `8`, `16`,
  `32`, and `64` before making final claims.

### P2P Chunk Server Runtime

The P2P chunk server uses an isolated async runtime. A value of `1` is a
current-thread runtime; values greater than `1` create an isolated multi-thread
Tokio runtime.

Initial principle:

- For put-only experiments, this should have little effect because the chunk
  server is mostly relevant to fetch/materialize paths.
- For P2P fetch/materialize experiments, use `4` as a practical fixed baseline
  on `c6620`.
- If studying the chunk server itself, sweep `1`, `2`, `4`, and `8`.

## P2P Acceleration

Source-host acceleration is a normal P2P fetch optimization. It helps receivers
reuse known recent source hosts before falling back to wider holder lookup.

Initial principle:

- Keep source-host acceleration enabled for normal P2P fetch/materialize
  performance runs.
- For put-only runs, the setting is mostly irrelevant and should not be used to
  explain put performance.
- Disable it only for a specific tracker-lookup isolation or debugging run.
- Record the source-host preference probability with the result.

## Workload Principles

### Setup Work

Setup and preload phases should avoid artificial serialization. If a workload
must create many independent blobs, metadata rows, or tracker records before
the measured phase starts, the setup path should use bounded concurrency by
default and record the concurrency used.

Initial principle:

- Do not use sequential preload for multi-object benchmark setup unless the
  experiment is explicitly studying serialized setup cost.
- Bound setup concurrency with an explicit knob so retries and managed-service
  throttling remain controlled.
- Keep setup latency out of the measured datapath, but record it separately
  when it can affect experiment feasibility.
- For blob multi-getter preload, default to `benchmark.concurrency` and allow
  `LC_BENCH_BLOB_PRELOAD_CONCURRENCY` to override it.

### Throughput Sweeps

Throughput sweeps must stop once a completed datapoint shows the system is past
the configured saturation threshold. This rule applies to every workload,
including workload-specific sweeps that list explicit candidate rates.

Initial principle:

- Treat `throughput_sweep.points_ops_per_s` as a candidate list, not as a list
  that must be exhausted unconditionally.
- After every successful datapoint, compare successful throughput with target
  throughput. Stop when `successful_ops_per_s / target_ops_per_s` is below
  `saturation_achieved_ratio`, which defaults to `0.5`.
- Failed tasks are datapoint quality signals, not saturation by themselves.
  Record them in the datapoint and continue the sweep by default.
- Use `stop_on_failure = true` only for experiments that explicitly want
  fail-fast semantics. If enabled, stop after a datapoint reports failed tasks
  instead of moving to a higher rate.
- Do not continue into more expensive setup/preload work for higher rates after
  the measured phase has already shown saturation.
- For `channel.single_sender_multi_receiver`, use receiver-delivered throughput
  as the primary saturation signal. The generic `target_ops_per_s`,
  `achieved_ops_per_s`, and `successful_ops_per_s` columns are mapped to
  expected receiver delivery rate and aggregate delivered receiver rate, not to
  sender push rate. Stop after sender-side saturation or after delivered
  throughput stops improving materially across completed datapoints.

### Channel Data-Plane Metrics

`channel.single_sender_multi_receiver` measures end-to-end channel delivery,
not a standalone blob or metadata operation. Interpret its CSV fields as
follows:

- `target_sender_ops_per_s` is the sender push target.
- `expected_receiver_ops_per_s` is the expected aggregate receiver delivery
  rate. For fanout it is `target_sender_ops_per_s * receiver_count`; for
  competitive receive it is `target_sender_ops_per_s`.
- `aggregate_delivered_ops_per_s` is the primary measured throughput. It is
  computed from all delivered receiver elements over the global receiver
  interval: earliest receiver start to latest receiver finish.
- `sum_receiver_ops_per_s`, `mean_receiver_ops_per_s`,
  `slowest_receiver_ops_per_s`, and `fastest_receiver_ops_per_s` are secondary
  per-receiver rate summaries. Use them to understand imbalance, not as the
  primary throughput number.
- `sender_successful_push_ops_per_s`, `sender_limited`, and
  `sender_limited_ratio` describe whether the sender could sustain the offered
  push rate. If the sender is limited, receiver throughput is still useful but
  should be reported as sender-limited.
- `delivery_latency_*_ms` is the channel end-to-end latency. It is measured
  from the sender timestamp patched into the payload immediately before
  `sender.push(...)` to the receiver time after materializing the blob and
  reading that payload header.
- `materialize_latency_*_ms` is only the receiver materialization portion:
  `BlobElemPtr::get_with_options(..., prefer_link = true)` until the file is
  available locally. It is a breakdown of the delivery path, not the whole
  channel latency.
- For channel rows, the generic `offered_*_ms` and `service_*_ms` columns are
  schema-compatibility aliases of `delivery_latency_*_ms`. Do not interpret
  them as paced-task offered or service latency for channel plots.
- For channel rows, `schedule_lag_*_ms` is not a meaningful datapath metric and
  should not be used for analysis.
- Per-receiver latency samples are collected in chunks by the orchestrator to
  avoid oversized RPC responses, then aggregated for the CSV. Detailed
  per-receiver behavior belongs in logs or JSON artifacts rather than expanded
  CSV columns.
- Failed tasks at the final high-pressure datapoints are overload signals. They
  may be retained for peak/saturation evidence, but normal-service latency
  claims should emphasize the pre-overload or low-failure datapoints.

### Workflow Health Checks

CloudLab workflows should tolerate short RPC or TCP hiccups around daemon
restart and between experiment TOMLs.

Initial principle:

- Do not fail a long multi-point workflow on a single transient RPC health
  timeout.
- Retry node health checks with a bounded attempt count, per-attempt timeout,
  and short delay before declaring the workflow failed.
- Keep retries limited so real daemon crashes still surface promptly and trigger
  normal log collection and cleanup.

### Object Size

`32B` objects mostly measure request/control-plane overhead. They are useful,
but they should not be presented as data-plane throughput.

Formal size sweep:

- `32B`
- `16MiB`
- `128MiB`
- `1GiB`

### Concurrency

Do not collapse latency-baseline and saturation experiments into one setting.

Recommended categories:

- Latency baseline: low concurrency such as `1`, `4`, and `16`
- Throughput saturation: higher concurrency such as `64`, `128`, and `256`

### Duration and Repetition

Short 10-second points are good for first CloudLab smoke runs. They are not
enough for formal AWS-backed conclusions.

Formal AWS-backed runs should prefer:

- At least `30s` per datapoint, often `60s` for final numbers
- Multiple independent repetitions
- Explicit recording of failed tasks, schedule lag, and achieved/target ratio

## Fairness Notes

`blob.put.s3` uploads to durable S3. Current `blob.put.p2p` with
`persist_backend = "none"` stages locally and registers metadata in DynamoDB,
but does not upload durable payload bytes to S3. These are useful baselines, but
they are not equivalent durability semantics.

For fair comparisons:

- Label P2P put without persistence as local-cache plus DynamoDB tracker put.
- Label S3 put as durable S3 object put.
- Add a separate P2P persist experiment before claiming durable write
  equivalence.

## Current Harness Gaps

Some library tuning knobs are not fully exposed by the benchmark TOML schema.
Before formal tuning, either expose them explicitly or record their hardcoded
values:

- P2P background persist settings
- Persist-store upload/head worker counts
