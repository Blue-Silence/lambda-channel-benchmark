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

### Object Size

`32B` objects mostly measure request/control-plane overhead. They are useful,
but they should not be presented as data-plane throughput.

Formal size sweep:

- `32B`
- `4KiB`
- `64KiB`
- `1MiB`

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
