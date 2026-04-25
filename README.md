# lambda-channel-benchmark

Rust local microbenchmark harness for `lambda-channel`.

This repository is the experiment-development workspace for the local
microbenchmarks described in `.lambda-channel-paper/sections/eval_plan.md`.
It links the library implementation through `.p2p-data-transfer` and is meant
to keep benchmark code separate from both the paper and the communication
library itself.

## Layout

- `.p2p-data-transfer`: linked checkout of the library under test.
- `.lambda-channel-paper`: linked checkout of the paper and evaluation plan.
- `config/`: local experiment specs plus topology-specific instance inventories.
- `src/`: Rust benchmark harness.

## Current CLI Shape

```bash
cargo run -- proxy --url 127.0.0.1:19000 --experiment config/experiments/metadata/append.toml
cargo run -- blob --backend local-file --object-size 64MiB --operations 1000
cargo run -- sender --backend local-file --operations 10000
cargo run -- receiver --backend local-file --operations 10000
```

The first project skeleton only wires the CLI, configuration model, JSON
report shape, and direct Rust dependency on `lambda-channel`. The benchmark
workloads are intentionally stubbed so they can be filled in one at a time:
metadata, blob, sender, and receiver.

## Coordination Model

The distributed benchmark model is deployment-symmetric and run-asymmetric:
every instance starts the same long-running `node` daemon. A thin proxy connects
to one node by RPC URL and asks it to orchestrate a single experiment run. That
node runs a workload-specific Rust function that directly calls simple peer RPC
primitives. Each node keeps one run-scoped expr state with resource slots for
blob store, metadata store, sender, and receiver. The orchestrating node can ask
a peer to initialize a slot, submit operations, poll peer-generated request ids
for operation return values, and reset the state after the run. For example,
blob get is:

```text
orchestrator -> peer.begin_expr + peer.init_blob_store
orchestrator -> peer.put_blob_batch(count, object_size)
orchestrator <- req_id
orchestrator -> peer.poll_request(req_id)
orchestrator <- BlobPutResult { blob_refs, ... }
orchestrator -> local begin_expr + init_blob_store + get_blob_batch(blob_refs)
orchestrator <- req_id
orchestrator -> local poll_request(req_id)
orchestrator <- BlobGetResult { materialized_paths, ... }
orchestrator -> peer.reset_expr + local reset_expr
```

The measured data path remains the selected `lambda-channel` backend under
test. RPC is only the experiment control plane: asking peers to prepare state,
return refs/metadata, collect status, and clean up run-scoped resources.

Local config entry points:

- `config/instances/`: node startup inventories/topologies with RPC addresses,
  P2P advertised endpoints, per-instance work directories, and optional
  capabilities. Nodes read only this long-lived inventory when they start.
  `local-two.toml` is the default same-host two-node topology, and
  `single-node.toml` contains one node named `node-0`.
- `config/local-instances.toml`: compatibility copy of the old local two-node
  inventory.
- `config/local-experiment.toml`: default experiment spec used by proxy or
  trigger. It contains workload, backend, benchmark knobs, coordination
  timeouts, participant labels, and environment values, then travels through
  `run_experiment` RPC.
- `config/experiments/blob/`: blob microbenchmark scenario templates.

Current coordination skeleton:

```bash
LC_BENCH_INSTANCE_ID=local-a cargo run -- node --instances config/instances/local-two.toml
LC_BENCH_INSTANCE_ID=local-b cargo run -- node --instances config/instances/local-two.toml
cargo run -- proxy --url 127.0.0.1:19000 --experiment config/experiments/blob/get-materialize.toml
cargo run -- blob-get --coordinator local-a --peer local-b --backend local-file --count 1000 --object-size 64KiB
```

Single-node topology:

```bash
cargo run -- node --instances config/instances/single-node.toml
cargo run -- proxy --url node-0:19000 --experiment config/experiments/blob/put.toml
```

`--instance-id` can still be passed explicitly for local development. The old
`agent` and `client` commands are kept as compatibility aliases. The old
`trigger --coordinator <id>` entry point is also still available, but the
preferred generic submission path is now `proxy --url <rpc-addr>`, which reads
only the experiment spec and sends it over RPC. Peer inventory comes from the
target node's startup instance list.

The RPC coordination layer uses `tarpc` over TCP. The `rpc_addr` values in the
instance list are tarpc control-plane addresses such as `127.0.0.1:19000`;
`p2p_advertise_endpoint` remains the data-plane chunk-server endpoint used by
the library under test.

RPC implementation layout:

- `src/rpc/protocol.rs`: tarpc service trait and request/response structs;
  `run_experiment` carries an `ExperimentSpec`, not an instance list.
- `src/rpc/client.rs`: client-side peer connection helpers.
- `src/rpc/server/listener.rs`: tarpc TCP listener for a node.
- `src/rpc/server/service.rs`: `NodeRpcService` method implementations.
- `src/rpc/server/state.rs`: peer-side run-scoped expr state lifecycle.
- `src/rpc/server/blob.rs`: peer-side blob-store initialization plus put/get
  primitives.
- `src/rpc/address.rs`: shared RPC address parsing.

Experiment implementation layout:

- `src/experiments/dispatcher.rs`: dispatches `run_experiment` by workload.
- `src/experiments/blob/put.rs`: blob put-throughput experiment flow.
- `src/experiments/blob/get_materialize.rs`: blob get/materialize flow.
- `src/experiments/blob/p2p_peer_fetch.rs`: remote-holder P2P fetch flow.
- `src/experiments/blob/control.rs`: shared helpers for asking target nodes
  to begin/reset expr state, submit operations, and poll request ids.
- `src/experiments/metadata/`: DynamoDB metadata-store experiments for append,
  prefix scan, and local competitive claim.

## Blob Experiment Group

The blob group is moving to direct experiment runners plus reusable peer RPC
primitives. Scenario configs live under `config/experiments/blob/`:

- `put.toml`
- `get-materialize.toml`
- `p2p-local-hit.toml`
- `p2p-peer-fetch.toml`
- `persist-upload.toml`
- `fallback-fetch.toml`

Currently implemented direct primitives:

- `begin_expr`: create or replace one run-scoped expr state on a node.
- `init_blob_store`: initialize the state's blob-store slot.
- `init_metadata_store`, `init_sender`, `init_receiver`: initialize the
  channel-related state slots for upcoming sender/receiver experiments.
- `put_blob_batch`: accepts a blob put operation and returns a peer-generated
  `req_id`; the final return value is available through `poll_request`.
- `get_blob_batch`: accepts a blob get operation and returns a peer-generated
  `req_id`; the final return value is available through `poll_request`.
- `poll_request`: returns the status and final return value for one submitted
  operation, such as `BlobPutResult` or `BlobGetResult`.
- `poll_expr`: returns resource status, artifacts, request summaries, and
  metrics from the active state for debugging/snapshot use.
- `reset_expr`: drops the current run-scoped state.
- `run_blob_get`: orchestrator asks a peer to `put_blob_batch`, then
  polls the returned `req_id` for `BlobPutResult` and materializes those refs.

`run_experiment` now dispatches by workload to direct Rust orchestration
instead of interpreting a generic prepare/run task pipeline. Local-file
`blob.put` and `blob.get_materialize` are wired; P2P and persist/fallback still
need backend-specific primitives.

## Metadata Experiment Group

The core metadata-store experiments are intentionally narrow and use DynamoDB as
the formal backend:

- `metadata.append`: paced `put_elem` append/write path.
- `metadata.prefix_scan`: pre-populated paced `list_elems` scan/poll path.
- `metadata.competitive_claim.local`: one-node paced `mark_consumed`
  competitive-claim path with success/conflict counters.

Configs live under `config/experiments/metadata/`. The distributed competitive
claim experiment is planned but not implemented yet. `proxy --csv` flattens
these metadata reports into the shared datapoint-v2 CSV schema with metadata
operation, channel, table, scan, and claim-counter columns.

### Planned get experiment groups

The two most important future blob-store experiments are get-heavy and will
need multi-machine coordination:

1. Single getter get sweep.
   This measures the maximum input rate one getter can sustain while tracking
   the latency/throughput curve. Variants should include S3 direct get, P2P
   local hit, and P2P remote get. The remote P2P variant should prepare blobs on
   multiple serving peers so a single getter can exercise peer-side load
   balancing. Sweep dimensions should include target ops/s and several object
   sizes.

2. Multi getter get sweep.
   This measures the maximum output rate one serving side can sustain while
   multiple getter instances fetch concurrently. For this group, compare S3
   against P2P with one holder/server, usually the coordinator, and all other
   instances acting as getters. Sweep aggregate target ops/s, divide it across
   getters, and report both aggregate and per-getter achieved throughput and
   latency. Sweep dimensions should include target ops/s, getter count, and
   several object sizes.
