# Single-Node CloudLab Blob Put Workflow

This workflow runs the single-machine blob put sweeps on one CloudLab node.

Model:

- CloudLab node runs the long-lived `lc-bench node` daemon.
- The local machine runs `lc-bench proxy`.
- CSV and proxy logs are written locally under `cloudlab/results/`.
- Blob put configs use `benchmark.duration_seconds = 10.0`, so each sweep point
  runs approximately `target_ops_per_s * 10s` operations. The fixed
  `benchmark.operations` value remains as a fallback when `duration_seconds` is
  omitted.
- S3 and P2P variants use AWS managed services by default in `us-west-2`.
  The workflow does not start MinIO or DynamoDB Local.

Run steps from the repository root:

```bash
cloudlab/scripts/workflows/single_node_blob_put/00_check_local.sh
cloudlab/scripts/workflows/single_node_blob_put/01_package.sh
cloudlab/scripts/workflows/single_node_blob_put/02_allocate.sh
cloudlab/scripts/workflows/single_node_blob_put/02c_check_ready.sh
cloudlab/scripts/workflows/single_node_blob_put/03_deploy.sh
cloudlab/scripts/workflows/single_node_blob_put/04_start_node.sh
cloudlab/scripts/workflows/single_node_blob_put/05_run_all_puts.sh
cloudlab/scripts/workflows/single_node_blob_put/06_stop_node.sh
```

To run only the local-file put variant after the node is started:

```bash
cloudlab/scripts/workflows/single_node_blob_put/05_run_blob_put.sh
```

If `02_allocate.sh` cannot reach the Portal API, instantiate the profile in the
CloudLab web UI and record the allocated node instead:

```bash
CLOUDLAB_HOST=<cloudlab-node-host> \
  cloudlab/scripts/workflows/single_node_blob_put/02b_record_existing_node.sh
```

Then continue with `03_deploy.sh`.

Before deploying, check whether the recorded node still looks ready:

```bash
cloudlab/scripts/workflows/single_node_blob_put/02c_check_ready.sh
```

The readiness check is read-only. It queries the Portal when configured, checks
DNS resolution, and verifies that each recorded node returns an SSH protocol
banner on port 22. If Portal access is unavailable, use:

```bash
cloudlab/scripts/workflows/single_node_blob_put/02c_check_ready.sh --skip-portal
```

Important config:

- `cloudlab/.config/allocate.ini`
  - `portal_cli = /home/usera/lambda-channel-benchmark/.venv/bin/portal-cli`
  - `[profile] name = LambdaChannel-1node`
- `cloudlab/.config/cloudlab.ini`
  - `benchmark_source = local` if you want to deploy the current working tree,
    including uncommitted benchmark/config changes
  - `remote_instances_file = /local/cloudlab-workspace/config/instances/single-node.toml`
  - `aws_env_file = cloudlab/.config/aws.env`
- `cloudlab/.config/aws.env`
  - optional AWS credential environment for managed S3/DynamoDB on the remote
    node daemon. Copy `cloudlab/examples/aws.env.example` and fill it locally
    if the CloudLab node does not have an instance role.

AWS service assumptions:

- `blob.put.s3`: needs permission to create/delete temporary S3 buckets and put
  objects in `us-west-2`.
- `blob.put.p2p`: needs permission to create/delete DynamoDB tracker tables in
  `us-west-2`.

If direct TCP to `<cloudlab-host>:19000` is blocked, create an SSH tunnel and
run the proxy step manually:

```bash
ssh -L 19000:node-0:19000 Finch@<cloudlab-host>
.venv/bin/python cloudlab/scripts/entrypoints/run_proxy_experiment.py \
  --rpc-url 127.0.0.1:19000 \
  --experiment config/experiments/blob/put.toml
```
