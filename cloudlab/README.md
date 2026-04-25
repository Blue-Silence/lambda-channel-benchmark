# CloudLab Deployment

This directory contains the CloudLab packaging and deployment workflow for
`lambda-channel-benchmark`.

The goal is simple: clone private dependencies locally, make one source bundle,
upload that bundle to CloudLab nodes, and build it there. CloudLab nodes do not
need GitHub credentials or private deploy keys.

## Layout

```text
cloudlab/
├── README.md
├── requirements.txt
├── examples/
│   ├── cloudlab.ini      # package/deploy config template
│   └── allocate.ini      # profile allocation config template
├── .config/              # local copied configs, not committed
│   ├── cloudlab.ini
│   └── allocate.ini
├── scripts/
│   ├── entrypoints/      # commands you run locally
│   │   ├── package.py
│   │   ├── allocate_profile.py
│   │   ├── refresh_nodes.py
│   │   ├── deploy.py
│   │   └── record_single.py
│   ├── lib/
│   │   └── nodes.py
│   └── remote/
│       └── remote_build.py
├── .generated/           # generated packages, node files, logs
└── .secrets/             # local keys, not committed
```

## Setup

Install the Python dependency used by deployment:

```bash
python -m pip install -r cloudlab/requirements.txt
```

`deploy.py` uses `fabric` for SSH/SFTP.

`allocate_profile.py` and `refresh_nodes.py` use `portal-cli`. If `portal-cli`
lives in another virtualenv, put its full path in
`cloudlab/.config/allocate.ini`.

## Configs

Create local configs:

```bash
mkdir -p cloudlab/.config
cp cloudlab/examples/cloudlab.ini cloudlab/.config/cloudlab.ini
cp cloudlab/examples/allocate.ini cloudlab/.config/allocate.ini
```

Edit both files for your local environment:

```bash
vim cloudlab/.config/cloudlab.ini
vim cloudlab/.config/allocate.ini
```

Do not commit `.config/`, `.generated/`, or `.secrets/`.

Put your CloudLab portal token here:

```bash
vim cloudlab/.secrets/cloudlab.jwt
```

## Workflow

Create a self-contained source bundle:

```bash
python cloudlab/scripts/entrypoints/package.py
```

Allocate an existing CloudLab profile:

```bash
python cloudlab/scripts/entrypoints/allocate_profile.py
```

Refresh `.generated/nodes.ini` from an existing CloudLab experiment without
allocating a new one:

```bash
python cloudlab/scripts/entrypoints/refresh_nodes.py
```

Deploy and build on every node in `.generated/nodes.ini`:

```bash
python cloudlab/scripts/entrypoints/deploy.py
```

The expected remote binary is:

```text
/local/cloudlab-workspace/target/release/lc-bench
```

The remote build log is copied back under:

```text
cloudlab/.generated/logs/
```

## Manual Node Mode

If you already have one CloudLab node and only want to test deployment:

```bash
python cloudlab/scripts/entrypoints/record_single.py \
  --experiment lc-manual-test \
  --host <cloudlab-node-hostname> \
  --user <cloudlab-username>

python cloudlab/scripts/entrypoints/deploy.py
```

## Re-Deploy

If the CloudLab experiment is still running and only the source bundle changed:

```bash
python cloudlab/scripts/entrypoints/package.py
python cloudlab/scripts/entrypoints/deploy.py
```

You only need to run `allocate_profile.py` again when creating a new CloudLab
experiment. If the experiment already exists but `.generated/nodes.ini` is
missing or stale, run `refresh_nodes.py` instead.

## Generated Files

The generated directory may contain:

```text
cloudlab/.generated/
├── nodes.ini
├── portal_create.json
├── portal_get.json
├── portal_manifests.json
├── package/
│   ├── source-bundle.tar.gz
│   ├── manifest.ini
│   └── work/
└── logs/
    ├── node-0-build.log
    └── node-1-build.log
```

## Troubleshooting

If `package.py` cannot clone `p2p-data-transfer`, check that your local shell can
access the private repo and that `p2p_ref` is a valid branch, tag, or commit.

If allocation fails, check:

```bash
test -s cloudlab/.secrets/cloudlab.jwt
portal-cli --output json experiment list
```

If deployment cannot SSH, inspect:

```bash
cat cloudlab/.generated/nodes.ini
ssh <cloudlab-username>@<node-hostname>
```

If remote build fails, inspect:

```bash
cat cloudlab/.generated/logs/node-0-build.log
```
