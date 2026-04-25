pub struct BlobScenario {
    pub name: &'static str,
    pub config_path: &'static str,
    pub purpose: &'static str,
}

pub const SCENARIOS: &[BlobScenario] = &[
    BlobScenario {
        name: "blob.put.local_file",
        config_path: "config/experiments/blob/put.toml",
        purpose: "Measure local-file blob-store put throughput and latency.",
    },
    BlobScenario {
        name: "blob.put.s3",
        config_path: "config/experiments/blob/put-s3.toml",
        purpose: "Measure S3 blob-store put throughput and latency.",
    },
    BlobScenario {
        name: "blob.put.p2p",
        config_path: "config/experiments/blob/put-p2p.toml",
        purpose: "Measure P2P blob-store put throughput and latency.",
    },
    BlobScenario {
        name: "blob.get_materialize",
        config_path: "config/experiments/blob/get-materialize.toml",
        purpose: "Measure materialization from existing blob refs.",
    },
    BlobScenario {
        name: "blob.p2p_local_hit",
        config_path: "config/experiments/blob/p2p-local-hit.toml",
        purpose: "Measure P2P materialization when chunks are already local.",
    },
    BlobScenario {
        name: "blob.p2p_peer_fetch",
        config_path: "config/experiments/blob/p2p-peer-fetch.toml",
        purpose: "Measure receiver fetch from a remote P2P holder.",
    },
    BlobScenario {
        name: "blob.persist_upload",
        config_path: "config/experiments/blob/persist-upload.toml",
        purpose: "Measure durable persist upload from local chunks.",
    },
    BlobScenario {
        name: "blob.fallback_fetch",
        config_path: "config/experiments/blob/fallback-fetch.toml",
        purpose: "Measure fetch from persist store when peer holders are absent.",
    },
];
