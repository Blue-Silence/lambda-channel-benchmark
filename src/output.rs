use crate::cli::BenchConfig;
use crate::workloads::WorkloadRun;

pub struct BenchmarkReport {
    run: WorkloadRun,
    backend: String,
    operations: u64,
    concurrency: usize,
    object_size_bytes: u64,
    warmup_operations: u64,
    offered_rate_per_s: Option<f64>,
    seed: u64,
}

impl BenchmarkReport {
    pub fn from_run(run: WorkloadRun, config: &BenchConfig) -> Self {
        Self {
            run,
            backend: config.backend.clone(),
            operations: config.operations,
            concurrency: config.concurrency,
            object_size_bytes: config.object_size_bytes,
            warmup_operations: config.warmup_operations,
            offered_rate_per_s: config.offered_rate_per_s,
            seed: config.seed,
        }
    }

    pub fn to_json_pretty(&self) -> String {
        let offered_rate = self
            .offered_rate_per_s
            .map(|value| value.to_string())
            .unwrap_or_else(|| "null".to_string());

        format!(
            concat!(
                "{{\n",
                "  \"schema\": \"lambda-channel-benchmark/v0\",\n",
                "  \"command\": \"{}\",\n",
                "  \"backend\": \"{}\",\n",
                "  \"status\": \"{}\",\n",
                "  \"note\": \"{}\",\n",
                "  \"config\": {{\n",
                "    \"operations\": {},\n",
                "    \"concurrency\": {},\n",
                "    \"object_size_bytes\": {},\n",
                "    \"warmup_operations\": {},\n",
                "    \"offered_rate_per_s\": {},\n",
                "    \"seed\": {}\n",
                "  }},\n",
                "  \"lambda_channel_utc_now\": \"{}\"\n",
                "}}"
            ),
            json_escape(self.run.command),
            json_escape(&self.backend),
            json_escape(self.run.status),
            json_escape(&self.run.note),
            self.operations,
            self.concurrency,
            self.object_size_bytes,
            self.warmup_operations,
            offered_rate,
            self.seed,
            json_escape(&self.run.lambda_channel_utc_now),
        )
    }
}

fn json_escape(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            ch if ch.is_control() => escaped.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => escaped.push(ch),
        }
    }
    escaped
}
