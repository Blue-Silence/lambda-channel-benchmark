use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

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

const EXPERIMENT_CSV_HEADER: &[&str] = &[
    "schema",
    "run_id",
    "workload",
    "backend",
    "instance_id",
    "datapoint_index",
    "resource_id",
    "stop_reason",
    "operations_per_point",
    "warmup_operations_per_point",
    "concurrency",
    "object_size_bytes",
    "target_ops_per_s",
    "achieved_ops_per_s",
    "successful_ops_per_s",
    "total_tasks",
    "completed_tasks",
    "failed_tasks",
    "wall_time_ms",
    "offered_min_ms",
    "offered_mean_ms",
    "offered_p50_ms",
    "offered_p90_ms",
    "offered_p95_ms",
    "offered_p99_ms",
    "offered_max_ms",
    "service_min_ms",
    "service_mean_ms",
    "service_p50_ms",
    "service_p90_ms",
    "service_p95_ms",
    "service_p99_ms",
    "service_max_ms",
    "schedule_lag_min_ms",
    "schedule_lag_mean_ms",
    "schedule_lag_p50_ms",
    "schedule_lag_p90_ms",
    "schedule_lag_p95_ms",
    "schedule_lag_p99_ms",
    "schedule_lag_max_ms",
    "store_resource_dir",
    "store_root_dir",
    "store_bucket",
    "store_key_prefix",
    "store_cache_dir",
    "store_tracker_blob_meta_table",
    "store_tracker_chunk_holders_table",
    "operation",
    "channel_id",
    "scan_limit",
    "claim_target_count",
    "metadata_table_name",
    "metadata_region",
    "metadata_endpoint_url",
    "metadata_appended_count",
    "metadata_scan_count",
    "metadata_listed_elem_count",
    "metadata_claim_attempt_count",
    "metadata_claimed_count",
    "metadata_already_consumed_count",
    "metadata_missing_count",
    "metadata_eof_count",
    "failure_messages",
];

pub fn append_experiment_csv(path: &Path, report_json: &str) -> Result<usize, String> {
    let rows = experiment_csv_rows(report_json)?;
    if rows.is_empty() {
        return Ok(0);
    }
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).map_err(|err| {
            format!(
                "failed to create CSV output dir {}: {err}",
                parent.display()
            )
        })?;
    }

    let write_header = csv_needs_header_or_validate(path)?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|err| format!("failed to open CSV output {}: {err}", path.display()))?;
    if write_header {
        write_csv_record(&mut file, EXPERIMENT_CSV_HEADER)?;
    }
    for row in &rows {
        write_csv_record(&mut file, row)?;
    }
    Ok(rows.len())
}

fn experiment_csv_rows(report_json: &str) -> Result<Vec<Vec<String>>, String> {
    let report: serde_json::Value = serde_json::from_str(report_json)
        .map_err(|err| format!("experiment result is not valid JSON: {err}"))?;
    let datapoints = report
        .get("datapoints")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "CSV export expects an experiment report with datapoints[]".to_string())?;

    let mut rows = Vec::with_capacity(datapoints.len());
    for (index, datapoint) in datapoints.iter().enumerate() {
        let paced = datapoint.get("paced").unwrap_or(&serde_json::Value::Null);
        let store = datapoint.get("store").unwrap_or(&serde_json::Value::Null);
        let counters = datapoint
            .get("counters")
            .unwrap_or(&serde_json::Value::Null);
        rows.push(vec![
            "lambda-channel-benchmark/datapoint-v2".to_string(),
            cell(report.get("run_id")),
            cell(report.get("workload")),
            cell(report.get("backend")),
            cell(report.get("instance_id")),
            index.to_string(),
            cell(datapoint.get("resource_id")),
            cell(report.get("stop_reason")),
            cell(report.get("operations_per_point")),
            cell(report.get("warmup_operations_per_point")),
            cell(report.get("concurrency")),
            cell(report.get("object_size_bytes")),
            cell(paced.get("target_ops_per_s")),
            cell(paced.get("achieved_ops_per_s")),
            cell(paced.get("successful_ops_per_s")),
            cell(paced.get("total_tasks")),
            cell(paced.get("completed_tasks")),
            cell(paced.get("failed_tasks")),
            cell(paced.get("wall_time_ms")),
            cell_at(paced, &["offered_latency", "min_ms"]),
            cell_at(paced, &["offered_latency", "mean_ms"]),
            cell_at(paced, &["offered_latency", "p50_ms"]),
            cell_at(paced, &["offered_latency", "p90_ms"]),
            cell_at(paced, &["offered_latency", "p95_ms"]),
            cell_at(paced, &["offered_latency", "p99_ms"]),
            cell_at(paced, &["offered_latency", "max_ms"]),
            cell_at(paced, &["service_latency", "min_ms"]),
            cell_at(paced, &["service_latency", "mean_ms"]),
            cell_at(paced, &["service_latency", "p50_ms"]),
            cell_at(paced, &["service_latency", "p90_ms"]),
            cell_at(paced, &["service_latency", "p95_ms"]),
            cell_at(paced, &["service_latency", "p99_ms"]),
            cell_at(paced, &["service_latency", "max_ms"]),
            cell_at(paced, &["schedule_lag", "min_ms"]),
            cell_at(paced, &["schedule_lag", "mean_ms"]),
            cell_at(paced, &["schedule_lag", "p50_ms"]),
            cell_at(paced, &["schedule_lag", "p90_ms"]),
            cell_at(paced, &["schedule_lag", "p95_ms"]),
            cell_at(paced, &["schedule_lag", "p99_ms"]),
            cell_at(paced, &["schedule_lag", "max_ms"]),
            cell(store.get("resource_dir")),
            cell(store.get("root_dir")),
            cell(store.get("bucket")),
            cell(store.get("key_prefix")),
            cell(store.get("cache_dir")),
            cell(store.get("tracker_blob_meta_table")),
            cell(store.get("tracker_chunk_holders_table")),
            cell(datapoint.get("operation")),
            cell(datapoint.get("channel_id")),
            cell(datapoint.get("scan_limit")),
            cell(datapoint.get("claim_target_count")),
            cell(store.get("table_name")),
            cell(store.get("region")),
            cell(store.get("endpoint_url")),
            cell(counters.get("appended_count")),
            cell(counters.get("scan_count")),
            cell(counters.get("listed_elem_count")),
            cell(counters.get("claim_attempt_count")),
            cell(counters.get("claimed_count")),
            cell(counters.get("already_consumed_count")),
            cell(counters.get("missing_count")),
            cell(counters.get("eof_count")),
            failure_messages_cell(paced.get("failure_messages")),
        ]);
    }
    Ok(rows)
}

fn csv_needs_header_or_validate(path: &Path) -> Result<bool, String> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.len() == 0 => Ok(true),
        Ok(_) => {
            let file = fs::File::open(path)
                .map_err(|err| format!("failed to open CSV output {}: {err}", path.display()))?;
            let mut reader = BufReader::new(file);
            let mut existing_header = String::new();
            reader
                .read_line(&mut existing_header)
                .map_err(|err| format!("failed to read CSV header {}: {err}", path.display()))?;
            let existing_header = existing_header.trim_end_matches(['\r', '\n']);
            let expected_header = EXPERIMENT_CSV_HEADER.join(",");
            if existing_header == expected_header {
                Ok(false)
            } else {
                Err(format!(
                    "CSV output {} has an incompatible header; write to a new file or migrate the existing CSV to the current datapoint-v2 schema",
                    path.display()
                ))
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(err) => Err(format!(
            "failed to inspect CSV output {}: {err}",
            path.display()
        )),
    }
}

fn cell_at(value: &serde_json::Value, path: &[&str]) -> String {
    let mut current = value;
    for key in path {
        let Some(next) = current.get(*key) else {
            return String::new();
        };
        current = next;
    }
    cell(Some(current))
}

fn cell(value: Option<&serde_json::Value>) -> String {
    match value {
        Some(serde_json::Value::String(value)) => value.clone(),
        Some(serde_json::Value::Number(value)) => value.to_string(),
        Some(serde_json::Value::Bool(value)) => value.to_string(),
        _ => String::new(),
    }
}

fn failure_messages_cell(value: Option<&serde_json::Value>) -> String {
    value
        .and_then(serde_json::Value::as_array)
        .map(|messages| {
            messages
                .iter()
                .filter_map(serde_json::Value::as_str)
                .collect::<Vec<_>>()
                .join("|")
        })
        .unwrap_or_default()
}

fn write_csv_record(
    writer: &mut impl Write,
    fields: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<(), String> {
    let mut first = true;
    for field in fields {
        if first {
            first = false;
        } else {
            writer
                .write_all(b",")
                .map_err(|err| format!("failed to write CSV field separator: {err}"))?;
        }
        writer
            .write_all(csv_escape(field.as_ref()).as_bytes())
            .map_err(|err| format!("failed to write CSV field: {err}"))?;
    }
    writer
        .write_all(b"\n")
        .map_err(|err| format!("failed to write CSV newline: {err}"))
}

fn csv_escape(input: &str) -> String {
    if input.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", input.replace('"', "\"\""))
    } else {
        input.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{experiment_csv_rows, EXPERIMENT_CSV_HEADER};

    #[test]
    fn flattens_metadata_datapoints() {
        let report = r#"{
            "run_id": "metadata-append-dynamodb",
            "workload": "metadata.append",
            "instance_id": "node-0",
            "backend": "dynamodb",
            "operations_per_point": 1000,
            "warmup_operations_per_point": 100,
            "concurrency": 64,
            "object_size_bytes": 32,
            "stop_reason": "saturated",
            "datapoints": [{
                "resource_id": "res-1",
                "channel_id": "ch-1",
                "operation": "put_elem",
                "scan_limit": null,
                "claim_target_count": null,
                "store": {
                    "table_name": "metadata-table",
                    "region": "us-east-1",
                    "endpoint_url": "http://127.0.0.1:8000"
                },
                "counters": {
                    "appended_count": 1000,
                    "claimed_count": 0
                },
                "paced": {
                    "target_ops_per_s": 100.0,
                    "achieved_ops_per_s": 98.0,
                    "successful_ops_per_s": 98.0,
                    "total_tasks": 1000,
                    "completed_tasks": 1000,
                    "failed_tasks": 0,
                    "wall_time_ms": 10200.0,
                    "offered_latency": {"min_ms": 1.0, "mean_ms": 2.0, "p50_ms": 2.0, "p90_ms": 3.0, "p95_ms": 4.0, "p99_ms": 5.0, "max_ms": 6.0},
                    "service_latency": {"min_ms": 1.0, "mean_ms": 2.0, "p50_ms": 2.0, "p90_ms": 3.0, "p95_ms": 4.0, "p99_ms": 5.0, "max_ms": 6.0},
                    "schedule_lag": {"min_ms": 0.0, "mean_ms": 0.1, "p50_ms": 0.1, "p90_ms": 0.2, "p95_ms": 0.3, "p99_ms": 0.4, "max_ms": 0.5},
                    "failure_messages": []
                }
            }]
        }"#;

        let rows = experiment_csv_rows(report).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), EXPERIMENT_CSV_HEADER.len());
        assert_eq!(
            cell(&rows[0], "schema"),
            "lambda-channel-benchmark/datapoint-v2"
        );
        assert_eq!(cell(&rows[0], "workload"), "metadata.append");
        assert_eq!(cell(&rows[0], "operation"), "put_elem");
        assert_eq!(cell(&rows[0], "channel_id"), "ch-1");
        assert_eq!(cell(&rows[0], "metadata_table_name"), "metadata-table");
        assert_eq!(cell(&rows[0], "metadata_appended_count"), "1000");
    }

    #[test]
    fn leaves_metadata_columns_empty_for_blob_datapoints() {
        let report = r#"{
            "run_id": "blob-put-local-file",
            "workload": "blob.put.local_file",
            "instance_id": "node-0",
            "backend": "local-file",
            "operations_per_point": 10,
            "warmup_operations_per_point": 1,
            "concurrency": 2,
            "object_size_bytes": 32,
            "stop_reason": "max_points",
            "datapoints": [{
                "resource_id": "res-1",
                "store": {"root_dir": "/tmp/blob"},
                "paced": {
                    "target_ops_per_s": 10.0,
                    "achieved_ops_per_s": 10.0,
                    "successful_ops_per_s": 10.0,
                    "total_tasks": 10,
                    "completed_tasks": 10,
                    "failed_tasks": 0,
                    "wall_time_ms": 1000.0,
                    "offered_latency": {},
                    "service_latency": {},
                    "schedule_lag": {},
                    "failure_messages": []
                }
            }]
        }"#;

        let rows = experiment_csv_rows(report).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), EXPERIMENT_CSV_HEADER.len());
        assert_eq!(cell(&rows[0], "workload"), "blob.put.local_file");
        assert_eq!(cell(&rows[0], "store_root_dir"), "/tmp/blob");
        assert_eq!(cell(&rows[0], "operation"), "");
        assert_eq!(cell(&rows[0], "metadata_table_name"), "");
    }

    fn cell<'a>(row: &'a [String], column: &str) -> &'a str {
        let index = EXPERIMENT_CSV_HEADER
            .iter()
            .position(|header| *header == column)
            .unwrap();
        &row[index]
    }
}
