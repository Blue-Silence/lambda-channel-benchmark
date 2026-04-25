mod blob;
mod metadata;
mod receiver;
mod sender;

use crate::cli::{BenchCommand, BenchConfig};

pub struct WorkloadRun {
    pub command: &'static str,
    pub status: &'static str,
    pub note: String,
    pub lambda_channel_utc_now: String,
}

pub fn run(command: BenchCommand, config: &BenchConfig) -> WorkloadRun {
    match command {
        BenchCommand::Metadata => metadata::run(config),
        BenchCommand::Blob => blob::run(config),
        BenchCommand::Sender => sender::run(config),
        BenchCommand::Receiver => receiver::run(config),
    }
}

fn stub_run(command: &'static str, note: impl Into<String>) -> WorkloadRun {
    WorkloadRun {
        command,
        status: "stub",
        note: note.into(),
        lambda_channel_utc_now: lambda_channel::metadata_store::utc_now_iso_string(),
    }
}
