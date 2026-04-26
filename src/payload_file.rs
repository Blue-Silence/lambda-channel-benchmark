use std::io::SeekFrom;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncSeekExt, AsyncWriteExt};

pub(crate) const PAYLOAD_TIMESTAMP_OFFSET: u64 = 0;
pub(crate) const PAYLOAD_TIMESTAMP_LEN: usize = 8;
const PAYLOAD_HEADER_LEN: usize = 32;

pub(crate) struct PayloadFileSpec<'a> {
    pub(crate) run_id: &'a str,
    pub(crate) seed: u64,
    pub(crate) index: u64,
    pub(crate) size_bytes: u64,
}

pub(crate) async fn create_timestamped_payload_file(
    path: &Path,
    spec: PayloadFileSpec<'_>,
) -> Result<u64, String> {
    if spec.size_bytes == 0 {
        return Err("payload file size must be greater than zero".to_string());
    }

    let mut file = tokio::fs::File::create(path)
        .await
        .map_err(|err| format!("failed to create payload {}: {err}", path.display()))?;
    file.set_len(spec.size_bytes)
        .await
        .map_err(|err| format!("failed to expand payload {}: {err}", path.display()))?;
    file.flush()
        .await
        .map_err(|err| format!("failed to flush expanded payload {}: {err}", path.display()))?;

    let header = unique_header_without_timestamp(&spec);
    let header_len = size_prefix_len(spec.size_bytes, header.len());
    file.seek(SeekFrom::Start(0))
        .await
        .map_err(|err| format!("failed to seek payload {}: {err}", path.display()))?;
    file.write_all(&header[..header_len])
        .await
        .map_err(|err| format!("failed to write payload header {}: {err}", path.display()))?;
    file.flush()
        .await
        .map_err(|err| format!("failed to flush payload header {}: {err}", path.display()))?;

    let timestamp_nanos = unix_timestamp_nanos_u64();
    let timestamp = timestamp_nanos.to_le_bytes();
    let timestamp_len = size_prefix_len(spec.size_bytes, PAYLOAD_TIMESTAMP_LEN);
    file.seek(SeekFrom::Start(PAYLOAD_TIMESTAMP_OFFSET))
        .await
        .map_err(|err| format!("failed to seek payload timestamp {}: {err}", path.display()))?;
    file.write_all(&timestamp[..timestamp_len])
        .await
        .map_err(|err| {
            format!(
                "failed to write payload timestamp {}: {err}",
                path.display()
            )
        })?;
    file.flush().await.map_err(|err| {
        format!(
            "failed to flush payload timestamp {}: {err}",
            path.display()
        )
    })?;

    Ok(timestamp_nanos)
}

fn unique_header_without_timestamp(spec: &PayloadFileSpec<'_>) -> [u8; PAYLOAD_HEADER_LEN] {
    let mut header = [0_u8; PAYLOAD_HEADER_LEN];
    let mut state = hash_run_id(spec.run_id)
        ^ spec.seed
        ^ spec.index.rotate_left(17)
        ^ spec.size_bytes.rotate_left(7);

    for chunk in header[PAYLOAD_TIMESTAMP_LEN..].chunks_mut(8) {
        state = splitmix64(state);
        let bytes = state.to_le_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
    header
}

fn unix_timestamp_nanos_u64() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

fn size_prefix_len(size_bytes: u64, max_len: usize) -> usize {
    usize::try_from(size_bytes)
        .unwrap_or(usize::MAX)
        .min(max_len)
}

fn hash_run_id(run_id: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in run_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e3779b97f4a7c15);
    let mut mixed = value;
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d049bb133111eb);
    mixed ^ (mixed >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn creates_sized_unique_payload_with_timestamp() {
        let dir = std::env::temp_dir().join(format!(
            "lcbench-payload-test-{}-{}",
            std::process::id(),
            unix_timestamp_nanos_u64()
        ));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let first = dir.join("first.bin");
        let second = dir.join("second.bin");

        create_timestamped_payload_file(
            &first,
            PayloadFileSpec {
                run_id: "run-a",
                seed: 7,
                index: 1,
                size_bytes: 32,
            },
        )
        .await
        .unwrap();
        create_timestamped_payload_file(
            &second,
            PayloadFileSpec {
                run_id: "run-a",
                seed: 7,
                index: 2,
                size_bytes: 32,
            },
        )
        .await
        .unwrap();

        let first_bytes = tokio::fs::read(&first).await.unwrap();
        let second_bytes = tokio::fs::read(&second).await.unwrap();
        assert_eq!(first_bytes.len(), 32);
        assert_eq!(second_bytes.len(), 32);
        assert_ne!(
            u64::from_le_bytes(first_bytes[..PAYLOAD_TIMESTAMP_LEN].try_into().unwrap()),
            0
        );
        assert_ne!(
            &first_bytes[PAYLOAD_TIMESTAMP_LEN..],
            &second_bytes[PAYLOAD_TIMESTAMP_LEN..]
        );

        tokio::fs::remove_dir_all(&dir).await.unwrap();
    }
}
