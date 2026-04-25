use std::net::SocketAddr;

pub(crate) fn parse_rpc_addr(value: &str) -> Result<SocketAddr, String> {
    let trimmed = value.trim();
    let without_scheme = trimmed
        .strip_prefix("tarpc://")
        .or_else(|| trimmed.strip_prefix("tcp://"))
        .unwrap_or(trimmed);
    if without_scheme.contains("://") {
        return Err(format!(
            "unsupported RPC address scheme in {value}; tarpc RPC addresses should look like 127.0.0.1:19000"
        ));
    }
    without_scheme
        .parse::<SocketAddr>()
        .map_err(|err| format!("invalid RPC address {value}: {err}"))
}
