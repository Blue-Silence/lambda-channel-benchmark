pub(crate) fn parse_rpc_addr(value: &str) -> Result<String, String> {
    let trimmed = value.trim();
    let without_scheme = trimmed
        .strip_prefix("tarpc://")
        .or_else(|| trimmed.strip_prefix("tcp://"))
        .unwrap_or(trimmed);
    if without_scheme.contains("://") {
        return Err(format!(
            "unsupported RPC address scheme in {value}; tarpc RPC addresses should look like node0:19000 or 127.0.0.1:19000"
        ));
    }
    validate_host_port(without_scheme, value)?;
    Ok(without_scheme.to_string())
}

fn validate_host_port(addr: &str, original: &str) -> Result<(), String> {
    let (host, port) = if let Some(rest) = addr.strip_prefix('[') {
        let Some((host, rest)) = rest.split_once(']') else {
            return Err(format!(
                "invalid RPC address {original}: missing closing ']'"
            ));
        };
        let Some(port) = rest.strip_prefix(':') else {
            return Err(format!("invalid RPC address {original}: missing port"));
        };
        (host, port)
    } else {
        let Some((host, port)) = addr.rsplit_once(':') else {
            return Err(format!("invalid RPC address {original}: missing port"));
        };
        if host.contains(':') {
            return Err(format!(
                "invalid RPC address {original}: IPv6 addresses must be written as [addr]:port"
            ));
        }
        (host, port)
    };

    if host.trim().is_empty() {
        return Err(format!("invalid RPC address {original}: empty host"));
    }
    port.parse::<u16>()
        .map_err(|_| format!("invalid RPC address {original}: invalid port"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::parse_rpc_addr;

    #[test]
    fn accepts_hostname_and_ip_addresses() {
        assert_eq!(parse_rpc_addr("node0:19000").unwrap(), "node0:19000");
        assert_eq!(
            parse_rpc_addr("tcp://127.0.0.1:19000").unwrap(),
            "127.0.0.1:19000"
        );
        assert_eq!(
            parse_rpc_addr("[fd00::1]:19000").unwrap(),
            "[fd00::1]:19000"
        );
    }

    #[test]
    fn rejects_missing_or_invalid_ports() {
        assert!(parse_rpc_addr("node0")
            .unwrap_err()
            .contains("missing port"));
        assert!(parse_rpc_addr("node0:http")
            .unwrap_err()
            .contains("invalid port"));
    }
}
