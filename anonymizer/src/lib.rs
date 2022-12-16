use itertools::Itertools;
use std::net::IpAddr;

// TODO: better API, possibly using Into
pub fn anonymize_ip(ip: String) -> String {
    let Ok(ip) = ip.parse() else {
        // TODO: check what's the expected behavior here (currntly non-IPs are _not_ anonymized)
        return ip;
    };

    match ip {
        IpAddr::V4(ip) => {
            // TODO: optimize (e.g. no allocs)
            let mut ip = ip.octets().into_iter().take(3).join(".");
            ip.push_str(".x");
            ip
        }
        IpAddr::V6(_ip) => {
            todo!("not implemented yet")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    // TODO: tests for IPv6
    #[rstest]
    #[case::valid("1.2.3.4".to_string(), "1.2.3.x".to_string())]
    #[case::invalid("a.b.c.d".to_string(), "a.b.c.d".to_string())]
    fn ip_anonymization(#[case] ip: String, #[case] expected: String) {
        assert_eq!(anonymize_ip(ip), expected);
    }
}
