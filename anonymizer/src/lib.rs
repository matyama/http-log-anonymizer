use itertools::Itertools;
use std::net::IpAddr;

pub mod limiter;

/// Anonymize an IP address by replacing the last octet with 'x'.
///
/// Any non-IP input is returned unchanged.
///
/// # Example
/// ```
/// # use anonymizer::anonymize_ip;
/// assert_eq!(&anonymize_ip("1.2.3.4".to_string()), "1.2.3.x");
/// ```
pub fn anonymize_ip(ip: String) -> String {
    let Ok(ip) = ip.parse() else {
        // XXX: check what's the expected behavior here (currntly non-IPs are _not_ anonymized)
        return ip;
    };

    match ip {
        IpAddr::V4(ip) => {
            let mut ip = ip.octets().into_iter().take(3).join(".");
            ip.push_str(".x");
            ip
        }
        IpAddr::V6(ip) => {
            let mut ip = ip.to_string().split(':').take(8).join(":");
            ip.push_str(":xxxx");
            ip
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    #[rstest]
    #[case::valid_v4("1.2.3.4".to_string(), "1.2.3.x".to_string())]
    #[case::valid_v6(
        "2001:0db8:85a3:0000:0000:8a2e:0370:7334".to_string(),
        "2001:db8:85a3::8a2e:370:7334:xxxx".to_string(),
    )]
    #[case::invalid("a.b.c.d".to_string(), "a.b.c.d".to_string())]
    fn ip_anonymization(#[case] ip: String, #[case] expected: String) {
        assert_eq!(anonymize_ip(ip), expected);
    }
}
