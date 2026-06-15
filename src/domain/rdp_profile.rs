use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::errors::{PortixError, Result};

/// Represents an RDP connection profile, either created manually or parsed from a .rdp file.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RdpProfile {
    pub id: String,
    pub name: String,
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: Option<String>,
    pub domain: Option<String>,
    pub width: u16,
    pub height: u16,
    /// Screen mode: 1 = windowed, 2 = fullscreen
    pub screen_mode: u8,
    /// Additional RDP settings parsed from file
    pub extra: HashMap<String, String>,
}

impl RdpProfile {
    pub fn validate(&self) -> Result<()> {
        if self.id.trim().is_empty() {
            return Err(PortixError::InvalidProfile(
                "profile id is required".to_owned(),
            ));
        }
        if self.host.trim().is_empty() {
            return Err(PortixError::InvalidProfile("host is required".to_owned()));
        }
        if self.port == 0 {
            return Err(PortixError::InvalidProfile(
                "port must be greater than 0".to_owned(),
            ));
        }
        if self.width == 0 || self.height == 0 {
            return Err(PortixError::InvalidProfile(
                "desktop size must be greater than 0".to_owned(),
            ));
        }
        Ok(())
    }

    /// Parse an .rdp file content into an RdpProfile.
    ///
    /// RDP file format is line-based:
    /// `key:type:value`
    /// where type is `s` (string), `i` (integer), or `b` (binary)
    pub fn from_rdp_file(id: String, name: String, content: &str) -> Result<Self> {
        let mut settings: HashMap<String, String> = HashMap::new();

        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
                continue;
            }

            // Format: key:type:value
            let parts: Vec<&str> = line.splitn(3, ':').collect();
            if parts.len() >= 3 {
                let key = parts[0].trim().to_lowercase();
                // parts[1] is the type (s, i, b) - we store the value regardless
                let value = parts[2].trim().to_owned();
                settings.insert(key, value);
            } else if parts.len() == 2 {
                // Some files use key:value without type
                let key = parts[0].trim().to_lowercase();
                let value = parts[1].trim().to_owned();
                settings.insert(key, value);
            }
        }

        // Extract host and port from "full address" field
        let full_address = settings
            .get("full address")
            .or_else(|| settings.get("full_address"))
            .cloned()
            .unwrap_or_default();

        let (host, addr_port) = parse_address(&full_address);

        if host.is_empty() {
            return Err(PortixError::InvalidProfile(
                "RDP file missing 'full address' field".to_owned(),
            ));
        }

        // "port:i:N" overrides port embedded in full address
        let port = settings
            .get("port")
            .and_then(|v| v.parse::<u16>().ok())
            .unwrap_or(addr_port);

        // Parse "domain\username" or plain username; domain field is fallback
        let raw_username = settings.get("username").cloned().unwrap_or_default();
        let (username, domain) = if let Some(pos) = raw_username.find('\\') {
            let d = raw_username[..pos].to_owned();
            let u = raw_username[pos + 1..].to_owned();
            (u, if d.is_empty() { None } else { Some(d) })
        } else {
            let d = settings.get("domain").cloned().filter(|d| !d.is_empty());
            (raw_username, d)
        };

        let width = settings
            .get("desktopwidth")
            .and_then(|v| v.parse::<u16>().ok())
            .unwrap_or(1920);

        let height = settings
            .get("desktopheight")
            .and_then(|v| v.parse::<u16>().ok())
            .unwrap_or(1080);

        let screen_mode = settings
            .get("screen mode id")
            .and_then(|v| v.parse::<u8>().ok())
            .unwrap_or(1);

        // Remove known fields from extra
        let known_keys = [
            "full address",
            "full_address",
            "port",
            "username",
            "domain",
            "desktopwidth",
            "desktopheight",
            "screen mode id",
        ];

        // Normalize CyberArk PSM / RemoteApp keys into our canonical form.
        // CyberArk generates files with `alternate shell:s:||PSM@<id>` and
        // `remoteapplicationprogram:s:||PSM@<id>` — we prefer `alternate shell`
        // for the Client Info PDU's AlternateShell field.
        let mut extra: HashMap<String, String> = settings
            .into_iter()
            .filter(|(k, _)| !known_keys.contains(&k.as_str()))
            .collect();

        // If only the RemoteApp key is present (no alternate shell), copy it
        // over so the connection code always finds `alternate shell`.
        if !extra.contains_key("alternate shell") {
            if let Some(remoteapp) = extra.get("remoteapplicationprogram").cloned() {
                extra.insert("alternate shell".to_owned(), remoteapp);
            }
        }

        Ok(Self {
            id,
            name,
            host,
            port,
            username,
            password: None, // Passwords are never stored in .rdp files
            domain,
            width,
            height,
            screen_mode,
            extra,
        })
    }

    /// Generate .rdp file content from this profile.
    pub fn to_rdp_file_content(&self) -> String {
        let mut lines = Vec::new();

        let address = if self.port == 3389 {
            self.host.clone()
        } else {
            format!("{}:{}", self.host, self.port)
        };

        lines.push(format!("full address:s:{}", address));
        if !self.username.is_empty() {
            lines.push(format!("username:s:{}", self.username));
        }
        if let Some(domain) = &self.domain {
            if !domain.is_empty() {
                lines.push(format!("domain:s:{}", domain));
            }
        }
        lines.push(format!("desktopwidth:i:{}", self.width));
        lines.push(format!("desktopheight:i:{}", self.height));
        lines.push(format!("screen mode id:i:{}", self.screen_mode));

        for (key, value) in &self.extra {
            lines.push(format!("{}:s:{}", key, value));
        }

        lines.join("\r\n")
    }
}

/// Parse "host:port" or just "host" from the full address field.
fn parse_address(address: &str) -> (String, u16) {
    let trimmed = address.trim();
    if trimmed.is_empty() {
        return (String::new(), 3389);
    }

    // Handle IPv6 addresses like [::1]:3389
    if trimmed.starts_with('[') {
        if let Some(bracket_end) = trimmed.find(']') {
            let host = trimmed[1..bracket_end].to_owned();
            let after = &trimmed[bracket_end + 1..];
            let port = if after.starts_with(':') {
                after[1..].parse::<u16>().unwrap_or(3389)
            } else {
                3389
            };
            return (host, port);
        }
    }

    // Standard host:port
    if let Some(colon_pos) = trimmed.rfind(':') {
        let potential_port = &trimmed[colon_pos + 1..];
        if let Ok(port) = potential_port.parse::<u16>() {
            let host = trimmed[..colon_pos].to_owned();
            return (host, port);
        }
    }

    (trimmed.to_owned(), 3389)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_rdp_file_basic() {
        let content = r#"
full address:s:testhost:3389
username:s:testuser
desktopwidth:i:1920
desktopheight:i:1080
screen mode id:i:2
"#;
        let profile = RdpProfile::from_rdp_file("test-1".into(), "Test".into(), content).unwrap();
        assert_eq!(profile.host, "testhost");
        assert_eq!(profile.port, 3389);
        assert_eq!(profile.username, "testuser");
        assert_eq!(profile.width, 1920);
        assert_eq!(profile.height, 1080);
        assert_eq!(profile.screen_mode, 2);
    }

    #[test]
    fn parse_rdp_file_no_port() {
        let content = "full address:s:myserver.local\nusername:s:user1\n";
        let profile = RdpProfile::from_rdp_file("test-2".into(), "Test2".into(), content).unwrap();
        assert_eq!(profile.host, "myserver.local");
        assert_eq!(profile.port, 3389);
    }

    #[test]
    fn parse_rdp_file_missing_address() {
        let content = "username:s:admin\n";
        let result = RdpProfile::from_rdp_file("test-3".into(), "Test3".into(), content);
        assert!(result.is_err());
    }

    #[test]
    fn parse_address_ipv6() {
        let (host, port) = parse_address("[::1]:3390");
        assert_eq!(host, "::1");
        assert_eq!(port, 3390);
    }

    #[test]
    fn validate_rejects_empty_host() {
        let profile = RdpProfile {
            id: "p1".into(),
            name: "test".into(),
            host: "".into(),
            port: 3389,
            username: "user".into(),
            password: None,
            domain: None,
            width: 1920,
            height: 1080,
            screen_mode: 1,
            extra: HashMap::new(),
        };
        assert!(profile.validate().is_err());
    }

    #[test]
    fn to_rdp_file_roundtrip() {
        let content = r#"full address:s:server.local:3390
username:s:testuser
desktopwidth:i:1280
desktopheight:i:720
screen mode id:i:1
"#;
        let profile = RdpProfile::from_rdp_file("rt".into(), "Roundtrip".into(), content).unwrap();
        let output = profile.to_rdp_file_content();
        assert!(output.contains("full address:s:server.local:3390"));
        assert!(output.contains("username:s:testuser"));
    }

    // ─── CyberArk PSM tests ───────────────────────────────────────────────────

    #[test]
    fn parse_cyberark_psm_rdp_file() {
        // Typical file generated by CyberArk PSM
        let content = r#"full address:s:psm.corp.com:3389
username:s:CORP\psmadmin
password 51:b:
screen mode id:i:2
desktopwidth:i:1920
desktopheight:i:1080
use multimon:i:0
audiomode:i:2
redirectprinters:i:0
autoreconnection enabled:i:1
authentication level:i:0
prompt for credentials:i:0
negotiate security layer:i:0
alternate shell:s:||PSM@SessionID123
remoteapplicationprogram:s:||PSM@SessionID123
"#;
        let profile =
            RdpProfile::from_rdp_file("psm-1".into(), "CyberArk PSM".into(), content).unwrap();
        assert_eq!(profile.host, "psm.corp.com");
        assert_eq!(profile.port, 3389);
        // domain\username split
        assert_eq!(profile.domain.as_deref(), Some("CORP"));
        assert_eq!(profile.username, "psmadmin");
        // alternate shell must be populated
        assert_eq!(
            profile.extra.get("alternate shell").map(|s| s.as_str()),
            Some("||PSM@SessionID123")
        );
    }

    #[test]
    fn remoteapplicationprogram_falls_back_to_alternate_shell() {
        // File with only remoteapplicationprogram (no alternate shell key)
        let content = r#"full address:s:rdsh.example.com
username:s:user1
remoteapplicationprogram:s:||Notepad
"#;
        let profile =
            RdpProfile::from_rdp_file("psm-2".into(), "RemoteApp".into(), content).unwrap();
        assert_eq!(
            profile.extra.get("alternate shell").map(|s| s.as_str()),
            Some("||Notepad"),
            "remoteapplicationprogram should be promoted to alternate shell"
        );
    }

    #[test]
    fn alternate_shell_takes_priority_over_remoteapplicationprogram() {
        let content = r#"full address:s:server.local
username:s:admin
alternate shell:s:||Shell1
remoteapplicationprogram:s:||Shell2
"#;
        let profile =
            RdpProfile::from_rdp_file("psm-3".into(), "PSM priority".into(), content).unwrap();
        assert_eq!(
            profile.extra.get("alternate shell").map(|s| s.as_str()),
            Some("||Shell1"),
            "explicit alternate shell should not be overwritten"
        );
    }

    #[test]
    fn parse_port_override_cyberark_style() {
        // CyberArk sometimes embeds the port separately from full address
        let content = r#"full address:s:psm.corp.com
port:i:13389
username:s:user
"#;
        let profile =
            RdpProfile::from_rdp_file("psm-4".into(), "Port override".into(), content).unwrap();
        assert_eq!(profile.port, 13389, "port:i field should override full address port");
    }
}
