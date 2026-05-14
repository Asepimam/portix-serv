use serde::{Deserialize, Serialize};

use super::errors::{PortixError, Result};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SshProfile {
    pub id: String,
    pub name: String,
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: Option<String>,
    pub private_key_path: Option<String>,
}

impl SshProfile {
    pub fn socket_addr(&self) -> (String, u16) {
        (self.host.clone(), self.port)
    }

    pub fn validate(&self) -> Result<()> {
        if self.id.trim().is_empty() {
            return Err(PortixError::InvalidProfile(
                "profile id is required".to_owned(),
            ));
        }
        if self.host.trim().is_empty() {
            return Err(PortixError::InvalidProfile("host is required".to_owned()));
        }
        if self.username.trim().is_empty() {
            return Err(PortixError::InvalidProfile(
                "username is required".to_owned(),
            ));
        }
        if self.port == 0 {
            return Err(PortixError::InvalidProfile(
                "port must be greater than 0".to_owned(),
            ));
        }

        let has_password = self
            .password
            .as_deref()
            .is_some_and(|password| !password.is_empty());
        let has_key = self
            .private_key_path
            .as_deref()
            .is_some_and(|path| !path.trim().is_empty());
        if !has_password && !has_key {
            return Err(PortixError::MissingAuthentication);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_profile() -> SshProfile {
        SshProfile {
            id: "profile-1".to_owned(),
            name: "Production".to_owned(),
            host: "192.168.1.10".to_owned(),
            port: 22,
            username: "deploy".to_owned(),
            password: Some("secret".to_owned()),
            private_key_path: None,
        }
    }

    #[test]
    fn validate_accepts_password_auth_profile() {
        assert!(valid_profile().validate().is_ok());
    }

    #[test]
    fn validate_rejects_missing_host() {
        let mut profile = valid_profile();
        profile.host.clear();

        assert!(profile.validate().is_err());
    }

    #[test]
    fn validate_rejects_missing_authentication() {
        let mut profile = valid_profile();
        profile.password = None;

        assert!(profile.validate().is_err());
    }
}
