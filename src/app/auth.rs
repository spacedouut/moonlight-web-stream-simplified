use std::future::{Ready, ready};

use actix_web::{FromRequest, HttpRequest, dev::Payload, http::header::HeaderMap, web::Data};

use crate::{
    app::{App, AppError},
    config::ForwardAuthConfig,
};

/// Who is making a request, as told by the forward-auth proxy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthUser {
    /// Forward auth is disabled: full access.
    Unrestricted,
    Admin(String),
    User(String),
}

impl AuthUser {
    pub fn from_headers(config: &ForwardAuthConfig, headers: &HeaderMap) -> Result<Self, AppError> {
        if !config.enabled {
            return Ok(Self::Unrestricted);
        }
        let name = headers
            .get(config.user_header.as_str())
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or(AppError::Unauthenticated)?;
        if config.admin_user.as_deref().map(str::trim) == Some(name) {
            Ok(Self::Admin(name.to_string()))
        } else {
            Ok(Self::User(name.to_string()))
        }
    }

    pub fn name(&self) -> Option<&str> {
        match self {
            Self::Unrestricted => None,
            Self::Admin(name) | Self::User(name) => Some(name),
        }
    }

    pub fn is_admin(&self) -> bool {
        !matches!(self, Self::User(_))
    }

    pub fn require_admin(&self) -> Result<(), AppError> {
        if self.is_admin() {
            Ok(())
        } else {
            Err(AppError::AdminOnly)
        }
    }

    pub fn can_access(&self, allowed_users: &[String]) -> bool {
        match self {
            Self::User(name) => allowed_users.iter().any(|user| user == name),
            _ => true,
        }
    }
}

impl FromRequest for AuthUser {
    type Error = AppError;
    type Future = Ready<Result<Self, AppError>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        ready(match req.app_data::<Data<App>>() {
            Some(app) => Self::from_headers(&app.config().forward_auth, req.headers()),
            None => Err(AppError::AppDestroyed),
        })
    }
}

/// Trims, drops empty names and dedupes an assigned user list.
pub fn normalize_users(users: Vec<String>) -> Vec<String> {
    let mut out: Vec<String> = Vec::with_capacity(users.len());
    for user in users {
        let user = user.trim();
        if !user.is_empty() && !out.iter().any(|existing| existing == user) {
            out.push(user.to_string());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use actix_web::http::header::{HeaderName, HeaderValue};

    use super::*;

    fn config() -> ForwardAuthConfig {
        ForwardAuthConfig {
            enabled: true,
            user_header: "Remote-User".into(),
            admin_user: Some("asher".into()),
        }
    }

    fn headers(user: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("remote-user"),
            HeaderValue::from_str(user).expect("header value"),
        );
        headers
    }

    #[test]
    fn disabled_is_unrestricted() {
        let config = ForwardAuthConfig::default();
        let user = AuthUser::from_headers(&config, &HeaderMap::new()).expect("user");
        assert_eq!(user, AuthUser::Unrestricted);
        assert!(user.can_access(&[]));
    }

    #[test]
    fn missing_header_is_rejected() {
        assert!(matches!(
            AuthUser::from_headers(&config(), &HeaderMap::new()),
            Err(AppError::Unauthenticated)
        ));
    }

    #[test]
    fn admin_sees_everything_users_only_assigned() {
        let admin = AuthUser::from_headers(&config(), &headers("asher")).expect("admin");
        assert_eq!(admin, AuthUser::Admin("asher".into()));
        assert!(admin.can_access(&[]));

        let user = AuthUser::from_headers(&config(), &headers("bob")).expect("user");
        assert_eq!(user, AuthUser::User("bob".into()));
        assert!(!user.can_access(&[]));
        assert!(!user.can_access(&["alice".into()]));
        assert!(user.can_access(&["alice".into(), "bob".into()]));
        assert!(user.require_admin().is_err());
    }

    #[test]
    fn normalize_trims_and_dedupes() {
        assert_eq!(
            normalize_users(vec![
                " bob ".into(),
                "".into(),
                "bob".into(),
                "alice".into()
            ]),
            vec!["bob".to_string(), "alice".to_string()]
        );
    }
}
