use std::{
    collections::HashMap,
    io,
    ops::Deref,
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use actix_web::{HttpResponse, ResponseError, body::BoxBody, http::StatusCode};
use hex::FromHexError;
use moonlight_common::{
    crypto::rustcrypto::{RustCryptoBackend, RustCryptoError},
    error::Error as MoonlightError,
    http::{
        ClientInfo,
        client::{async_client::RequestClient as _, tokio::TokioClient},
        pair::PairingCryptoBackend,
        server_info::{ServerInfoEndpoint, ServerInfoRequest},
    },
};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};

use crate::{
    api::stream::web_transport::AutoCertificate,
    app::{
        auth::AuthUser,
        host::{AppId, Host, HostId},
        storage::{Storage, StorageHostAdd, StorageHostCache, create_storage},
        stream::{Stream, StreamId},
    },
    config::Config,
};

pub mod auth;
pub mod host;
pub mod storage;
pub mod stream;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("the app got destroyed")]
    AppDestroyed,
    #[error("the host was not found")]
    HostNotFound,
    #[error("the host was already paired")]
    HostPaired,
    #[error("the host must be paired for this action")]
    HostNotPaired,
    #[error("the client doesn't support the required codecs")]
    WebRtcClientCodecNotSupported,
    #[error("the stream was already closed")]
    StreamClosed,
    #[error("web transport is disabled")]
    WebTransportDisabled,
    #[error("no forward-auth user on the request")]
    Unauthenticated,
    #[error("only the admin can do this")]
    AdminOnly,
    #[error("failed to parse the given sdp: {0}")]
    WebRTCParse(MoonlightError),
    #[error("rustcrypto error occurred: {0}")]
    RustCrypto(#[from] RustCryptoError),
    #[error("hex error occurred: {0}")]
    Hex(#[from] FromHexError),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("moonlight error: {0}")]
    Moonlight(#[from] MoonlightError),
    /// An error from connecting to or running the stream itself, after the host accepted it.
    #[error("moonlight stream error: {0}")]
    MoonlightStream(MoonlightError),
    #[error("webrtc: {0}")]
    WebRTC(#[from] webrtc::error::Error),
}

/// How the relay reports an error to the web client.
///
/// The goal is that a user never has to open the server logs to understand
/// why something failed: every error is classified into a stable `code` and
/// an actionable `message`.
pub struct ErrorDescription {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
}

fn describe_client_error(err: &MoonlightError) -> ErrorDescription {
    let not_paired = || ErrorDescription {
        status: StatusCode::FORBIDDEN,
        code: "not_paired",
        message: "This host isn't paired. Pair it and try again.".into(),
    };
    let host_unreachable = || ErrorDescription {
        status: StatusCode::BAD_GATEWAY,
        code: "host_unreachable",
        message: "Couldn't reach the host. Make sure it's on and Sunshine is running.".into(),
    };

    match err {
        MoonlightError::ConnectionFailed | MoonlightError::ConnectionTimeout => host_unreachable(),
        MoonlightError::NotPaired | MoonlightError::Unauthenticated => not_paired(),
        MoonlightError::ConnectionAlreadyExists => ErrorDescription {
            status: StatusCode::CONFLICT,
            code: "host_busy",
            message:
                "The host is already streaming to another device. Stop that stream and try again."
                    .into(),
        },
        MoonlightError::InstanceAquire => ErrorDescription {
            status: StatusCode::BAD_GATEWAY,
            code: "host_stream_refused",
            message: "The host refused the stream. Check that a firewall or VPN isn't blocking it."
                .into(),
        },
        MoonlightError::SettingHdrNotSupported
        | MoonlightError::Setting4kNotSupported
        | MoonlightError::Setting4kNotSupportedCodecMissing
        | MoonlightError::Setting4kNotSupportedUpdateGfe => ErrorDescription {
            status: StatusCode::BAD_REQUEST,
            code: "unsupported_stream_config",
            message: format!("The host doesn't support these stream settings: {err}"),
        },
        // The host answered but reported a failure, e.g.
        // "Failed to initialize video capture/encoding. Is a display connected and turned on?"
        // The host no longer trusts this device's certificate (it was unpaired on the host), e.g.
        // "The client is not authorized. Certificate verification failed."
        MoonlightError::StatusCode { reason, .. } if is_certificate_rejection(reason) => {
            ErrorDescription {
                status: StatusCode::FORBIDDEN,
                code: "pairing_invalid",
                message: "The host no longer recognizes this device. Pair it again.".into(),
            }
        }
        MoonlightError::StatusCode { reason, .. } => ErrorDescription {
            status: StatusCode::BAD_GATEWAY,
            code: "host_error",
            message: sunshine_error_hint((!reason.is_empty()).then_some(reason.as_str())),
        },
        MoonlightError::PairingFailed
        | MoonlightError::PairingFailedWrongPin
        | MoonlightError::PairingFailedAlreadyInProgress => ErrorDescription {
            status: StatusCode::FORBIDDEN,
            code: "pairing_failed",
            message: format!("Pairing failed: {err}. Check the PIN and try again."),
        },
        MoonlightError::LockPoisoned => ErrorDescription {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "internal",
            message: "Something went wrong on the server. Restart it and try again.".into(),
        },
        MoonlightError::Io(_) | MoonlightError::Other(_) => ErrorDescription {
            status: StatusCode::BAD_GATEWAY,
            code: "host_request_failed",
            message: format!("Couldn't talk to the host: {err}"),
        },
        other => ErrorDescription {
            status: StatusCode::BAD_GATEWAY,
            code: "host_error",
            message: format!("The host reported an error: {other}"),
        },
    }
}

/// Turns the raw `status_message` a Sunshine like host sent back into an
/// actionable message. Unknown messages are passed through verbatim.
fn sunshine_error_hint(message: Option<&str>) -> String {
    let Some(message) = message else {
        return "The host rejected the request.".into();
    };
    let lower = message.to_lowercase();
    if lower.contains("video capture") || lower.contains("encoding") || lower.contains("display") {
        "The host couldn't start the video. Make sure its display is connected and turned on."
            .into()
    } else {
        format!("The host reported an error: {message}")
    }
}

/// Whether a host `status_message` says it rejected this device's client certificate.
fn is_certificate_rejection(reason: &str) -> bool {
    let lower = reason.to_lowercase();
    lower.contains("authorized") || lower.contains("certificate")
}

fn describe_stream_error(err: &MoonlightError) -> ErrorDescription {
    match err {
        MoonlightError::Io(io_err) => match io_err.kind() {
            io::ErrorKind::ConnectionRefused
            | io::ErrorKind::TimedOut
            | io::ErrorKind::AddrNotAvailable
            | io::ErrorKind::NotConnected => ErrorDescription {
                status: StatusCode::BAD_GATEWAY,
                code: "stream_unreachable",
                message: "Couldn't connect to the stream. A firewall or VPN may be blocking the streaming ports (TCP 47984, 47989, 48010 and UDP 47998-48010).".into(),
            },
            _ => ErrorDescription {
                status: StatusCode::BAD_GATEWAY,
                code: "stream_failed",
                message: format!("The stream connection failed: {io_err}"),
            },
        },
        MoonlightError::ConnectionTimeout => ErrorDescription {
            status: StatusCode::BAD_GATEWAY,
            code: "stream_timeout",
            message: "Connecting to the stream timed out. A firewall or router may be blocking the streaming ports (UDP 47998-48010).".into(),
        },
        other => ErrorDescription {
            status: StatusCode::BAD_GATEWAY,
            code: "stream_failed",
            message: format!("The stream connection failed: {other}"),
        },
    }
}

impl AppError {
    /// Classified, user facing description of this error.
    pub fn describe(&self) -> ErrorDescription {
        match self {
            Self::AppDestroyed => ErrorDescription {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: "internal",
                message: "The server is restarting. Try again in a moment.".into(),
            },
            Self::HostNotFound => ErrorDescription {
                status: StatusCode::NOT_FOUND,
                code: "host_not_found",
                message: "Host not found. It may have been removed.".into(),
            },
            Self::HostPaired => ErrorDescription {
                status: StatusCode::CONFLICT,
                code: "host_paired",
                message: "This host is already paired.".into(),
            },
            Self::HostNotPaired => ErrorDescription {
                status: StatusCode::FORBIDDEN,
                code: "not_paired",
                message: "This host isn't paired. Pair it first.".into(),
            },
            Self::WebRtcClientCodecNotSupported => ErrorDescription {
                status: StatusCode::BAD_REQUEST,
                code: "codec_unsupported",
                message: "Your browser can't play any video format the host supports. Try setting Video Codec to H264.".into(),
            },
            Self::StreamClosed => ErrorDescription {
                status: StatusCode::NOT_FOUND,
                code: "stream_closed",
                message: "This stream has already ended.".into(),
            },
            Self::WebTransportDisabled => ErrorDescription {
                status: StatusCode::NOT_FOUND,
                code: "transport_disabled",
                message: "WebTransport is turned off on the server. Choose a different Data Transport in the settings.".into(),
            },
            Self::Unauthenticated => ErrorDescription {
                status: StatusCode::UNAUTHORIZED,
                code: "unauthenticated",
                message: "You're not signed in. If you run this server, check your login proxy setup.".into(),
            },
            Self::AdminOnly => ErrorDescription {
                status: StatusCode::FORBIDDEN,
                code: "admin_only",
                message: "Only an admin can do this.".into(),
            },
            Self::WebRTCParse(parse_err) => ErrorDescription {
                status: StatusCode::BAD_REQUEST,
                code: "bad_request",
                message: format!("The server couldn't read the connection request: {parse_err}"),
            },
            Self::Moonlight(err) => describe_client_error(err),
            Self::MoonlightStream(err) => describe_stream_error(err),
            Self::WebRTC(err) => ErrorDescription {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: "webrtc_relay_error",
                message: format!("WebRTC error on the server: {err}"),
            },
            _ => ErrorDescription {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: "internal",
                message: format!("Server error: {self}"),
            },
        }
    }

    /// The body the relay sends to clients for this error.
    pub fn api_error_body(&self) -> crate::api::bindings::ApiErrorBody {
        let description = self.describe();
        crate::api::bindings::ApiErrorBody {
            code: description.code.into(),
            message: description.message,
        }
    }
}

impl ResponseError for AppError {
    fn status_code(&self) -> StatusCode {
        self.describe().status
    }

    fn error_response(&self) -> HttpResponse<BoxBody> {
        HttpResponse::build(self.status_code()).json(self.api_error_body())
    }
}

#[derive(Clone)]
pub(crate) struct AppRef {
    inner: Weak<AppInner>,
}

impl AppRef {
    pub(crate) fn access(&self) -> Result<impl Deref<Target = AppInner> + 'static, AppError> {
        self.inner.upgrade().ok_or(AppError::AppDestroyed)
    }
}

pub(crate) struct AppInner {
    config: Config,
    pub(crate) storage: Arc<dyn Storage + Send + Sync>,
    pub(crate) app_image_cache: RwLock<HashMap<(HostId, AppId), actix_web::web::Bytes>>,
    pub(crate) streams: RwLock<HashMap<StreamId, Stream>>,
    pub(crate) ice_server_script_cache:
        Mutex<Option<(Instant, Vec<crate::api::bindings::RtcIceServer>)>>,
    pub(crate) web_transport_tickets: Mutex<HashMap<String, (AuthUser, Instant)>>,
    pub(crate) web_transport_certificate: Option<Arc<AutoCertificate>>,
}

/// How long a WebTransport ticket from `GET /api/host/stream/web_transport` stays valid.
const WEB_TRANSPORT_TICKET_TTL: Duration = Duration::from_secs(60);

pub type RequestClient = TokioClient;

#[derive(Clone)]
pub struct App {
    pub(crate) inner: Arc<AppInner>,
}

impl App {
    pub async fn new(config: Config) -> Result<Self, anyhow::Error> {
        let web_transport_certificate = config
            .web_server
            .web_transport
            .as_ref()
            .filter(|web_transport| web_transport.auto_certificate)
            .map(AutoCertificate::start)
            .transpose()?;

        Ok(Self {
            inner: Arc::new(AppInner {
                storage: create_storage(config.data_storage.clone()).await?,
                config,
                app_image_cache: Default::default(),
                streams: Default::default(),
                ice_server_script_cache: Default::default(),
                web_transport_tickets: Default::default(),
                web_transport_certificate,
            }),
        })
    }

    pub(crate) fn new_ref(&self) -> AppRef {
        AppRef {
            inner: Arc::downgrade(&self.inner),
        }
    }

    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    pub fn web_transport_certificate(&self) -> Option<&Arc<AutoCertificate>> {
        self.inner.web_transport_certificate.as_ref()
    }

    pub async fn client_unique_id(&self) -> Result<String, AppError> {
        self.inner.storage.client_unique_id().await
    }

    /// Hosts `user` is allowed to see.
    pub async fn hosts(&self, user: &AuthUser) -> Result<Vec<Host>, AppError> {
        Ok(self
            .inner
            .storage
            .list_hosts()
            .await?
            .into_iter()
            .filter(|host| user.can_access(&host.allowed_users))
            .map(|host| Host {
                app: self.new_ref(),
                id: host.id,
                cache_storage: Some(host),
                cache_host_info: None,
            })
            .collect())
    }

    /// A host `user` is allowed to see. Hidden hosts look like missing ones.
    pub async fn host(&self, user: &AuthUser, host_id: HostId) -> Result<Host, AppError> {
        let host = self.inner.storage.get_host(host_id).await?;
        if !user.can_access(&host.allowed_users) {
            return Err(AppError::HostNotFound);
        }
        Ok(Host {
            app: self.new_ref(),
            id: host.id,
            cache_storage: Some(host),
            cache_host_info: None,
        })
    }

    pub async fn host_add(&self, address: String, http_port: u16) -> Result<Host, AppError> {
        let unique_id = self.client_unique_id().await?;
        let client = RequestClient::with_defaults()?;
        let info = match client
            .send_http::<ServerInfoEndpoint>(
                ClientInfo {
                    uuid: uuid::Uuid::new_v4(),
                    unique_id,
                },
                &format!("{address}:{http_port}"),
                &ServerInfoRequest {},
            )
            .await
        {
            Ok(info) => info,
            Err(MoonlightError::ConnectionFailed | MoonlightError::ConnectionTimeout) => {
                return Err(AppError::HostNotFound);
            }
            Err(err) => return Err(err.into()),
        };
        let host = self
            .inner
            .storage
            .add_host(StorageHostAdd {
                address,
                http_port,
                pair_info: None,
                cache: StorageHostCache {
                    name: info.host_name,
                    mac: info.mac,
                },
                allowed_users: Vec::new(),
            })
            .await?;
        Ok(Host {
            app: self.new_ref(),
            id: host.id,
            cache_storage: Some(host),
            cache_host_info: None,
        })
    }

    pub async fn host_delete(&self, host_id: HostId) -> Result<(), AppError> {
        self.inner.storage.remove_host(host_id).await?;
        let mut images = self.inner.app_image_cache.write().await;
        images.retain(|(id, _), _| *id != host_id);
        Ok(())
    }

    pub(crate) async fn insert_stream(
        &self,
        f: impl FnOnce(StreamId) -> Stream,
    ) -> Result<Stream, AppError> {
        let mut streams = self.inner.streams.write().await;
        let mut id = StreamId(0);
        while streams.contains_key(&id) {
            let mut random = [0; 4];
            RustCryptoBackend.random_bytes(&mut random)?;
            id = StreamId(u32::from_be_bytes(random));
        }
        let stream = f(id);
        streams.insert(id, stream.clone());
        Ok(stream)
    }

    /// A stream `user` started (the admin can reach every stream).
    pub async fn stream_by_id(&self, user: &AuthUser, id: StreamId) -> Result<Stream, AppError> {
        self.inner
            .streams
            .read()
            .await
            .get(&id)
            .filter(|stream| user.is_admin() || stream.owner() == user)
            .cloned()
            .ok_or(AppError::StreamClosed)
    }

    /// One-shot ticket that carries `user` over to the WebTransport endpoint,
    /// which the forward-auth proxy doesn't sit in front of.
    pub async fn issue_web_transport_ticket(&self, user: AuthUser) -> Result<String, AppError> {
        let mut bytes = [0; 16];
        RustCryptoBackend.random_bytes(&mut bytes)?;
        let ticket = hex::encode(bytes);
        let mut tickets = self.inner.web_transport_tickets.lock().await;
        tickets.retain(|_, (_, issued)| issued.elapsed() < WEB_TRANSPORT_TICKET_TTL);
        tickets.insert(ticket.clone(), (user, Instant::now()));
        Ok(ticket)
    }

    pub async fn redeem_web_transport_ticket(&self, ticket: &str) -> Option<AuthUser> {
        let (user, issued) = self
            .inner
            .web_transport_tickets
            .lock()
            .await
            .remove(ticket)?;
        (issued.elapsed() < WEB_TRANSPORT_TICKET_TTL).then_some(user)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status(code: i32, reason: &str) -> MoonlightError {
        MoonlightError::StatusCode {
            code,
            reason: reason.into(),
        }
    }

    #[test]
    fn rejected_certificate_asks_to_pair_again() {
        let description = describe_client_error(&status(
            401,
            "The client is not authorized. Certificate verification failed.",
        ));
        assert_eq!(description.code, "pairing_invalid");
        assert_eq!(description.status, StatusCode::FORBIDDEN);
        assert_eq!(
            description.message,
            "The host no longer recognizes this device. Pair it again."
        );
    }

    #[test]
    fn other_host_status_errors_stay_host_error() {
        let description = describe_client_error(&status(500, "Something broke"));
        assert_eq!(description.code, "host_error");
        assert_eq!(description.status, StatusCode::BAD_GATEWAY);
        assert_eq!(
            description.message,
            "The host reported an error: Something broke"
        );

        let description = describe_client_error(&status(
            503,
            "Failed to initialize video capture/encoding. Is a display connected and turned on?",
        ));
        assert_eq!(description.code, "host_error");

        let description = describe_client_error(&status(500, ""));
        assert_eq!(description.code, "host_error");
        assert_eq!(description.message, "The host rejected the request.");
    }

    #[test]
    fn existing_client_errors_keep_their_codes() {
        let cases = [
            (MoonlightError::ConnectionFailed, "host_unreachable"),
            (MoonlightError::ConnectionTimeout, "host_unreachable"),
            (MoonlightError::NotPaired, "not_paired"),
            (MoonlightError::Unauthenticated, "not_paired"),
            (MoonlightError::PairingFailedWrongPin, "pairing_failed"),
            (MoonlightError::ConnectionAlreadyExists, "host_busy"),
        ];
        for (err, code) in cases {
            assert_eq!(describe_client_error(&err).code, code, "{err}");
        }
    }
}
