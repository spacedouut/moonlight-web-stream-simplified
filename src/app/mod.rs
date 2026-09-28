use std::{
    collections::HashMap,
    io,
    ops::Deref,
    sync::{Arc, Weak},
    time::Instant,
};

use actix_web::{HttpResponse, ResponseError, body::BoxBody, http::StatusCode};
use hex::FromHexError;
use moonlight_common::{
    MoonlightError,
    crypto::rustcrypto::{RustCryptoBackend, RustCryptoError},
    high::{MoonlightClientError, StreamConfigError},
    http::{
        ClientInfo, ParseError,
        client::{
            RequestError,
            async_client::RequestClient as _,
            tokio_hyper::{HyperError, TokioHyperClient},
        },
        pair::PairingCryptoBackend,
        server_info::{ServerInfoEndpoint, ServerInfoRequest},
    },
    stream::tokio::MoonlightStreamError,
    webrtc::WebRTCParseError,
};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};

use crate::{
    app::{
        host::{AppId, Host, HostId},
        storage::{Storage, StorageHostAdd, StorageHostCache, create_storage},
        stream::{Stream, StreamId},
    },
    config::Config,
};

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
    #[error("the host doesn't support the given config: {0}")]
    StreamConfig(#[from] StreamConfigError),
    #[error("failed to parse the given sdp: {0}")]
    WebRTCParse(#[from] WebRTCParseError),
    #[error("rustcrypto error occurred: {0}")]
    RustCrypto(#[from] RustCryptoError),
    #[error("hex error occurred: {0}")]
    Hex(#[from] FromHexError),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("moonlight error: {0}")]
    Moonlight(#[from] MoonlightClientError),
    #[error("moonlight error: {0}")]
    MoonlightStream(#[from] MoonlightStreamError),
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

fn describe_client_error(err: &MoonlightClientError) -> ErrorDescription {
    let not_paired = || {
        ErrorDescription {
        status: StatusCode::FORBIDDEN,
        code: "not_paired",
        message: "This device isn't paired with the host. Pair it first; if it was paired before, the host may have forgotten it so pair again.".into(),
    }
    };
    let host_unreachable = || ErrorDescription {
        status: StatusCode::BAD_GATEWAY,
        code: "host_unreachable",
        message: "Couldn't reach Sunshine. Make sure the host is on and Sunshine is running."
            .into(),
    };

    match err {
        MoonlightClientError::Offline => host_unreachable(),
        MoonlightClientError::NotPaired | MoonlightClientError::Unauthenticated => not_paired(),
        MoonlightClientError::Moonlight(inner) => match inner {
            MoonlightError::NotPaired => not_paired(),
            MoonlightError::ConnectionAlreadyExists => ErrorDescription {
                status: StatusCode::CONFLICT,
                code: "host_busy",
                message: "The host is busy streaming to another client. Stop that stream and try again.".into(),
            },
            MoonlightError::ConnectionFailed | MoonlightError::InstanceAquire => ErrorDescription {
                status: StatusCode::BAD_GATEWAY,
                code: "host_stream_refused",
                message: "The host refused the stream connection. Check that the Moonlight ports aren't blocked by a firewall or VPN.".into(),
            },
            other => ErrorDescription {
                status: StatusCode::BAD_GATEWAY,
                code: "host_error",
                message: format!("The host reported an error: {other}"),
            },
        },
        MoonlightClientError::StreamConfig(config_err) => ErrorDescription {
            status: StatusCode::BAD_REQUEST,
            code: "unsupported_stream_config",
            message: format!("The host doesn't support this stream configuration: {config_err}"),
        },
        MoonlightClientError::Backend(backend) => {
            let Some(hyper_err) = backend.downcast_ref::<HyperError>() else {
                return ErrorDescription {
                    status: StatusCode::BAD_GATEWAY,
                    code: "host_request_failed",
                    message: format!("Couldn't talk to the host: {backend}"),
                };
            };
            match hyper_err {
                // The host answered but reported a failure, e.g.
                // "Failed to initialize video capture/encoding. Is a display connected and turned on?"
                HyperError::Parse(ParseError::InvalidXmlStatusCode { message }) => {
                    ErrorDescription {
                        status: StatusCode::BAD_GATEWAY,
                        code: "host_error",
                        message: sunshine_error_hint(message.as_deref()),
                    }
                }
                _ if hyper_err.is_encryption() => ErrorDescription {
                    status: StatusCode::FORBIDDEN,
                    code: "pairing_invalid",
                    message: "The host rejected this device's certificate: it was probably unpaired. Re-pair and try again.".into(),
                },
                _ if hyper_err.is_connect() => host_unreachable(),
                _ => ErrorDescription {
                    status: StatusCode::BAD_GATEWAY,
                    code: "host_request_failed",
                    message: format!("Couldn't talk to the host: {hyper_err}"),
                },
            }
        }
        MoonlightClientError::Pairing(err) => ErrorDescription {
            status: StatusCode::FORBIDDEN,
            code: "pairing_failed",
            message: format!("Pairing with the host failed: {err}. Check the PIN shown on the host."),
        },
        MoonlightClientError::Poisoned(_) => ErrorDescription {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "internal",
            message: "Relay error: the host client is in a broken state, restart the relay.".into(),
        },
    }
}

/// Turns the raw `status_message` a Sunshine like host sent back into an
/// actionable message. Unknown messages are passed through verbatim.
fn sunshine_error_hint(message: Option<&str>) -> String {
    let Some(message) = message else {
        return "Sunshine rejected the request.".into();
    };
    let lower = message.to_lowercase();
    if lower.contains("video capture") || lower.contains("encoding") || lower.contains("display") {
        "Sunshine failed to start encoding. Make sure the host's display is connected and on."
            .into()
    } else if lower.contains("authorized") || lower.contains("certificate") {
        "The host rejected this device's certificate: it may have been unpaired. Re-pair and try again.".into()
    } else {
        format!("Sunshine reported an error: {message}")
    }
}

fn describe_stream_error(err: &MoonlightStreamError) -> ErrorDescription {
    match err {
        MoonlightStreamError::Io(io_err) => match io_err.kind() {
            io::ErrorKind::ConnectionRefused
            | io::ErrorKind::TimedOut
            | io::ErrorKind::AddrNotAvailable
            | io::ErrorKind::NotConnected => ErrorDescription {
                status: StatusCode::BAD_GATEWAY,
                code: "stream_unreachable",
                message: "The host accepted the stream but the streaming ports are unreachable. Check that the Moonlight ports (TCP 47984, 47989, 48010 and UDP 47998-48010) aren't blocked by a firewall or VPN.".into(),
            },
            _ => ErrorDescription {
                status: StatusCode::BAD_GATEWAY,
                code: "stream_failed",
                message: format!("The connection to the host's stream failed: {io_err}"),
            },
        },
        MoonlightStreamError::ConnectionTimeout => ErrorDescription {
            status: StatusCode::BAD_GATEWAY,
            code: "stream_timeout",
            message: "Timed out connecting to the host's stream. The host is reachable but the streaming ports look blocked: check firewall and NAT forwarding for the Moonlight ports (UDP 47998-48010).".into(),
        },
        MoonlightStreamError::Setup(setup_err) => ErrorDescription {
            status: StatusCode::BAD_GATEWAY,
            code: "stream_setup_failed",
            message: format!("The host accepted the stream but the stream setup failed: {setup_err}"),
        },
        MoonlightStreamError::Closed => ErrorDescription {
            status: StatusCode::GONE,
            code: "stream_closed",
            message: "The stream is already closed.".into(),
        },
        other => ErrorDescription {
            status: StatusCode::BAD_GATEWAY,
            code: "stream_failed",
            message: format!("The connection to the host's stream failed: {other}"),
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
                message: "The relay is restarting. Try again in a moment.".into(),
            },
            Self::HostNotFound => ErrorDescription {
                status: StatusCode::NOT_FOUND,
                code: "host_not_found",
                message: "The relay doesn't know this host.".into(),
            },
            Self::HostPaired => ErrorDescription {
                status: StatusCode::CONFLICT,
                code: "host_paired",
                message: "This host is already paired.".into(),
            },
            Self::HostNotPaired => ErrorDescription {
                status: StatusCode::FORBIDDEN,
                code: "not_paired",
                message: "This device isn't paired with the host. Pair it first.".into(),
            },
            Self::WebRtcClientCodecNotSupported => ErrorDescription {
                status: StatusCode::BAD_REQUEST,
                code: "codec_unsupported",
                message: "This browser doesn't support any video codec the host can use. Try selecting H264 in the settings.".into(),
            },
            Self::StreamClosed => ErrorDescription {
                status: StatusCode::NOT_FOUND,
                code: "stream_closed",
                message: "The stream is already closed.".into(),
            },
            Self::WebTransportDisabled => ErrorDescription {
                status: StatusCode::NOT_FOUND,
                code: "transport_disabled",
                message: "The relay isn't exposing WebTransport. It may be disabled in the relay config.".into(),
            },
            Self::StreamConfig(config_err) => ErrorDescription {
                status: StatusCode::BAD_REQUEST,
                code: "unsupported_stream_config",
                message: format!("The host doesn't support this stream configuration: {config_err}"),
            },
            Self::WebRTCParse(parse_err) => ErrorDescription {
                status: StatusCode::BAD_REQUEST,
                code: "bad_request",
                message: format!("The relay couldn't parse the WebRTC offer: {parse_err}"),
            },
            Self::Moonlight(err) => describe_client_error(err),
            Self::MoonlightStream(err) => describe_stream_error(err),
            Self::WebRTC(err) => ErrorDescription {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: "webrtc_relay_error",
                message: format!("The relay hit a WebRTC error: {err}"),
            },
            _ => ErrorDescription {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: "internal",
                message: format!("Relay error: {self}"),
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
}

pub type RequestClient = TokioHyperClient;

#[derive(Clone)]
pub struct App {
    pub(crate) inner: Arc<AppInner>,
}

impl App {
    pub async fn new(config: Config) -> Result<Self, anyhow::Error> {
        Ok(Self {
            inner: Arc::new(AppInner {
                storage: create_storage(config.data_storage.clone()).await?,
                config,
                app_image_cache: Default::default(),
                streams: Default::default(),
                ice_server_script_cache: Default::default(),
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

    pub async fn client_unique_id(&self) -> Result<String, AppError> {
        self.inner.storage.client_unique_id().await
    }

    pub async fn hosts(&self) -> Result<Vec<Host>, AppError> {
        Ok(self
            .inner
            .storage
            .list_hosts()
            .await?
            .into_iter()
            .map(|host| Host {
                app: self.new_ref(),
                id: host.id,
                cache_storage: Some(host),
                cache_host_info: None,
            })
            .collect())
    }

    pub async fn host(&self, host_id: HostId) -> Result<Host, AppError> {
        let host = self.inner.storage.get_host(host_id).await?;
        Ok(Host {
            app: self.new_ref(),
            id: host.id,
            cache_storage: Some(host),
            cache_host_info: None,
        })
    }

    pub async fn host_add(&self, address: String, http_port: u16) -> Result<Host, AppError> {
        let unique_id = self.client_unique_id().await?;
        let client = RequestClient::with_defaults()
            .map_err(|err| MoonlightClientError::Backend(Box::new(err)))?;
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
            Err(err) if err.is_connect() => return Err(AppError::HostNotFound),
            Err(err) => return Err(MoonlightClientError::Backend(Box::new(err)).into()),
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

    pub async fn stream_by_id(&self, id: StreamId) -> Result<Stream, AppError> {
        self.inner
            .streams
            .read()
            .await
            .get(&id)
            .cloned()
            .ok_or(AppError::StreamClosed)
    }
}
