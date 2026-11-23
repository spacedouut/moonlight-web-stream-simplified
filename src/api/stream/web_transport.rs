//! Experimental WebTransport transport.
//!
//! The message stream uses `u32` big-endian length-prefixed frames with a
//! one-byte kind: `0` contains UTF-8 JSON and `1` contains the existing
//! WebSocket binary layout. Control packets use kind-1 frames for reliable
//! ordered delivery.
//! Video packets use unidirectional streams, while audio packets use
//! datagrams when they fit and unidirectional streams otherwise.
//! Ping datagrams (`PING` channel byte followed by a `u32` id) are echoed back
//! as datagrams so the measured RTT isn't queued behind stream data.
//!
//! Stream priorities: the message stream (control and stats) is sent before
//! audio, which is sent before video. When more than
//! `max_in_flight_video_frames` video frames are unacknowledged, the relay
//! resets them, drops frames until the next keyframe and requests one.

use std::{
    sync::{Arc, RwLock as StdRwLock},
    time::{Duration, Instant},
};

use actix_web::{
    get,
    web::{Data, Json},
};
use bytes::Bytes;
use moonlight_common::stream::{
    proto::{
        audio::AudioStreamEvent,
        control::{
            ControlStreamEvent,
            packet::{ControlPacket, PacketDirection},
        },
        video::VideoStreamEvent,
    },
    tokio::MoonlightStreamEvent,
};
use rustls::{
    ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, pem::PemObject},
    server::ResolvesServerCert,
    sign::CertifiedKey,
};
use sha2::{Digest, Sha256};
use tokio::{
    select,
    sync::{
        OwnedSemaphorePermit, Semaphore,
        mpsc::{self, Sender, error::TrySendError},
        watch,
    },
    time::{interval, timeout},
};
use tracing::{Instrument, debug, debug_span, error, info, instrument, trace, warn};
use wtransport::{Connection, Endpoint, VarInt};

use crate::{
    api::{
        bindings::{
            StreamStatsClientboundMessage, StreamStatsServerboundMessage, WebSocketChannel,
            WebSocketClientboundMessage, WebSocketServerboundMessage, WebTransportConfigResponse,
        },
        stream::{
            create_control_packet_config, start_moonlight_stream,
            telemetry::{RelayClock, RelayTelemetry, VIDEO_HEADER_LEN, write_video_header},
        },
    },
    app::{App, AppError, auth::AuthUser},
    config::WebTransportConfig,
};

const MESSAGE_STREAM_PRIORITY: i32 = 2;
const AUDIO_STREAM_PRIORITY: i32 = 1;
const VIDEO_STREAM_PRIORITY: i32 = 0;
const PING_DATAGRAM_LEN: usize = 5;
const IDR_REQUEST_RETRY: Duration = Duration::from_secs(1);

enum Outgoing {
    Message(String),
    Binary(Bytes),
    Uni(Bytes),
    Datagram(Bytes),
}

enum Incoming {
    Message(WebSocketServerboundMessage),
    Binary(Bytes),
    Closed,
}

#[get("/host/stream/web_transport")]
#[instrument(skip(app))]
pub async fn web_transport_config(
    app: Data<App>,
    user: AuthUser,
) -> Result<Json<WebTransportConfigResponse>, AppError> {
    let Some(config) = app.config().web_server.web_transport.as_ref() else {
        return Err(AppError::WebTransportDisabled);
    };
    if app.web_transport_certificate().is_none() && app.config().web_server.certificate.is_none() {
        return Err(AppError::WebTransportDisabled);
    }
    let prefix = &app.config().web_server.url_path_prefix;
    let path = format!("{prefix}/api/host/stream/web_transport");
    let certificate_hash = if let Some(auto_certificate) = app.web_transport_certificate() {
        Some(auto_certificate.leaf_hash())
    } else if config.advertise_certificate_hash {
        app.config()
            .web_server
            .certificate
            .as_ref()
            .and_then(|certificate| certificate_hash_from_pem(&certificate.certificate_pem))
    } else {
        None
    };
    let ticket = if app.config().forward_auth.enabled {
        Some(app.issue_web_transport_ticket(user).await?)
    } else {
        None
    };
    Ok(Json(WebTransportConfigResponse {
        url: config.public_url.as_ref().map(|url| format!("{url}{path}")),
        port: config.bind_address.port(),
        certificate_hash,
        ticket,
    }))
}

fn certificate_hash_from_pem(path: &str) -> Option<String> {
    let mut certs = CertificateDer::pem_file_iter(path).ok()?;
    let cert = certs.next()?.ok()?;
    Some(hex::encode(Sha256::digest(cert.as_ref())))
}

/// Chrome only accepts a `serverCertificateHashes` match when the certificate
/// lives for two weeks or less, so the generated certificate gets ten days and
/// is swapped out halfway through.
const AUTO_CERTIFICATE_LIFETIME: time::Duration = time::Duration::days(10);
const AUTO_CERTIFICATE_RENEW: Duration = Duration::from_secs(5 * 24 * 60 * 60);
const AUTO_CERTIFICATE_RETRY: Duration = Duration::from_secs(60 * 60);

/// Self-signed certificate the WebTransport endpoint generates and rotates on
/// its own. Browsers trust it through `serverCertificateHashes`, advertised by
/// the config endpoint.
pub struct AutoCertificate {
    sans: Vec<String>,
    current: StdRwLock<CurrentCertificate>,
}

struct CurrentCertificate {
    certified_key: Arc<CertifiedKey>,
    leaf_hash: String,
    renew_at: Instant,
}

impl AutoCertificate {
    /// Generates the first certificate and spawns the rotation task.
    pub fn start(cfg: &WebTransportConfig) -> Result<Arc<Self>, anyhow::Error> {
        let mut sans = vec!["localhost".to_string()];
        if let Some(host) = cfg
            .public_url
            .as_deref()
            .and_then(|url| url.split("://").nth(1))
            .and_then(|rest| rest.split('/').next())
            .map(|authority| {
                authority
                    .strip_prefix('[')
                    .and_then(|rest| rest.split(']').next())
                    .unwrap_or_else(|| authority.split(':').next().unwrap_or(authority))
            })
            && !host.is_empty()
        {
            sans.push(host.to_string());
        }

        let certificate = Arc::new(Self {
            current: StdRwLock::new(Self::generate(&sans)?),
            sans,
        });
        info!("[Server]: Generated self-signed WebTransport certificate");

        tokio::spawn({
            let certificate = certificate.clone();
            async move {
                loop {
                    let delay = certificate
                        .current
                        .read()
                        .map(|current| current.renew_at.saturating_duration_since(Instant::now()))
                        .unwrap_or(AUTO_CERTIFICATE_RETRY);
                    tokio::time::sleep(delay).await;
                    match Self::generate(&certificate.sans) {
                        Ok(next) => {
                            if let Ok(mut current) = certificate.current.write() {
                                *current = next;
                            }
                            info!("[Server]: Rotated WebTransport certificate");
                        }
                        Err(err) => {
                            error!(error = %err, "web transport certificate rotation failed");
                            if let Ok(mut current) = certificate.current.write() {
                                current.renew_at = Instant::now() + AUTO_CERTIFICATE_RETRY;
                            }
                        }
                    }
                }
            }
        });

        Ok(certificate)
    }

    fn generate(sans: &[String]) -> Result<CurrentCertificate, anyhow::Error> {
        let mut params = rcgen::CertificateParams::new(sans.to_vec())?;
        let now = time::OffsetDateTime::now_utc();
        params.not_before = now - time::Duration::minutes(5);
        params.not_after = now + AUTO_CERTIFICATE_LIFETIME;

        let key_pair = rcgen::KeyPair::generate()?;
        let cert = params.self_signed(&key_pair)?;
        let leaf = cert.der().clone();
        let private_key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_pair.serialize_der()));
        let signing_key = rustls::crypto::ring::sign::any_supported_type(&private_key)?;

        Ok(CurrentCertificate {
            certified_key: Arc::new(CertifiedKey::new(vec![leaf.clone()], signing_key)),
            leaf_hash: hex::encode(Sha256::digest(leaf.as_ref())),
            renew_at: Instant::now() + AUTO_CERTIFICATE_RENEW,
        })
    }

    /// SHA-256 of the current leaf certificate, for `serverCertificateHashes`.
    pub fn leaf_hash(&self) -> String {
        self.current
            .read()
            .map(|current| current.leaf_hash.clone())
            .unwrap_or_default()
    }

    /// TLS config that always serves the latest rotated certificate.
    pub fn tls_config(self: &Arc<Self>) -> ServerConfig {
        ServerConfig::builder()
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(AutoCertificateResolver(self.clone())))
    }
}

#[derive(Debug)]
struct AutoCertificateResolver(Arc<AutoCertificate>);

impl std::fmt::Debug for AutoCertificate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AutoCertificate")
            .field("sans", &self.sans)
            .finish_non_exhaustive()
    }
}

impl ResolvesServerCert for AutoCertificateResolver {
    fn resolve(&self, _client_hello: rustls::server::ClientHello) -> Option<Arc<CertifiedKey>> {
        self.0
            .current
            .read()
            .ok()
            .map(|current| current.certified_key.clone())
    }
}

const MAX_FRAME: usize = 1 << 20;

async fn read_frame(stream: &mut wtransport::RecvStream) -> Result<Option<Vec<u8>>, AppError> {
    let mut header = [0; 4];
    stream
        .read_exact(&mut header[..1])
        .await
        .map_err(|_| AppError::StreamClosed)?;
    timeout(Duration::from_secs(5), stream.read_exact(&mut header[1..]))
        .await
        .map_err(|_| AppError::StreamClosed)?
        .map_err(|_| AppError::StreamClosed)?;
    let len = u32::from_be_bytes(header) as usize;
    if len > MAX_FRAME {
        warn!(length = len, "web transport frame exceeds maximum size");
        return Err(AppError::StreamClosed);
    }
    let mut body = vec![0; len];
    timeout(Duration::from_secs(5), stream.read_exact(&mut body))
        .await
        .map_err(|_| AppError::StreamClosed)?
        .map_err(|_| AppError::StreamClosed)?;
    Ok(Some(body))
}

pub fn spawn_web_transport_server(
    app: Data<App>,
    mut tls: ServerConfig,
    cfg: &WebTransportConfig,
    url_path_prefix: &str,
) -> anyhow::Result<()> {
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let path = format!("{url_path_prefix}/api/host/stream/web_transport");
    let server_config = wtransport::ServerConfig::builder()
        .with_bind_address(cfg.bind_address)
        .with_custom_tls(tls)
        .keep_alive_interval(Some(Duration::from_secs(3)))
        .build();
    let endpoint = Endpoint::server(server_config)?;
    info!(
        "[Server]: Running WebTransport server on {}",
        cfg.bind_address
    );
    tokio::spawn(async move {
        loop {
            let incoming = endpoint.accept().await;
            let app = app.clone();
            let path = path.clone();
            tokio::spawn(
                async move {
                    match incoming.await {
                        Ok(session_request) if path_matches(session_request.path(), &path) => {
                            // The forward-auth proxy isn't in front of this endpoint, so the
                            // user comes from a ticket minted by the (proxied) config endpoint.
                            let user = if app.config().forward_auth.enabled {
                                let ticket = query_param(session_request.path(), "ticket");
                                let user = match ticket {
                                    Some(ticket) => app.redeem_web_transport_ticket(ticket).await,
                                    None => None,
                                };
                                let Some(user) = user else {
                                    warn!("web transport session without a valid ticket");
                                    session_request.forbidden().await;
                                    return;
                                };
                                user
                            } else {
                                AuthUser::Unrestricted
                            };
                            match session_request.accept().await {
                                Ok(connection) => {
                                    if let Err(err) = handle_session(app, user, connection).await {
                                        error!(error = %err, "web transport session failed");
                                    }
                                }
                                Err(err) => error!(error = %err, "web transport accept failed"),
                            }
                        }
                        Ok(session_request) => session_request.not_found().await,
                        Err(err) => error!(error = %err, "web transport incoming session failed"),
                    }
                }
                .instrument(debug_span!("web transport handler")),
            );
        }
    });
    Ok(())
}

fn path_matches(request_path: &str, path: &str) -> bool {
    request_path
        .split_once('?')
        .map_or(request_path, |(path, _)| path)
        == path
}

fn query_param<'a>(request_path: &'a str, name: &str) -> Option<&'a str> {
    let (_, query) = request_path.split_once('?')?;
    query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find(|(key, _)| *key == name)
        .map(|(_, value)| value)
}

async fn handle_session(
    app: Data<App>,
    user: AuthUser,
    connection: Connection,
) -> Result<(), AppError> {
    let (mut message_send, mut message_recv) =
        timeout(Duration::from_secs(10), connection.accept_bi())
            .await
            .map_err(|_| AppError::StreamClosed)?
            .map_err(|_| AppError::StreamClosed)?;
    message_send.set_priority(MESSAGE_STREAM_PRIORITY);
    let request = read_frame(&mut message_recv)
        .await?
        .ok_or(AppError::StreamClosed)?;
    if request.first() != Some(&0) {
        warn!("web transport stream request was not a JSON frame");
        return Err(AppError::StreamClosed);
    }
    let request: WebSocketServerboundMessage =
        serde_json::from_slice(&request[1..]).map_err(|_| AppError::StreamClosed)?;
    let WebSocketServerboundMessage::Request(request) = request else {
        return Err(AppError::StreamClosed);
    };
    let (stream, response) = match start_moonlight_stream(&app, &user, &request).await {
        Ok(value) => value,
        Err(err) => {
            // Tell the client why the stream couldn't be started before
            // closing: a silent close would leave it with no reason at all.
            if let Ok(json) =
                serde_json::to_vec(&WebSocketClientboundMessage::Error(err.api_error_body()))
            {
                let mut frame = Vec::with_capacity(5 + json.len());
                frame.extend_from_slice(&((json.len() + 1) as u32).to_be_bytes());
                frame.push(0);
                frame.extend_from_slice(&json);
                let _ = message_send.write_all(&frame).await;
                let _ = message_send.finish().await;
            }
            return Err(err);
        }
    };

    let (sender, mut sender_rx) = mpsc::channel::<Outgoing>(512);
    let sender_connection = connection.clone();
    let uni_semaphore = Arc::new(Semaphore::new(64));
    tokio::spawn(
        async move {
            while let Some(outgoing) = sender_rx.recv().await {
                match outgoing {
                    Outgoing::Message(message) => {
                        let bytes = message.as_bytes();
                        let mut frame = Vec::with_capacity(5 + bytes.len());
                        frame.extend_from_slice(&((bytes.len() + 1) as u32).to_be_bytes());
                        frame.push(0);
                        frame.extend_from_slice(bytes);
                        if message_send.write_all(&frame).await.is_err() {
                            break;
                        }
                    }
                    Outgoing::Binary(bytes) => {
                        let mut frame = Vec::with_capacity(5 + bytes.len());
                        frame.extend_from_slice(&((bytes.len() + 1) as u32).to_be_bytes());
                        frame.push(1);
                        frame.extend_from_slice(&bytes);
                        if message_send.write_all(&frame).await.is_err() {
                            break;
                        }
                    }
                    Outgoing::Datagram(bytes) => {
                        if sender_connection.send_datagram(bytes).is_err() {
                            break;
                        }
                    }
                    Outgoing::Uni(bytes) => {
                        let Ok(permit) = uni_semaphore.clone().try_acquire_owned() else {
                            trace!("dropping web transport unidirectional frame: too many streams");
                            continue;
                        };
                        let connection = sender_connection.clone();
                        tokio::spawn(async move {
                            let _permit = permit;
                            let Ok(opening) = connection.open_uni().await else {
                                return;
                            };
                            if let Ok(mut stream) = opening.await {
                                stream.set_priority(AUDIO_STREAM_PRIORITY);
                                let _ = stream.write_all(&bytes).await;
                                let _ = stream.finish().await;
                            }
                        });
                    }
                }
            }
        }
        .instrument(debug_span!("web transport sender")),
    );
    if !send_message(&sender, WebSocketClientboundMessage::Response(response)) {
        return Err(AppError::StreamClosed);
    }

    let (incoming_tx, mut incoming_rx) = mpsc::channel::<Incoming>(256);
    let reader_tx = incoming_tx.clone();
    tokio::spawn(async move {
        loop {
            match read_frame(&mut message_recv).await {
                Ok(Some(frame)) => {
                    let Some((kind, payload)) = frame.split_first() else {
                        warn!("received empty web transport frame");
                        continue;
                    };
                    let incoming = match *kind {
                        0 => match serde_json::from_slice(payload) {
                            Ok(message) => Incoming::Message(message),
                            Err(err) => {
                                warn!(error = %err, "failed to deserialize web transport message");
                                continue;
                            }
                        },
                        1 => Incoming::Binary(Bytes::copy_from_slice(payload)),
                        kind => {
                            warn!(kind, "received unknown web transport frame kind");
                            continue;
                        }
                    };
                    match reader_tx.try_send(incoming) {
                        Ok(()) => {}
                        Err(TrySendError::Full(_)) => {
                            warn!("web transport incoming queue is full");
                            let _ = reader_tx.send(Incoming::Closed).await;
                            break;
                        }
                        Err(TrySendError::Closed(_)) => break,
                    }
                }
                _ => {
                    let _ = reader_tx.send(Incoming::Closed).await;
                    break;
                }
            }
        }
    });

    let max_in_flight_video_frames = app
        .config()
        .web_server
        .web_transport
        .as_ref()
        .map_or(64, |config| config.max_in_flight_video_frames)
        .max(1);
    let mut video_sender = VideoSender::new(connection.clone(), max_in_flight_video_frames);
    let clock = RelayClock::new();
    let mut telemetry = RelayTelemetry::new();

    let control_config = create_control_packet_config();
    let mut relay_stats_ticker = interval(Duration::from_secs(1));
    let mut stream = stream;
    loop {
        if !stream.is_alive() {
            break;
        }
        select! {
            result = stream.drive() => {
                let event = result?;
                let data = match event {
                    MoonlightStreamEvent::Audio(AudioStreamEvent::OnFrame(frame)) => {
                        let mut buffer = vec![WebSocketChannel::AUDIO];
                        buffer.extend_from_slice(&frame.buffer);
                        if connection.max_datagram_size().is_some_and(|max| buffer.len() <= max) {
                            Outgoing::Datagram(buffer.into())
                        } else { Outgoing::Uni(buffer.into()) }
                    }
                    MoonlightStreamEvent::Video(VideoStreamEvent::SignalIdr) => {
                        telemetry.on_host_idr_request();
                        if let Err(err) = stream.send_raw(ControlPacket::RequestIdr) {
                            warn!(error = %err, "failed to request idr");
                        }
                        continue;
                    }
                    MoonlightStreamEvent::Video(VideoStreamEvent::OnFrame(frame)) => {
                        let idr = frame.metadata().frame_type.serialize() == 2;
                        let raw = frame.raw();
                        let metadata = frame.metadata();
                        telemetry.on_frame(&metadata, raw.len());
                        let mut buffer = vec![0; 1 + VIDEO_HEADER_LEN + raw.len()];
                        buffer[0] = WebSocketChannel::VIDEO;
                        write_video_header(&mut buffer[1..], &metadata, clock.now_us());
                        buffer[(1 + VIDEO_HEADER_LEN)..].copy_from_slice(raw);
                        let outcome = video_sender.send(buffer.into(), idr);
                        if outcome.dropped_backlog {
                            telemetry.on_backlog_drop();
                        }
                        if outcome.request_idr {
                            telemetry.on_relay_idr_request();
                            if let Err(err) = stream.send_raw(ControlPacket::RequestIdr) {
                                warn!(error = %err, "failed to request idr after dropping video backlog");
                            }
                        }
                        continue;
                    }
                    MoonlightStreamEvent::Control(ControlStreamEvent::Packet(packet)) => {
                        let mut buffer = vec![0; ControlPacket::MAX_SIZE + 1];
                        buffer[0] = WebSocketChannel::CONTROL;
                        #[allow(clippy::unwrap_used)]
                        let len = packet.serialize(&control_config, buffer[1..].as_mut_array().unwrap()).unwrap();
                        buffer.truncate(1 + len);
                        Outgoing::Binary(buffer.into())
                    }
                    _ => continue,
                };
                if !enqueue(&sender, data) {
                    break;
                }
            }
            _ = relay_stats_ticker.tick() => {
                let rtt = stream.estimated_rtt().ok().map(|rtt| (rtt.rtt, rtt.rtt_variance));
                let stats = telemetry.take(clock.now_us(), rtt, Some(video_sender.in_flight()));
                if !send_message(&sender, WebSocketClientboundMessage::Stats(StreamStatsClientboundMessage::RelayStats(stats))) {
                    break;
                }
            }
            incoming = incoming_rx.recv() => {
                match incoming {
                    Some(Incoming::Message(WebSocketServerboundMessage::Stats(StreamStatsServerboundMessage::Ping(id)))) => {
                        if !send_message(&sender, WebSocketClientboundMessage::Stats(StreamStatsClientboundMessage::Pong {
                            id,
                            relay_time_us: clock.now_us() as f64,
                        })) {
                            break;
                        }
                    }
                    Some(Incoming::Binary(bytes)) => {
                        if bytes.first() == Some(&WebSocketChannel::CONTROL)
                            && let Some(packet) = ControlPacket::deserialize(PacketDirection::ServerBound, &control_config, &bytes[1..])
                            && let Err(err) = stream.send_raw(packet)
                        {
                            warn!(error = %err, "failed to send control packet");
                        }
                    }
                    Some(Incoming::Closed) | None => break,
                    _ => {}
                }
            }
            datagram = connection.receive_datagram() => {
                let Ok(datagram) = datagram else {
                    break;
                };
                let payload = datagram.payload();
                if payload.len() == PING_DATAGRAM_LEN && payload[0] == WebSocketChannel::PING {
                    // Echo the ping and append the relay clock so the client can map frame timestamps.
                    let mut pong = [0; PING_DATAGRAM_LEN + 8];
                    pong[..PING_DATAGRAM_LEN].copy_from_slice(&payload[..]);
                    pong[PING_DATAGRAM_LEN..].copy_from_slice(&clock.now_us().to_be_bytes());
                    if connection.send_datagram(pong).is_err() {
                        break;
                    }
                }
            }
            _ = connection.closed() => break,
        }
    }
    let _ = stream.disconnect();
    Ok(())
}

struct SendOutcome {
    dropped_backlog: bool,
    request_idr: bool,
}

/// Sends each video frame on its own unidirectional stream. When too many
/// frames are unacknowledged, the backlog is reset instead of delivered late:
/// frames are dropped until the next keyframe, which the caller requests.
struct VideoSender {
    connection: Connection,
    max_in_flight: usize,
    in_flight: Arc<Semaphore>,
    cancel: watch::Sender<()>,
    last_idr_request: Option<Instant>,
}

impl VideoSender {
    fn new(connection: Connection, max_in_flight: usize) -> Self {
        Self {
            connection,
            max_in_flight,
            in_flight: Arc::new(Semaphore::new(max_in_flight)),
            cancel: watch::Sender::new(()),
            last_idr_request: None,
        }
    }

    fn in_flight(&self) -> u32 {
        (self.max_in_flight - self.in_flight.available_permits()) as u32
    }

    fn send(&mut self, frame: Bytes, idr: bool) -> SendOutcome {
        if idr {
            // Everything still in flight precedes this keyframe and is obsolete.
            self.cancel_in_flight();
            self.last_idr_request = None;
        } else if self.last_idr_request.is_some() {
            return SendOutcome {
                dropped_backlog: false,
                request_idr: self.should_request_idr(),
            };
        }

        let Ok(permit) = self.in_flight.clone().try_acquire_owned() else {
            debug!(
                max_in_flight = self.max_in_flight,
                "dropping web transport video backlog and requesting idr"
            );
            self.cancel_in_flight();
            return SendOutcome {
                dropped_backlog: true,
                request_idr: self.should_request_idr(),
            };
        };

        tokio::spawn(send_video_frame(
            self.connection.clone(),
            frame,
            self.cancel.subscribe(),
            permit,
        ));
        SendOutcome {
            dropped_backlog: false,
            request_idr: false,
        }
    }

    fn cancel_in_flight(&mut self) {
        self.cancel.send_replace(());
        // Cancelled frames release permits on the old semaphore once their
        // streams are reset, so new frames don't wait on them.
        self.in_flight = Arc::new(Semaphore::new(self.max_in_flight));
    }

    fn should_request_idr(&mut self) -> bool {
        let now = Instant::now();
        if self
            .last_idr_request
            .is_some_and(|last| now.duration_since(last) < IDR_REQUEST_RETRY)
        {
            return false;
        }
        self.last_idr_request = Some(now);
        true
    }
}

async fn send_video_frame(
    connection: Connection,
    frame: Bytes,
    mut cancel: watch::Receiver<()>,
    _permit: OwnedSemaphorePermit,
) {
    let opening = select! {
        biased;
        _ = cancel.changed() => return,
        opening = connection.open_uni() => opening,
    };
    let Ok(opening) = opening else {
        return;
    };
    let mut stream = select! {
        biased;
        _ = cancel.changed() => return,
        stream = opening => match stream {
            Ok(stream) => stream,
            Err(_) => return,
        },
    };
    stream.set_priority(VIDEO_STREAM_PRIORITY);

    let cancelled = select! {
        biased;
        _ = cancel.changed() => true,
        _ = async {
            if stream.write_all(&frame).await.is_ok() {
                let _ = stream.finish().await;
            }
        } => false,
    };
    if cancelled {
        let _ = stream.reset(VarInt::from_u32(0));
    }
}

fn enqueue(sender: &Sender<Outgoing>, out: Outgoing) -> bool {
    match sender.try_send(out) {
        Ok(()) => true,
        Err(TrySendError::Full(out)) => match out {
            Outgoing::Uni(_) | Outgoing::Datagram(_) => {
                trace!("dropping web transport media frame: outgoing queue is full");
                true
            }
            Outgoing::Message(_) | Outgoing::Binary(_) => {
                warn!("web transport outgoing queue is full");
                false
            }
        },
        Err(TrySendError::Closed(_)) => false,
    }
}

fn send_message(sender: &Sender<Outgoing>, message: WebSocketClientboundMessage) -> bool {
    trace!(message = ?message, "sending web transport message");
    if let Ok(text) = serde_json::to_string(&message) {
        enqueue(sender, Outgoing::Message(text))
    } else {
        warn!("failed to serialize web transport message");
        false
    }
}
