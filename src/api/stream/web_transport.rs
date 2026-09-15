//! Experimental WebTransport transport.
//!
//! The message stream uses `u32` big-endian length-prefixed frames with a
//! one-byte kind: `0` contains UTF-8 JSON and `1` contains the existing
//! WebSocket binary layout. Control packets use kind-1 frames for reliable
//! ordered delivery.
//! Video packets use unidirectional streams, while audio packets use
//! datagrams when they fit and unidirectional streams otherwise.

use std::{sync::Arc, time::Duration};

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
use rustls::{ServerConfig, pki_types::pem::PemObject};
use sha2::{Digest, Sha256};
use tokio::{
    select,
    sync::{
        Semaphore,
        mpsc::{self, Sender, error::TrySendError},
    },
    time::{interval, timeout},
};
use tracing::{Instrument, debug_span, error, info, instrument, trace, warn};
use wtransport::{Connection, Endpoint};

use crate::{
    api::{
        bindings::{
            StreamStatsClientboundMessage, StreamStatsServerboundMessage, WebSocketChannel,
            WebSocketClientboundMessage, WebSocketServerboundMessage, WebTransportConfigResponse,
        },
        stream::{create_control_packet_config, start_moonlight_stream},
    },
    app::{App, AppError},
    config::WebTransportConfig,
};

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
) -> Result<Json<WebTransportConfigResponse>, AppError> {
    let Some(config) = app.config().web_server.web_transport.as_ref() else {
        return Err(AppError::WebTransportDisabled);
    };
    if app.config().web_server.certificate.is_none() {
        return Err(AppError::WebTransportDisabled);
    }
    let prefix = &app.config().web_server.url_path_prefix;
    let path = format!("{prefix}/api/host/stream/web_transport");
    let certificate_hash = if config.advertise_certificate_hash {
        app.config()
            .web_server
            .certificate
            .as_ref()
            .and_then(|certificate| certificate_hash_from_pem(&certificate.certificate_pem))
    } else {
        None
    };
    Ok(Json(WebTransportConfigResponse {
        url: config.public_url.as_ref().map(|url| format!("{url}{path}")),
        port: config.bind_address.port(),
        certificate_hash,
    }))
}

fn certificate_hash_from_pem(path: &str) -> Option<String> {
    let mut certs = rustls::pki_types::CertificateDer::pem_file_iter(path).ok()?;
    let cert = certs.next()?.ok()?;
    Some(hex::encode(Sha256::digest(cert.as_ref())))
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
                        Ok(session_request) if session_request.path() == path => {
                            match session_request.accept().await {
                                Ok(connection) => {
                                    if let Err(err) = handle_session(app, connection).await {
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

async fn handle_session(app: Data<App>, connection: Connection) -> Result<(), AppError> {
    let (mut message_send, mut message_recv) =
        timeout(Duration::from_secs(10), connection.accept_bi())
            .await
            .map_err(|_| AppError::StreamClosed)?
            .map_err(|_| AppError::StreamClosed)?;
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
    let (stream, response) = start_moonlight_stream(&app, &request).await?;

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
                        if let Err(err) = stream.send_raw(ControlPacket::RequestIdr) {
                            warn!(error = %err, "failed to request idr");
                        }
                        continue;
                    }
                    MoonlightStreamEvent::Video(VideoStreamEvent::OnFrame(frame)) => {
                        let mut buffer = vec![0; 1 + 5 + frame.raw().len()];
                        buffer[0] = WebSocketChannel::VIDEO;
                        buffer[1] = if frame.metadata().frame_type.serialize() == 2 { 1 } else { 0 };
                        buffer[2..6].copy_from_slice(&(frame.metadata().timestamp.as_micros() as u32).to_be_bytes());
                        buffer[6..].copy_from_slice(frame.raw());
                        Outgoing::Uni(buffer.into())
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
                if let Ok(rtt) = stream.estimated_rtt()
                    && !send_message(&sender, WebSocketClientboundMessage::Stats(StreamStatsClientboundMessage::RelayRtt {
                        rtt_ms: rtt.rtt.as_millis() as u32,
                        rtt_variance_ms: rtt.rtt_variance.as_millis() as u32,
                    })) {
                    break;
                }
            }
            incoming = incoming_rx.recv() => {
                match incoming {
                    Some(Incoming::Message(WebSocketServerboundMessage::Stats(StreamStatsServerboundMessage::Ping(id)))) => {
                        if !send_message(&sender, WebSocketClientboundMessage::Stats(StreamStatsClientboundMessage::Pong(id))) {
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
            _ = connection.closed() => break,
        }
    }
    let _ = stream.disconnect();
    Ok(())
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
