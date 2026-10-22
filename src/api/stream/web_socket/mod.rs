use std::{pin::pin, time::Duration};

use crate::api::bindings::{
    StreamStatsClientboundMessage, StreamStatsServerboundMessage, WebSocketChannel,
    WebSocketClientboundMessage, WebSocketServerboundMessage,
};
use actix_web::{Error, HttpRequest, HttpResponse, get, rt::spawn, web::Payload};
use actix_ws::{Message, MessageStream, Session};
use bytes::Bytes;
use moonlight_common::stream::{
    proto::{
        audio::AudioStreamEvent,
        control::{
            ControlStreamEvent,
            packet::{ControlPacket, ControlPacketConfig, PacketDirection},
        },
        video::VideoStreamEvent,
    },
    tokio::{MoonlightStream, MoonlightStreamEvent},
};
use tokio::{
    select,
    sync::mpsc::{UnboundedSender, unbounded_channel},
    time::{Instant, interval, sleep},
};
use tracing::{Instrument, debug, debug_span, error, info, instrument, trace, warn};

use crate::{
    api::stream::{
        create_control_packet_config, start_moonlight_stream,
        telemetry::{RelayClock, RelayTelemetry, VIDEO_HEADER_LEN, write_video_header},
        web_socket::delay_controller::{DelayController, DelayState},
    },
    app::{App, AppError, auth::AuthUser},
};

/// Queue delay below which the client has caught up and video is sent again.
const RESUME_DELAY: Duration = Duration::from_millis(50);
/// Minimum time between idr requests while video is dropped.
const IDR_REQUEST_INTERVAL: Duration = Duration::from_millis(250);
/// If the delay is still high after dropping video for this long, nothing is queued anymore
/// and the latency of the link itself has changed.
const REBASE_AFTER: Duration = Duration::from_millis(500);
/// After resuming, the idr itself briefly raises the delay, so only severe delay drops video again.
const RESUME_GRACE: Duration = Duration::from_millis(300);

mod delay_controller;

enum WsData {
    Ping(Bytes),
    Bytes(Bytes),
    Text(String),
}

#[get("/host/stream/web_socket")]
#[instrument(skip(app, req, body_stream))]
pub async fn web_socket_stream(
    app: actix_web::web::Data<App>,
    user: AuthUser,
    req: HttpRequest,
    body_stream: Payload,
) -> Result<HttpResponse, Error> {
    // upgrade connection to web socket connection
    let (res, ws_sender, ws_receiver) = actix_ws::handle(&req, body_stream)?;

    spawn(
        async move {
            match handle_ws(app, user, ws_sender, ws_receiver).await {
                Ok(_) => {}
                Err(err) => {
                    error!(error = %err, "stream failed");
                }
            }
        }
        .instrument(debug_span!("ws handler")),
    );

    Ok(res)
}

async fn handle_ws(
    app: actix_web::web::Data<App>,
    user: AuthUser,
    mut ws_sender: Session,
    mut ws_receiver: MessageStream,
) -> Result<(), AppError> {
    let control_config = create_control_packet_config();

    // Wait for stream request
    let stream_request = select! {
        _ = sleep(Duration::from_secs(10)) => {
            return Err(AppError::StreamClosed);
        }
        request = ws_receiver.recv() => request
    };

    // Deserialize message
    let stream_request = match stream_request.expect("stream request") {
        Ok(Message::Text(text)) => text,
        Ok(message) => {
            error!(message = ?message, "web socket received unexpected start message");
            return Err(AppError::StreamClosed);
        }
        Err(err) => {
            error!(error = %err, "web socket protocol error");
            return Err(AppError::StreamClosed);
        }
    };
    let stream_request = match serde_json::from_str::<WebSocketServerboundMessage>(&stream_request)
    {
        Ok(WebSocketServerboundMessage::Request(request)) => request,
        Ok(message) => {
            error!(message = ?message, "expected web socket stream request but got another message");
            return Err(AppError::StreamClosed);
        }
        Err(err) => {
            error!(error = %err, "failed to deserialize json");
            return Err(AppError::StreamClosed);
        }
    };

    let (stream, stream_response) = match start_moonlight_stream(&app, &user, &stream_request).await
    {
        Ok(value) => value,
        Err(err) => {
            // Tell the client why the stream couldn't be started before
            // closing: a silent close would leave it with no reason at all.
            let body = WebSocketClientboundMessage::Error(err.api_error_body());
            if let Ok(text) = serde_json::to_string(&body) {
                let _ = ws_sender.text(text).await;
            }
            let reason = actix_ws::CloseReason {
                code: actix_ws::CloseCode::Error,
                description: Some(err.describe().code.to_string()),
            };
            let _ = ws_sender.close(Some(reason)).await;
            return Err(err);
        }
    };
    let response = WebSocketClientboundMessage::Response(stream_response);
    info!(response = ?response, "sending response to client");

    let (mut ws_channel_sender, mut ws_channel_receiver) = unbounded_channel();
    spawn(
        async move {
            while let Some(data) = ws_channel_receiver.recv().await {
                match data {
                    WsData::Bytes(bytes) => {
                        if ws_sender.binary(bytes).await.is_err() {
                            break;
                        }
                    }
                    WsData::Text(text) => {
                        if ws_sender.text(text).await.is_err() {
                            break;
                        }
                    }
                    WsData::Ping(bytes) => {
                        if ws_sender.ping(&bytes).await.is_err() {
                            break;
                        }
                    }
                }
            }

            debug!("stopped web socket sending task");
        }
        .instrument(debug_span!("ws_sender")),
    );

    // send response
    send_ws_message(&mut ws_channel_sender, response);

    // main loop
    if let Err(err) = ws_loop(ws_channel_sender, ws_receiver, stream, control_config).await {
        error!(error = %err, "web socket main loop errored, closing stream");
    }

    Ok(())
}

async fn ws_loop(
    mut ws_sender: UnboundedSender<WsData>,
    mut ws_receiver: MessageStream,
    mut stream: MoonlightStream,
    control_config: ControlPacketConfig,
) -> Result<(), AppError> {
    let mut relay_stats_ticker = pin!(interval(Duration::from_secs(1)));
    let clock = RelayClock::new();
    let mut telemetry = RelayTelemetry::new();

    let mut ping_ticker = pin!(interval(Duration::from_millis(100)));
    let mut delay_controller = DelayController::default();

    let mut dropping_video_since: Option<Instant> = None;
    let mut last_idr_request = Instant::now();
    let mut resumed_at = Instant::now();
    let mut ws_stopped = false;

    loop {
        if !stream.is_alive() {
            break;
        }

        delay_controller.update(Instant::now());

        select! {
            // drive the moonlight stream forward
            result = stream.drive() => {
                let event = result?;

                match event {
                    MoonlightStreamEvent::Audio(AudioStreamEvent::OnFrame(frame)) => {
                        if ws_stopped {
                            continue;
                        }

                        if matches!(delay_controller.delay_state(Instant::now()), DelayState::Severe) {
                            trace!("dropping audio packet because of congestion");
                            continue;
                        }

                        let mut buffer = vec![0; 1 + frame.buffer.len()];
                        buffer[1..].copy_from_slice(&frame.buffer);

                        buffer[0] = WebSocketChannel::AUDIO;

                        let _ = ws_sender.send(WsData::Bytes(buffer.into()));
                    }
                    MoonlightStreamEvent::Video(VideoStreamEvent::SignalIdr) => {
                        telemetry.on_host_idr_request();
                        if let Err(err)=  stream.send_raw(ControlPacket::RequestIdr) {
                            warn!(error = %err, "failed to request idr after the moonlight video stream requested an idr");
                        }
                    }
                    MoonlightStreamEvent::Video(VideoStreamEvent::OnFrame(frame)) => {
                        if ws_stopped {
                            continue;
                        }

                        // TODO: make frame type from video packet public, 2==Idr
                        let is_idr = frame.metadata().frame_type.serialize() == 2;

                        let now = Instant::now();

                        if let Some(since) = dropping_video_since {
                            // A stalled client is behind, not on a slower link, so only answered pings may move the base.
                            if now.duration_since(since) >= REBASE_AFTER
                                && delay_controller.queue_delay(now) > RESUME_DELAY
                                && !delay_controller.is_stalled(now)
                            {
                                debug!(queue_delay = ?delay_controller.queue_delay(now), "delay stays high without video, taking it as the new base latency");
                                delay_controller.rebase();
                            }

                            let caught_up = delay_controller.queue_delay(now) <= RESUME_DELAY;

                            if is_idr && caught_up {
                                info!(dropped_for = ?now.duration_since(since), "web socket client caught up, resuming video");

                                dropping_video_since = None;
                                resumed_at = now;
                            } else {
                                // Only request an idr once the queue has drained, an earlier one would be dropped too.
                                if caught_up && now.duration_since(last_idr_request) >= IDR_REQUEST_INTERVAL {
                                    last_idr_request = now;

                                    if let Err(err) = stream.send_raw(ControlPacket::RequestIdr) {
                                        warn!(error = %err, "failed to request idr");
                                    }
                                }
                                continue;
                            }
                        } else if match delay_controller.delay_state(now) {
                            DelayState::Normal => false,
                            DelayState::Congested => now.duration_since(resumed_at) >= RESUME_GRACE,
                            DelayState::Severe => true,
                        } {
                            warn!(queue_delay = ?delay_controller.queue_delay(now), "web socket client is behind, dropping video until it caught up");

                            dropping_video_since = Some(now);
                            continue;
                        }

                        // TODO: avoid using payloading and depayloading the frame like this
                        let raw = frame.raw();
                        let metadata = frame.metadata();
                        telemetry.on_frame(&metadata, raw.len());

                        let mut buffer = vec![0; 1 + VIDEO_HEADER_LEN + raw.len()];
                        buffer[0] = WebSocketChannel::VIDEO;
                        write_video_header(&mut buffer[1..], &metadata, clock.now_us());
                        buffer[(1 + VIDEO_HEADER_LEN)..].copy_from_slice(raw);

                        let _ = ws_sender.send(WsData::Bytes(buffer.into()));
                    }
                    MoonlightStreamEvent::Control(ControlStreamEvent::Packet(packet)) => {
                        let mut buffer = vec![0; ControlPacket::MAX_SIZE + 1];

                        buffer[0] = WebSocketChannel::CONTROL;

                        #[allow(clippy::unwrap_used)]
                        let packet_len = packet
                            .serialize(&control_config, buffer[1..].as_mut_array().unwrap())
                            .unwrap();

                        buffer.truncate(1 + packet_len);
                        let _ = ws_sender.send(WsData::Bytes(buffer.into()));
                    }
                    _ => {}
                }
            }
            // delay controller
            _ = ping_ticker.tick(), if !ws_stopped => {
                let bytes = delay_controller.on_ping_send(Instant::now());

                if ws_sender.send(WsData::Ping(bytes)).is_err() {
                    warn!("failed to send ping");
                }
            }
            // relay stats
            _ = relay_stats_ticker.tick() => {
                let rtt = match stream.estimated_rtt() {
                    Ok(value) => value,
                    Err(err) => {
                        warn!(error = %err, "failed to send rtt to client");
                        break;
                    }
                };

                let stats = telemetry.take(clock.now_us(), Some((rtt.rtt, rtt.rtt_variance)), None);
                send_ws_message(
                    &mut ws_sender,
                    WebSocketClientboundMessage::Stats(StreamStatsClientboundMessage::RelayStats(stats)),
                );
            }
            // Handle incoming ws requests
            result = ws_receiver.recv(), if !ws_stopped => {
                let Some(Ok(message)) = result else {
                    ws_stopped = true;
                    let _ = stream.disconnect();
                    continue;
                };

                match message {
                    Message::Binary(message) => {
                        if message.is_empty() {
                            continue;
                        }

                        if message[0] == WebSocketChannel::CONTROL {
                            let Some(packet) = ControlPacket::deserialize(
                                PacketDirection::ServerBound,
                                &control_config,
                                &message[1..],
                            ) else {
                                warn!(message = ?message, "received unknown control packet");
                                continue;
                            };

                            if let Err(err) = stream.send_raw(packet) {
                                warn!(error = %err, "failed to send control packet");
                            }
                        }
                    }
                    Message::Text(text) => {
                        let message = match serde_json::from_str::<WebSocketServerboundMessage>(&text) {
                            Ok(value) => value,
                            Err(err) => {
                                warn!(error = %err, "failed to deserialize serverbound web socket message");
                                continue;
                            }
                        };

                        if let WebSocketServerboundMessage::Stats(StreamStatsServerboundMessage::Ping(id)) =
                            message
                        {
                            send_ws_message(&mut ws_sender, WebSocketClientboundMessage::Stats(StreamStatsClientboundMessage::Pong {
                                id,
                                relay_time_us: clock.now_us() as f64,
                            }));
                        }
                    }
                    Message::Pong(bytes) => {
                        delay_controller.on_pong_receive(&bytes, Instant::now());
                    }
                    Message::Close(_) => {
                        // Stop the host stream but keep driving it so the host gets a graceful
                        // disconnect, and stop relaying video and audio to the closed connection.
                        info!("client closed the web socket, stopping the host stream");
                        ws_stopped = true;
                        let _ = stream.disconnect();
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}

fn send_ws_message(
    sender: &mut UnboundedSender<WsData>,
    message: WebSocketClientboundMessage,
) -> bool {
    trace!(message = ?message, "sending text message to client");

    let text = match serde_json::to_string(&message) {
        Ok(value) => value,
        Err(err) => {
            warn!(error = %err, "failed to send web socket message");
            return false;
        }
    };

    if let Err(err) = sender.send(WsData::Text(text)) {
        warn!(error = %err, "failed to send web socket message");
        return false;
    }

    true
}
