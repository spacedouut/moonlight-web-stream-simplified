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
    time::{interval, sleep},
};
use tracing::{Instrument, debug, debug_span, error, info, instrument, trace, warn};

use crate::{
    api::stream::{create_control_packet_config, start_moonlight_stream},
    app::{App, AppError},
};

enum WsData {
    Bytes(Bytes),
    Text(String),
}

#[get("/host/stream/web_socket")]
#[instrument(skip(app, req, body_stream))]
pub async fn web_socket_stream(
    app: actix_web::web::Data<App>,
    req: HttpRequest,
    body_stream: Payload,
) -> Result<HttpResponse, Error> {
    // upgrade connection to web socket connection
    let (res, ws_sender, ws_receiver) = actix_ws::handle(&req, body_stream)?;

    spawn(
        async move {
            match handle_ws(app, ws_sender, ws_receiver).await {
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

    let (stream, stream_response) = start_moonlight_stream(&app, &stream_request).await?;
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

    let mut ws_stopped = false;

    loop {
        if !stream.is_alive() {
            break;
        }

        select! {
            // drive the moonlight stream forward
            result = stream.drive() => {
                let event = result?;

                match event {
                    MoonlightStreamEvent::Audio(AudioStreamEvent::OnFrame(frame)) => {
                        let mut buffer = vec![0; 1 + frame.buffer.len()];
                        buffer[1..].copy_from_slice(&frame.buffer);

                        buffer[0] = WebSocketChannel::AUDIO;

                        let _ = ws_sender.send(WsData::Bytes(buffer.into()));
                    }
                    MoonlightStreamEvent::Video(VideoStreamEvent::SignalIdr) => {
                        if let Err(err)=  stream.send_raw(ControlPacket::RequestIdr) {
                            warn!(error = %err, "failed to request idr after the moonlight video stream requested an idr");
                        }
                    }
                    MoonlightStreamEvent::Video(VideoStreamEvent::OnFrame(frame)) => {
                        // TODO: avoid using payloading and depayloading the frame like this
                        let mut buffer = vec![0; 1 + 5 + frame.raw().len()];
                        buffer[(1 + 5)..].copy_from_slice(frame.raw());

                        buffer[0] = WebSocketChannel::VIDEO;
                        // TODO: make frame type from video packet public, 2==Idr
                        buffer[1] = if frame.metadata().frame_type.serialize() == 2 {
                            1
                        } else {
                            0
                        };
                        buffer[2..6].copy_from_slice(
                            &(frame.metadata().timestamp.as_micros() as u32).to_be_bytes(),
                        );

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
            // relay stats
            _ = relay_stats_ticker.tick() => {
                let rtt = match stream.estimated_rtt() {
                    Ok(value) => value,
                    Err(err) => {
                        warn!(error = %err, "failed to send rtt to client");
                        break;
                    }
                };

                send_ws_message(
                    &mut ws_sender,
                    WebSocketClientboundMessage::Stats(StreamStatsClientboundMessage::RelayRtt {
                        rtt_ms: rtt.rtt.as_millis() as u32,
                        rtt_variance_ms: rtt.rtt_variance.as_millis() as u32,
                    })
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
                            send_ws_message(&mut ws_sender, WebSocketClientboundMessage::Stats(StreamStatsClientboundMessage::Pong(id)));
                        }
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
