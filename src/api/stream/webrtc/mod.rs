use crate::api::stream::webrtc::audio::AudioChannel;
use crate::api::stream::webrtc::control::ControlChannel;
use crate::api::stream::webrtc::ext_color_space::COLOR_SPACE_URI;
use crate::api::stream::webrtc::stream::webrtc_loop;
use crate::api::stream::webrtc::video::VideoChannel;
use crate::config::{PortRange, WebRtcConfig};
use actix_web::HttpRequest;
use actix_web::body::BoxBody;
use actix_web::web::{Data, Path};
use actix_web::{
    HttpResponse, HttpResponseBuilder, delete, get, http::StatusCode, http::header, options, patch,
    post,
};
use async_trait::async_trait;
use moonlight_common::AppId;
use moonlight_common::crypto::rustcrypto::RustCryptoBackend;
use moonlight_common::stream::audio::AudioConfig;
use moonlight_common::stream::control::ActiveGamepads;
use moonlight_common::stream::proto::MoonlightStreamSetup;
use moonlight_common::stream::tokio::MoonlightStream;
use moonlight_common::stream::video::{
    ColorRange, ColorSpace, VideoCapabilities, VideoFormat, VideoFormats,
};
use moonlight_common::stream::{
    AesIv, AesKey, EncryptionFlags, MoonlightStreamSettings, StreamingConfig,
};
use moonlight_common::webrtc::WebRTCParseError;
use moonlight_common::webrtc::answer::WebRTCSessionAnswer;
use moonlight_common::webrtc::header::WebRTCLinkHeader;
use moonlight_common::webrtc::offer::WebRTCSessionOffer;
use moonlight_common::webrtc::sdp::Session;
use rtc::ice::network_type::NetworkType;
use rtc::interceptor::Registry;
use rtc::peer_connection::configuration::media_engine::MIME_TYPE_OPUS;
use rtc::peer_connection::transport::RTCDtlsRole;
use rtc::rtp_transceiver::rtp_sender::{
    RTCPFeedback, RTCRtpCodec, RTCRtpCodecParameters, RTCRtpHeaderExtensionCapability, RtpCodecKind,
};
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::Notify;
use tokio::sync::mpsc::{self};
use tokio::time::sleep;
use tokio::{select, spawn};
use tracing::{Instrument, debug, debug_span, error, info, instrument, warn};
use webrtc::data_channel::DataChannel;
use webrtc::peer_connection::{
    MediaEngine, PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler,
    RTCConfigurationBuilder, RTCIceCandidateInit, RTCIceGatheringState, RTCIceServer,
    RTCPeerConnectionState, RTCSessionDescription, SettingEngineBuilder,
    register_default_interceptors,
};

use crate::api::stream::webrtc::convert::into_webrtc_ice_candidate;
use crate::api::stream::webrtc::ice_servers::generate_ice_servers;
use crate::app::App;
use crate::app::AppError;
use crate::app::host::HostId;
use crate::app::stream::{ExternalStreamEvent, Stream, StreamId};

mod audio;
mod control;
mod convert;
mod ext_color_space;
mod ice_servers;
mod stream;
mod video;

#[options("")]
pub async fn webrtc_options(app: Data<App>) -> Result<HttpResponse, AppError> {
    let mut response = HttpResponseBuilder::new(StatusCode::OK);
    response.append_header(("Accept-Post", "application/sdp"));

    // Add ice servers
    let ice_servers = generate_ice_servers(&app).await?;
    for ice_server in ice_servers {
        let username = if ice_server.username.is_empty() {
            None
        } else {
            Some(ice_server.username)
        };
        let credential = if ice_server.credential.is_empty() {
            None
        } else {
            Some(ice_server.credential)
        };

        for url in ice_server.urls {
            let header_value = WebRTCLinkHeader::IceServer {
                url,
                username: username.clone(),
                credential: credential.clone(),
            };

            response.append_header((header::LINK, header_value.to_string()));
        }
    }

    Ok(response.finish())
}

#[get("")]
pub async fn webrtc_get() -> Result<HttpResponse, AppError> {
    Ok(HttpResponse::MethodNotAllowed().finish())
}

fn opus_codec() -> RTCRtpCodec {
    RTCRtpCodec {
        mime_type: MIME_TYPE_OPUS.to_owned(),
        clock_rate: 48000,
        channels: 2,
        sdp_fmtp_line: "minptime=10;useinbandfec=1".to_owned(),
        rtcp_feedback: vec![RTCPFeedback {
            // negative acknowledgement
            typ: "nack".to_string(),
            parameter: "".to_string(),
        }],
    }
}

fn create_media_engine(video_formats: &HashMap<VideoFormat, RTCRtpCodecParameters>) -> MediaEngine {
    // The media engine contains all supported codecs this peer has
    let mut media_engine = MediaEngine::default();

    // register audio
    media_engine
        .register_codec(
            RTCRtpCodecParameters {
                rtp_codec: opus_codec(),
                payload_type: 111,
            },
            RtpCodecKind::Audio,
        )
        .expect("register audio opus codec");

    // register video
    for codec in video_formats.values() {
        media_engine
            .register_codec(codec.clone(), RtpCodecKind::Video)
            .expect("register video codec");
    }

    // register extensions
    const PLAYOUT_DELAY_URI: &str = "http://www.webrtc.org/experiments/rtp-hdrext/playout-delay";

    media_engine
        .register_header_extension(
            RTCRtpHeaderExtensionCapability {
                uri: PLAYOUT_DELAY_URI.to_string(),
            },
            RtpCodecKind::Video,
            None,
        )
        .expect("register playout delay extension");
    media_engine
        .register_header_extension(
            RTCRtpHeaderExtensionCapability {
                uri: COLOR_SPACE_URI.to_string(),
            },
            RtpCodecKind::Video,
            None,
        )
        .expect("register color space extension");
    media_engine
        .register_header_extension(
            RTCRtpHeaderExtensionCapability {
                uri: PLAYOUT_DELAY_URI.to_string(),
            },
            RtpCodecKind::Audio,
            None,
        )
        .expect("register playout delay extension");

    media_engine
}

fn create_setting_engine(config: &WebRtcConfig) -> SettingEngineBuilder {
    let mut setting_engine = SettingEngineBuilder::new();
    if let Some(mapping) = config.nat_1to1.as_ref() {
        setting_engine = setting_engine.with_nat_1to1_ips(
            mapping.ips.clone(),
            into_webrtc_ice_candidate(mapping.ice_candidate_type),
        );
    }

    setting_engine
        .with_include_loopback_candidate(config.include_loopback_candidates)
        .with_ice_timeouts(
            Some(Duration::from_secs(5)),
            Some(Duration::from_secs(15)),
            Some(Duration::from_secs(2)),
        )
        .with_network_types(vec![NetworkType::Udp4])
        // Answer with a=setup:passive so the browser sends the DTLS ClientHello.
        .with_answering_dtls_role(RTCDtlsRole::Server)
}

struct WebRtcHandler {
    on_ice_gathering_finished: Notify,
    on_data_channel_sender: mpsc::UnboundedSender<Arc<dyn DataChannel>>,
    peer_state: Mutex<RTCPeerConnectionState>,
}

#[async_trait]
impl PeerConnectionEventHandler for WebRtcHandler {
    async fn on_connection_state_change(&self, state: RTCPeerConnectionState) {
        *self.peer_state.lock().expect("lock peer state") = state;

        info!(state = %state, "webrtc peer state changed");
    }

    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        info!(state = %state, "ice gathering state changed");

        if matches!(state, RTCIceGatheringState::Complete) {
            self.on_ice_gathering_finished.notify_one();
        }
    }

    async fn on_data_channel(&self, data_channel: Arc<dyn DataChannel>) {
        let _ = self.on_data_channel_sender.send(data_channel);
    }
}

#[post("")]
#[instrument(skip(app, req, session_description))]
pub async fn webrtc_post(
    app: Data<App>,
    req: HttpRequest,
    session_description: String,
) -> Result<HttpResponse, AppError> {
    debug!(req = ?req, session_description = ?session_description, "webrtc request");

    let offer_sdp =
        Session::parse(session_description.as_bytes()).map_err(WebRTCParseError::from)?;
    let session = WebRTCSessionOffer::from_sdp(&offer_sdp)?;
    debug!(moonlight_session = ?session, "moonlight session extensions", );

    let Some(host_id) = session.host_id else {
        return Err(AppError::HostNotFound);
    };
    let host_id = HostId(host_id);

    // Get host
    let mut host = app.host(host_id).await?;
    let host = host.use_host().await?;

    if !host.is_paired().await? {
        return Err(AppError::HostNotPaired);
    }

    // Get app
    let app_id = AppId(session.app_id);

    // Create offer based on the sdp
    let offer = RTCSessionDescription::offer(session_description)?;

    // -- Create WebRtc peer
    let setting_engine = create_setting_engine(&app.config().webrtc);

    // Create video
    let mut video_channel = VideoChannel::new(
        &offer_sdp,
        session.preferred_codecs.unwrap_or(VideoFormats::all()),
    )?;

    info!("querying client for supported video and audio codecs");

    // Video Formats
    let supported_video_formats = video_channel
        .supported_video_formats()
        .keys()
        .fold(VideoFormats::empty(), |formats, format| {
            formats | format.into_formats()
        });

    // TODO: query microphone support
    let microphone_enabled = false;

    // TODO: query audio support
    let audio_config = Some(AudioConfig::STEREO);

    debug!(
        supported_video_formats = %supported_video_formats,
        audio_config = ?audio_config,
        "found codecs"
    );

    // Cancel connection if no audio or video format was detected as supported
    let Some(_audio_config) = audio_config else {
        error!("failed to start stream because no audio codec is supported by the client");
        return Err(AppError::WebRtcClientCodecNotSupported);
    };

    if supported_video_formats.is_empty() {
        error!("failed to start stream because no video codec is supported by the client");
        return Err(AppError::WebRtcClientCodecNotSupported);
    }

    let mut settings = MoonlightStreamSettings {
        width: session.width,
        height: session.height,
        fps: session.fps,
        fps_x100: session.fps * 100,
        bitrate: session.bitrate,
        packet_size: 2048,
        // There's not need to encrypt video
        encryption_flags: EncryptionFlags::AUDIO | EncryptionFlags::FOUNDATION_MICROPHONE,
        streaming_remotely: StreamingConfig::Auto,
        sops: true,
        hdr: session.hdr,
        supported_video_formats,
        // TODO: what color space / range? is this in the sdp?
        color_space: ColorSpace::Rec709,
        color_range: ColorRange::Limited,
        local_audio_play_mode: session.local_audio_play_mode,
        // TODO: what audio config?
        audio_config: AudioConfig::STEREO,
        gamepads_attached: ActiveGamepads::empty(),
        gamepads_persist_after_disconnect: false,
        enable_mic: microphone_enabled,
    };

    // Adjust settings
    let server_version = host.version().await?;
    let gfe_version = host.gfe_version().await?;
    let server_codec_mode_support = host.server_codec_mode_support().await?;
    settings.adjust_for_server(server_version, &gfe_version, server_codec_mode_support)?;

    // Generate key and iv
    let aes_key = AesKey::new_random(&RustCryptoBackend)?;
    let aes_iv = AesIv::new_random(&RustCryptoBackend)?;

    // Create media engine
    let mut media_engine = create_media_engine(video_channel.supported_video_formats());

    // Interceptor Registry
    let interceptor_registry = register_default_interceptors(Registry::new(), &mut media_engine)
        .expect("register default interceptors");

    let peer_app = app.clone();
    let peer_offer = offer.clone();
    let peer_future = async move {
        let ice_servers = generate_ice_servers(&peer_app).await?;

        // Find available port
        let port = if let Some(PortRange { min, max }) = peer_app.config().webrtc.port_range {
            let mut valid_port = None;

            // Try to bind a udp socket to see if the port is available
            for port in min..=max {
                let addr = SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port);

                if UdpSocket::bind(addr).await.is_ok() {
                    valid_port = Some(port);
                    break;
                }
            }

            match valid_port {
                Some(port) => port,
                None => {
                    error!(port_min = %min, port_max = %max, "No available udp port found in given port range. Cannot create webrtc peer!");
                    return Err(AppError::WebRTC(
                        webrtc::error::Error::ErrAddressAlreadyInUse,
                    ));
                }
            }
        } else {
            0
        };
        let local_addrs = vec![SocketAddr::new(Ipv4Addr::new(0, 0, 0, 0).into(), port)];

        // Initialize senders and receivers for events
        let (on_data_channel_sender, on_data_channel) =
            mpsc::unbounded_channel::<Arc<dyn DataChannel>>();

        let handler = Arc::new(WebRtcHandler {
            peer_state: Mutex::new(RTCPeerConnectionState::New),
            on_ice_gathering_finished: Notify::new(),
            on_data_channel_sender,
        });

        // Create new peer
        let peer = PeerConnectionBuilder::default()
            .with_media_engine(media_engine)
            .with_interceptor_registry(interceptor_registry)
            .with_setting_engine(setting_engine.build())
            .with_udp_addrs(local_addrs)
            .with_handler(handler.clone())
            .with_configuration(
                RTCConfigurationBuilder::default()
                    .with_ice_servers(
                        ice_servers
                            .iter()
                            .map(|x| RTCIceServer {
                                username: x.username.clone(),
                                credential: x.credential.clone(),
                                urls: x.urls.clone(),
                            })
                            .collect(),
                    )
                    .build(),
            )
            .build()
            .await?;
        let peer = Arc::new(peer) as Arc<dyn PeerConnection>;

        info!("created server webrtc peer");

        // Set remote description
        if let Err(err) = peer.set_remote_description(peer_offer.clone()).await {
            error!(error = %err, description = %peer_offer, "failed to set remote description");

            peer.close().await?;
            return Err(err.into());
        }

        Ok::<_, AppError>((peer, handler, on_data_channel))
    };

    let launch_host = host.clone();
    let launch_future = async move {
        info!(settings = ?settings, "starting stream");

        let apps = launch_host.app_list().await?;
        let app_title = apps
            .into_iter()
            .find(|app| app.id == app_id)
            .map(|app| app.title);

        let config = launch_host
            .start_stream(
                app_id,
                &settings,
                aes_key,
                aes_iv,
                // TODO: replace with normal `MoonlightStream::launch`
                MoonlightStreamSetup::launch_query_parameters(),
            )
            .await?;

        let moonlight_stream = match MoonlightStream::connect(
            config,
            settings,
            Arc::new(RustCryptoBackend),
            VideoCapabilities::default(),
        )
        .await
        {
            Ok(value) => value,
            Err(err) => {
                error!(error = %err, "failed to start stream");
                return Err(err.into());
            }
        };

        Ok::<_, AppError>((app_title, moonlight_stream))
    };

    let (peer_result, stream_result) = tokio::join!(peer_future, launch_future);
    let (peer, handler, on_data_channel, app_title, mut moonlight_stream) =
        match (peer_result, stream_result) {
            (Ok((peer, handler, on_data_channel)), Ok((app_title, moonlight_stream))) => {
                (peer, handler, on_data_channel, app_title, moonlight_stream)
            }
            (Err(peer_err), Ok((_, mut moonlight_stream))) => {
                let _ = moonlight_stream.disconnect();
                return Err(peer_err);
            }
            (Ok((peer, _, _)), Err(stream_err)) => {
                peer.close().await?;
                return Err(stream_err);
            }
            (Err(peer_err), Err(_)) => return Err(peer_err),
        };

    // Add audio and video track forwarding
    let audio_channel = match AudioChannel::new_track(&moonlight_stream, &*peer).await {
        Ok(value) => value,
        Err(err) => {
            error!(error = %err, "failed to add audio track to webrtc peer");

            let _ = moonlight_stream.disconnect();
            peer.close().await?;
            return Err(err);
        }
    };
    if let Err(err) = video_channel
        .on_video_format_selected(moonlight_stream.video_setup(), &*peer)
        .await
    {
        error!(error = %err, "failed to add video track to webrtc peer");

        let _ = moonlight_stream.disconnect();
        peer.close().await?;
        return Err(err);
    }

    info!("started moonlight stream");

    // -- Create control channel
    let result = ControlChannel::new(&*peer).await;
    let control_channel = match result {
        Err(err) => {
            error!("failed to add control stream to webrtc peer");

            let _ = moonlight_stream.disconnect();
            peer.close().await?;
            return Err(err);
        }
        Ok(value) => value,
    };

    info!("configured server webrtc peer, waiting for ice gathering to complete");

    // The selected video payload type, captured before the channel moves into
    // the webrtc loop, used to correct the H264 fmtp in the answer below.
    let video_answer_payload_type = video_channel.h264_answer_payload_type();

    // Complete negotiation
    let answer = peer.create_answer(None).await?;

    if let Err(err) = peer.set_local_description(answer.clone()).await {
        error!(error = %err, description = %answer, "failed to set local description");

        peer.close().await?;
        return Err(err.into());
    }

    // Keep the Moonlight transport alive while STUN/ICE gathering runs. Waiting here can
    // take several seconds when a configured STUN server is unreachable.
    spawn({
        let peer = peer.clone();
        let handler = handler.clone();

        async move {
            if let Err(err) = webrtc_loop(
                moonlight_stream,
                &*peer,
                audio_channel,
                video_channel,
                control_channel,
                on_data_channel,
                &handler,
            )
            .await
            {
                error!(error = %err, "webrtc main loop errored, closing stream");
            }

            info!("stopped main webrtc loop, cleaning up");

            if let Err(err) = peer.close().await {
                warn!(error = %err, "failed to close webrtc peer");
            }

            // IMPORTANT: we need to manually trigger the close event because the peer doesn't do it
            handler
                .on_connection_state_change(RTCPeerConnectionState::Closed)
                .await;
        }
        .instrument(debug_span!("moonlight stream"))
    });

    // Wait for ice gathering to complete or 1.5 seconds to pass
    select! {
        _ = handler.on_ice_gathering_finished.notified() => {},
        _ = sleep(Duration::from_millis(1500)) => {
            info!("Couldn't fully gather ice candidates after 1.5 seconds! Sending answer with partial ice candidates.");
        }
    }

    // Use the local description with video and audio tracks, control channel and all ice candidates included
    let answer = peer
        .local_description()
        .await
        .expect("web_post: peer.local_description()");

    // Append additional data to the response
    let mut answer_sdp =
        Session::parse(answer.sdp.as_bytes()).expect("failed to get parse sdp answer");

    if let Some(payload_type) = video_answer_payload_type {
        video::patch_answer_h264_profile(&mut answer_sdp, payload_type);
    }

    let additional_answer = WebRTCSessionAnswer {
        app_name: app_title,
        microphone: false,
    };
    additional_answer.apply(&mut answer_sdp);

    let mut answer = Vec::new();
    answer_sdp
        .write(&mut answer)
        .expect("failed to write sdp answer");
    let answer = String::from_utf8_lossy(&answer).to_string();

    info!("sending answer to client");

    // Add stream to the list of streams
    let (event_sender, mut event_receiver) = mpsc::channel(20);
    let stream = match Stream::new(&app, event_sender).await {
        Ok(value) => value,
        Err(err) => {
            // TODO: cleanup the stream

            return Err(err);
        }
    };
    let stream_id = stream.id();
    debug!(stream_id = ?stream_id, "registered stream inside of the app");

    spawn({
        let peer = peer.clone();
        let handler = handler.clone();

        async move {
            loop {
                let event = select! {
                    _ = sleep(Duration::from_secs(10)) => {
                        let state = {
                            *handler.peer_state.lock().expect("lock peer state")
                        };

                        // Check if the connection was closed
                        if matches!(state, RTCPeerConnectionState::Closed) {
                            // Close this thread -> drops receiver -> the stream will be cleaned up on the app
                            return;
                        }

                        continue;
                    }
                    event = event_receiver.recv() => event
                };
                let Some(event) = event else {
                    // The channel was closed, shouldn't happen
                    warn!("the external event receiver was closed");
                    return;
                };

                match event {
                    ExternalStreamEvent::WebRTCAddIceCandidate { ice_sdp_frag } => {
                        for line in ice_sdp_frag.lines() {
                            #[allow(clippy::collapsible_if)]
                            if let Some(candidate) = line.strip_prefix("a=") {
                                if let Err(err) = peer.add_ice_candidate(RTCIceCandidateInit {
                                    candidate: candidate.to_string(),
                                    ..Default::default()
                                })
                                .await {
                                    warn!(error = %err, candidate = ?candidate, "failed to add trickle ice candidate");
                                } else {
                                    debug!(candidate = ?candidate, "added remote ice candidate");
                                };
                            }
                        }
                    }
                    ExternalStreamEvent::Stop => {
                        info!("closing the stream");

                        // IMPORTANT: we need to manually trigger the close event because the peer doesn't do it
                        handler.on_connection_state_change(RTCPeerConnectionState::Closed).await;

                        if let Err(err) = peer.close().await {
                            warn!(error = %err, "error whilst closing the webrtc peer");
                        }
                    }
                }
            }
        }
        .instrument(debug_span!("external event handler", stream_id = ?stream_id))
    });

    debug!(answer = ?answer, "sending answer to client");

    let mut response = HttpResponse::Created();

    // Set location
    let path_prefix = &app.config().web_server.url_path_prefix;
    response.insert_header((
        "Location",
        format!("{path_prefix}/api/host/stream/webrtc/{}", stream_id.0),
    ));

    Ok(response.content_type("application/sdp").body(answer))
}

#[patch("/{stream_id}")]
pub async fn webrtc_patch(
    app: Data<App>,
    stream_id: Path<u32>,
    request: HttpRequest,
    body: String,
) -> Result<HttpResponse, AppError> {
    let stream_id = StreamId(stream_id.into_inner());

    let stream = app.stream_by_id(stream_id).await?;

    match request.headers().get(header::CONTENT_TYPE) {
        Some(x)
            if x.to_str()
                .map(|x| x.starts_with("application/trickle-ice-sdpfrag"))
                .unwrap_or(false) =>
        {
            // Don't support ice restarts
            // -> ICE restart requests use If-Match: *
            if let Some("*") = request
                .headers()
                .get(header::IF_MATCH)
                .and_then(|v| v.to_str().ok())
            {
                Ok(HttpResponse::UnprocessableEntity().finish())
            } else {
                stream
                    .send_event(ExternalStreamEvent::WebRTCAddIceCandidate { ice_sdp_frag: body })
                    .await?;

                Ok(HttpResponse::NoContent().finish())
            }
        }
        _ => Ok(HttpResponse::UnsupportedMediaType().finish()),
    }
}

#[delete("/{stream_id}")]
#[instrument(skip(app))]
pub async fn webrtc_delete(app: Data<App>, stream_id: Path<u32>) -> Result<HttpResponse, AppError> {
    let stream_id = StreamId(stream_id.into_inner());

    let stream = app.stream_by_id(stream_id).await?;

    stream.send_event(ExternalStreamEvent::Stop).await?;

    Ok(HttpResponse::Ok()
        .finish()
        .set_body(BoxBody::new("stream not found")))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn wait_for_connected(handler: &WebRtcHandler) -> RTCPeerConnectionState {
        for _ in 0..100 {
            let state = *handler.peer_state.lock().expect("lock peer state");
            if matches!(
                state,
                RTCPeerConnectionState::Connected
                    | RTCPeerConnectionState::Failed
                    | RTCPeerConnectionState::Closed
            ) {
                return state;
            }
            sleep(Duration::from_millis(100)).await;
        }
        *handler.peer_state.lock().expect("lock peer state")
    }

    async fn create_peer(
        setting_engine: SettingEngineBuilder,
        addr: SocketAddr,
    ) -> (
        Arc<dyn PeerConnection>,
        Arc<WebRtcHandler>,
        mpsc::UnboundedReceiver<Arc<dyn DataChannel>>,
    ) {
        let mut media_engine = create_media_engine(&HashMap::new());
        let interceptor_registry =
            register_default_interceptors(Registry::new(), &mut media_engine)
                .expect("register default interceptors");

        let (on_data_channel_sender, on_data_channel) = mpsc::unbounded_channel();
        let handler = Arc::new(WebRtcHandler {
            peer_state: Mutex::new(RTCPeerConnectionState::New),
            on_ice_gathering_finished: Notify::new(),
            on_data_channel_sender,
        });

        let peer = PeerConnectionBuilder::default()
            .with_media_engine(media_engine)
            .with_interceptor_registry(interceptor_registry)
            .with_setting_engine(setting_engine.build())
            .with_udp_addrs(vec![addr])
            .with_handler(handler.clone())
            .with_configuration(RTCConfigurationBuilder::default().build())
            .build()
            .await
            .expect("build peer");

        (Arc::new(peer), handler, on_data_channel)
    }

    async fn gathered_description(
        peer: &dyn PeerConnection,
        handler: &WebRtcHandler,
    ) -> RTCSessionDescription {
        select! {
            _ = handler.on_ice_gathering_finished.notified() => {},
            _ = sleep(Duration::from_secs(5)) => {},
        }
        peer.local_description().await.expect("local description")
    }

    #[actix_web::test]
    async fn loopback_dtls_handshake_completes() {
        let config = WebRtcConfig {
            include_loopback_candidates: true,
            ..Default::default()
        };

        // Browser-like offerer: default DTLS role, offers a=setup:actpass.
        let (offerer, offerer_handler, _) = create_peer(
            SettingEngineBuilder::new()
                .with_include_loopback_candidate(true)
                .with_network_types(vec![NetworkType::Udp4]),
            SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0),
        )
        .await;
        let (answerer, answerer_handler, mut answerer_data_channels) = create_peer(
            create_setting_engine(&config),
            SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 0),
        )
        .await;

        let _control = offerer
            .create_data_channel("moonlight.control", None)
            .await
            .expect("create data channel");

        let offer = offerer.create_offer(None).await.expect("create offer");
        offerer
            .set_local_description(offer)
            .await
            .expect("set offerer local description");
        let offer = gathered_description(&*offerer, &offerer_handler).await;
        assert!(offer.sdp.contains("a=setup:actpass"), "{}", offer.sdp);

        answerer
            .set_remote_description(offer)
            .await
            .expect("set answerer remote description");
        let answer = answerer.create_answer(None).await.expect("create answer");
        answerer
            .set_local_description(answer)
            .await
            .expect("set answerer local description");
        let answer = gathered_description(&*answerer, &answerer_handler).await;
        assert!(answer.sdp.contains("a=setup:passive"), "{}", answer.sdp);

        offerer
            .set_remote_description(answer)
            .await
            .expect("set offerer remote description");

        assert_eq!(
            wait_for_connected(&answerer_handler).await,
            RTCPeerConnectionState::Connected
        );
        assert_eq!(
            wait_for_connected(&offerer_handler).await,
            RTCPeerConnectionState::Connected
        );

        // Data channels open over SCTP, which only runs once DTLS has completed.
        let data_channel = select! {
            data_channel = answerer_data_channels.recv() => data_channel.expect("data channel"),
            _ = sleep(Duration::from_secs(10)) => panic!("data channel never opened"),
        };
        assert_eq!(
            data_channel.label().await.expect("label"),
            "moonlight.control"
        );

        offerer.close().await.expect("close offerer");
        answerer.close().await.expect("close answerer");
    }
}
