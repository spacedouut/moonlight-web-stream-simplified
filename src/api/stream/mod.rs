use moonlight_common::{
    ServerVersion,
    stream::proto::control::packet::{ControlPacketConfig, RawControlPacketType},
};

pub mod web_socket;
pub mod web_transport;

use std::sync::Arc;

use crate::{
    api::bindings::{WebSocketStreamRequest, WebSocketStreamResponse},
    app::{App, AppError, host::HostId},
};
use moonlight_common::{
    AppId,
    crypto::rustcrypto::RustCryptoBackend,
    stream::{
        AesIv, AesKey, EncryptionFlags, MoonlightStreamSettings, StreamingConfig,
        audio::AudioConfig,
        control::ActiveGamepads,
        proto::MoonlightStreamSetup,
        tokio::MoonlightStream,
        video::{ColorRange, ColorSpace, VideoCapabilities, VideoFormats},
    },
};
use tracing::info;

pub async fn start_moonlight_stream(
    app: &App,
    request: &WebSocketStreamRequest,
) -> Result<(MoonlightStream, WebSocketStreamResponse), AppError> {
    let host_id = HostId(request.host_id);
    let mut host = app.host(host_id).await?;
    let host = host.use_host().await?;

    if !host.is_paired().await.map_err(AppError::from)? {
        return Err(AppError::HostNotPaired);
    }

    let app_id = AppId(request.app_id);
    let apps = host.app_list().await?;
    let app_name = apps
        .into_iter()
        .find(|app| app.id == app_id)
        .map(|app| app.title);

    let mut settings = MoonlightStreamSettings {
        width: request.width,
        height: request.height,
        fps: request.fps,
        fps_x100: request.fps * 100,
        bitrate: request.bitrate,
        packet_size: 2048,
        encryption_flags: EncryptionFlags::AUDIO | EncryptionFlags::FOUNDATION_MICROPHONE,
        streaming_remotely: StreamingConfig::Auto,
        sops: true,
        hdr: request.hdr,
        supported_video_formats: VideoFormats::from_bits_retain(request.supported_codecs),
        color_space: ColorSpace::Rec709,
        color_range: ColorRange::Limited,
        local_audio_play_mode: request.local_audio_play_mode,
        audio_config: AudioConfig::STEREO,
        gamepads_attached: ActiveGamepads::empty(),
        gamepads_persist_after_disconnect: false,
        enable_mic: false,
    };

    let server_version = host.version().await?;
    let gfe_version = host.gfe_version().await?;
    let server_codec_mode_support = host.server_codec_mode_support().await?;
    settings.adjust_for_server(server_version, &gfe_version, server_codec_mode_support)?;

    let aes_key = AesKey::new_random(&RustCryptoBackend)?;
    let aes_iv = AesIv::new_random(&RustCryptoBackend)?;
    info!(settings = ?settings, "starting stream");

    let config = host
        .start_stream(
            app_id,
            &settings,
            aes_key,
            aes_iv,
            MoonlightStreamSetup::launch_query_parameters(),
        )
        .await?;
    let stream = MoonlightStream::connect(
        config,
        settings,
        Arc::new(RustCryptoBackend),
        VideoCapabilities::default(),
    )
    .await?;
    let audio_setup = stream.audio_setup();
    let video_setup = stream.video_setup();
    let response = WebSocketStreamResponse {
        video_codec: video_setup.format as u32,
        audio_sample_rate: audio_setup.sample_rate,
        audio_channel_count: audio_setup.channel_count,
        audio_streams: audio_setup.streams,
        audio_coupled_streams: audio_setup.coupled_streams,
        audio_samples_per_frame: audio_setup.samples_per_frame,
        audio_mapping: audio_setup.mapping,
        app_name,
    };
    Ok((stream, response))
}
pub mod webrtc;

fn server_version() -> ServerVersion {
    ServerVersion::new(7, 0, 0, 0)
}
fn create_control_packet_config() -> ControlPacketConfig {
    let mut config =
        ControlPacketConfig::new(server_version(), true).expect("control packet config");

    config.web_state = Some(RawControlPacketType(0x7001));

    config
}
