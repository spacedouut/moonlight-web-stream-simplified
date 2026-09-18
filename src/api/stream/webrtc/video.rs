use std::{collections::HashMap, mem::swap, sync::Arc};

use bytes::{Bytes, BytesMut};
use moonlight_common::{
    crypto::rustcrypto::RustCryptoBackend,
    http::pair::PairingCryptoBackend,
    stream::{
        proto::video::frame::OwnedVideoFrame,
        video::{SunshineHdrMetadata, VideoFormat, VideoFormats, VideoSetup},
    },
    webrtc::sdp::Session,
};
use rtc::{
    media_stream::MediaStreamTrack,
    peer_connection::configuration::media_engine::{MIME_TYPE_AV1, MIME_TYPE_H264, MIME_TYPE_HEVC},
    rtcp::payload_feedbacks::{
        picture_loss_indication::PictureLossIndication,
        receiver_estimated_maximum_bitrate::ReceiverEstimatedMaximumBitrate,
    },
    rtp::{
        Header, Packet,
        codec::{av1::Av1Payloader, h264::H264Payloader, h265::RTP_OUTBOUND_MTU},
        packetizer::Payloader,
    },
    rtp_transceiver::{
        SSRC,
        rtp_sender::{
            RTCPFeedback, RTCRtpCodec, RTCRtpCodecParameters, RTCRtpCodingParameters,
            RTCRtpEncodingParameters, RtpCodecKind,
        },
    },
};
use tokio::{
    select, spawn,
    sync::mpsc::{UnboundedSender, unbounded_channel},
};
use tracing::{Instrument, debug, debug_span, info, warn};
use webrtc::{
    media_stream::track_local::{TrackLocal, TrackLocalEvent, static_rtp::TrackLocalStaticRTP},
    peer_connection::PeerConnection,
};

use crate::{api::stream::webrtc::video::h265::H265Payloader, app::AppError};

mod h265;

pub enum VideoChannelEvent {
    SignalIdr,
}

enum Message {
    Frame(OwnedVideoFrame),
    // Carried for future HDR support; currently dropped on receipt.
    #[allow(dead_code)]
    HdrMetadata(Option<SunshineHdrMetadata>),
}

enum State {
    SelectVideoFormat,
    Panic,
    Sending {
        track: Arc<TrackLocalStaticRTP>,
        frame_sender: UnboundedSender<Message>,
    },
}

pub struct VideoChannel {
    video_formats: HashMap<VideoFormat, RTCRtpCodecParameters>,
    state: State,
    selected_format: Option<VideoFormat>,
    selected_payload_type: Option<u8>,
}

impl VideoChannel {
    pub fn new(sdp: &Session, preferred_formats: VideoFormats) -> Result<Self, AppError> {
        let mut video_formats_mapping = get_video_formats(sdp);

        // Remove all not preferred codecs
        for format in VideoFormat::all() {
            if !format.contained_in(preferred_formats) {
                video_formats_mapping.remove(&format);
            }
        }

        debug!(formats = ?video_formats_mapping, "found video codecs");

        Ok(Self {
            video_formats: video_formats_mapping,
            state: State::SelectVideoFormat,
            selected_format: None,
            selected_payload_type: None,
        })
    }

    pub fn supported_video_formats(&self) -> &HashMap<VideoFormat, RTCRtpCodecParameters> {
        &self.video_formats
    }

    pub async fn on_video_format_selected(
        &mut self,
        setup: VideoSetup,
        peer: &dyn PeerConnection,
    ) -> Result<(), AppError> {
        let mut new_state = State::Panic;
        swap(&mut new_state, &mut self.state);

        // Check video format
        let format = setup.format;
        let Some(codec) = self.video_formats.remove(&format) else {
            return Err(AppError::WebRtcClientCodecNotSupported);
        };

        // Create video track
        let mut ssrc = [0; 4];
        RustCryptoBackend.random_bytes(&mut ssrc)?;
        let ssrc = SSRC::from_ne_bytes(ssrc);

        let payload_type = codec.payload_type;
        let clock_rate = codec.rtp_codec.clock_rate;
        self.selected_format = Some(format);
        self.selected_payload_type = Some(payload_type);

        let track = Arc::new(TrackLocalStaticRTP::new(MediaStreamTrack::new(
            "video".to_string(),
            "moonlight".to_string(),
            "video".to_string(),
            RtpCodecKind::Video,
            vec![RTCRtpEncodingParameters {
                rtp_coding_parameters: RTCRtpCodingParameters {
                    ssrc: Some(ssrc),
                    ..Default::default()
                },
                codec: codec.rtp_codec.clone(),
                ..Default::default()
            }],
        )));

        peer.add_track(Arc::clone(&track) as Arc<dyn TrackLocal>)
            .await?;

        let mut payloader = if format.contained_in(VideoFormats::MASK_H264) {
            Box::new(H264Payloader::default()) as Box<dyn Payloader + Send + Sync>
        } else if format.contained_in(VideoFormats::MASK_H265) {
            Box::new(H265Payloader::default()) as Box<dyn Payloader + Send + Sync>
        } else {
            Box::new(Av1Payloader::default()) as Box<dyn Payloader + Send + Sync>
        };

        debug!(codec = ?format, webrtc_codec = ?codec, "webrtc video channel codec selected");

        let (frame_sender, mut frame_receiver) = unbounded_channel();

        self.state = State::Sending {
            track: track.clone(),
            frame_sender,
        };

        spawn(
            async move {
                let mut sequence_number = 0u16;

                while let Some(message) = frame_receiver.recv().await {
                    let frame = match message {
                        // TODO: apply HDR metadata via a negotiated header extension
                        Message::HdrMetadata(_) => continue,
                        Message::Frame(frame) => frame,
                    };
                    let frame = frame.as_ref();

                    let timestamp =
                        (frame.metadata.timestamp.as_nanos() * clock_rate as u128 / 1_000_000_000)
                            as u32;

                    let mut payloads = Vec::with_capacity(10);

                    // Create RTP Packets based on codec
                    match format {
                        // H264 / H265
                        VideoFormat::H264
                        | VideoFormat::H264High8_444
                        | VideoFormat::H265
                        | VideoFormat::H265Main10
                        | VideoFormat::H265Rext8_444
                        | VideoFormat::H265Rext10_444 => {
                            // Each buffer is one nalu, beginning with start code
                            for buffer in &frame.buffers {
                                // strip start code
                                let data = if buffer.data.starts_with(&[0, 0, 1]) {
                                    &buffer.data[3..]
                                } else if buffer.data.starts_with(&[0, 0, 0, 1]) {
                                    &buffer.data[4..]
                                } else {
                                    warn!(data_start = ?buffer.data[0..10], "got h264 or h265 annex b data without a 3 or 4 byte start code");
                                    buffer.data
                                };

                                let nal_payloads = payloader
                                    .payload(RTP_OUTBOUND_MTU - 12, &Bytes::copy_from_slice(data))
                                    .expect("failed to payload frame");

                                payloads.extend(nal_payloads);
                            }
                        }
                        VideoFormat::Av1Main8
                        | VideoFormat::Av1Main10
                        | VideoFormat::Av1High8_444
                        | VideoFormat::Av1High10_444 => {
                            // Put all buffers inside one array and let the payloader payload
                            let full_frame = if frame.buffers.len() == 1 {
                                // fast path
                                Bytes::copy_from_slice(frame.buffers[0].data)
                            } else {
                                let mut full_frame = BytesMut::new();

                                for buffer in &frame.buffers {
                                    full_frame.extend_from_slice(buffer.data);
                                }

                                full_frame.freeze()
                            };

                            let nal_payloads = payloader
                                .payload(RTP_OUTBOUND_MTU, &full_frame)
                                .expect("failed to payload frame");

                            payloads.extend(nal_payloads);
                        }
                    }

                    let len = payloads.len();
                    for (i, payload) in payloads.into_iter().enumerate() {
                        sequence_number = sequence_number.wrapping_add(1);

                        let is_last = i == len - 1;

                        if let Err(err) = track
                            .write_rtp(Packet {
                                header: Header {
                                    version: 2,
                                    // Marker needs to mark the end of one frame
                                    marker: is_last,
                                    sequence_number,
                                    timestamp,
                                    payload_type,
                                    ssrc,
                                    ..Default::default()
                                },
                                payload,
                            })
                            .await
                        {
                            warn!(error = %err, "failed to send video packet");
                        }
                    }
                }
            }
            .instrument(debug_span!("video frame relay")),
        );

        info!(setup = ?setup, codec = ?codec, "finished video track setup");

        Ok(())
    }

    /// The payload type the answer should claim the true H264 profile for,
    /// or None when the negotiated format is not plain H264.
    pub fn h264_answer_payload_type(&self) -> Option<u8> {
        (self.selected_format == Some(VideoFormat::H264))
            .then_some(self.selected_payload_type)
            .flatten()
    }

    pub fn on_frame(&mut self, frame: OwnedVideoFrame) {
        match &mut self.state {
            State::SelectVideoFormat | State::Panic => {
                panic!("VideoChannel is in an invalid state")
            }
            State::Sending { frame_sender, .. } => {
                let _ = frame_sender.send(Message::Frame(frame));
            }
        }
    }

    pub fn set_hdr_enabled(&mut self, _enabled: bool, metadata: Option<SunshineHdrMetadata>) {
        match &mut self.state {
            State::SelectVideoFormat | State::Panic => {
                panic!("VideoChannel is in an invalid state")
            }
            State::Sending { frame_sender, .. } => {
                let _ = frame_sender.send(Message::HdrMetadata(metadata));
            }
        }
    }

    pub async fn drive(&mut self) -> Result<VideoChannelEvent, AppError> {
        loop {
            let State::Sending { track, .. } = &mut self.state else {
                panic!("VideoChannel is in an invalid state");
            };

            select! {
                // This function seems cancel safe
                result = track.poll() => {
                    let Some(event) = result else {
                        continue;
                    };

                    if let TrackLocalEvent::OnRtcpPacket(packets) = event {
                        for packet in packets {
                            let packet = packet.as_any();

                            if packet.downcast_ref::<PictureLossIndication>().is_some() {
                                debug!("got picture loss indication, set need idr flag");
                                return Ok(VideoChannelEvent::SignalIdr);
                            } else if let Some(ReceiverEstimatedMaximumBitrate { bitrate: _, .. }) =
                                packet.downcast_ref::<ReceiverEstimatedMaximumBitrate>()
                            {
                                // TODO
                            }
                        }
                    }
                }
            }
        }
    }
}

fn get_video_formats(sdp: &Session) -> HashMap<VideoFormat, RTCRtpCodecParameters> {
    let mut formats = HashMap::<VideoFormat, RTCRtpCodecParameters>::default();

    for media in &sdp.medias {
        // -- Find and extract codec and sdp fmtp line
        let mut codec_and_clock_rate = HashMap::<u8, (&str, u32)>::default();
        let mut sdp_fmtp_lines = HashMap::<u8, &str>::default();

        for attribute in &media.attributes {
            let Some(value) = &attribute.value else {
                continue;
            };

            match attribute.attribute.as_str() {
                "rtpmap" => {
                    let Some((pt, codec, clock_rate)) = parse_rtpmap(value) else {
                        warn!(attribute = ?attribute, "failed to parse rtpmap");
                        continue;
                    };

                    codec_and_clock_rate.insert(pt, (codec, clock_rate));
                }
                "fmtp" => {
                    let Some((pt, sdp_fmtp_line)) = parse_fmtp(value) else {
                        warn!(attribute = ?attribute, "failed to parse fmtp");
                        continue;
                    };

                    sdp_fmtp_lines.insert(pt, sdp_fmtp_line);
                }
                _ => {}
            }
        }

        // -- Add all recognized codecs
        // The m= line lists payload types in the client's preference order,
        // so the first pt mapping to a format wins over later aliases.
        for pt in media
            .fmt
            .split_whitespace()
            .filter_map(|pt| pt.parse::<u8>().ok())
        {
            let Some((codec, clock_rate)) = codec_and_clock_rate.get(&pt) else {
                continue;
            };
            let sdp_fmtp_line = sdp_fmtp_lines.get(&pt).unwrap_or(&"");
            debug!(pt = pt, codec = ?codec, clock_rate = ?clock_rate, sdp_fmtp_line = ?sdp_fmtp_line, "got codec");

            if codec.eq_ignore_ascii_case("H264") {
                if !sdp_fmtp_line.contains("packetization-mode=1") {
                    // Single NAL mode is not supported
                    continue;
                }

                // Get profile
                let profile_level_id = sdp_fmtp_line
                    .split(";")
                    .filter_map(|attribute| attribute.split_once("="))
                    .find(|(attribute, _)| attribute.trim() == "profile-level-id")
                    .map(|(_, value)| value.trim());

                let mut format = VideoFormat::H264;
                if let Some(value) = profile_level_id {
                    if value.starts_with("64") {
                        format = VideoFormat::H264;
                    } else if value.starts_with("f4") {
                        format = VideoFormat::H264High8_444;
                    } else {
                        debug!(profile_level_id = ?value, "found unknown h264 profile-level-id");
                    }
                }

                formats.entry(format).or_insert(RTCRtpCodecParameters {
                    rtp_codec: RTCRtpCodec {
                        mime_type: MIME_TYPE_H264.to_string(),
                        sdp_fmtp_line: sdp_fmtp_line.to_string(),
                        clock_rate: *clock_rate,
                        rtcp_feedback: rtcp_feedback(),
                        ..Default::default()
                    },
                    payload_type: pt,
                });
            } else if codec.eq_ignore_ascii_case("H265") {
                // Get profile
                let mut format = VideoFormat::H265;

                let attributes = sdp_fmtp_line.split(";");
                for (attribute, value) in
                    attributes.filter_map(|attribute| attribute.split_once("="))
                {
                    if attribute == "profile-id" {
                        match value {
                            "1" => format = VideoFormat::H265,
                            "2" => format = VideoFormat::H265Main10,
                            _ => debug!(profile_id = ?value, "unknown h265 profile-id"),
                        }
                    }
                }

                formats.entry(format).or_insert(RTCRtpCodecParameters {
                    rtp_codec: RTCRtpCodec {
                        mime_type: MIME_TYPE_HEVC.to_string(),
                        sdp_fmtp_line: sdp_fmtp_line.to_string(),
                        clock_rate: *clock_rate,
                        rtcp_feedback: rtcp_feedback(),
                        ..Default::default()
                    },
                    payload_type: pt,
                });
            } else if codec.eq_ignore_ascii_case("AV1") {
                // Get profile
                let mut format = VideoFormat::Av1Main8;

                let attributes = sdp_fmtp_line.split(";");
                for (attribute, value) in
                    attributes.filter_map(|attribute| attribute.split_once("="))
                {
                    if attribute == "profile" {
                        match value {
                            "1" => format = VideoFormat::Av1Main8,
                            "2" => format = VideoFormat::Av1High8_444,
                            "4" => {
                                // TODO: range extensions
                            }
                            // TODO: how do the Main10 / High10 profiles work?
                            _ => debug!(profile = ?value, "unknown av1 profile"),
                        }
                    }
                }

                formats.entry(format).or_insert(RTCRtpCodecParameters {
                    rtp_codec: RTCRtpCodec {
                        mime_type: MIME_TYPE_AV1.to_string(),
                        sdp_fmtp_line: sdp_fmtp_line.to_string(),
                        clock_rate: *clock_rate,
                        rtcp_feedback: rtcp_feedback(),
                        ..Default::default()
                    },
                    payload_type: pt,
                });
            }
        }
    }

    formats
}
/// Patches an SDP answer so `payload_type` on the video media advertises the
/// High profile the host actually emits. `VideoFormat::H264` is High profile,
/// but clients only offer baseline/main fmtp lines and the answer copies the
/// offer's fmtp verbatim — without this a strict hardware decoder (e.g.
/// ChromeOS) configures itself for baseline and wedges on the real High
/// profile bitstream.
pub fn patch_answer_h264_profile(answer_sdp: &mut Session, payload_type: u8) {
    let prefix = format!("{payload_type} ");

    for media in &mut answer_sdp.medias {
        if media.media != "video" {
            continue;
        }

        for attribute in &mut media.attributes {
            if attribute.attribute != "fmtp" {
                continue;
            }
            let Some(value) = &mut attribute.value else {
                continue;
            };
            if !value.starts_with(&prefix) {
                continue;
            }

            *value = format!(
                "{prefix}{}",
                rewrite_h264_profile_level_id(&value[prefix.len()..], "640034")
            );
        }
    }
}

/// Rewrites the `profile-level-id` value inside an H264 fmtp line, appending
/// it when absent.
fn rewrite_h264_profile_level_id(sdp_fmtp_line: &str, profile_level_id: &str) -> String {
    let mut replaced = false;

    let mut parts: Vec<String> = Vec::new();
    for attribute in sdp_fmtp_line.split(";") {
        if attribute.trim().starts_with("profile-level-id=") {
            parts.push(format!("profile-level-id={profile_level_id}"));
            replaced = true;
        } else {
            parts.push(attribute.to_string());
        }
    }
    if !replaced {
        parts.push(format!("profile-level-id={profile_level_id}"));
    }

    parts.join(";")
}

fn parse_rtpmap(attribute_value: &str) -> Option<(u8, &str, u32)> {
    let (pt_str, full_codec) = attribute_value.split_once(' ')?;
    let pt = pt_str.parse::<u8>().ok()?;

    // identify codec
    let (codec_str, clock_rate_str) = full_codec.split_once('/')?;

    let clock_rate = clock_rate_str.parse::<u32>().ok()?;

    Some((pt, codec_str, clock_rate))
}
fn parse_fmtp(attribute_value: &str) -> Option<(u8, &str)> {
    let (pt_str, sdp_fmtp_line) = attribute_value.split_once(' ')?;
    let pt = pt_str.parse::<u8>().ok()?;

    Some((pt, sdp_fmtp_line))
}

fn rtcp_feedback() -> Vec<RTCPFeedback> {
    vec![
        RTCPFeedback {
            // negative acknowledgement
            typ: "nack".to_string(),
            parameter: "".to_string(),
        },
        RTCPFeedback {
            // picture loss indicator (idr)
            typ: "nack".to_string(),
            parameter: "pli".to_string(),
        },
        RTCPFeedback {
            // receiver estimated maximum bitrate
            typ: "goog-remb".to_string(),
            parameter: "".to_string(),
        },
    ]
}
