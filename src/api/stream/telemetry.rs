//! Per-frame timing data and relay counters for the stats overlay.
//!
//! Data transports prefix each video frame with [`VIDEO_HEADER_LEN`] bytes:
//! `flags: u8, timestamp_us: u32, frame_index: u32, host_latency: u16 (0.1 ms, 0 = unknown), relay_time_us: u64`.
//! `relay_time_us` uses [`RelayClock`], which is also echoed in pongs so the
//! client can map relay time onto its own clock.

use std::time::{Duration, Instant};

use moonlight_common::stream::proto::video::frame::VideoFrameMetadata;

use crate::api::bindings::RelayStats;

pub const VIDEO_HEADER_LEN: usize = 19;

#[derive(Debug, Clone, Copy)]
pub struct RelayClock(Instant);

impl RelayClock {
    pub fn new() -> Self {
        Self(Instant::now())
    }

    pub fn now_us(&self) -> u64 {
        self.0.elapsed().as_micros() as u64
    }
}

/// Writes the video header into `out[..VIDEO_HEADER_LEN]`.
pub fn write_video_header(out: &mut [u8], metadata: &VideoFrameMetadata, relay_time_us: u64) {
    // TODO: make frame type from video packet public, 2==Idr
    out[0] = u8::from(metadata.frame_type.serialize() == 2);
    out[1..5].copy_from_slice(&(metadata.timestamp.as_micros() as u32).to_be_bytes());
    out[5..9].copy_from_slice(&metadata.frame_index.0.to_be_bytes());
    out[9..11]
        .copy_from_slice(&encode_host_latency(metadata.host_processing_latency).to_be_bytes());
    out[11..19].copy_from_slice(&relay_time_us.to_be_bytes());
}

fn encode_host_latency(latency: Option<Duration>) -> u16 {
    match latency {
        Some(latency) => (latency.as_micros() / 100).clamp(1, u16::MAX as u128) as u16,
        None => 0,
    }
}

/// Counters for one stats interval, reset by [`RelayTelemetry::take`].
#[derive(Debug)]
pub struct RelayTelemetry {
    interval_start: Instant,
    frames: u32,
    keyframes: u32,
    bytes: u64,
    frame_index_gaps: u32,
    last_frame_index: Option<u32>,
    host_latency_sum_us: u64,
    host_latency_count: u32,
    host_latency_min_us: u64,
    host_latency_max_us: u64,
    host_idr_requests: u32,
    relay_idr_requests: u32,
    backlog_drops: u32,
    max_frame_bytes: u32,
}

impl Default for RelayTelemetry {
    fn default() -> Self {
        Self::new()
    }
}

impl RelayTelemetry {
    pub fn new() -> Self {
        Self {
            interval_start: Instant::now(),
            frames: 0,
            keyframes: 0,
            bytes: 0,
            frame_index_gaps: 0,
            last_frame_index: None,
            host_latency_sum_us: 0,
            host_latency_count: 0,
            host_latency_min_us: u64::MAX,
            host_latency_max_us: 0,
            host_idr_requests: 0,
            relay_idr_requests: 0,
            backlog_drops: 0,
            max_frame_bytes: 0,
        }
    }

    pub fn on_frame(&mut self, metadata: &VideoFrameMetadata, len: usize) {
        self.frames += 1;
        self.bytes += len as u64;
        self.max_frame_bytes = self.max_frame_bytes.max(len as u32);
        if metadata.frame_type.serialize() == 2 {
            self.keyframes += 1;
        }

        let index = metadata.frame_index.0;
        if let Some(last) = self.last_frame_index {
            let gap = index.wrapping_sub(last);
            // Large jumps are stream restarts, not loss.
            if gap > 1 && gap < 1000 {
                self.frame_index_gaps += gap - 1;
            }
        }
        self.last_frame_index = Some(index);

        if let Some(latency) = metadata.host_processing_latency {
            let us = latency.as_micros() as u64;
            self.host_latency_sum_us += us;
            self.host_latency_count += 1;
            self.host_latency_min_us = self.host_latency_min_us.min(us);
            self.host_latency_max_us = self.host_latency_max_us.max(us);
        }
    }

    /// The host (Sunshine) asked for a keyframe, e.g. after frame loss.
    pub fn on_host_idr_request(&mut self) {
        self.host_idr_requests += 1;
    }

    /// The relay asked for a keyframe itself, e.g. after dropping a backlog.
    pub fn on_relay_idr_request(&mut self) {
        self.relay_idr_requests += 1;
    }

    pub fn on_backlog_drop(&mut self) {
        self.backlog_drops += 1;
    }

    pub fn take(
        &mut self,
        relay_time_us: u64,
        rtt: Option<(Duration, Duration)>,
        frames_in_flight: Option<u32>,
    ) -> RelayStats {
        let interval = self.interval_start.elapsed();
        let host_latency = (self.host_latency_count > 0).then(|| {
            (
                self.host_latency_sum_us as f64 / self.host_latency_count as f64 / 1000.0,
                self.host_latency_min_us as f64 / 1000.0,
                self.host_latency_max_us as f64 / 1000.0,
            )
        });

        let stats = RelayStats {
            relay_time_us: relay_time_us as f64,
            interval_ms: interval.as_secs_f64() * 1000.0,
            host_rtt_ms: rtt.map(|(rtt, _)| rtt.as_secs_f64() * 1000.0),
            host_rtt_variance_ms: rtt.map(|(_, var)| var.as_secs_f64() * 1000.0),
            frames: self.frames,
            keyframes: self.keyframes,
            bytes: self.bytes as f64,
            max_frame_bytes: self.max_frame_bytes,
            frame_index_gaps: self.frame_index_gaps,
            host_latency_avg_ms: host_latency.map(|(avg, _, _)| avg),
            host_latency_min_ms: host_latency.map(|(_, min, _)| min),
            host_latency_max_ms: host_latency.map(|(_, _, max)| max),
            host_idr_requests: self.host_idr_requests,
            relay_idr_requests: self.relay_idr_requests,
            backlog_drops: self.backlog_drops,
            frames_in_flight,
        };

        let last_frame_index = self.last_frame_index;
        *self = Self::new();
        self.last_frame_index = last_frame_index;
        stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_latency_encoding() {
        assert_eq!(encode_host_latency(None), 0);
        assert_eq!(encode_host_latency(Some(Duration::from_micros(10))), 1);
        assert_eq!(encode_host_latency(Some(Duration::from_micros(4_250))), 42);
        assert_eq!(
            encode_host_latency(Some(Duration::from_secs(100))),
            u16::MAX
        );
    }
}
