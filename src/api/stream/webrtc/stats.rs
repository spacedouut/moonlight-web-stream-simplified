use std::{sync::Arc, time::Duration};

use rtc::data_channel::RTCDataChannelState;
use tokio::{spawn, sync::mpsc, time::sleep};
use tracing::{Instrument, debug_span, warn};
use webrtc::{data_channel::DataChannel, peer_connection::PeerConnection};

use crate::{
    api::bindings::{StreamStatsClientboundMessage, WebSocketClientboundMessage},
    app::AppError,
};

/// Relay -> client data channel carrying `WebSocketClientboundMessage::Stats` as JSON text.
pub struct StatsChannel {
    on_send: mpsc::UnboundedSender<String>,
}

impl StatsChannel {
    pub async fn new(peer: &dyn PeerConnection) -> Result<Self, AppError> {
        let channel: Arc<dyn DataChannel> =
            peer.create_data_channel("moonlight.stats", None).await?;
        let (on_send, mut on_send_receiver) = mpsc::unbounded_channel::<String>();

        // Nothing here needs the channel's events, but they still have to be read. The webrtc
        // driver queues at most 256 events per channel and once one queue is full it stops
        // reading inbound SCTP for every channel. Each acked stats message adds a
        // BufferedAmountLow event, so after a few minutes the input channels went silent.
        spawn({
            let channel = channel.clone();
            async move { while channel.poll().await.is_some() {} }
                .instrument(debug_span!("stats data channel event drain"))
        });

        spawn(
            async move {
                let mut connected = false;
                while let Some(message) = on_send_receiver.recv().await {
                    while !connected {
                        match channel.ready_state().await {
                            Ok(RTCDataChannelState::Connecting) => {
                                sleep(Duration::from_millis(100)).await
                            }
                            Ok(RTCDataChannelState::Open) => connected = true,
                            _ => return,
                        }
                    }
                    if let Err(err) = channel.send_text(&message).await {
                        warn!(error = %err, "failed to send stats over data channel");
                    }
                }
            }
            .instrument(debug_span!("stats data channel sender")),
        );

        Ok(Self { on_send })
    }

    pub fn send(&self, message: StreamStatsClientboundMessage) {
        let message = WebSocketClientboundMessage::Stats(message);
        match serde_json::to_string(&message) {
            Ok(json) => {
                let _ = self.on_send.send(json);
            }
            Err(err) => warn!(error = %err, "failed to serialize stats message"),
        }
    }
}
