import { ClientInputEvent, ControlPacket, ControlPacketConfig, controlPacketConfigNew, ServerType, VideoFormats } from "../../uniffi/moonlight_common_bindings"
import { AudioPlayer, AudioPlayerSetup, TrackAudioPlayer } from "../audio/index"
import { StreamCapabilities } from "../index"
import { DataPipe } from "../pipeline/pipes"
import { StatValue } from "../stats"
import { TrackVideoRenderer, VideoRenderer, VideoRendererSetup } from "../video/index"

export type TransportVideoType = "videotrack" // TrackTransportChannel
    | "data" // Data like https://github.com/moonlight-stream/moonlight-common-c/blob/b126e481a195fdc7152d211def17190e3434bcce/src/Limelight.h#L298

export type TransportAudioType = "audiotrack" // TrackTransportChannel
    | "data" // Data like https://github.com/moonlight-stream/moonlight-common-c/blob/b126e481a195fdc7152d211def17190e3434bcce/src/Limelight.h#L356

// failednoconnect => a connection failed without firstly being established
// failed => a connection was ungracefully closed
// disconnect => a connection was gracefully closed
export type TransportShutdown = "failednoconnect" | "failed" | "disconnect"

export type TransportOptions = {
    appId: number,
    width: number,
    height: number,
    fps: number,
    bitrate: number,
    hdr: boolean,
    localAudioPlayMode: boolean,
    /// These are the available video codecs when using data transport
    supportedCodecs: VideoFormats,
    preferredCodecs?: VideoFormats,
    preferredAudio?: number,
    hostId: number,
}

export type TransportConnectData = {
    capabilities: StreamCapabilities,
    videoType: TransportVideoType,
    videoSetup: VideoRendererSetup,
    audioType: TransportAudioType,
    audioSetup: AudioPlayerSetup,
    appName: string,
}

export interface Transport {
    readonly implementationName: string

    readonly controlStream: IControlStream

    onconnect: ((connectData: TransportConnectData) => void) | null
    onclose: ((shutdown: TransportShutdown) => void) | null
    close(): Promise<void>

    // -- Only allowed after onconnect was called
    setVideoPipeline(type: "videotrack", pipeline: (TrackVideoRenderer & VideoRenderer)): Promise<void>
    setVideoPipeline(type: "data", pipeline: (DataPipe & VideoRenderer)): Promise<void>

    /** Stop feeding video to the decoder (e.g. while the tab is hidden). Resuming requests an idr. */
    setVideoPaused(paused: boolean): void

    setAudioPipeline(type: "audiotrack", pipeline: (TrackAudioPlayer & AudioPlayer)): Promise<void>
    setAudioPipeline(type: "data", pipeline: (DataPipe & AudioPlayer)): Promise<void>

    getStats(): Promise<Record<string, StatValue>>

    /// Raw transport internals for the exported debug report
    getDebugInfo?(): Promise<Record<string, unknown>>
}

export function generateControlPacketConfig(): ControlPacketConfig {
    const config = controlPacketConfigNew(
        { major: 7, minor: 0, patch: 0, sunshineIdentifier: -1, serverType: ServerType.Sunshine },
        true
    )
    if (!config) {
        throw "generated invalid packet config"
    }

    config.webState = 0x7001;

    return config
}

/**
 * Drops video packets while paused and, after resuming, until the next key frame.
 * Used by the data transports so a hidden tab doesn't keep a stalled decoder busy.
 */
export class VideoGate {
    private paused = false
    private awaitingKeyFrame = false

    get isPaused(): boolean {
        return this.paused
    }

    pause(): void {
        this.paused = true
    }

    /** Returns true if an idr should be requested from the host. */
    resume(): boolean {
        if (!this.paused) {
            return false
        }
        this.paused = false
        this.awaitingKeyFrame = true
        return true
    }

    /** `payload` is the video channel payload; its first byte is the key frame flag. */
    accept(payload: Uint8Array): boolean {
        if (this.paused) {
            return false
        }
        if (this.awaitingKeyFrame) {
            if (payload[0] == 0) {
                return false
            }
            this.awaitingKeyFrame = false
        }
        return true
    }
}

export interface IControlStream {
    send(input: ClientInputEvent): void
    sendRaw(packet: ControlPacket): void

    onreceive: ((packet: ControlPacket) => void) | null
}
