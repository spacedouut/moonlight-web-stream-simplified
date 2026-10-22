import { currentTelemetry, parseVideoFrameHeader, VIDEO_FRAME_HEADER_LENGTH } from "../telemetry"
import { Logger } from "../log"
import { Pipe, PipeInfo } from "../pipeline/index"
import { addPipePassthrough, DataPipe } from "../pipeline/pipes"
import { allVideoCodecs } from "../video"
import { DataVideoRenderer, VideoRendererSetup } from "./index"

export class DepacketizeVideoPipe implements DataPipe {
    static readonly pipeName = "DepacketizeVideoPipe"

    static readonly baseType = "videodata"
    static readonly type = "wsdata"

    static async getInfo(): Promise<PipeInfo> {
        // no link
        return {
            environmentSupported: true,
            supportedVideoCodecs: allVideoCodecs()
        }
    }

    readonly implementationName: string

    private base: DataVideoRenderer

    private lastTimestampMicroseconds = 0

    constructor(base: DataVideoRenderer, logger?: Logger) {
        this.implementationName = `depacketize_video -> ${base.implementationName}`
        this.base = base

        addPipePassthrough(this)
    }

    submitPacket(buffer: Uint8Array) {
        const header = parseVideoFrameHeader(buffer)
        if (!header) {
            return
        }
        const timestamp = header.timestampUs
        const data = buffer.slice(VIDEO_FRAME_HEADER_LENGTH)
        currentTelemetry()?.onFrameReceived(header, data.length)

        // The u32 timestamp wraps (~71 min of microseconds) and resets on
        // host reconnect; a negative delta makes EncodedVideoChunk throw
        // (duration must fit unsigned long long). Clamp to zero in that case.
        const duration = Math.max(0, timestamp - this.lastTimestampMicroseconds)
        this.base.submitDecodeUnit({
            type: header.keyframe ? "key" : "delta",
            data,
            durationMicroseconds: duration,
            timestampMicroseconds: timestamp,
        })
        this.lastTimestampMicroseconds = timestamp

        addPipePassthrough(this)
    }

    setup(setup: VideoRendererSetup) {
        if ("setup" in this.base && typeof this.base.setup == "function") {
            return this.base.setup(...arguments)
        }
    }

    getBase(): Pipe | null {
        return this.base
    }
}
