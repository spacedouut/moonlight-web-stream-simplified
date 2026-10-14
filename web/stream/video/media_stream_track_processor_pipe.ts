import { globalObject } from "../../util"
import { Pipe, PipeInfo } from "../pipeline/index"
import { addPipePassthrough } from "../pipeline/pipes"
import { allVideoCodecs } from "../video"
import { FrameVideoRenderer, TrackVideoRenderer, VideoRendererSetup } from "./index"

function wait(time: number): Promise<void> {
    return new Promise((resolve, _reject) => {
        setTimeout(resolve, time)
    })
}

export class VideoMediaStreamTrackProcessorPipe implements TrackVideoRenderer {
    static readonly pipeName = "VideoMediaStreamTrackProcessorPipe"

    static readonly baseType = "videoframe"
    static readonly type = "videotrack"

    static async getInfo(): Promise<PipeInfo> {
        // https://developer.mozilla.org/en-US/docs/Web/API/MediaStreamTrackProcessor
        return {
            environmentSupported: "MediaStreamTrackProcessor" in globalObject(),
            supportedVideoCodecs: allVideoCodecs()
        }
    }

    readonly implementationName: string

    private running: boolean = false
    private newProcessor: boolean = false
    private trackProcessor: MediaStreamTrackProcessor | null = null
    private reader: ReadableStreamDefaultReader<VideoFrame> | null = null

    private base: FrameVideoRenderer

    constructor(base: FrameVideoRenderer) {
        this.implementationName = `media_stream_track_processor -> ${base.implementationName}`
        this.base = base

        addPipePassthrough(this)
    }

    setTrack(track: MediaStreamTrack): void {
        this.trackProcessor = new MediaStreamTrackProcessor({ track })
        this.newProcessor = true
    }

    private async readTrack() {
        while (this.running) {
            if (!this.reader || this.newProcessor) {
                this.newProcessor = false

                this.releaseReader()
                if (this.trackProcessor?.readable.locked) {
                    // Shouldn't happen
                    throw "Canvas video track processor is locked"
                }

                const newReader = this.trackProcessor?.readable.getReader()
                if (newReader) {
                    this.reader = newReader
                }
                await wait(100)
                continue
            }

            // TODO: byob?
            let result
            try {
                result = await this.reader.read()
            } catch (e) {
                if (!this.running) return
                throw e
            }
            if (result.done) {
                if (this.running) console.error("Track Processor is done!")
                return
            }

            this.base.submitFrame(result.value)
        }
    }

    /** The readable can't be cancelled while a reader holds it, so cancel through the reader. */
    private releaseReader() {
        const reader = this.reader
        this.reader = null
        if (reader) {
            reader.cancel().catch(e => console.error(e))
        }
    }

    setup(setup: VideoRendererSetup) {
        this.running = true
        this.readTrack()

        if ("setup" in this.base && typeof this.base.setup == "function") {
            return this.base.setup(setup)
        }
    }
    cleanup() {
        this.running = false
        this.releaseReader()
        if (this.trackProcessor && !this.trackProcessor.readable.locked) {
            this.trackProcessor.readable.cancel().catch(e => console.error(e))
        }
        this.trackProcessor = null

        if ("cleanup" in this.base && typeof this.base.cleanup == "function") {
            return this.base.cleanup(...arguments)
        }
    }

    getBase(): Pipe | null {
        return this.base
    }
}
