import { globalObject } from "../../util"
import { Pipe, PipeInfo } from "../pipeline/index"
import { addPipePassthrough } from "../pipeline/pipes"
import { allVideoCodecs } from "../video"
import { CanvasRenderer, FrameVideoRenderer, VideoRendererSetup } from "./index"

// Draws decoded VideoFrames onto a canvas via WebGPU.
// Uses copyExternalImageToTexture so the browser keeps the frame on the GPU.
export class WebGpuFrameDrawPipe implements FrameVideoRenderer {
    static readonly pipeName = "WebGpuFrameDrawPipe"
    static readonly baseType = "canvas"
    static readonly type = "videoframe"

    static async getInfo(): Promise<PipeInfo> {
        let supported = "GPUCanvasContext" in globalObject() && "gpu" in navigator
        if (supported) {
            try {
                const adapter = await navigator.gpu!.requestAdapter()
                supported = adapter != null
            } catch (_error) {
                supported = false
            }
        }
        return {
            environmentSupported: supported,
            supportedVideoCodecs: allVideoCodecs()
        }
    }

    readonly implementationName = "WebGpuFrameDrawPipe"

    private base: CanvasRenderer

    private context: GPUCanvasContext | null = null
    private device: GPUDevice | null = null

    constructor(base: CanvasRenderer) {
        this.base = base
        addPipePassthrough(this)
    }

    async setup(_setup: VideoRendererSetup): Promise<void> {
        if (!navigator.gpu) {
            throw new Error("WebGPU is not supported")
        }

        const result = this.base.useCanvasContext("webgpu")
        if (result.error != null || !result.context) {
            throw new Error(`Failed to create WebGPU canvas context: ${result.error}`)
        }

        const adapter = await navigator.gpu.requestAdapter()
        if (!adapter) {
            throw new Error("Failed to request a WebGPU adapter")
        }

        this.device = await adapter.requestDevice()
        this.context = result.context
        this.configure()

        this.device.lost.then(() => {
            this.device = null
            this.context = null
        })

        if ("setup" in this.base && typeof this.base.setup == "function") {
            return this.base.setup(...arguments)
        }
    }

    private configure() {
        if (!this.context || !this.device) {
            return
        }
        // GPUTextureUsage: RENDER_ATTACHMENT (0x10) | COPY_DST (0x02)
        this.context.configure({
            device: this.device,
            format: navigator.gpu!.getPreferredCanvasFormat(),
            alphaMode: "opaque",
            usage: 0x10 | 0x02
        })
    }

    private canvasWidth = 0
    private canvasHeight = 0

    setCanvasSize(width: number, height: number): void {
        this.canvasWidth = width
        this.canvasHeight = height
        this.base.setCanvasSize(width, height)
        this.configure()
    }

    submitFrame(frame: VideoFrame): void {
        if (this.context && this.device) {
            const width = frame.displayWidth
            const height = frame.displayHeight
            if (width > 0 && height > 0) {
                if (width != this.canvasWidth || height != this.canvasHeight) {
                    this.setCanvasSize(width, height)
                }
                this.device.queue.copyExternalImageToTexture(
                    { source: frame },
                    { texture: this.context.getCurrentTexture() },
                    { width, height }
                )
            }
        }
        frame.close()

        this.base.commitFrame()
    }

    cleanup() {
        if (this.context) {
            this.context.unconfigure()
            this.context = null
        }
        this.device = null

        if ("cleanup" in this.base && typeof this.base.cleanup == "function") {
            return this.base.cleanup(...arguments)
        }
    }

    getBase(): Pipe | null {
        return this.base
    }
}
