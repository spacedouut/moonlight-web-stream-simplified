declare global {
    interface Navigator {
        // Keyboard Lock: https://developer.mozilla.org/en-US/docs/Web/API/Keyboard/lock
        keyboard: {
            lock(): Promise<void>;
            unlock(): void;
        };
    }

    // MediaStreamTrackProcessor: https://developer.mozilla.org/en-US/docs/Web/API/MediaStreamTrackProcessor
    interface MediaStreamTrackProcessor {
        readonly readable: ReadableStream<VideoFrame>
    }

    var MediaStreamTrackProcessor: {
        prototype: MediaStreamTrackProcessor
        new(options: { track: MediaStreamTrack, maxBufferSize?: number }): MediaStreamTrackProcessor
        new(): MediaStreamTrackProcessor
    }

    // MediaStreamTrackGenerator: https://developer.mozilla.org/en-US/docs/Web/API/MediaStreamTrackGenerator
    interface MediaStreamTrackGenerator extends MediaStreamTrack {
        readonly writable: WritableStream<VideoFrame | AudioData>
    }

    var MediaStreamTrackGenerator: {
        prototype: MediaStreamTrackGenerator
        new(options: { kind: "audio" | "video" }): MediaStreamTrackGenerator
    }

    // VideoTrackGenerator: https://developer.mozilla.org/en-US/docs/Web/API/VideoTrackGenerator
    interface VideoTrackGenerator {
        readonly muted: boolean
        readonly track: MediaStreamTrack
        readonly writable: WritableStream<VideoFrame>
    }

    var VideoTrackGenerator: {
        prototype: VideoTrackGenerator
        new(): VideoTrackGenerator
    }

    // WebGPU (minimal declarations, Chromium only): https://developer.mozilla.org/en-US/docs/Web/API/WebGPU_API
    interface GPUAdapter {
        requestDevice(): Promise<GPUDevice>
    }

    interface GPUTexture {
        // opaque handle
    }

    interface GPUQueue {
        copyExternalImageToTexture(
            source: { source: VideoFrame | ImageBitmap | HTMLVideoElement | HTMLCanvasElement | OffscreenCanvas | ImageData },
            destination: { texture: GPUTexture },
            copySize: { width: number, height: number } | [number, number]
        ): void
    }

    interface GPUDevice {
        readonly queue: GPUQueue
        readonly lost: Promise<{ reason: string, message: string }>
        destroy(): void
    }

    interface GPUCanvasContext {
        configure(configuration: { device: GPUDevice, format: string, alphaMode?: "opaque" | "premultiplied", usage?: number }): void
        unconfigure(): void
        getCurrentTexture(): GPUTexture
    }

    interface GPU {
        requestAdapter(): Promise<GPUAdapter | null>
        getPreferredCanvasFormat(): string
    }

    interface Navigator {
        readonly gpu?: GPU
    }

    var GPUCanvasContext: {
        prototype: GPUCanvasContext
        new(): GPUCanvasContext
    }
}


export { };