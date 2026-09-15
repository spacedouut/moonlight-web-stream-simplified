import { Api, apiWebTransportConfig } from "../../api"
import { WebSocketChannel, WebSocketClientboundMessage, WebSocketServerboundMessage, WebTransportConfigResponse } from "../../api_bindings"
import { ClientInputEvent, ControlPacket, ControlPacketConfig, controlPacketDeserialize, controlPacketSerialize, InputBatcher, PacketDirection } from "../../uniffi/moonlight_common_bindings"
import { globalObject } from "../../util"
import { AudioPlayer, TrackAudioPlayer } from "../audio/index"
import { Logger } from "../log"
import { DataPipe } from "../pipeline/pipes"
import { StatValue } from "../stats"
import { createSupportedVideoFormatsBits, getSelectedVideoCodec } from "../video"
import { VideoRenderer, TrackVideoRenderer } from "../video/index"
import { generateControlPacketConfig, IControlStream, Transport, TransportAudioType, TransportConnectData, TransportOptions, TransportShutdown, TransportVideoType } from "./index"

export class WebTransportTransport implements Transport {
    readonly implementationName = "web_transport"
    private transport: WebTransport
    private messageStream: WebTransportBidirectionalStream | null = null
    private messageWriter: WritableStreamDefaultWriter<Uint8Array> | null = null
    private logger?: Logger
    private options: TransportOptions | null = null
    private wasConnected = false
    private closedDispatched = false
    private onOpen: Promise<void>
    private writeChain: Promise<void> = Promise.resolve()
    private internalOnConnect: () => void = () => { }
    private onConnected = new Promise<void>(resolve => this.internalOnConnect = resolve)
    private connectData: TransportConnectData | null = null
    private videoPipeline: DataPipe | null = null
    private audioPipeline: DataPipe | null = null
    private relayStats: { rttMs: number, rttVarianceMs: number } | null = null
    private pongReceiveResolve: (() => void) | null = null
    private onPongPromise: Promise<number> | null = null

    controlStream: WebTransportControlStream
    onconnect: ((connectData: TransportConnectData) => void) | null = null
    onclose: ((shutdown: TransportShutdown) => void) | null = null

    constructor(api: Api, config: WebTransportConfigResponse, logger?: Logger) {
        this.logger = logger
        const apiPath = new URL(api.host_url).pathname
        const url = config.url ?? `https://${location.hostname}:${config.port}${apiPath}/host/stream/web_transport`
        const options = config.certificate_hash ? {
            serverCertificateHashes: [{ algorithm: "sha-256" as const, value: hexToArrayBuffer(config.certificate_hash) }]
        } : undefined
        this.transport = new WebTransport(url, options)
        this.controlStream = new WebTransportControlStream(this, generateControlPacketConfig())
        this.onOpen = this.initialize()
        this.transport.closed.then(() => this.close(), () => this.close())
    }

    private async initialize(): Promise<void> {
        try {
            await this.transport.ready
            this.messageStream = await this.transport.createBidirectionalStream()
            this.messageWriter = this.messageStream.writable.getWriter()
            void this.readMessages(this.messageStream.readable)
            void this.readUnidirectionalStreams()
            void this.readDatagrams()
        } catch {
            await this.close()
        }
    }

    private enqueueFrame(kind: number, bytes: Uint8Array): void {
        const frame = new Uint8Array(5 + bytes.length)
        new DataView(frame.buffer).setUint32(0, bytes.length + 1)
        frame[4] = kind
        frame.set(bytes, 5)
        this.writeChain = this.writeChain
            .then(() => this.onOpen)
            .then(() => this.messageWriter?.write(frame))
            .catch(() => { })
    }

    private writeMessage(message: WebSocketServerboundMessage | WebSocketClientboundMessage): void {
        this.enqueueFrame(0, new TextEncoder().encode(JSON.stringify(message)))
    }

    private async readMessages(readable: ReadableStream<Uint8Array>): Promise<void> {
        try {
            const reader = readable.getReader()
            let buffered = new Uint8Array()
            while (true) {
                const result = await reader.read()
                if (result.done) return
                const merged = new Uint8Array(buffered.length + result.value.length)
                merged.set(buffered)
                merged.set(result.value, buffered.length)
                buffered = merged
                while (buffered.length >= 4) {
                    const length = new DataView(buffered.buffer, buffered.byteOffset).getUint32(0)
                    if (buffered.length < length + 4) break
                    if (length < 1) throw new Error("empty WebTransport frame")
                    const kind = buffered[4]
                    const payload = buffered.subarray(5, length + 4)
                    buffered = buffered.slice(length + 4)
                    if (kind == 0) {
                        this.onMessage(JSON.parse(new TextDecoder().decode(payload)) as WebSocketClientboundMessage)
                    } else if (kind == 1) {
                        this.onBinary(copyBytes(payload))
                    } else {
                        this.logger?.debug(`received unknown WebTransport frame kind ${kind}`)
                    }
                }
            }
        } catch {
            await this.close()
        }
    }

    private async readUnidirectionalStreams(): Promise<void> {
        let stopDispatcher: (() => void) | null = null
        try {
            const streams = this.transport.incomingUnidirectionalStreams.getReader()
            const pending: Promise<Uint8Array<ArrayBuffer> | null>[] = []
            let accepting = true
            let wakeDispatcher: (() => void) | null = null
            const notifyDispatcher = () => {
                const wake = wakeDispatcher
                wakeDispatcher = null
                wake?.()
            }
            stopDispatcher = () => {
                accepting = false
                notifyDispatcher()
            }
            const dispatch = async() => {
                while (accepting || pending.length > 0) {
                    if (pending.length == 0) {
                        await new Promise<void>(resolve => wakeDispatcher = resolve)
                        continue
                    }
                    const bytes = await pending.shift()!
                    if (bytes) this.onBinary(bytes)
                }
            }
            const dispatchPromise = dispatch()
            while (true) {
                const streamResult = await streams.read()
                if (streamResult.done) {
                    accepting = false
                    notifyDispatcher()
                    await dispatchPromise
                    return
                }
                while (pending.length >= 64) await pending[0]
                pending.push(this.readAll(streamResult.value))
                notifyDispatcher()
            }
        } catch {
            stopDispatcher?.()
            await this.close()
        }
    }

    private async readAll(readable: ReadableStream<Uint8Array>): Promise<Uint8Array<ArrayBuffer> | null> {
        const reader = readable.getReader()
        let timeoutId: number | null = null
        const read = async(): Promise<Uint8Array<ArrayBuffer>> => {
            const chunks: Uint8Array[] = []
            while (true) {
                const result = await reader.read()
                if (result.done) break
                chunks.push(result.value)
            }
            const total = new Uint8Array(chunks.reduce((size, chunk) => size + chunk.length, 0))
            let offset = 0
            for (const chunk of chunks) {
                total.set(chunk, offset)
                offset += chunk.length
            }
            return total
        }
        const timedOut = new Promise<null>(resolve => {
            timeoutId = globalObject().setTimeout(() => {
                void reader.cancel().then(() => resolve(null), () => resolve(null))
            }, 2000)
        })
        try {
            return await Promise.race([read(), timedOut])
        } catch {
            return null
        } finally {
            if (timeoutId != null) globalObject().clearTimeout(timeoutId)
        }
    }

    private async readDatagrams(): Promise<void> {
        try {
            const reader = this.transport.datagrams.readable.getReader()
            while (true) {
                const result = await reader.read()
                if (result.done) return
                this.onBinary(copyBytes(result.value))
            }
        } catch {
            this.close()
        }
    }

    private onMessage(message: WebSocketClientboundMessage): void {
        if ("Response" in message) {
            const response = message.Response
            this.logger?.debug(`received stream response: ${JSON.stringify(response)}`)
            this.wasConnected = true
            this.connectData = {
                videoType: "data",
                videoSetup: { codec: getSelectedVideoCodec(response.video_codec) ?? "h264", width: this.options?.width ?? 0, height: this.options?.height ?? 0, fps: this.options?.fps ?? 0 },
                audioType: "data",
                audioSetup: { channels: response.audio_channel_count, sampleRate: response.audio_sample_rate, streams: response.audio_coupled_streams, coupledStreams: response.audio_coupled_streams, samplesPerFrame: response.audio_samples_per_frame, mapping: response.audio_mapping },
                capabilities: { touch: true },
                appName: response.app_name ?? "Unknown",
            }
            this.internalOnConnect()
            this.onconnect?.(this.connectData)
        } else if ("Stats" in message) {
            if ("Pong" in message.Stats) this.onPongReceive(message.Stats.Pong)
            else if ("RelayRtt" in message.Stats) this.relayStats = { rttMs: message.Stats.RelayRtt.rtt_ms, rttVarianceMs: message.Stats.RelayRtt.rtt_variance_ms }
        }
    }

    private onBinary(data: Uint8Array<ArrayBuffer>): void {
        if (data.length == 0) return
        const channel = data[0]
        if (channel == WebSocketChannel.CONTROL) this.controlStream.onRawPacket(data.subarray(1))
        else if (channel == WebSocketChannel.VIDEO) {
            this.videoPipeline?.submitPacket(data.subarray(1))
            if (this.videoPipeline && "pollRequestIdr" in this.videoPipeline && typeof this.videoPipeline.pollRequestIdr == "function" && this.videoPipeline.pollRequestIdr()) {
                this.controlStream.sendRaw(new ControlPacket.RequestIdr())
            }
        } else if (channel == WebSocketChannel.AUDIO) this.audioPipeline?.submitPacket(data.subarray(1))
    }

    async startStream(options: TransportOptions): Promise<void> {
        try {
            await this.onOpen
        } catch {
            return
        }
        this.options = options
        this.writeMessage({
            Request: {
                host_id: options.hostId, app_id: options.appId, width: options.width, height: options.height, fps: options.fps, bitrate: options.bitrate,
                hdr: options.hdr, local_audio_play_mode: options.localAudioPlayMode, supported_codecs: createSupportedVideoFormatsBits(options.supportedCodecs),
                preferred_codecs: options.preferredCodecs ? createSupportedVideoFormatsBits(options.preferredCodecs) : 0,
            }
        })
    }

    setVideoPipeline(type: "videotrack", pipeline: (TrackVideoRenderer & VideoRenderer)): Promise<void>
    setVideoPipeline(type: "data", pipeline: (DataPipe & VideoRenderer)): Promise<void>
    async setVideoPipeline(type: TransportVideoType, pipeline: unknown): Promise<void> {
        if (type != "data") throw `invalid web transport video pipeline type ${type}`
        this.videoPipeline = pipeline as DataPipe
    }

    setAudioPipeline(type: "audiotrack", pipeline: (TrackAudioPlayer & AudioPlayer)): Promise<void>
    setAudioPipeline(type: "data", pipeline: (DataPipe & AudioPlayer)): Promise<void>
    async setAudioPipeline(type: TransportAudioType, pipeline: unknown): Promise<void> {
        if (type != "data") throw `invalid web transport audio pipeline type ${type}`
        this.audioPipeline = pipeline as DataPipe
    }

    async getStats(): Promise<Record<string, StatValue>> {
        const out: Record<string, StatValue> = {}
        if (this.connectData) {
            out.codec = this.connectData.videoSetup.codec
            out.resolution = `Width: ${this.connectData.videoSetup.width}, Height: ${this.connectData.videoSetup.height}, Fps: ${this.connectData.videoSetup.fps}`
        }
        if (this.relayStats) {
            out.hostToRelayRttMs = this.relayStats.rttMs
            out.hostToRelayRttVarianceMs = this.relayStats.rttVarianceMs
        }
        out.relayToClientRttMs = await this.doPing()
        return out
    }

    private onPongReceive(_id: number): void {
        this.pongReceiveResolve?.()
    }

    private async doPing(): Promise<number> {
        await this.onConnected
        if (this.onPongPromise) return await this.onPongPromise
        this.onPongPromise = new Promise((resolve, reject) => {
            const start = performance.now()
            const timeoutId = globalObject().setTimeout(() => {
                this.pongReceiveResolve = null
                this.onPongPromise = null
                reject(new Error("pong timeout"))
            }, 5000)
            this.pongReceiveResolve = () => {
                globalObject().clearTimeout(timeoutId)
                this.pongReceiveResolve = null
                this.onPongPromise = null
                resolve(performance.now() - start)
            }
        })
        this.writeMessage({ Stats: { Ping: Math.floor(Math.random() * 1000) } })
        return await this.onPongPromise
    }

    sendControl(packet: ControlPacket, config: ControlPacketConfig): void {
        const raw = controlPacketSerialize(config, packet)
        if (!raw) return
        const packetView = new Uint8Array(raw)
        const message = new Uint8Array(1 + packetView.length)
        message[0] = WebSocketChannel.CONTROL
        message.set(packetView, 1)
        this.enqueueFrame(1, message)
    }

    async close(): Promise<void> {
        this.transport.close()
        if (!this.closedDispatched) {
            this.closedDispatched = true
            this.onclose?.(this.wasConnected ? "failed" : "failednoconnect")
        }
    }
}

class WebTransportControlStream implements IControlStream {
    onreceive: ((packet: ControlPacket) => void) | null = null
    private batcher = new InputBatcher()
    private batchSendTimeout: number | null = null
    constructor(private transport: WebTransportTransport, private config: ControlPacketConfig) { }
    send(input: ClientInputEvent): void {
        for (const packet of this.batcher.batchInput(input)) this.sendRaw(packet)
        if (this.batchSendTimeout == null) this.batchSendTimeout = globalObject().setTimeout(this.boundSendBatchedInputs, 1)
    }
    sendRaw(packet: ControlPacket): void {
        this.transport.sendControl(packet, this.config)
    }
    onRawPacket(packetBuffer: Uint8Array): void {
        const packet = controlPacketDeserialize(this.config, PacketDirection.ClientBound, packetBuffer.slice().buffer)
        if (packet && this.onreceive) this.onreceive(packet)
    }
    private boundSendBatchedInputs = this.sendBatchedInputs.bind(this)
    private sendBatchedInputs(): void {
        this.batchSendTimeout = null
        for (const packet of this.batcher.removeBatchedInputs()) this.sendRaw(packet)
    }
}

function hexToArrayBuffer(value: string): ArrayBuffer {
    const bytes = new Uint8Array(value.length / 2)
    for (let i = 0; i < bytes.length; i++) bytes[i] = parseInt(value.substring(i * 2, i * 2 + 2), 16)
    return bytes.buffer
}

function copyBytes(value: Uint8Array): Uint8Array<ArrayBuffer> {
    const bytes = new Uint8Array(value.length)
    bytes.set(value)
    return bytes
}
