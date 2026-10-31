import { RelayStats } from "../api_bindings"

// Per-frame latency tracking for the stats overlay and the debug report.
//
// Data transports (WebSocket / WebTransport) get a relay timestamp in every video frame header.
// Pongs carry the relay clock as well, so we can map relay time onto performance.now() and measure the
// relay -> client hop of every frame. WebRTC has no custom header, so its numbers come from
// requestVideoFrameCallback metadata and RTCPeerConnection.getStats().

const WINDOW_MS = 10_000
const MAX_SAMPLES = 2400
const MAX_PENDING_FRAMES = 240
const RECENT_FRAMES = 300
const CLOCK_SAMPLES = 30

export type StageKey =
    | "host"
    | "hostToRelay"
    | "relayToClient"
    | "clientQueue"
    | "decode"
    | "render"
    | "total"
    | "frameInterval"
    | "frameBytes"
    | "decodeQueue"
    | "webrtcJitterBuffer"
    | "webrtcReceiveToDisplay"

export type RollingSummary = {
    last: number
    avg: number
    min: number
    max: number
    p50: number
    p95: number
    p99: number
    count: number
}

export class RollingStat {
    private times: number[] = []
    private values: number[] = []

    push(value: number, now = performance.now()) {
        if (!Number.isFinite(value)) {
            return
        }
        this.times.push(now)
        this.values.push(value)
        this.trim(now)
    }

    private trim(now: number) {
        let drop = 0
        while (drop < this.times.length && (now - this.times[drop] > WINDOW_MS || this.times.length - drop > MAX_SAMPLES)) {
            drop++
        }
        if (drop > 0) {
            this.times.splice(0, drop)
            this.values.splice(0, drop)
        }
    }

    summary(windowMs = 5000, now = performance.now()): RollingSummary | null {
        this.trim(now)
        const values: number[] = []
        for (let i = this.times.length - 1; i >= 0 && now - this.times[i] <= windowMs; i--) {
            values.push(this.values[i])
        }
        if (values.length == 0) {
            return null
        }
        const last = values[0]
        values.sort((a, b) => a - b)
        const sum = values.reduce((a, b) => a + b, 0)
        const pick = (q: number) => values[Math.min(values.length - 1, Math.floor(q * values.length))]
        return {
            last,
            avg: sum / values.length,
            min: values[0],
            max: values[values.length - 1],
            p50: pick(0.5),
            p95: pick(0.95),
            p99: pick(0.99),
            count: values.length,
        }
    }

    /** Samples inside the window, oldest first, as [msAgo, value]. */
    series(windowMs = WINDOW_MS, now = performance.now()): Array<[number, number]> {
        const out: Array<[number, number]> = []
        for (let i = 0; i < this.times.length; i++) {
            const age = now - this.times[i]
            if (age <= windowMs) {
                out.push([age, this.values[i]])
            }
        }
        return out
    }

    clear() {
        this.times = []
        this.values = []
    }
}

/** Counts events and reports a per-second rate over the last few seconds. */
export class RateCounter {
    private times: number[] = []
    total = 0

    add(count = 1, now = performance.now()) {
        this.total += count
        for (let i = 0; i < count; i++) {
            this.times.push(now)
        }
        while (this.times.length > 0 && now - this.times[0] > WINDOW_MS) {
            this.times.shift()
        }
    }

    perSecond(windowMs = 2000, now = performance.now()): number {
        let count = 0
        for (let i = this.times.length - 1; i >= 0 && now - this.times[i] <= windowMs; i--) {
            count++
        }
        return count * 1000 / windowMs
    }
}

type ClockSample = { rttMs: number, offsetMs: number, at: number }

/** Maps the relay clock onto performance.now(), using the lowest-RTT pong of the recent ones. */
export class RelayClockSync {
    private samples: ClockSample[] = []

    onPong(sentAtMs: number, receivedAtMs: number, relayTimeUs: number) {
        const rttMs = receivedAtMs - sentAtMs
        if (rttMs < 0) {
            return
        }
        // Relay time is taken when the relay answers, assume the halfway point.
        const offsetMs = relayTimeUs / 1000 - (sentAtMs + rttMs / 2)
        this.samples.push({ rttMs, offsetMs, at: receivedAtMs })
        if (this.samples.length > CLOCK_SAMPLES) {
            this.samples.shift()
        }
    }

    private best(): ClockSample | null {
        let best: ClockSample | null = null
        for (const sample of this.samples) {
            if (!best || sample.rttMs < best.rttMs) {
                best = sample
            }
        }
        return best
    }

    isSynced(): boolean {
        return this.samples.length > 0
    }

    /** Converts relay time to local performance.now() time. */
    relayToLocalMs(relayTimeUs: number): number | null {
        const best = this.best()
        if (!best) {
            return null
        }
        return relayTimeUs / 1000 - best.offsetMs
    }

    describe() {
        const best = this.best()
        return {
            samples: this.samples.length,
            bestRttMs: best?.rttMs ?? null,
            // The true offset is somewhere in +-rtt/2 of the estimate.
            uncertaintyMs: best ? best.rttMs / 2 : null,
            offsetMs: best?.offsetMs ?? null,
        }
    }
}

export type VideoFrameHeader = {
    keyframe: boolean
    timestampUs: number
    frameIndex: number
    hostLatencyMs: number | null
    relayTimeUs: number
}

export type FrameRecord = {
    timestampUs: number
    frameIndex: number | null
    keyframe: boolean
    bytes: number
    hostLatencyMs: number | null
    /** Estimated local time when the relay sent the frame. */
    relaySentAt: number | null
    receivedAt: number
    decodeSubmittedAt: number | null
    decodedAt: number | null
    presentedAt: number | null
    presentation: PresentationKind | null
    droppedReason: string | null
}

export type PresentationKind = "canvas_draw" | "video_element" | "track_written"

export type TelemetryEvent = {
    time: number
    kind: string
    detail?: unknown
}

const VIDEO_HEADER_LENGTH = 19

export function parseVideoFrameHeader(buffer: Uint8Array): VideoFrameHeader | null {
    if (buffer.length < VIDEO_HEADER_LENGTH) {
        return null
    }
    const view = new DataView(buffer.buffer, buffer.byteOffset, VIDEO_HEADER_LENGTH)
    const hostLatency = view.getUint16(9)
    return {
        keyframe: view.getUint8(0) != 0,
        timestampUs: view.getUint32(1),
        frameIndex: view.getUint32(5),
        hostLatencyMs: hostLatency == 0 ? null : hostLatency / 10,
        relayTimeUs: Number(view.getBigUint64(11)),
    }
}
export const VIDEO_FRAME_HEADER_LENGTH = VIDEO_HEADER_LENGTH

export type TelemetrySnapshot = {
    stages: Partial<Record<StageKey, RollingSummary>>
    rates: {
        receivedFps: number
        decodedFps: number
        presentedFps: number
        receivedKbps: number
    }
    counters: {
        framesReceived: number
        framesDecoded: number
        framesPresented: number
        keyframes: number
        frameIndexGaps: number
        droppedBeforeDecode: number
        droppedBeforePresent: number
        gatedFrames: number
        decoderResets: number
        clientIdrRequests: number
        lateFrames: number
    }
    clock: ReturnType<RelayClockSync["describe"]>
    relay: RelayStats | null
    relayHistory: Array<RelayStats & { receivedAt: number }>
    presentation: PresentationKind | null
    decodeQueueSize: number | null
    lastFrameAgoMs: number | null
}

export class StreamTelemetry {
    readonly clock = new RelayClockSync()

    private stages = new Map<StageKey, RollingStat>()
    private pending = new Map<number, FrameRecord>()
    private recent: FrameRecord[] = []
    private events: TelemetryEvent[] = []

    private received = new RateCounter()
    private decoded = new RateCounter()
    private presented = new RateCounter()
    private bytes = new RateCounter()

    private counters: TelemetrySnapshot["counters"] = {
        framesReceived: 0,
        framesDecoded: 0,
        framesPresented: 0,
        keyframes: 0,
        frameIndexGaps: 0,
        droppedBeforeDecode: 0,
        droppedBeforePresent: 0,
        gatedFrames: 0,
        decoderResets: 0,
        clientIdrRequests: 0,
        lateFrames: 0,
    }

    private lastFrameIndex: number | null = null
    private lastReceivedAt: number | null = null
    private lastWrittenToTrack: FrameRecord | null = null
    private relay: RelayStats | null = null
    private relayHistory: Array<RelayStats & { receivedAt: number }> = []
    private presentation: PresentationKind | null = null
    private webrtc = false
    private warnedNoReceiveTime = false
    private decodeQueueSize: number | null = null
    private eventListeners: Array<(event: TelemetryEvent) => void> = []

    stage(key: StageKey): RollingStat {
        let stat = this.stages.get(key)
        if (!stat) {
            stat = new RollingStat()
            this.stages.set(key, stat)
        }
        return stat
    }

    // -- Events
    addEventListener(listener: (event: TelemetryEvent) => void) {
        this.eventListeners.push(listener)
    }

    event(kind: string, detail?: unknown) {
        const event = { time: Date.now(), kind, detail }
        this.events.push(event)
        if (this.events.length > 500) {
            this.events.shift()
        }
        for (const listener of this.eventListeners) {
            listener(event)
        }
    }

    getEvents(): TelemetryEvent[] {
        return this.events
    }

    // -- Relay
    onRelayStats(stats: RelayStats) {
        this.relay = stats
        this.relayHistory.push({ ...stats, receivedAt: Date.now() })
        if (this.relayHistory.length > 900) {
            this.relayHistory.shift()
        }
        if (stats.host_rtt_ms != null) {
            this.stage("hostToRelay").push(stats.host_rtt_ms / 2)
        }
        if (this.webrtc && stats.host_latency_avg_ms != null) {
            // WebRTC frames carry no relay header, so host latency only arrives in these 1 s summaries.
            this.stage("host").push(stats.host_latency_avg_ms)
        }
        if (stats.backlog_drops > 0) {
            this.event("relay_backlog_drop", { count: stats.backlog_drops, inFlight: stats.frames_in_flight })
        }
        if (stats.frame_index_gaps > 0) {
            this.event("host_frames_lost", { count: stats.frame_index_gaps })
        }
    }

    // -- Data transport frames
    onFrameGated() {
        this.counters.gatedFrames++
    }

    onFrameReceived(header: VideoFrameHeader, bytes: number, now = performance.now()) {
        const relaySentAt = this.clock.relayToLocalMs(header.relayTimeUs)

        const record: FrameRecord = {
            timestampUs: header.timestampUs,
            frameIndex: header.frameIndex,
            keyframe: header.keyframe,
            bytes,
            hostLatencyMs: header.hostLatencyMs,
            relaySentAt,
            receivedAt: now,
            decodeSubmittedAt: null,
            decodedAt: null,
            presentedAt: null,
            presentation: null,
            droppedReason: null,
        }

        this.counters.framesReceived++
        if (header.keyframe) {
            this.counters.keyframes++
        }
        this.received.add(1, now)
        this.bytes.add(bytes, now)
        this.stage("frameBytes").push(bytes, now)

        if (this.lastFrameIndex != null) {
            const gap = (header.frameIndex - this.lastFrameIndex) >>> 0
            if (gap > 1 && gap < 1000) {
                this.counters.frameIndexGaps += gap - 1
            }
        }
        this.lastFrameIndex = header.frameIndex

        if (this.lastReceivedAt != null) {
            this.stage("frameInterval").push(now - this.lastReceivedAt, now)
        }
        this.lastReceivedAt = now

        if (header.hostLatencyMs != null) {
            this.stage("host").push(header.hostLatencyMs, now)
        }
        if (relaySentAt != null) {
            this.stage("relayToClient").push(now - relaySentAt, now)
        }

        this.track(record)
    }

    private track(record: FrameRecord) {
        const previous = this.pending.get(record.timestampUs)
        if (previous) {
            this.finish(previous, "replaced")
        }
        this.pending.set(record.timestampUs, record)
        if (this.pending.size > MAX_PENDING_FRAMES) {
            const oldest = this.pending.keys().next().value
            if (oldest != null) {
                const old = this.pending.get(oldest)
                if (old) {
                    this.finish(old, old.decodedAt == null ? "never_decoded" : "never_presented")
                }
            }
        }
    }

    onDecodeSkipped(timestampUs: number, reason: string) {
        const record = this.pending.get(timestampUs)
        this.counters.droppedBeforeDecode++
        if (record) {
            this.finish(record, reason)
        }
    }

    onDecodeSubmitted(timestampUs: number, queueSize: number, now = performance.now()) {
        this.decodeQueueSize = queueSize
        this.stage("decodeQueue").push(queueSize, now)
        const record = this.pending.get(timestampUs)
        if (record) {
            record.decodeSubmittedAt = now
            this.stage("clientQueue").push(now - record.receivedAt, now)
        }
    }

    onDecoded(timestampUs: number, now = performance.now()) {
        this.counters.framesDecoded++
        this.decoded.add(1, now)
        const record = this.pending.get(timestampUs)
        if (record) {
            record.decodedAt = now
            if (record.decodeSubmittedAt != null) {
                this.stage("decode").push(now - record.decodeSubmittedAt, now)
            }
        }
    }

    onDecoderReset(reason: string) {
        this.counters.decoderResets++
        this.event("decoder_reset", { reason })
    }

    onClientIdrRequest(reason: string) {
        this.counters.clientIdrRequests++
        this.event("idr_request", { reason })
    }

    /** A canvas pipe drew this frame. */
    onFrameDrawn(timestampUs: number, now = performance.now()) {
        this.presentation = "canvas_draw"
        this.present(timestampUs, now, "canvas_draw")
    }

    /** A frame was handed to a MediaStreamTrack, the <video> element shows it later. */
    onFrameWrittenToTrack(timestampUs: number, now = performance.now()) {
        const record = this.pending.get(timestampUs)
        if (!record) {
            return
        }
        if (this.lastWrittenToTrack && this.lastWrittenToTrack.presentedAt == null && this.lastWrittenToTrack !== record) {
            // A newer frame replaced it before the compositor picked it up.
            this.finish(this.lastWrittenToTrack, "replaced_before_present")
        }
        this.lastWrittenToTrack = record
    }

    /** requestVideoFrameCallback on the <video> element. */
    onVideoElementFrame(now: number, metadata: VideoFrameCallbackMetadata) {
        const meta = metadata as VideoFrameCallbackMetadata & { receiveTime?: number, processingDuration?: number }
        const hasReceiveTime = meta.receiveTime != null && meta.receiveTime > 0
        if (hasReceiveTime || this.webrtc) {
            // WebRTC: the browser knows when the packets arrived and how long decoding took.
            this.webrtc = true
            this.presentation = "video_element"
            this.counters.framesPresented++
            this.presented.add(1, now)
            if (hasReceiveTime) {
                this.stage("webrtcReceiveToDisplay").push(metadata.expectedDisplayTime - meta.receiveTime!, now)
            } else if (!this.warnedNoReceiveTime) {
                this.warnedNoReceiveTime = true
                this.event("rvfc_missing_receive_time", { keys: Object.keys(metadata) })
            }
            if (meta.processingDuration != null) {
                this.stage("decode").push(meta.processingDuration * 1000, now)
            }
            return
        }

        const record = this.lastWrittenToTrack
        if (!record || record.presentedAt != null) {
            return
        }
        this.presentation = "video_element"
        this.present(record.timestampUs, metadata.expectedDisplayTime, "video_element")
    }

    private present(timestampUs: number, at: number, kind: PresentationKind) {
        const record = this.pending.get(timestampUs)
        if (!record) {
            return
        }
        record.presentedAt = at
        record.presentation = kind
        this.counters.framesPresented++
        this.presented.add(1, at)
        if (record.decodedAt != null) {
            this.stage("render").push(at - record.decodedAt, at)
        }
        if (record.relaySentAt != null) {
            const total = at - record.relaySentAt
                + (record.hostLatencyMs ?? 0)
                + (this.stage("hostToRelay").summary()?.last ?? 0)
            this.stage("total").push(total, at)
            if (total > 150) {
                this.counters.lateFrames++
            }
        }
        this.finish(record, null)
    }

    private finish(record: FrameRecord, droppedReason: string | null) {
        this.pending.delete(record.timestampUs)
        if (droppedReason != null) {
            record.droppedReason = droppedReason
            if (record.decodedAt != null) {
                this.counters.droppedBeforePresent++
            }
        }
        this.recent.push(record)
        if (this.recent.length > RECENT_FRAMES) {
            this.recent.shift()
        }
    }

    // -- WebRTC
    onWebRtcJitterBuffer(delayMs: number) {
        this.webrtc = true
        this.stage("webrtcJitterBuffer").push(delayMs)
    }

    // -- Output
    snapshot(): TelemetrySnapshot {
        const now = performance.now()
        const stages: TelemetrySnapshot["stages"] = {}
        for (const [key, stat] of this.stages) {
            const summary = stat.summary(5000, now)
            if (summary) {
                stages[key] = summary
            }
        }
        return {
            stages,
            rates: {
                receivedFps: this.received.perSecond(2000, now),
                decodedFps: this.decoded.perSecond(2000, now),
                presentedFps: this.presented.perSecond(2000, now),
                receivedKbps: this.bytes.perSecond(2000, now) * 8 / 1000,
            },
            counters: { ...this.counters },
            clock: this.clock.describe(),
            relay: this.relay,
            relayHistory: this.relayHistory,
            presentation: this.presentation,
            decodeQueueSize: this.decodeQueueSize,
            lastFrameAgoMs: this.lastReceivedAt == null ? null : now - this.lastReceivedAt,
        }
    }

    series(key: StageKey, windowMs?: number): Array<[number, number]> {
        return this.stages.get(key)?.series(windowMs) ?? []
    }

    recentFrames(): FrameRecord[] {
        return this.recent
    }
}

let current: StreamTelemetry | null = null

/** Pipes are built generically, so they reach the telemetry of the running stream through this. */
export function currentTelemetry(): StreamTelemetry | null {
    return current
}

export function setCurrentTelemetry(telemetry: StreamTelemetry | null) {
    current = telemetry
}
