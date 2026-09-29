import { globalObject } from "../util"
import { Logger } from "./log"
import { Pipe } from "./pipeline/index"
import { Transport } from "./transport/index"

export type StatValue = string | number

export type StatsLevel = "off" | "minimal" | "medium" | "max"
const STATS_LEVELS: StatsLevel[] = ["minimal", "medium", "max"]

const MINIMAL_TRANSPORT_KEYS = ["resolution", "codec", "currentFps", "relayToClientRttMs"]

const BACKGROUND_UPDATE_INTERVAL_MS = 1000
const OVERLAY_UPDATE_INTERVAL_MS = 100
const HISTORY_SAMPLE_INTERVAL_MS = 1000
const HISTORY_MAX_SAMPLES = 900

export type StreamStatsSample = {
    time: number
    transport: Record<string, StatValue>
    video: Record<string, StatValue>
    audio: Record<string, StatValue>
}

export type StreamStatsData = {
    videoPipeline: string | null
    audioPipeline: string | null
    transport: Record<string, StatValue>
    video: Record<string, StatValue>
    audio: Record<string, StatValue>
}

function num(value: number | null | undefined, suffix?: string): string | null {
    if (value == null) {
        return null
    } else {
        return `${value.toFixed(2)}${suffix ?? ""}`
    }
}

function formatStatsSection(section: Record<string, StatValue>, keys?: string[]): string {
    let text = ""
    const entries = keys ? keys.map(key => [key, section[key]] as const).filter((e): e is readonly [string, StatValue] => e[1] != null) : Object.entries(section)
    for (const [key, value] of entries) {
        let valuePretty = value
        if (typeof value == "number" && key.endsWith("Ms")) {
            valuePretty = `${num(value, "ms")}`
        }
        text += `${key}: ${valuePretty}\n`
    }
    return text
}

export function streamStatsToText(statsData: StreamStatsData, level: StatsLevel = "max"): string {
    if (level === "minimal") {
        return `stats:\n` + formatStatsSection(statsData.transport, MINIMAL_TRANSPORT_KEYS)
    }

    let text = `stats:
video pipeline: ${statsData.videoPipeline}
audio pipeline: ${statsData.audioPipeline}
`
    text += formatStatsSection(statsData.transport)

    if (level === "max") {
        text += formatStatsSection(statsData.video)
        text += formatStatsSection(statsData.audio)
    }

    return text
}

export class StreamStats {

    private logger: Logger | null = null

    private level: StatsLevel = "off"
    private transport: Transport | null = null
    private updateIntervalId: number | null = null
    private updateIntervalMs: number | null = null
    private updatingTransport: Transport | null = null

    private history: StreamStatsSample[] = []
    private lastHistoryTime = 0

    private videoPipe: Pipe | null = null
    private audioPipe: Pipe | null = null
    private statsData: StreamStatsData = {
        videoPipeline: null,
        audioPipeline: null,
        transport: {},
        video: {},
        audio: {}
    }

    constructor(logger?: Logger) {
        if (logger) {
            this.logger = logger
        }
    }

    setTransport(transport: Transport) {
        this.transport = transport

        this.checkEnabled()
    }
    getLevel(): StatsLevel {
        return this.level
    }
    setLevel(level: StatsLevel) {
        this.level = level

        this.checkEnabled()
    }
    isEnabled(): boolean {
        return this.level != "off"
    }
    toggle() {
        const index = STATS_LEVELS.indexOf(this.level)
        this.setLevel(index < STATS_LEVELS.length - 1 ? STATS_LEVELS[index + 1] : "off")
    }

    // Stats are always sampled in the background so the debug report has a history,
    // the overlay only raises the sampling rate.
    private checkEnabled() {
        const intervalMs = this.isEnabled() ? OVERLAY_UPDATE_INTERVAL_MS : BACKGROUND_UPDATE_INTERVAL_MS
        if (this.updateIntervalId != null && this.updateIntervalMs == intervalMs) {
            return
        }

        if (this.updateIntervalId != null) {
            globalObject().clearInterval(this.updateIntervalId)
        }
        this.updateIntervalId = globalObject().setInterval(this.updateLocalStats.bind(this), intervalMs)
        this.updateIntervalMs = intervalMs
    }

    stop() {
        if (this.updateIntervalId != null) {
            globalObject().clearInterval(this.updateIntervalId)
            this.updateIntervalId = null
            this.updateIntervalMs = null
        }
    }

    // Only one update per transport runs at a time; a hung update on an old transport must not block the new one.
    private async updateLocalStats() {
        const transport = this.transport
        if (transport != null && this.updatingTransport == transport) {
            return
        }
        this.updatingTransport = transport
        try {
            await Promise.all([
                this.updateTransportStats(transport),
                this.updateVideoStats(),
                this.updateAudioStats(),
            ])

            if (this.transport != transport || this.updateIntervalId == null) {
                return
            }
            this.recordHistorySample()
        } finally {
            if (this.updatingTransport == transport) {
                this.updatingTransport = null
            }
        }
    }
    private recordHistorySample() {
        if (!this.transport) {
            return
        }

        const now = Date.now()
        if (now - this.lastHistoryTime < HISTORY_SAMPLE_INTERVAL_MS) {
            return
        }
        this.lastHistoryTime = now

        this.history.push({
            time: now,
            transport: { ...this.statsData.transport },
            video: { ...this.statsData.video },
            audio: { ...this.statsData.audio },
        })
        if (this.history.length > HISTORY_MAX_SAMPLES) {
            this.history.splice(0, this.history.length - HISTORY_MAX_SAMPLES)
        }
    }
    private async updateTransportStats(transport: Transport | null) {
        if (!transport) {
            console.debug("Cannot query stats without transport")
            return
        }

        try {
            const stats = await transport.getStats()
            if (this.transport != transport) {
                return
            }
            for (const key in stats) {
                const value = stats[key]

                this.statsData.transport[key] = value
            }
        } catch (error) {
            console.debug(`Failed to query transport stats: ${error}`)
        }
    }
    private async updateVideoStats() {
        const stats = {}

        if (this.videoPipe && this.videoPipe.reportStats) {
            this.videoPipe.reportStats(stats)
        }

        this.statsData.video = stats
    }
    private async updateAudioStats() {
        const stats = {}

        if (this.audioPipe && this.audioPipe.reportStats) {
            this.audioPipe.reportStats(stats)
        }

        this.statsData.audio = stats
    }

    setVideoPipeline(name: string, pipe: Pipe | null) {
        this.statsData.videoPipeline = name
        this.videoPipe = pipe
    }
    setAudioPipeline(name: string, pipe: Pipe | null) {
        this.statsData.audioPipeline = name
        this.audioPipe = pipe
    }

    getHistory(): StreamStatsSample[] {
        return this.history.slice()
    }

    getCurrentStats(): StreamStatsData {
        const data = {}
        Object.assign(data, this.statsData)
        return data as StreamStatsData
    }
}
