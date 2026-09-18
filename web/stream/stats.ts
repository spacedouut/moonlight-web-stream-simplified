import { globalObject } from "../util"
import { Logger } from "./log"
import { Pipe } from "./pipeline/index"
import { Transport } from "./transport/index"

export type StatValue = string | number

export type StatsLevel = "off" | "minimal" | "medium" | "max"
const STATS_LEVELS: StatsLevel[] = ["minimal", "medium", "max"]

const MINIMAL_TRANSPORT_KEYS = ["resolution", "codec", "currentFps", "relayToClientRttMs"]

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

    private checkEnabled() {
        if (this.isEnabled() && this.updateIntervalId == null) {
            this.updateIntervalId = globalObject().setInterval(this.updateLocalStats.bind(this), 100)
        } else if (!this.isEnabled() && this.updateIntervalId != null) {
            globalObject().clearInterval(this.updateIntervalId)
            this.updateIntervalId = null
        }
    }

    private async updateLocalStats() {
        Promise.all([
            this.updateTransportStats(),
            this.updateVideoStats(),
            this.updateAudioStats(),
        ])
    }
    private async updateTransportStats() {
        if (!this.transport) {
            console.debug("Cannot query stats without transport")
            return
        }

        try {
            const stats = await this.transport.getStats()
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

    getCurrentStats(): StreamStatsData {
        const data = {}
        Object.assign(data, this.statsData)
        return data as StreamStatsData
    }
}
