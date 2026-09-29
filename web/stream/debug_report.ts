import { Settings } from "../component/settings_menu"
import { LogMessageType } from "./log"
import { StreamStatsData, StreamStatsSample } from "./stats"

const MAX_LOG_ENTRIES = 5000
const MAX_EVENT_ENTRIES = 1000

export type DebugLogEntry = {
    time: number
    type: LogMessageType | null
    line: string
}

export type DebugEventEntry = {
    time: number
    event: string
    detail?: unknown
}

export type StreamDebugState = {
    hostId: number
    appId: number
    streamerSize: [number, number]
    desiredTransport: string
    transport: string | null
    failureReason: string | null
    stopped: boolean
    settings: Settings
    stats: StreamStatsData
    statsHistory: StreamStatsSample[]
    transportDebug: Record<string, unknown> | null
}

type NetworkInformationLike = EventTarget & {
    type?: string
    effectiveType?: string
    downlink?: number
    downlinkMax?: number
    rtt?: number
    saveData?: boolean
}

function getNetworkInformation(): NetworkInformationLike | null {
    const connection = (navigator as Navigator & { connection?: NetworkInformationLike }).connection
    return connection ?? null
}

function describeNetworkInformation(): Record<string, unknown> | null {
    const connection = getNetworkInformation()
    if (!connection) {
        return null
    }
    return {
        type: connection.type,
        effectiveType: connection.effectiveType,
        downlinkMbps: connection.downlink,
        downlinkMaxMbps: connection.downlinkMax,
        rttMs: connection.rtt,
        saveData: connection.saveData,
    }
}

function describeError(error: unknown): unknown {
    if (error instanceof Error) {
        return { name: error.name, message: error.message, stack: error.stack }
    }
    return String(error)
}

function pushCapped<T>(list: T[], entry: T, max: number) {
    list.push(entry)
    if (list.length > max) {
        list.splice(0, list.length - max)
    }
}

/// Collects timestamped logs and browser events so they can be exported as a JSON debug report.
export class StreamDebugRecorder {

    private startedAt = Date.now()
    private logs: DebugLogEntry[] = []
    private events: DebugEventEntry[] = []

    constructor() {
        window.addEventListener("online", () => this.event("online"))
        window.addEventListener("offline", () => this.event("offline"))
        document.addEventListener("visibilitychange", () => this.event("visibilitychange", document.visibilityState))
        window.addEventListener("error", event => this.event("error", {
            message: event.message,
            source: event.filename,
            line: event.lineno,
            column: event.colno,
            error: event.error != null ? describeError(event.error) : undefined,
        }))
        window.addEventListener("unhandledrejection", event => this.event("unhandledrejection", describeError(event.reason)))
        getNetworkInformation()?.addEventListener("change", () => this.event("networkchange", describeNetworkInformation()))
    }

    log(line: string, type: LogMessageType | null) {
        pushCapped(this.logs, { time: Date.now(), type, line }, MAX_LOG_ENTRIES)
    }

    event(event: string, detail?: unknown) {
        pushCapped(this.events, { time: Date.now(), event, detail }, MAX_EVENT_ENTRIES)
    }

    buildReport(state: StreamDebugState): Record<string, unknown> {
        const now = Date.now()

        return {
            format: "moonlight-web-debug-report",
            version: 1,
            generatedAt: new Date(now).toISOString(),
            streamStartedAt: new Date(this.startedAt).toISOString(),
            uptimeMs: now - this.startedAt,
            browser: {
                userAgent: navigator.userAgent,
                language: navigator.language,
                platform: navigator.platform,
                hardwareConcurrency: navigator.hardwareConcurrency,
                deviceMemoryGb: (navigator as Navigator & { deviceMemory?: number }).deviceMemory,
                online: navigator.onLine,
                visibilityState: document.visibilityState,
                screen: {
                    width: screen.width,
                    height: screen.height,
                    devicePixelRatio: window.devicePixelRatio,
                },
                viewport: {
                    width: window.innerWidth,
                    height: window.innerHeight,
                },
                secureContext: window.isSecureContext,
                crossOriginIsolated: window.crossOriginIsolated,
                network: describeNetworkInformation(),
            },
            stream: {
                hostId: state.hostId,
                appId: state.appId,
                streamerSize: state.streamerSize,
                desiredTransport: state.desiredTransport,
                transport: state.transport,
                failureReason: state.failureReason,
                stopped: state.stopped,
            },
            settings: state.settings,
            stats: {
                current: state.stats,
                history: state.statsHistory,
            },
            transportDebug: state.transportDebug,
            events: this.events,
            logs: this.logs,
        }
    }
}

export function debugReportFileName(date: Date = new Date()): string {
    const pad = (value: number) => value.toString().padStart(2, "0")
    const stamp = `${date.getFullYear()}${pad(date.getMonth() + 1)}${pad(date.getDate())}-${pad(date.getHours())}${pad(date.getMinutes())}${pad(date.getSeconds())}`
    return `moonlight-debug-${stamp}.json`
}

export function downloadJson(fileName: string, data: unknown) {
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" })
    const url = URL.createObjectURL(blob)

    const anchor = document.createElement("a")
    anchor.href = url
    anchor.download = fileName
    anchor.style.display = "none"
    document.body.appendChild(anchor)
    anchor.click()
    document.body.removeChild(anchor)

    setTimeout(() => URL.revokeObjectURL(url), 1000)
}
