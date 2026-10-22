import { DebugLogEntry } from "./debug_report"
import { STATS_LEVEL_LABELS, StatsLevel, StatValue, StreamStatsData } from "./stats"
import { RollingSummary, StageKey, StreamTelemetry, TelemetrySnapshot } from "./telemetry"

// "Stats for nerds" overlay. Levels:
// - minimal: one HUD line
// - medium: the latency breakdown plus the key numbers per section
// - max: everything, with graphs, relay counters, raw transport/pipe stats and a log tail

type Row = [string, string]

const LATENCY_STAGES: Array<{ key: StageKey, label: string, color: string, estimated?: boolean }> = [
    { key: "host", label: "host (sunshine)", color: "#f5a524" },
    { key: "hostToRelay", label: "host → relay (rtt/2)", color: "#a0a0a0", estimated: true },
    { key: "relayToClient", label: "relay → client", color: "#ff5c5c" },
    { key: "clientQueue", label: "client queue", color: "#c46bff" },
    { key: "decode", label: "decode", color: "#3fd07f" },
    { key: "render", label: "decoded → shown", color: "#36c5f0" },
]

const WEBRTC_STAGES: Array<{ key: StageKey, label: string, color: string, estimated?: boolean }> = [
    { key: "host", label: "host (sunshine)", color: "#f5a524" },
    { key: "hostToRelay", label: "host → relay (rtt/2)", color: "#a0a0a0", estimated: true },
    { key: "webrtcJitterBuffer", label: "jitter buffer", color: "#c46bff" },
    { key: "decode", label: "decode", color: "#3fd07f" },
    { key: "webrtcReceiveToDisplay", label: "received → shown", color: "#36c5f0" },
]

const GRAPHS: Array<{ key: StageKey, label: string, color: string, unit: string }> = [
    { key: "total", label: "end-to-end", color: "#ffffff", unit: "ms" },
    { key: "relayToClient", label: "relay → client", color: "#ff5c5c", unit: "ms" },
    { key: "frameInterval", label: "frame interval", color: "#f5a524", unit: "ms" },
    { key: "decode", label: "decode", color: "#3fd07f", unit: "ms" },
    { key: "render", label: "decoded → shown", color: "#36c5f0", unit: "ms" },
    { key: "frameBytes", label: "frame size", color: "#c46bff", unit: "KB" },
]

function ms(value: number | null | undefined, digits = 1): string {
    return value == null || !Number.isFinite(value) ? "–" : `${value.toFixed(digits)} ms`
}

function fmt(value: StatValue | null | undefined, key = ""): string {
    if (value == null) return "–"
    if (typeof value == "number") {
        if (key.endsWith("Ms")) return ms(value, 2)
        if (key.endsWith("Kbps")) return `${(value / 1000).toFixed(2)} Mbps`
        if (key.endsWith("Percent")) return `${value.toFixed(2)} %`
        return Number.isInteger(value) ? `${value}` : value.toFixed(2)
    }
    return value
}

function summaryText(summary: RollingSummary | undefined, scale = 1): string {
    if (!summary) return "–"
    return `${(summary.avg * scale).toFixed(1)} / ${(summary.p95 * scale).toFixed(1)} / ${(summary.max * scale).toFixed(1)}`
}

function el<K extends keyof HTMLElementTagNameMap>(tag: K, className?: string, text?: string): HTMLElementTagNameMap[K] {
    const element = document.createElement(tag)
    if (className) element.className = className
    if (text != null) element.textContent = text
    return element
}

export type StatsOverlayInput = {
    level: StatsLevel
    stats: StreamStatsData
    telemetry: StreamTelemetry | null
    transportName: string | null
    logs: DebugLogEntry[]
}

export class StatsOverlay {
    readonly root = el("div", "video-stats")
    private graphCanvases = new Map<StageKey, HTMLCanvasElement>()

    constructor() {
        this.root.hidden = true
    }

    update(input: StatsOverlayInput | null) {
        if (!input || input.level == "off") {
            this.root.hidden = true
            return
        }
        this.root.hidden = false
        this.root.dataset.level = input.level

        const snapshot = input.stats.telemetry
        const isWebRtc = input.transportName?.toLowerCase().includes("webrtc") ?? false

        this.root.replaceChildren()
        if (input.level == "minimal") {
            this.root.appendChild(el("div", "video-stats-hud", this.hudLine(input, snapshot, isWebRtc)))
            return
        }

        const header = el("div", "video-stats-header")
        header.appendChild(el("span", "video-stats-title", "stats for nerds"))
        header.appendChild(el("span", "video-stats-level", STATS_LEVEL_LABELS[input.level]))
        this.root.appendChild(header)

        const grid = el("div", "video-stats-grid")
        this.root.appendChild(grid)

        grid.appendChild(this.latencySection(snapshot, isWebRtc))
        grid.appendChild(this.section("stream", this.streamRows(input, snapshot)))
        grid.appendChild(this.section("network", this.networkRows(input.stats, snapshot, isWebRtc)))
        grid.appendChild(this.section("decoder / renderer", this.decoderRows(input.stats, snapshot, isWebRtc)))

        if (input.level != "max") {
            return
        }

        if (input.telemetry) {
            grid.appendChild(this.graphSection(input.telemetry))
        }
        if (snapshot?.relay) {
            grid.appendChild(this.section("relay (last second)", this.relayRows(snapshot)))
        }
        grid.appendChild(this.section("clock sync", this.clockRows(snapshot)))
        grid.appendChild(this.rawSection("transport (raw)", input.stats.transport))
        if (Object.keys(input.stats.video).length > 0) grid.appendChild(this.rawSection("video pipe", input.stats.video))
        if (Object.keys(input.stats.audio).length > 0) grid.appendChild(this.rawSection("audio pipe", input.stats.audio))
        grid.appendChild(this.section("pipelines", [
            ["video", input.stats.videoPipeline ?? "–"],
            ["audio", input.stats.audioPipeline ?? "–"],
        ], true))
        this.root.appendChild(this.logSection(input.logs))
    }

    private hudLine(input: StatsOverlayInput, snapshot: TelemetrySnapshot | null, isWebRtc: boolean): string {
        const transport = input.stats.transport
        const fps = snapshot && snapshot.rates.presentedFps > 0 ? snapshot.rates.presentedFps : Number(transport.currentFps ?? 0)
        const parts = [`${fps.toFixed(0)} fps`]
        const total = snapshot?.stages.total
        if (total && !isWebRtc) {
            parts.push(`e2e ${total.avg.toFixed(0)} ms (p95 ${total.p95.toFixed(0)})`)
        } else if (typeof transport.relayToClientRttMs == "number") {
            parts.push(`rtt ${transport.relayToClientRttMs.toFixed(0)} ms`)
        }
        const kbps = snapshot && snapshot.rates.receivedKbps > 0 ? snapshot.rates.receivedKbps : Number(transport.videoBitrateKbps ?? 0)
        if (kbps > 0) parts.push(`${(kbps / 1000).toFixed(1)} Mbps`)
        if (transport.codec) parts.push(`${transport.codec}`)
        if (input.transportName) parts.push(input.transportName)
        return parts.join(" · ")
    }

    private section(title: string, rows: Row[], wide = false): HTMLElement {
        const section = el("section", wide ? "video-stats-section video-stats-wide" : "video-stats-section")
        section.appendChild(el("h4", undefined, title))
        const table = el("table")
        for (const [key, value] of rows) {
            const tr = el("tr")
            tr.appendChild(el("th", undefined, key))
            tr.appendChild(el("td", undefined, value))
            table.appendChild(tr)
        }
        section.appendChild(table)
        return section
    }

    private rawSection(title: string, values: Record<string, StatValue>): HTMLElement {
        return this.section(title, Object.entries(values).map(([key, value]) => [key, fmt(value, key)]))
    }

    private latencySection(snapshot: TelemetrySnapshot | null, isWebRtc: boolean): HTMLElement {
        const section = el("section", "video-stats-section video-stats-wide")
        section.appendChild(el("h4", undefined, "latency breakdown (avg / p95 / max, last 5 s)"))

        const stages = isWebRtc ? WEBRTC_STAGES : LATENCY_STAGES
        const values = stages.map(stage => ({ ...stage, summary: snapshot?.stages[stage.key] }))
        const sum = values.reduce((acc, stage) => acc + Math.max(0, stage.summary?.avg ?? 0), 0)

        const bar = el("div", "video-stats-bar")
        for (const stage of values) {
            const avg = Math.max(0, stage.summary?.avg ?? 0)
            if (sum <= 0 || avg <= 0) continue
            const part = el("div")
            part.style.width = `${avg / sum * 100}%`
            part.style.background = stage.color
            part.title = `${stage.label}: ${ms(avg)}`
            bar.appendChild(part)
        }
        section.appendChild(bar)

        const table = el("table")
        for (const stage of values) {
            const tr = el("tr")
            const th = el("th")
            const swatch = el("span", "video-stats-swatch")
            swatch.style.background = stage.color
            th.appendChild(swatch)
            th.appendChild(document.createTextNode(stage.label + (stage.estimated ? " *" : "")))
            tr.appendChild(th)
            tr.appendChild(el("td", undefined, summaryText(stage.summary)))
            table.appendChild(tr)
        }
        const total = el("tr", "video-stats-total")
        total.appendChild(el("th", undefined, isWebRtc ? "sum of stages" : "end-to-end"))
        total.appendChild(el("td", undefined, isWebRtc || !snapshot?.stages.total ? `${sum.toFixed(1)} ms` : summaryText(snapshot.stages.total)))
        table.appendChild(total)
        section.appendChild(table)

        const notes: string[] = ["* estimated"]
        if (!isWebRtc && snapshot && !snapshot.clock.bestRttMs && snapshot.clock.samples == 0) {
            notes.push("relay → client needs clock sync (waiting for pongs)")
        } else if (!isWebRtc && snapshot?.clock.uncertaintyMs != null) {
            notes.push(`relay → client ±${snapshot.clock.uncertaintyMs.toFixed(1)} ms clock error`)
        }
        if (isWebRtc) {
            notes.push("webrtc hides relay → client per frame, see network rtt")
        }
        if (snapshot?.presentation == "canvas_draw") {
            notes.push("shown = canvas draw call, the compositor adds up to one more frame")
        }
        section.appendChild(el("div", "video-stats-note", notes.join(" · ")))
        return section
    }

    private streamRows(input: StatsOverlayInput, snapshot: TelemetrySnapshot | null): Row[] {
        const t = input.stats.transport
        const rows: Row[] = [
            ["transport", input.transportName ?? "–"],
            ["codec", fmt(t.codec)],
            ["resolution", fmt(t.resolution)],
        ]
        if (snapshot && snapshot.counters.framesReceived > 0) {
            rows.push(["fps recv / dec / shown", `${snapshot.rates.receivedFps.toFixed(0)} / ${snapshot.rates.decodedFps.toFixed(0)} / ${snapshot.rates.presentedFps.toFixed(0)}`])
            rows.push(["bitrate", `${(snapshot.rates.receivedKbps / 1000).toFixed(2)} Mbps`])
            rows.push(["frame interval", summaryText(snapshot.stages.frameInterval)])
            rows.push(["frame size KB", summaryText(snapshot.stages.frameBytes, 1 / 1000)])
            rows.push(["keyframes", `${snapshot.counters.keyframes}`])
            if (snapshot.lastFrameAgoMs != null) rows.push(["last frame", ms(snapshot.lastFrameAgoMs, 0) + " ago"])
        } else {
            rows.push(["fps", fmt(t.currentFps)])
            if (t.videoBitrateKbps != null) rows.push(["bitrate", fmt(t.videoBitrateKbps, "videoBitrateKbps")])
            if (snapshot && snapshot.rates.presentedFps > 0) rows.push(["fps shown", snapshot.rates.presentedFps.toFixed(0)])
        }
        return rows
    }

    private networkRows(stats: StreamStatsData, snapshot: TelemetrySnapshot | null, isWebRtc: boolean): Row[] {
        const t = stats.transport
        const rows: Row[] = [
            ["host ↔ relay rtt", `${fmt(t.hostToRelayRttMs, "Ms")} ± ${fmt(t.hostToRelayRttVarianceMs, "Ms")}`],
            ["relay ↔ client rtt", fmt(t.relayToClientRttMs, "Ms")],
        ]
        if (isWebRtc) {
            rows.push(["packet loss", fmt(t.packetLossPercent, "Percent")])
            rows.push(["packets lost / recv", `${fmt(t.packetsLost)} / ${fmt(t.packetsReceived)}`])
            rows.push(["jitter", fmt(t.jitterMs, "Ms")])
            rows.push(["nack / pli / fir", `${fmt(t.nackCount)} / ${fmt(t.pliCount)} / ${fmt(t.firCount)}`])
            if (t.availableIncomingBitrateKbps != null) rows.push(["est. bandwidth", fmt(t.availableIncomingBitrateKbps, "Kbps")])
            if (t.localCandidate) rows.push(["ice", `${t.localCandidate} → ${t.remoteCandidate ?? "?"}`])
        }
        if (snapshot) {
            rows.push(["frames lost (host → relay)", `${snapshot.relayHistory.reduce((acc, r) => acc + r.frame_index_gaps, 0)}`])
            if (!isWebRtc) {
                rows.push(["frames missing at client", `${snapshot.counters.frameIndexGaps}`])
                rows.push(["relay backlog drops", `${snapshot.relayHistory.reduce((acc, r) => acc + r.backlog_drops, 0)}`])
                if (snapshot.relay?.frames_in_flight != null) rows.push(["frames in flight", `${snapshot.relay.frames_in_flight}`])
            }
        }
        return rows
    }

    private decoderRows(stats: StreamStatsData, snapshot: TelemetrySnapshot | null, isWebRtc: boolean): Row[] {
        const t = stats.transport
        const rows: Row[] = []
        if (isWebRtc) {
            rows.push(["decoder", fmt(t.decoderImplementation)])
            rows.push(["hw decoder", fmt(t.powerEfficientDecoder)])
            rows.push(["decode / frame", fmt(t.decodeTimePerFrameMs, "Ms")])
            rows.push(["processing / frame", fmt(t.processingDelayPerFrameMs, "Ms")])
            rows.push(["assembly / frame", fmt(t.assemblyTimePerFrameMs, "Ms")])
            rows.push(["jitter buffer (target)", `${fmt(t.jitterBufferDelayMs, "Ms")} (${fmt(t.jitterBufferTargetMs, "Ms")})`])
            rows.push(["frames dec / dropped", `${fmt(t.framesDecoded)} / ${fmt(t.framesDropped)}`])
            rows.push(["freezes", `${fmt(t.freezeCount)} (${fmt(t.totalFreezesDurationMs, "Ms")})`])
        }
        if (snapshot) {
            if (!isWebRtc) {
                rows.push(["decode queue", summaryText(snapshot.stages.decodeQueue)])
                rows.push(["skipped (no keyframe)", `${snapshot.counters.droppedBeforeDecode}`])
                rows.push(["decoded, never shown", `${snapshot.counters.droppedBeforePresent}`])
                rows.push(["hidden-tab frames", `${snapshot.counters.gatedFrames}`])
            }
            rows.push(["decoder resets", `${snapshot.counters.decoderResets}`])
            rows.push(["idr requests (client)", `${snapshot.counters.clientIdrRequests}`])
            rows.push(["frames > 150 ms", `${snapshot.counters.lateFrames}`])
            rows.push(["presentation", snapshot.presentation ?? "–"])
        }
        return rows
    }

    private relayRows(snapshot: TelemetrySnapshot): Row[] {
        const r = snapshot.relay!
        return [
            ["frames / keyframes", `${r.frames} / ${r.keyframes}`],
            ["bitrate in", `${(r.bytes * 8 / 1000 / Math.max(1, r.interval_ms)).toFixed(2)} Mbps`],
            ["largest frame", `${(r.max_frame_bytes / 1000).toFixed(1)} KB`],
            ["host latency min / avg / max", `${ms(r.host_latency_min_ms)} / ${ms(r.host_latency_avg_ms)} / ${ms(r.host_latency_max_ms)}`],
            ["frames lost host → relay", `${r.frame_index_gaps}`],
            ["idr host / relay", `${r.host_idr_requests} / ${r.relay_idr_requests}`],
            ["backlog drops", `${r.backlog_drops}`],
            ["in flight", r.frames_in_flight == null ? "–" : `${r.frames_in_flight}`],
        ]
    }

    private clockRows(snapshot: TelemetrySnapshot | null): Row[] {
        const clock = snapshot?.clock
        return [
            ["samples", `${clock?.samples ?? 0}`],
            ["best rtt", ms(clock?.bestRttMs)],
            ["uncertainty", clock?.uncertaintyMs == null ? "–" : `±${clock.uncertaintyMs.toFixed(2)} ms`],
        ]
    }

    private graphSection(telemetry: StreamTelemetry): HTMLElement {
        const section = el("section", "video-stats-section video-stats-wide")
        section.appendChild(el("h4", undefined, "graphs (last 10 s)"))
        const graphs = el("div", "video-stats-graphs")
        for (const graph of GRAPHS) {
            const series = telemetry.series(graph.key)
            const scale = graph.unit == "KB" ? 1 / 1000 : 1
            const wrapper = el("div", "video-stats-graph")
            const max = series.reduce((acc, [, value]) => Math.max(acc, value * scale), 0)
            const last = series.length > 0 ? series[series.length - 1][1] * scale : null
            wrapper.appendChild(el("div", "video-stats-graph-label", `${graph.label}: ${last == null ? "–" : last.toFixed(1)} ${graph.unit} (max ${max.toFixed(1)})`))
            const canvas = this.graphCanvas(graph.key)
            this.drawGraph(canvas, series, scale, graph.color)
            wrapper.appendChild(canvas)
            graphs.appendChild(wrapper)
        }
        section.appendChild(graphs)
        return section
    }

    private graphCanvas(key: StageKey): HTMLCanvasElement {
        let canvas = this.graphCanvases.get(key)
        if (!canvas) {
            canvas = el("canvas")
            canvas.width = 240
            canvas.height = 48
            this.graphCanvases.set(key, canvas)
        }
        return canvas
    }

    private drawGraph(canvas: HTMLCanvasElement, series: Array<[number, number]>, scale: number, color: string) {
        const context = canvas.getContext("2d")
        if (!context) return
        const { width, height } = canvas
        context.clearRect(0, 0, width, height)
        context.fillStyle = "rgba(255,255,255,0.06)"
        context.fillRect(0, 0, width, height)
        if (series.length < 2) return

        const values = series.map(([, value]) => value * scale)
        const sorted = [...values].sort((a, b) => a - b)
        // Scale to p99 so one spike doesn't flatten the graph, spikes clip at the top.
        const top = Math.max(sorted[Math.floor(sorted.length * 0.99)] * 1.2, 1e-3)

        context.strokeStyle = color
        context.lineWidth = 1
        context.beginPath()
        for (let i = 0; i < series.length; i++) {
            const x = width - series[i][0] / 10_000 * width
            const y = height - Math.min(1, values[i] / top) * (height - 2) - 1
            if (i == 0) context.moveTo(x, y)
            else context.lineTo(x, y)
        }
        context.stroke()
    }

    private logSection(logs: DebugLogEntry[]): HTMLElement {
        const section = el("section", "video-stats-section video-stats-wide video-stats-log")
        section.appendChild(el("h4", undefined, "log (last 12)"))
        for (const entry of logs.slice(-12)) {
            const line = el("div", `video-stats-log-${entry.level}`)
            const time = new Date(entry.time).toISOString().substring(11, 23)
            line.textContent = `${time} ${entry.level.padEnd(5)} ${entry.category ? `[${entry.category}] ` : ""}${entry.line}`
            section.appendChild(line)
        }
        return section
    }
}
