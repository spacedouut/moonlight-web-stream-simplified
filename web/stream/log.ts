
export type LogMessageType = "fatal" | "ifErrorDescription" | "fatalDescription" | "recover" | "informError"

export type LogLevel = "debug" | "info" | "warn" | "error"

export type LogMessageInfo = {
    type?: LogMessageType
    /** Subsystem the message comes from, e.g. "transport" or "video". Shown and filterable in the stats log console. */
    category?: string
    level?: LogLevel
}

export type LogListener = (fullRawText: string, type: LogMessageType | null, info: LogMessageInfo) => void

export function logLevelOf(info: LogMessageInfo | undefined): LogLevel {
    if (info?.level) {
        return info.level
    }
    switch (info?.type) {
        case "fatal":
        case "fatalDescription":
            return "error"
        case "ifErrorDescription":
        case "informError":
            return "warn"
        case "recover":
            return "info"
        default:
            return "debug"
    }
}

export class Logger {

    constructor() { }

    debug(message: string, info?: LogMessageInfo) {
        this.callListeners(message, info ?? {})
    }

    private callListeners(message: string, info: LogMessageInfo) {
        for (const listener of this.infoListeners) {
            listener(message, info.type ?? null, info)
        }
    }

    private infoListeners: Array<LogListener> = []
    addInfoListener(listener: LogListener) {
        this.infoListeners.push(listener)
    }
    removeInfoListener(listener: LogListener) {
        const index = this.infoListeners.indexOf(listener)
        if (index != -1) {
            this.infoListeners.splice(index, 1)
        }
    }
}
