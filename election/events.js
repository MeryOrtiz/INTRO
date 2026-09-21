const MAX = 500

let buffer = []
let seq = 0
let subscribers = new Set()

const counters = { protocol: 0, ping: 0, sent: 0, received: 0, byType: {} }

function emit(event) {
    const entry = { seq: ++seq, at: Date.now(), ...event }

    buffer.push(entry)
    if (buffer.length > MAX) buffer.shift()

    subscribers.forEach(res => {
        try {
            res.write(`data: ${JSON.stringify(entry)}\n\n`)
        } catch {
            subscribers.delete(res)
        }
    })

    return entry
}

// Los pings del detector de fallos NO cuentan como trafico del algoritmo:
// si los mezclamos, el contador deja de mostrar la diferencia entre
// el O(n^2) de bully y el O(n) del anillo, que es justo lo que se demuestra.
function count(type, direction) {
    counters[direction] = (counters[direction] || 0) + 1

    if (type === "__ping") {
        counters.ping++
        return
    }

    counters.protocol++
    counters.byType[type] = (counters.byType[type] || 0) + 1
}

function resetCounters() {
    counters.protocol = 0
    counters.ping = 0
    counters.sent = 0
    counters.received = 0
    counters.byType = {}
}

function history() {
    return buffer
}

function subscribe(res) {
    subscribers.add(res)
    return () => subscribers.delete(res)
}

function clear() {
    buffer = []
    resetCounters()
}

module.exports = { emit, count, counters, resetCounters, history, subscribe, clear }
