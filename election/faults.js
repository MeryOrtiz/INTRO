// Inyeccion de fallos para la demo. Todo esto es mentira controlada:
// el proceso sigue vivo, simplemente deja de hablar con quien le digamos.
const state = {
    paused: false,          // congelado: ni envia ni responde (crash simulado)
    blocked: new Set(),     // ids de peers incomunicados (particion)
    latency: 0,             // ms de retardo artificial
    jitter: 0,
    drop: 0,                // probabilidad 0..1 de perder el mensaje
    clockSkew: 0            // ms de desfase de reloj (para romper leases)
}

function now() {
    return Date.now() + state.clockSkew
}

function canTalkTo(peerId) {
    if (state.paused) return false
    if (peerId && state.blocked.has(String(peerId))) return false
    return true
}

function shouldDrop() {
    return state.drop > 0 && Math.random() < state.drop
}

function delay() {
    if (!state.latency && !state.jitter) return 0
    return state.latency + Math.floor(Math.random() * (state.jitter + 1))
}

function partition(ids) {
    state.blocked = new Set((ids || []).map(String))
}

function heal() {
    state.blocked = new Set()
    state.drop = 0
    state.latency = 0
    state.jitter = 0
    state.clockSkew = 0
}

function snapshot() {
    return {
        paused: state.paused,
        blocked: [...state.blocked],
        latency: state.latency,
        jitter: state.jitter,
        drop: state.drop,
        clockSkew: state.clockSkew
    }
}

module.exports = { state, now, canTalkTo, shouldDrop, delay, partition, heal, snapshot }
