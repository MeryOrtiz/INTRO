const axios = require("axios")
const faults = require("./faults")
const events = require("./events")

// Saltar la página de ngrok (opcional)
const HEADERS = { "ngrok-skip-browser-warning": "true" }

async function post(url, path, body, timeout) {
    const wait = faults.delay()
    if (wait) await new Promise(resolve => setTimeout(resolve, wait))

    const res = await axios.post(`${url}${path}`, body, { timeout, headers: HEADERS })
    return res.data
}

async function get(url, path, timeout) {
    const res = await axios.get(`${url}${path}`, { timeout, headers: HEADERS })
    return res.data
}

// Envio de un mensaje del protocolo de eleccion a un peer concreto.
// Devuelve null si el mensaje no salio (particion, pausa, drop o error de red):
// las estrategias tratan ese null como "no me consta que llegara", que es
// exactamente la incertidumbre real de un sistema distribuido.
async function send(peer, envelope, timeout) {
    if (!faults.canTalkTo(peer.id)) {
        events.emit({ kind: "blocked", to: peer.id || peer.url, type: envelope.type })
        return null
    }

    if (faults.shouldDrop()) {
        events.emit({ kind: "dropped", to: peer.id || peer.url, type: envelope.type })
        return null
    }

    events.count(envelope.type, "sent")

    if (envelope.type !== "__ping") {
        events.emit({ kind: "send", to: peer.id || peer.url, from: envelope.from.id, type: envelope.type, term: envelope.term })
    }

    try {
        return await post(peer.url, "/election/message", envelope, timeout)
    } catch (err) {
        const status = err.response ? err.response.status : null
        events.emit({ kind: "unreachable", to: peer.id || peer.url, type: envelope.type, status })
        return null
    }
}

module.exports = { send, post, get, HEADERS }
