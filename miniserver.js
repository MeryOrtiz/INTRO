
const os = require("os")
const express = require("express")
const axios = require("axios")

const app = express()
app.use(express.json())

app.use((req, res, next) => {
    res.setHeader("Access-Control-Allow-Origin", "*")
    res.setHeader("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, x-server-token, ngrok-skip-browser-warning")
    if (req.method === "OPTIONS") return res.sendStatus(204)
    next()
})

// ============================================================================
// CONFIGURACION (argv > entorno)  -> distribucion real
// ============================================================================

const PORT = Number(process.argv[2] || process.env.PORT || 4000)
const HOST = process.env.HOST || "0.0.0.0"

// El NOMBRE es obligatorio: no se inventa ninguno automatico.
const NAME = String(process.argv[3] || process.env.NAME || "").trim()

if (!NAME) {
    console.error("")
    console.error("  Falta el NOMBRE del miniserver.")
    console.error("")
    console.error("  Uso:  node miniserver.js <PUERTO> <NOMBRE> [URL_PADRE] [URL_PROPIA]")
    console.error("  Ej.:  node miniserver.js 4000 alfa")
    console.error("        node miniserver.js 4000 alfa https://acetylic-unrelaxed-elle.ngrok-free.dev")
    console.error("")
    process.exit(1)
}

if (!/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,39}$/.test(NAME)) {
    console.error(`\n  Nombre invalido: '${NAME}'`)
    console.error("  Use 1-40 caracteres [a-zA-Z0-9._-] y empiece con letra o numero.\n")
    process.exit(1)
}

const HOST_INFO = {
    hostname: os.hostname(),
    platform: os.platform(),
    arch: os.arch(),
    node: process.version
}

function defaultSelfUrl() {
    const nets = os.networkInterfaces()
    const ip = Object.values(nets).flat().find(i => i && i.family === "IPv4" && !i.internal)
    return `http://${ip ? ip.address : "localhost"}:${PORT}`
}

// Configuracion viva del hijo
const state = {
    name: NAME,
    parentUrl: (process.argv[4] || process.env.PARENT_URL || "http://localhost:3000").replace(/\/+$/, ""),
    selfUrl: (process.argv[5] || process.env.SELF_URL || defaultSelfUrl()).replace(/\/+$/, ""),
    token: null,
    registered: false,
    pulseMs: Number(process.env.PULSE_MS || 5000),
    pulsing: true,
    pulsesSent: 0,
    pulsesFailed: 0,
    messagesSent: 0,
    messagesReceived: 0,
    startedAt: Date.now()
}

let pulseInterval = null

// ============================================================================
// ELECCION DE LIDER - coordinadores conocidos
// ============================================================================
// Cada respuesta del padre trae la lista de sus companeros y quien es el
// lider. Asi el hijo acaba conociendo a todo el cluster sin configurar nada,
// y sabe a quien preguntar cuando su padre desaparezca.

const clean = url => String(url || "").trim().replace(/\/+$/, "")
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))

const coordinators = new Set([state.parentUrl])
let hunting = false
let huntTimer = null

function rememberCluster(data) {
    if (!data) return
    for (const url of [...(data.peers || []), data.leader]) {
        if (url && !coordinators.has(clean(url))) {
            coordinators.add(clean(url))
            log("info", "coordinator-known", { url: clean(url) })
        }
    }
}

// ============================================================================
// OBSERVABILIDAD - log local con buffer
// ============================================================================

const logs = []
let logSeq = 0

function log(level, event, data = {}) {
    const entry = { id: ++logSeq, ts: Date.now(), level, event, data }
    logs.push(entry)
    if (logs.length > 200) logs.shift()
    console.log(`[${new Date(entry.ts).toLocaleTimeString()}] ${String(level).toUpperCase().padEnd(7)} ${event}`,
        Object.keys(data).length ? JSON.stringify(data) : "")
    return entry
}

const inbox = []   // mensajes recibidos desde el padre / otros hijos

// ============================================================================
// REGISTRO - alta en el padre con reintentos y resolucion de colision de nombre
// ============================================================================

const http = axios.create({
    timeout: 6000,
    headers: { "ngrok-skip-browser-warning": "true" }
})

async function register({ retries = 5 } = {}) {
    let hops = 0   // saltos de redireccion al lider (max 3 por si dos se apuntan entre si)

    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            const { data } = await http.post(`${state.parentUrl}/register`, {
                name: state.name,
                url: state.selfUrl,
                host: HOST_INFO,
                token: state.token || undefined
            }, {
                headers: state.token ? { "x-server-token": state.token } : {}
            })

            state.token = data.token || state.token
            state.registered = true
            if (data.suggestedPulseMs) state.pulseMs = data.suggestedPulseMs
            if (data.name) state.name = data.name
            rememberCluster(data)

            log("success", "registered", { name: state.name, parent: state.parentUrl, self: state.selfUrl })
            startPulsing()
            return { ok: true, data }

        } catch (error) {
            const status = error.response?.status
            const body = error.response?.data
            rememberCluster(body)

            // Ese padre no es el lider y nos dice quien lo es: saltamos alli
            if (status === 409 && body?.leader && !body?.suggestion && hops < 3) {
                hops++
                log("info", "redirect", { from: state.parentUrl, to: clean(body.leader) })
                state.parentUrl = clean(body.leader)
                attempt--
                continue
            }

            // Eleccion en curso o padre fuera de servicio: toca buscar al lider
            if (status === 503 || (status === 409 && !body?.suggestion)) {
                log("warn", "no-leader", { parent: state.parentUrl, error: body?.error })
                state.registered = false
                scheduleHunt()
                return { ok: false, error: body?.error }
            }

            // Colision de nombres: el padre sugiere uno libre y lo adoptamos
            if (status === 409 && body?.suggestion) {
                log("warn", "name-taken", { tried: state.name, using: body.suggestion })
                state.name = body.suggestion
                state.token = null
                continue
            }

            if (status === 400) {
                log("error", "register-rejected", { error: body?.error })
                return { ok: false, error: body?.error }
            }

            const waitMs = Math.min(1000 * 2 ** (attempt - 1), 15000)
            log("warn", "register-retry", { attempt, in: `${waitMs}ms`, error: error.message })
            state.registered = false
            if (attempt < retries) await new Promise(r => setTimeout(r, waitMs))
        }
    }

    log("error", "register-failed", { parent: state.parentUrl })
    // No se queda zombi: sigue buscando un padre que lo acepte
    scheduleHunt()
    return { ok: false, error: "no se pudo registrar en el padre" }
}

// ============================================================================
// ELECCION DE LIDER - buscar al nuevo lider cuando el padre desaparece
// ============================================================================

// Registrarse en otro padre. Si falla, se queda el anterior.
async function switchTo(url) {
    const previous = state.parentUrl
    state.parentUrl = clean(url)

    const result = await register({ retries: 1 })
    if (!result.ok) {
        state.parentUrl = previous
        return false
    }

    if (state.parentUrl !== previous) {
        log("success", "new-parent", { from: previous, to: state.parentUrl })
    }
    return true
}

// Pregunta a cada coordinador conocido quien manda ahora y se va con el
async function huntForLeader() {
    // Si le pidieron apagarse (/shutdown) no vuelve a engancharse solo
    if (hunting || !state.pulsing) return false
    hunting = true
    clearTimeout(huntTimer)

    // Los descartados en ESTA ronda: si el resto aun cree que manda el nodo
    // caido, no hay que insistir con el una y otra vez.
    const descartados = new Set()
    const candidates = [...new Set([state.parentUrl, ...coordinators])]

    try {
        for (const url of candidates) {
            if (descartados.has(url)) continue

            let info
            try {
                info = (await http.get(`${url}/election/state`, { timeout: 2500 })).data
            } catch {
                descartados.add(url)
                continue
            }

            rememberCluster({ peers: (info.peers || []).map(p => p.url) })

            // Un nodo congelado dice ser lider, pero no atiende a nadie
            if (info.faults?.paused) {
                descartados.add(url)
                continue
            }

            const leader = info.role === "leader" ? url : info.leaderUrl
            if (!leader || descartados.has(clean(leader))) continue

            if (await switchTo(leader)) return true
            descartados.add(clean(leader))
        }

        log("warn", "hunting-leader", { asked: candidates.length, result: "nadie sabe quien manda", retryInMs: 3000 })
        return false

    } finally {
        hunting = false
        if (!state.registered) scheduleHunt()
    }
}

function scheduleHunt(ms = 3000) {
    if (hunting || !state.pulsing) return
    clearTimeout(huntTimer)
    huntTimer = setTimeout(() => huntForLeader(), ms)
}

// ============================================================================
// TIMEOUT - envio de pulsos al padre (deteccion de caidas)
// ============================================================================

function startPulsing() {
    stopPulsing()
    if (!state.pulsing) return

    pulseInterval = setInterval(async () => {
        if (hunting) return   // mientras se busca lider no se molesta a nadie

        try {
            const { data } = await http.post(`${state.parentUrl}/pulse/${encodeURIComponent(state.name)}`)
            state.pulsesSent++
            rememberCluster(data)
            if (!state.registered) {
                state.registered = true
                log("success", "link-recovered", { parent: state.parentUrl })
            }
        } catch (error) {
            state.pulsesFailed++
            state.registered = false

            const status = error.response?.status
            const body = error.response?.data
            rememberCluster(body)

            if (status === 404) {
                // El padre no nos conoce (se reinicio o nos purgo) -> re-registro solo
                log("warn", "pulse-unknown", { action: "re-register" })
                register({ retries: 1 })
            } else if (status === 409 && body?.leader) {
                // El padre vive, pero ya no es el lider: nos dice a quien ir
                log("warn", "not-leader", { parent: state.parentUrl, leader: body.leader })
                switchTo(body.leader).then(ok => { if (!ok) huntForLeader() })
            } else {
                // Silencio, eleccion en curso o padre congelado: buscar al lider
                log("warn", "pulse-failed", { error: body?.error || error.message })
                huntForLeader()
            }
        }
    }, state.pulseMs)

    log("info", "pulsing", { everyMs: state.pulseMs })
}

function stopPulsing() {
    if (pulseInterval) {
        clearInterval(pulseInterval)
        pulseInterval = null
    }
}

// ============================================================================
// RUTAS
// ============================================================================

app.get("/", (req, res) => {
    res.json({
        name: state.name,
        role: "miniserver",
        port: PORT,
        selfUrl: state.selfUrl,
        parentUrl: state.parentUrl,
        registered: state.registered,
        host: HOST_INFO
    })
})

// Sonda que usa el padre para la deteccion ACTIVA de caidas
app.get("/health", (req, res) => {
    res.json({
        status: "ok",
        name: state.name,
        uptimeMs: Date.now() - state.startedAt,
        pulsing: state.pulsing,
        registered: state.registered,
        host: HOST_INFO
    })
})

// ------------------------ COMUNICACION POR MENSAJES ------------------------

// hijo -> padre
// Cuerpo esperado:  { "name": "quien envia", "message": "texto" }
app.post("/send-message", async (req, res) => {
    const name = String(req.body?.name || "").trim().slice(0, 40)
    const message = req.body?.message

    if (!name) {
        return res.status(400).json({
            error: "El campo 'name' es obligatorio",
            ejemplo: { name: state.name, message: "hola" }
        })
    }

    if (typeof message !== "string" || !message.trim()) {
        return res.status(400).json({
            error: "El campo 'message' es obligatorio",
            ejemplo: { name: state.name, message: "hola" }
        })
    }

    // 'from' es la firma que escribio el usuario en el campo 'name'
    // x-worker: el padre sabe que es uno de sus hijos y que solo debe
    // atenderlo si es el lider
    const entregar = () => http.post(
        `${state.parentUrl}/send-message/${encodeURIComponent(state.name)}`,
        { message, from: name },
        { headers: { "x-worker": state.name } }
    )

    try {
        let respuesta

        try {
            respuesta = await entregar()
        } catch (error) {
            const status = error.response?.status
            const body = error.response?.data

            if (status === 404) {
                // El padre no nos conoce (se reinicio o nos purgo por timeout):
                // nos damos de alta otra vez y reintentamos una sola vez, para que
                // el usuario solo tenga que mandar 'name' y 'message'.
                log("warn", "not-registered", { action: "re-register" })
                await register({ retries: 1 })
            } else if (status === 409 && body?.leader) {
                // El padre dejo de ser lider entre un pulso y este mensaje
                log("warn", "not-leader", { parent: state.parentUrl, leader: body.leader })
                if (!(await switchTo(body.leader))) throw error
            } else if (status === 503 || !error.response) {
                // Eleccion en curso o padre caido: buscamos al lider y reintentamos
                if (!(await huntForLeader())) throw error
            } else {
                throw error
            }

            respuesta = await entregar()
        }

        state.messagesSent++
        log("info", "message-sent", { from: name, to: "server", message })
        res.json({ status: "success", from: name, serverResponse: respuesta.data })

    } catch (error) {
        const detail = error.response?.data?.error || error.message
        log("error", "message-error", { error: detail })
        res.status(502).json({ error: "No se pudo entregar el mensaje al server", detail })
    }
})

// Buzon: aqui llega lo que el padre nos envia (directo o broadcast)
app.post("/inbox", (req, res) => {
    const msg = { ...req.body, receivedAt: Date.now() }
    inbox.push(msg)
    if (inbox.length > 100) inbox.shift()
    state.messagesReceived++

    log("success", "message-in", { from: msg.from, message: msg.message })
    res.json({ status: "ok", by: state.name })
})

app.get("/inbox", (req, res) => res.json(inbox))

// ------------------------------ CONFIGURACION ------------------------------

app.get("/config", (req, res) => {
    res.json({
        name: state.name,
        parentUrl: state.parentUrl,
        selfUrl: state.selfUrl,
        pulseMs: state.pulseMs,
        pulsing: state.pulsing,
        registered: state.registered,
        hunting,
        coordinators: [...coordinators]
    })
})

// ------------------------- CONTROL DE PULSOS -------------------------------

app.post("/shutdown", (req, res) => {
    state.pulsing = false
    stopPulsing()
    log("warn", "pulses-stopped", { name: state.name })
    res.json({ message: `${state.name} dejo de enviar pulsos` })
})

app.post("/resume", (req, res) => {
    state.pulsing = true
    startPulsing()
    log("success", "pulses-resumed", { name: state.name })
    res.json({ message: `${state.name} reanudo los pulsos` })
})

// ------------------------------ OBSERVABILIDAD -----------------------------

app.get("/logs", (req, res) => {
    const limit = Math.min(Number(req.query.limit) || 100, 200)
    res.json(logs.slice(-limit))
})

app.get("/stats", (req, res) => {
    res.json({
        name: state.name,
        uptimeMs: Date.now() - state.startedAt,
        parentUrl: state.parentUrl,
        selfUrl: state.selfUrl,
        registered: state.registered,
        pulsing: state.pulsing,
        pulsesSent: state.pulsesSent,
        pulsesFailed: state.pulsesFailed,
        messagesSent: state.messagesSent,
        messagesReceived: state.messagesReceived,
        inbox: inbox.length,
        coordinators: [...coordinators],
        host: HOST_INFO
    })
})

// ============================================================================
// ARRANQUE Y APAGADO LIMPIO
// ============================================================================

const server = app.listen(PORT, HOST, async error => {
    // Express 5 llama aqui tambien si el puerto ya esta ocupado
    if (error) {
        console.error(`\n  No se pudo arrancar en el puerto ${PORT}: ${error.code === "EADDRINUSE" ? "ya esta en uso" : error.message}\n`)
        process.exit(1)
    }

    console.log("--------------------------------------------------------")
    console.log(` MINISERVER '${state.name}' en ${HOST}:${PORT}`)
    console.log(` Self URL  : ${state.selfUrl}`)
    console.log(` Parent    : ${state.parentUrl}`)
    console.log(` Host      : ${HOST_INFO.hostname} (${HOST_INFO.platform}/${HOST_INFO.arch})`)
    console.log("--------------------------------------------------------")

    await register()
})

async function shutdown(signal) {
    log("warn", "shutting-down", { signal })
    stopPulsing()
    try {
        await http.delete(`${state.parentUrl}/unregister/${encodeURIComponent(state.name)}`, {
            headers: state.token ? { "x-server-token": state.token } : {}
        })
    } catch { /* el padre ya lo detectara por timeout */ }
    server.close(() => process.exit(0))
    setTimeout(() => process.exit(0), 2000)
}

process.on("SIGINT", () => shutdown("SIGINT"))
process.on("SIGTERM", () => shutdown("SIGTERM"))
