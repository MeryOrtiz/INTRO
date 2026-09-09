// ============================================================================
// SERVER (PADRE)
// ----------------------------------------------------------------------------
// Cubre:
//   1. Multiplicidad ................ N hijos simultaneos, N clientes web
//   2. Comunicacion por mensajes .... hijo->padre, padre->hijo, hijo->hijo,
//                                     broadcast y envio a una URL libre
//   3. Distribucion fisica real ..... escucha en 0.0.0.0, CORS, URLs configurables
//   4. Registro por nombre .......... validacion, colisiones, token de dueno
//   5. Timeout ...................... pulse + sonda activa, estados ALIVE/SUSPECT/DEAD
//   6. Observabilidad ............... log de eventos, metricas, stream SSE en vivo
// ============================================================================

const os = require("os")
const net = require("net")
const path = require("path")
const { spawn } = require("child_process")
const express = require("express")
const axios = require("axios")

const app = express()

// ============================================================================
// 3. DISTRIBUCION FISICA REAL - configuracion por variables de entorno
// ============================================================================
// node server.js
// El PUERTO es fijo (3000) para que el tunel de ngrok siempre apunte al mismo
// sitio: `ngrok http 3000`. El resto (HOST, PUBLIC_URL, TIMEOUT, ADMIN_KEY)
// si se puede pasar por entorno, asi el mismo codigo corre en Windows,
// Linux o Mac y en cualquier maquina.

const PORT = 3000

const config = {
    port: PORT,
    host: "0.0.0.0",                              // 0.0.0.0 = accesible desde otras maquinas
    publicUrl: process.env.PUBLIC_URL || null,     // URL publica (ngrok, IP LAN, dominio)
    timeout: Number(process.env.TIMEOUT || 15000), // sin pulso -> SUSPECT
    checkInterval: Number(process.env.CHECK_INTERVAL || 5000),
    probeInterval: Number(process.env.PROBE_INTERVAL || 10000),
    deadRetention: Number(process.env.DEAD_RETENTION || 60000),
    adminKey: process.env.ADMIN_KEY || "admin",
    childHost: process.env.CHILD_HOST || "localhost", // host con el que se registran los hijos lanzados
    basePort: Number(process.env.BASE_PORT || 4000)   // primer puerto que se prueba al lanzar
}

const STARTED_AT = Date.now()

// ----------------------------------------------------------------------------
// NGROK: la URL publica se detecta sola
// ----------------------------------------------------------------------------
// Cuando la peticion llega por un tunel (ngrok, cloudflare, un proxy), esta
// trae las cabeceras x-forwarded-*. De ahi sacamos la URL publica real, sin
// que haya que configurar nada al encender el tunel. PUBLIC_URL sigue
// sirviendo como valor fijo si se prefiere.

let tunnelUrl = null

function detectPublicUrl(req) {
    const proto = (req.get("x-forwarded-proto") || "").split(",")[0].trim()
    const host = (req.get("x-forwarded-host") || "").split(",")[0].trim()

    if (proto && host) {
        const url = `${proto}://${host}`
        if (url !== tunnelUrl) {
            tunnelUrl = url
            log("success", "tunnel-detected", { publicUrl: url })
        }
        return url
    }

    return config.publicUrl || tunnelUrl
}

const HOST_INFO = {
    hostname: os.hostname(),
    platform: os.platform(),
    arch: os.arch(),
    node: process.version
}

// ============================================================================
// 7. OBSERVABILIDAD - log de eventos en memoria + stream en vivo (SSE)
// ============================================================================

const LOG_LIMIT = 500
const logs = []
let logSeq = 0

const sseClients = new Set()

// Empuja cualquier evento a los dashboards conectados
function emit(payload) {
    for (const client of sseClients) {
        try {
            client.write(`data: ${JSON.stringify(payload)}\n\n`)
        } catch {
            sseClients.delete(client)
        }
    }
}

function log(level, event, data = {}) {
    const entry = {
        kind: "log",
        id: ++logSeq,
        ts: Date.now(),
        level,                 // info | warn | error | success
        event,                 // register, pulse-lost, spawn, message, ...
        data
    }

    logs.push(entry)
    if (logs.length > LOG_LIMIT) logs.shift()

    const tag = String(level).toUpperCase().padEnd(7)
    console.log(`[${new Date(entry.ts).toISOString()}] ${tag} ${event} ${JSON.stringify(data)}`)

    emit(entry)
    return entry
}

// Contadores globales para /metrics
const metrics = {
    registers: 0,
    reRegisters: 0,
    nameConflicts: 0,
    pulses: 0,
    messagesIn: 0,
    messagesRouted: 0,
    messagesFailed: 0,
    broadcasts: 0,
    timeouts: 0,
    probesFailed: 0
}

// ============================================================================
// MIDDLEWARES DE EXPRESS
// ============================================================================

app.use(express.json())

// CORS: permite que un dashboard o un hijo en OTRA maquina consuma esta API
app.use((req, res, next) => {
    res.setHeader("Access-Control-Allow-Origin", "*")
    res.setHeader("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, x-server-token, x-admin-key, ngrok-skip-browser-warning")
    if (req.method === "OPTIONS") return res.sendStatus(204)
    next()
})

// Traza de todas las peticiones (observabilidad)
app.use((req, res, next) => {
    if (req.path === "/events") return next() // el stream no se loguea en cada tick
    res.on("finish", () => {
        if (res.statusCode >= 400) {
            log("warn", "http", { method: req.method, path: req.originalUrl, status: res.statusCode })
        }
    })
    next()
})

app.use(express.static("public"))

// JSON invalido
app.use((err, req, res, next) => {
    if (err && err.type === "entity.parse.failed") {
        return res.status(400).json({ error: "JSON invalido" })
    }
    next(err)
})

// ============================================================================
// 4. SERVICIO DE NOMBRES - registro robusto
// ============================================================================
// registry: clave = nombre normalizado (minusculas) -> entrada
// Reglas de robustez:
//   - formato validado (2-40 chars, letras/numeros/. _ -)
//   - nombres reservados bloqueados
//   - colision con un hijo VIVO -> 409 + sugerencia de nombre libre
//   - colision con un hijo CAIDO -> takeover permitido (se registra en el log)
//   - cada hijo recibe un token; solo el dueno (o el admin) puede mutar su registro

const registry = new Map()

const NAME_RE = /^[a-zA-Z0-9][a-zA-Z0-9._-]{1,39}$/
const RESERVED = new Set(["admin", "all", "broadcast", "middleware", "server", "null", "undefined", "api"])

function normalize(name) {
    return String(name || "").trim().toLowerCase()
}

function validateName(name) {
    const raw = String(name || "").trim()
    if (!raw) return { ok: false, error: "El nombre es obligatorio" }
    if (!NAME_RE.test(raw)) {
        return { ok: false, error: "Nombre invalido: use 2-40 caracteres [a-zA-Z0-9._-] y empiece con letra o numero" }
    }
    if (RESERVED.has(normalize(raw))) {
        return { ok: false, error: `El nombre '${raw}' esta reservado` }
    }
    return { ok: true, key: normalize(raw), display: raw }
}

function validateUrl(url) {
    const raw = String(url || "").trim().replace(/\/+$/, "")
    if (!raw) return { ok: false, error: "La URL es obligatoria" }
    try {
        const parsed = new URL(raw)
        if (!/^https?:$/.test(parsed.protocol)) {
            return { ok: false, error: "La URL debe usar http o https" }
        }
        return { ok: true, url: raw }
    } catch {
        return { ok: false, error: "URL malformada (ej: http://192.168.1.20:4000)" }
    }
}

function suggestName(key) {
    let i = 2
    while (registry.has(`${key}-${i}`)) i++
    return `${key}-${i}`
}

function makeToken() {
    return Math.random().toString(36).slice(2) + Date.now().toString(36)
}

// El dueno del registro (token) o el admin pueden modificarlo
function authorize(req, entry) {
    const token = req.get("x-server-token") || req.body?.token
    const admin = req.get("x-admin-key") || req.body?.adminKey
    if (admin && admin === config.adminKey) return true
    if (token && token === entry.token) return true
    return false
}

// ============================================================================
// 5. TIMEOUT - estados derivados del ultimo pulso
// ============================================================================
// ALIVE   : pulso reciente
// SUSPECT : sin pulso > timeout        (posible caida)
// DEAD    : sin pulso > timeout * 2    (caida confirmada)

function computeStatus(entry, now = Date.now()) {
    const age = now - entry.lastPulse
    if (age <= config.timeout) return "ALIVE"
    if (age <= config.timeout * 2) return "SUSPECT"
    return "DEAD"
}

function publicEntry(entry, now = Date.now()) {
    const proc = serverProcesses.get(entry.key)
    return {
        managed: Boolean(proc),        // lanzado desde el panel por el server
        pid: proc ? proc.pid : null,
        name: entry.display,
        key: entry.key,
        url: entry.url,
        status: entry.status,
        hasPulse: entry.status === "ALIVE",
        lastHeartbeat: entry.lastPulse,
        secondsSincePulse: Math.round((now - entry.lastPulse) / 1000),
        pulses: entry.pulses,
        missed: entry.missed,
        registeredAt: entry.registeredAt,
        uptimeMs: now - entry.registeredAt,
        host: entry.host,
        reachable: entry.reachable,
        latencyMs: entry.latencyMs,
        urlHistory: entry.urlHistory,
        messages: entry.messages.length,
        lastMessage: entry.messages.length ? entry.messages[entry.messages.length - 1] : null
    }
}

// ============================================================================
// RUTAS - SERVICIO DE NOMBRES
// ============================================================================

// Registro / re-registro de un hijo
app.post("/register", (req, res) => {
    const { name, url, host, force } = req.body || {}

    const n = validateName(name)
    if (!n.ok) return res.status(400).json({ error: n.error })

    const u = validateUrl(url)
    if (!u.ok) return res.status(400).json({ error: u.error })

    const now = Date.now()
    const existing = registry.get(n.key)

    if (existing) {
        const owner = authorize(req, existing)
        const alive = computeStatus(existing, now) === "ALIVE"

        // Colision real: alguien mas intenta usar un nombre vivo
        if (alive && !owner && existing.url !== u.url && !force) {
            metrics.nameConflicts++
            const suggestion = suggestName(n.key)
            log("warn", "name-conflict", { name: n.display, ownerUrl: existing.url, tried: u.url, suggestion })
            return res.status(409).json({
                error: `El nombre '${n.display}' ya esta en uso por un servidor activo`,
                suggestion,
                hint: "Use otro nombre, o envie force:true / x-admin-key para tomar el nombre"
            })
        }

        // Re-registro del mismo hijo (reinicio, cambio de URL, takeover de un muerto)
        if (existing.url !== u.url) {
            existing.urlHistory.push({ from: existing.url, to: u.url, ts: now, reason: "re-register" })
            log("info", "name-rebind", { name: n.display, from: existing.url, to: u.url, wasAlive: alive })
        }

        existing.display = n.display
        existing.url = u.url
        existing.lastPulse = now
        existing.status = "ALIVE"
        existing.missed = 0
        existing.host = host || existing.host
        metrics.reRegisters++

        log("success", "re-register", { name: n.display, url: u.url })
        return res.json({
            message: "servidor re-registrado",
            name: existing.display,
            token: existing.token,
            timeout: config.timeout,
            suggestedPulseMs: Math.floor(config.timeout / 3)
        })
    }

    // Alta nueva
    const entry = {
        key: n.key,
        display: n.display,
        url: u.url,
        token: makeToken(),
        registeredAt: now,
        lastPulse: now,
        status: "ALIVE",
        pulses: 0,
        missed: 0,
        host: host || null,
        reachable: null,
        latencyMs: null,
        deadAt: null,
        urlHistory: [],
        messages: []
    }

    registry.set(n.key, entry)
    metrics.registers++
    log("success", "register", { name: n.display, url: u.url, host: entry.host })

    res.json({
        message: "servidor registrado",
        name: entry.display,
        token: entry.token,
        timeout: config.timeout,
        suggestedPulseMs: Math.floor(config.timeout / 3)
    })
})

// Baja explicita
app.delete("/unregister/:name", (req, res) => {
    const entry = registry.get(normalize(req.params.name))
    if (!entry) return res.status(404).json({ error: "server not found" })
    if (!authorize(req, entry)) return res.status(403).json({ error: "no autorizado (token o x-admin-key)" })

    registry.delete(entry.key)
    log("warn", "unregister", { name: entry.display })
    res.json({ message: `${entry.display} dado de baja` })
})

// ============================================================================
// 5. TIMEOUT - recepcion de pulsos
// ============================================================================

function handlePulse(req, res) {
    const key = normalize(req.params.name)
    const entry = registry.get(key)

    // Si el padre se reinicio o el hijo fue purgado -> le pedimos re-registrarse
    if (!entry) {
        log("warn", "pulse-unknown", { name: req.params.name })
        return res.status(404).json({
            error: "server not registered",
            action: "re-register"
        })
    }

    const now = Date.now()
    if (entry.status !== "ALIVE") {
        log("success", "recovered", { name: entry.display, downMs: now - entry.lastPulse })
    }

    entry.lastPulse = now
    entry.status = "ALIVE"
    entry.missed = 0
    entry.deadAt = null
    entry.pulses++
    metrics.pulses++

    res.json({
        message: "pulse received",
        name: entry.display,
        nextPulseMs: Math.floor(config.timeout / 3)
    })
}

app.post("/pulse/:name", handlePulse)
app.post("/heartbeat/:name", handlePulse) // alias historico

// ============================================================================
// 2. COMUNICACION POR MENSAJES
// ============================================================================

let messageSeq = 0
const messageLog = []   // historial global (ultimos 300)

function recordMessage(msg) {
    messageLog.push(msg)
    if (messageLog.length > 300) messageLog.shift()

    // Se cuelga tambien del emisor, si es un nodo registrado, para poder
    // mostrar en el panel que esta enviando cada quien. Si el mensaje viene
    // firmado con otro nombre, se cuelga del nodo por el que entro ('via').
    const sender = registry.get(normalize(msg.via || msg.from))
    if (sender) {
        sender.messages.push(msg)
        if (sender.messages.length > 100) sender.messages.shift()
    }

    return msg
}

// hijo -> padre
app.post("/send-message/:name", (req, res) => {
    const entry = registry.get(normalize(req.params.name))
    if (!entry) return res.status(404).json({ error: "server not registered" })

    const text = req.body?.message
    if (typeof text !== "string" || !text.trim()) {
        return res.status(400).json({ error: "El campo 'message' es obligatorio" })
    }

    // El hijo puede firmar el mensaje con el nombre que escribio el usuario;
    // si no manda firma, vale su nombre registrado.
    const signature = String(req.body?.from || "").trim().slice(0, 40) || entry.display

    const msg = recordMessage({
        id: ++messageSeq,
        from: signature,
        to: "server",
        server: entry.display,     // el nodo por el que entro realmente
        via: entry.display,
        message: text,
        timestamp: Date.now()
    })

    metrics.messagesIn++

    log("info", "message-in", { from: signature, via: entry.display, message: text })
    res.json({ status: "ok", received: true, id: msg.id, from: signature })
})

// --------------------------------------------------------------------------
// Buzon publico: cualquiera puede escribirle a ESTE server con { name, message }
// --------------------------------------------------------------------------
// Es la otra mitad del acuerdo: si nosotros entregamos con ese cuerpo, aqui
// tambien lo aceptamos. Sirve para que un companero apunte a nuestra URL
// (con /inbox, con /send-message o pelada) y el mensaje aparezca en el panel.

function recibirDeFuera(req, res) {
    const name = String(req.body?.name || req.body?.from || "").trim().slice(0, 40)
    const text = req.body?.message

    if (!name) {
        return res.status(400).json({
            error: "El campo 'name' es obligatorio",
            ejemplo: { name: "tu-nombre", message: "hola" }
        })
    }

    if (typeof text !== "string" || !text.trim()) {
        return res.status(400).json({
            error: "El campo 'message' es obligatorio",
            ejemplo: { name, message: "hola" }
        })
    }

    const msg = recordMessage({
        id: ++messageSeq,
        from: name,
        to: "server",
        server: name,
        message: text,
        timestamp: Date.now(),
        external: true
    })

    metrics.messagesIn++
    log("success", "message-in", { from: name, via: "externo", message: text })
    res.json({ status: "ok", received: true, id: msg.id, from: name })
}

app.post("/inbox", recibirDeFuera)
app.post("/send-message", recibirDeFuera)
app.post("/mensaje", recibirDeFuera)
app.post("/", recibirDeFuera)   // por si el emisor manda a la URL pelada


// padre -> hijo  /  hijo -> hijo (ruteo por NOMBRE, no por URL)
app.post("/route/:to", async (req, res) => {
    const target = registry.get(normalize(req.params.to))
    if (!target) return res.status(404).json({ error: "destino no registrado", to: req.params.to })

    const from = req.body?.from || "server"
    const text = req.body?.message
    if (typeof text !== "string" || !text.trim()) {
        return res.status(400).json({ error: "El campo 'message' es obligatorio" })
    }

    const msg = recordMessage({
        id: ++messageSeq,
        from,
        to: target.display,
        server: from,
        message: text,
        timestamp: Date.now()
    })

    try {
        const { data } = await axios.post(`${target.url}/inbox`, msg, {
            timeout: 5000,
            headers: { "ngrok-skip-browser-warning": "true" }
        })
        metrics.messagesRouted++
        msg.delivered = true
        log("success", "message-routed", { from, to: target.display, message: text })
        return res.json({ status: "delivered", id: msg.id, response: data })
    } catch (error) {
        error.message = describeDeliveryError(error)
        metrics.messagesFailed++
        msg.delivered = false
        msg.error = error.message
        log("error", "message-failed", { from, to: target.display, error: error.message })
        return res.status(502).json({ error: "No se pudo entregar el mensaje", detail: error.message })
    }
})

// padre -> TODOS los hijos vivos
app.post("/broadcast", async (req, res) => {
    const text = req.body?.message
    if (typeof text !== "string" || !text.trim()) {
        return res.status(400).json({ error: "El campo 'message' es obligatorio" })
    }
    const from = req.body?.from || "server"

    const targets = [...registry.values()].filter(e => e.status === "ALIVE")
    metrics.broadcasts++

    const results = await Promise.all(targets.map(async target => {
        const msg = recordMessage({
            id: ++messageSeq,
            from,
            to: target.display,
            server: from,
            message: text,
            timestamp: Date.now(),
            broadcast: true
        })
        try {
            await axios.post(`${target.url}/inbox`, msg, {
                timeout: 5000,
                headers: { "ngrok-skip-browser-warning": "true" }
            })
            metrics.messagesRouted++
            msg.delivered = true
            return { to: target.display, delivered: true }
        } catch (error) {
            metrics.messagesFailed++
            msg.delivered = false
            return { to: target.display, delivered: false, error: error.message }
        }
    }))

    log("info", "broadcast", { from, message: text, targets: results.length })
    res.json({ sent: results.length, results })
})

// --------------------------------------------------------------------------
// Envio a una URL LIBRE (sin pasar por el servicio de nombres)
// --------------------------------------------------------------------------
// Permite hablarle a un destino que no esta registrado: otro server,
// un hijo de otro equipo, una URL de ngrok... Si la URL no trae ruta, se
// entrega en /inbox.

// Traduce el fallo de una entrega a algo legible. Los tuneles (ngrok) no
// responden JSON cuando algo va mal: devuelven una pagina HTML de error, y
// "Request failed with status code 404" no le dice nada a nadie.
// Mensajes de ayuda por codigo de ngrok. Todos estos fallos son del DESTINO,
// no de este server: el tunel contesta, pero detras no hay nadie o la URL ya
// no vale. Se explican en cristiano para no quedarse mirando un codigo.
const AYUDA_NGROK = {
    ERR_NGROK_8012: "El tunel esta vivo pero no hay nadie detras: el problema esta en la maquina de destino, no aqui. " +
        "O ese server no esta corriendo, o su ngrok apunta a un puerto distinto del que usa el server " +
        "(el server en 3000 necesita 'ngrok http 3000'). " +
        "Si la URL es de un companero, no hay nada que puedas hacer desde tu panel: que la abra el en su navegador y vera el mismo error.",
    ERR_NGROK_3200: "El tunel de ngrok esta apagado o esa URL ya no existe. " +
        "Pide la URL nueva: cada vez que se reinicia ngrok cambia.",
    ERR_NGROK_6024: "El tunel existe pero no hay nada escuchando detras.",
    ERR_NGROK_3004: "El tunel existe pero no hay nada escuchando detras.",
    ERR_NGROK_3202: "Ese tunel pide autenticacion y no la tenemos.",
    ERR_NGROK_6022: "La cuenta de ngrok del destino excedio su limite de peticiones."
}

// Un fallo del TUNEL (no del camino) no se arregla probando otra ruta: ngrok
// contesta lo mismo en todas. Se reconoce porque el cuerpo trae su codigo de
// error, aunque venga disfrazado de 404.
function esFalloDeTunel(error) {
    const data = error?.response?.data
    return typeof data === "string" && /ERR_NGROK_[0-9]+/.test(data)
}

function describeDeliveryError(error) {
    if (!error) return "No se pudo entregar el mensaje"

    const data = error.response?.data
    const body = typeof data === "string" ? data : ""

    const ngrok = body.match(/ERR_NGROK_\d+/)
    if (ngrok) {
        const codigo = ngrok[0]
        const ayuda = AYUDA_NGROK[codigo]
        return ayuda ? `${ayuda} (${codigo})` : `ngrok rechazo la peticion (${codigo})`
    }

    if (/^\s*<(!doctype|html)/i.test(body)) {
        return "El destino devolvio una pagina HTML, no un nodo. Revisa que la URL sea la del server o del miniserver."
    }

    switch (error.code) {
        case "ECONNREFUSED": return "No hay nada escuchando en esa direccion"
        case "ENOTFOUND": return "Ese dominio no existe"
        case "ETIMEDOUT":
        case "ECONNABORTED": return "El destino no respondio a tiempo"
    }

    const detalle = error.response?.data?.error
    if (detalle) return detalle

    const status = error.response?.status
    if (status) return `El destino respondio ${status} y no acepto el mensaje`

    return error.message
}

// Un nodo de verdad responde JSON. Si llega HTML, el "200" es de la pagina
// intermedia del tunel, no del miniserver.
function looksLikeHtml(data) {
    return typeof data === "string" && /^\s*<(!doctype|html)/i.test(data)
}

// Rutas donde suele escucharse un mensaje. No hay una sola convencion: cada
// equipo llamo distinto a su endpoint, asi que si la URL no trae ruta se
// prueban todas hasta que una acepte el cuerpo { name, message }.
const RUTAS_ENTREGA = [
    "/inbox", "/send-message", "/mensaje", "/mensajes", "/messages",
    "/api/mensajes", "/api/messages", "/api/send-message", "/api/inbox",
    "/enviar", "/send", "/chat", "/"
]

function deliveryTargets(rawUrl) {
    const parsed = new URL(rawUrl)

    // Si quien envia escribio una ruta concreta, se respeta tal cual
    if (parsed.pathname && parsed.pathname !== "/") {
        return [parsed.toString().replace(new RegExp("/+$"), "")]
    }

    return RUTAS_ENTREGA.map(ruta => ruta === "/" ? parsed.origin : parsed.origin + ruta)
}

// --------------------------------------------------------------------------
// Deducir la ruta leyendo la propia pagina del destino
// --------------------------------------------------------------------------
// Cuando el enlace de un companero es una PAGINA (no un nodo), esa pagina
// tiene un formulario que manda mensajes, y en su JavaScript esta escrita la
// ruta exacta que su server escucha. Si ninguna ruta conocida sirvio, se
// descarga la pagina y sus scripts y se saca de ahi.

function extraerRutas(texto) {
    const encontradas = []

    // fetch("/algo"), axios.post('/algo'), $.post(`/algo`)
    const llamadas = /(?:fetch|post|ajax|open)\s*\(\s*['"`]([^'"`]{2,80})['"`]/gi
    // <form action="/algo">
    const formularios = /<form[^>]+action=["']([^"']{2,80})["']/gi

    for (const patron of [llamadas, formularios]) {
        for (const m of texto.matchAll(patron)) {
            const ruta = m[1].trim()
            if (!ruta || ruta.includes("${")) continue          // plantilla con variables
            if (!ruta.startsWith("/") && !/^https?:/i.test(ruta)) continue
            if (/\.(js|css|png|jpg|svg|ico|map|woff2?)$/i.test(ruta)) continue
            if (!encontradas.includes(ruta)) encontradas.push(ruta)
        }
    }

    return encontradas
}

async function descubrirRutas(origen) {
    const paginas = []

    try {
        const { data } = await axios.get(origen, {
            timeout: 6000,
            headers: { "ngrok-skip-browser-warning": "true" },
            validateStatus: () => true
        })
        if (typeof data !== "string") return []
        paginas.push(data)

        // El fetch casi nunca esta en el HTML: esta en el script que carga
        const scripts = [...data.matchAll(/<script[^>]+src=["']([^"']+)["']/gi)]
            .map(m => m[1])
            .slice(0, 4)

        for (const src of scripts) {
            try {
                const url = new URL(src, origen)
                if (url.origin !== new URL(origen).origin) continue   // solo lo suyo
                const r = await axios.get(url.toString(), {
                    timeout: 5000,
                    headers: { "ngrok-skip-browser-warning": "true" }
                })
                if (typeof r.data === "string") paginas.push(r.data)
            } catch { /* un script que no carga no estorba */ }
        }
    } catch {
        return []
    }

    const rutas = []
    for (const pagina of paginas) {
        for (const ruta of extraerRutas(pagina)) {
            const absoluta = /^https?:/i.test(ruta) ? ruta : new URL(ruta, origen).toString()
            if (!rutas.includes(absoluta)) rutas.push(absoluta)
        }
    }

    // Primero las que suenan a recibir un mensaje. Una pagina llama a muchos
    // endpoints (estado, listados...) y postear en el equivocado podria dar un
    // 200 enganoso; empezando por los que tocan, eso casi no pasa.
    const suenaAMensaje = /(mensaj|message|inbox|recib|enviar|send|chat|buzon)/i
    rutas.sort((a, b) => (suenaAMensaje.test(b) ? 1 : 0) - (suenaAMensaje.test(a) ? 1 : 0))

    return rutas.slice(0, 6)
}

app.post("/send-to-url", async (req, res) => {
    const u = validateUrl(req.body?.url)
    if (!u.ok) return res.status(400).json({ error: u.error })

    const text = req.body?.message
    if (typeof text !== "string" || !text.trim()) {
        return res.status(400).json({ error: "El campo 'message' es obligatorio" })
    }

    // Quien firma el mensaje. Se acepta 'name' (el campo que usamos todos) o
    // 'from'; si va vacio lo firma el propio server.
    const from = String(req.body?.name || req.body?.from || "").trim().slice(0, 40) || "server"

    const msg = recordMessage({
        id: ++messageSeq,
        from,
        to: u.url,
        server: from,
        message: text,
        timestamp: Date.now(),
        direct: true
    })

    // El cuerpo que viaja: 'name' y 'message' son los dos campos acordados
    // entre todos los equipos; el resto es informacion extra que quien reciba
    // puede ignorar sin problema.
    const cuerpo = {
        name: from,
        message: text,
        from,
        to: u.url,
        id: msg.id,
        timestamp: msg.timestamp,
        direct: true
    }

    // Presupuesto de tiempo: probar muchas rutas no puede dejar la peticion
    // colgada medio minuto, o el navegador (o el tunel que hay delante del
    // panel) corta antes por su cuenta con su propio error.
    const INTENTO_MS = 6000
    const PRESUPUESTO_MS = 20000
    const empezoEn = Date.now()

    const intentados = []
    let ultimoError = null
    let respondioPagina = false     // alguna ruta contesto HTML en vez de JSON

    // Devuelve la respuesta si la ruta acepto el mensaje; null si hay que
    // seguir probando; lanza si no tiene sentido insistir con este destino.
    async function intentar(destination) {
        if (intentados.includes(destination)) return null
        if (intentados.length && Date.now() - empezoEn > PRESUPUESTO_MS) return null
        intentados.push(destination)

        try {
            const { data, status } = await axios.post(destination, cuerpo, {
                timeout: INTENTO_MS,
                headers: { "ngrok-skip-browser-warning": "true" }
            })

            // Una pagina web contesta HTML a todo, incluso a rutas que no
            // maneja. No es una entrega: seguimos buscando el endpoint bueno.
            if (looksLikeHtml(data)) {
                respondioPagina = true
                ultimoError = Object.assign(new Error("respuesta HTML"), { response: { data, status } })
                return null
            }

            return { data, destination }

        } catch (error) {
            ultimoError = error
            const status = error.response?.status

            // ngrok devuelve 404 cuando el tunel no existe: eso NO significa
            // que falte la ruta, y repetir la peticion en cada camino solo
            // suma esperas y confunde al que lee el error.
            if (esFalloDeTunel(error)) throw error

            // 404/405: esa ruta no existe ahi. 400/422: existe, pero no le
            // gusto el cuerpo; tampoco es la nuestra. Seguimos.
            if (status === 404 || status === 405 || status === 400 || status === 422) return null

            // Conexion, timeout o error interno del destino: insistir no ayuda
            throw error
        }
    }

    function entregado(exito) {
        metrics.messagesRouted++
        msg.delivered = true
        msg.endpoint = exito.destination
        log("success", "message-direct", { from, to: exito.destination, message: text })
        return res.json({ status: "delivered", id: msg.id, to: exito.destination, response: exito.data })
    }

    let descubiertas = []

    try {
        for (const destination of deliveryTargets(u.url)) {
            const exito = await intentar(destination)
            if (exito) return entregado(exito)
        }

        // Ninguna ruta conocida sirvio y el destino es una pagina: la ruta
        // buena esta escrita en su propio JavaScript. Se lee y se prueba.
        if (respondioPagina && Date.now() - empezoEn < PRESUPUESTO_MS) {
            descubiertas = await descubrirRutas(new URL(u.url).origin)
            for (const destination of descubiertas) {
                const exito = await intentar(destination)
                if (exito) return entregado(exito)
            }
        }
    } catch { /* ultimoError ya guarda el motivo */ }

    const detail = describeDeliveryError(ultimoError)

    // Listar rutas solo aporta cuando de verdad se probaron varias porque el
    // destino iba contestando "aqui no". Si fallo el tunel o la conexion, la
    // ruta no tiene nada que ver y mencionarlas despista.
    const problemaDeRuta = intentados.length > 1

    metrics.messagesFailed++
    msg.delivered = false
    msg.error = detail
    log("error", "message-failed", { from, to: u.url, error: detail, rutas: intentados.length })

    return res.status(502).json({
        error: "No se pudo entregar el mensaje",
        detail: respondioPagina
            ? "Esa URL es una pagina web y ninguna de sus rutas acepto el mensaje. Preguntale a tu companero en que ruta escucha su server (por ejemplo /mensajes) y pegala en la URL: https://su-url.ngrok-free.dev/mensajes"
            : detail,
        destino: u.url,
        probado: problemaDeRuta ? intentados : undefined,
        descubiertas: descubiertas.length ? descubiertas : undefined
    })
})

// --------------------------------------------------------------------------
// Comprobar una URL antes de escribirle
// --------------------------------------------------------------------------
// Cuando un envio falla por ERR_NGROK_8012 y compania, el problema esta en la
// maquina de destino y desde aqui no hay nada que arreglar. Lo unico util es
// poder decir con certeza QUE hay al otro lado, para saber a quien reclamar.

app.post("/probe-url", async (req, res) => {
    const u = validateUrl(req.body?.url)
    if (!u.ok) return res.status(400).json({ error: u.error })

    let respuesta
    try {
        respuesta = await axios.get(u.url, {
            timeout: 6000,
            headers: { "ngrok-skip-browser-warning": "true" },
            validateStatus: () => true       // un 404 tambien nos dice cosas
        })
    } catch (error) {
        return res.json({
            vivo: false,
            estado: "sin respuesta",
            detalle: describeDeliveryError(error),
            url: u.url
        })
    }

    const { status, data } = respuesta
    const texto = typeof data === "string" ? data : ""

    const ngrok = texto.match(/ERR_NGROK_[0-9]+/)
    if (ngrok) {
        // Un tunel apagado y un tunel sin server detras se arreglan distinto:
        // uno pidiendo la URL nueva, el otro arrancando el server.
        const urlMuerta = ngrok[0] === "ERR_NGROK_3200" || ngrok[0] === "ERR_NGROK_3202"
        return res.json({
            vivo: false,
            estado: urlMuerta ? "esa URL de ngrok ya no vale" : "el tunel contesta, el server no",
            detalle: describeDeliveryError({ response: { data: texto, status } }),
            url: u.url
        })
    }

    // Un nodo de verdad responde JSON; lo normal es que diga su nombre
    if (data && typeof data === "object") {
        const quien = data.name ? `${data.name}${data.role ? ` (${data.role})` : ""}` : "responde JSON"
        return res.json({
            vivo: true,
            estado: "hay un server ahi",
            detalle: `${quien}. Puedes escribirle.`,
            url: u.url,
            info: data
        })
    }

    if (/^\s*<(!doctype|html)/i.test(texto)) {
        return res.json({
            vivo: true,
            estado: "hay algo, pero devuelve una pagina web",
            detalle: "Contesta HTML en vez de JSON. Puede ser el panel de tu companero (y entonces el mensaje si le llega) o una URL equivocada.",
            url: u.url
        })
    }

    res.json({
        vivo: true,
        estado: `contesta ${status}`,
        detalle: "Responde algo que no es JSON ni HTML.",
        url: u.url
    })
})


// Historial
app.get("/messages", (req, res) => {
    res.json(messageLog.slice().sort((a, b) => a.timestamp - b.timestamp))
})

app.get("/messages/:name", (req, res) => {
    const entry = registry.get(normalize(req.params.name))
    if (!entry) return res.status(404).json({ error: "server not found" })
    res.json(entry.messages)
})

// ============================================================================
// CONFIGURACION DEL SERVER (solo lectura)
// ============================================================================

app.get("/admin/config", (req, res) => {
    res.json({ ...config, adminKey: undefined, host: HOST_INFO })
})

// ============================================================================
// 1. MULTIPLICIDAD - lanzar y apagar hijos desde la interfaz (sin terminal)
// ============================================================================
// El server actua como supervisor: arranca procesos `node miniserver.js`,
// captura su salida y los apaga. Todo desde el panel web.

const serverProcesses = new Map()   // clave normalizada -> proceso supervisado
let nextPort = config.basePort

// Comprueba si un puerto esta libre intentando escuchar en el
function portFree(port) {
    return new Promise(resolve => {
        const probe = net.createServer()
        probe.once("error", () => resolve(false))
        probe.once("listening", () => probe.close(() => resolve(true)))
        probe.listen(port, "0.0.0.0")
    })
}

async function findFreePort() {
    let port = nextPort
    for (let i = 0; i < 200; i++, port++) {
        if ([...serverProcesses.values()].some(p => p.port === port)) continue
        if (await portFree(port)) {
            nextPort = port + 1
            return port
        }
    }
    throw new Error("no hay puertos libres en el rango")
}

function autoName() {
    let i = 1
    while (registry.has(`nodo-${i}`) || serverProcesses.has(`nodo-${i}`)) i++
    return `nodo-${i}`
}

// Salida de los hijos: se guarda por proceso y se transmite en vivo al panel
function captureOutput(proc, stream, level) {
    stream.setEncoding("utf8")
    stream.on("data", chunk => {
        String(chunk).split(/\r?\n/).filter(l => l.trim()).forEach(line => {
            const item = { ts: Date.now(), level, line }
            proc.output.push(item)
            if (proc.output.length > 200) proc.output.shift()
            emit({ kind: "proc", name: proc.name, ...item })
        })
    })
}

app.post("/spawn", async (req, res) => {
    let name

    if (req.body?.name) {
        const n = validateName(req.body.name)
        if (!n.ok) return res.status(400).json({ error: n.error })
        if (registry.has(n.key) || serverProcesses.has(n.key)) {
            return res.status(409).json({ error: `El nombre '${n.display}' ya esta en uso`, suggestion: suggestName(n.key) })
        }
        name = n.display
    } else {
        name = autoName()
    }

    let port = Number(req.body?.port) || 0
    try {
        if (port) {
            if (!Number.isInteger(port) || port < 1024 || port > 65535) {
                return res.status(400).json({ error: "Puerto invalido (1024-65535)" })
            }
            if (!(await portFree(port))) {
                return res.status(409).json({ error: `El puerto ${port} esta ocupado` })
            }
        } else {
            port = await findFreePort()
        }
    } catch (error) {
        return res.status(500).json({ error: error.message })
    }

    const parentUrl = config.publicUrl || `http://localhost:${config.port}`
    const selfUrl = `http://${config.childHost}:${port}`
    const script = path.join(__dirname, "miniserver.js")

    let child
    try {
        child = spawn(process.execPath, [script, String(port), name, parentUrl, selfUrl], {
            cwd: __dirname,
            windowsHide: true
        })
    } catch (error) {
        log("error", "spawn-failed", { name, error: error.message })
        return res.status(500).json({ error: "No se pudo lanzar el proceso", detail: error.message })
    }

    const proc = {
        key: normalize(name),
        name,
        port,
        url: selfUrl,
        pid: child.pid,
        startedAt: Date.now(),
        output: [],
        child
    }

    serverProcesses.set(proc.key, proc)
    captureOutput(proc, child.stdout, "info")
    captureOutput(proc, child.stderr, "error")

    child.on("error", error => {
        log("error", "child-error", { name, error: error.message })
    })

    child.on("exit", (code, signal) => {
        serverProcesses.delete(proc.key)
        registry.delete(proc.key)
        log(code === 0 || signal ? "warn" : "error", "child-exit", { name, code, signal })
    })

    log("success", "spawn", { name, port, pid: child.pid, url: selfUrl })
    res.json({ message: "miniserver lanzado", name, port, pid: child.pid, url: selfUrl, parentUrl })
})

// Procesos supervisados
app.get("/processes", (req, res) => {
    const now = Date.now()
    res.json([...serverProcesses.values()].map(p => ({
        name: p.name,
        port: p.port,
        url: p.url,
        pid: p.pid,
        startedAt: p.startedAt,
        uptimeMs: now - p.startedAt,
        lines: p.output.length
    })))
})

// Salida (stdout/stderr) de un hijo lanzado desde el panel
app.get("/processes/:name/output", (req, res) => {
    const proc = serverProcesses.get(normalize(req.params.name))
    if (!proc) return res.status(404).json({ error: "proceso no supervisado" })
    const limit = Math.min(Number(req.query.limit) || 200, 200)
    res.json({ name: proc.name, output: proc.output.slice(-limit) })
})

// Apagar: si el server lo lanzo, mata el proceso; si no, le pide apagarse
app.post("/kill-server/:name", async (req, res) => {
    const key = normalize(req.params.name)
    const entry = registry.get(key)
    const proc = serverProcesses.get(key)

    if (!entry && !proc) return res.status(404).json({ error: "server not found" })

    let mode = "remoto"
    let detail = "no-response"

    if (proc) {
        // Proceso local supervisado: cierre limpio y, si no cede, forzado
        mode = "proceso"
        try {
            proc.child.kill()
            setTimeout(() => {
                if (serverProcesses.has(key)) {
                    try { proc.child.kill("SIGKILL") } catch { /* ya murio */ }
                }
            }, 3000)
            detail = `pid ${proc.pid} terminado`
        } catch (error) {
            detail = error.message
        }
    } else {
        // Hijo externo (otra maquina / otra terminal): se le pide apagarse
        try {
            await axios.post(`${entry.url}/shutdown`, {}, {
                timeout: 4000,
                headers: { "ngrok-skip-browser-warning": "true" }
            })
            detail = "ok"
        } catch (error) {
            detail = error.message
        }
    }

    registry.delete(key)
    log("warn", "kill-server", { name: entry?.display || proc.name, mode, detail })
    res.json({ message: `${entry?.display || proc.name} eliminado`, mode, detail })
})

// ============================================================================
// 1 + 7. ESTADO, METRICAS Y LOGS
// ============================================================================

app.get("/servers", (req, res) => {
    const now = Date.now()
    res.json([...registry.values()].map(e => publicEntry(e, now)))
})

app.get("/api/status", (req, res) => {
    const now = Date.now()
    const servers = [...registry.values()].map(e => publicEntry(e, now))
    res.json({
        server: {
            publicUrl: detectPublicUrl(req),
            tunnel: Boolean(req.get("x-forwarded-host")),
            port: config.port,
            host: HOST_INFO,
            uptimeMs: now - STARTED_AT,
            timeout: config.timeout,
            startedAt: STARTED_AT
        },
        counts: {
            total: servers.length,
            alive: servers.filter(s => s.status === "ALIVE").length,
            suspect: servers.filter(s => s.status === "SUSPECT").length,
            dead: servers.filter(s => s.status === "DEAD").length,
            messages: messageLog.length,
            dashboards: sseClients.size
        },
        metrics,
        servers
    })
})

app.get("/metrics", (req, res) => {
    res.json({ uptimeMs: Date.now() - STARTED_AT, servers: registry.size, ...metrics })
})

app.get("/health", (req, res) => {
    res.json({ status: "ok", role: "server", uptimeMs: Date.now() - STARTED_AT, host: HOST_INFO })
})

app.get("/logs", (req, res) => {
    const limit = Math.min(Number(req.query.limit) || 100, LOG_LIMIT)
    const level = req.query.level
    const since = Number(req.query.since) || 0
    let out = logs.filter(l => l.id > since)
    if (level) out = out.filter(l => l.level === level)
    res.json(out.slice(-limit))
})

// Stream de eventos en vivo para el dashboard
app.get("/events", (req, res) => {
    res.set({
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive"
    })
    res.flushHeaders?.()
    res.write(`data: ${JSON.stringify({ kind: "hello", ts: Date.now() })}\n\n`)

    sseClients.add(res)
    const keepAlive = setInterval(() => res.write(": ping\n\n"), 20000)

    req.on("close", () => {
        clearInterval(keepAlive)
        sseClients.delete(res)
    })
})

// ============================================================================
// 5. TIMEOUT - deteccion pasiva (pulsos perdidos) + purga de muertos
// ============================================================================

setInterval(() => {
    const now = Date.now()

    for (const entry of [...registry.values()]) {
        const status = computeStatus(entry, now)

        if (status !== entry.status) {
            entry.status = status

            if (status === "SUSPECT") {
                entry.missed++
                log("warn", "pulse-lost", {
                    name: entry.display,
                    silentMs: now - entry.lastPulse,
                    status
                })
            }

            if (status === "DEAD") {
                entry.deadAt = now
                metrics.timeouts++
                log("error", "server-down", {
                    name: entry.display,
                    url: entry.url,
                    silentMs: now - entry.lastPulse
                })
            }
        }

        // Se mantiene un rato como DEAD (visible en el dashboard) y luego se purga
        if (entry.status === "DEAD" && entry.deadAt && now - entry.deadAt > config.deadRetention) {
            registry.delete(entry.key)
            log("warn", "purged", { name: entry.display, reason: "timeout" })
        }
    }
}, config.checkInterval)

// ============================================================================
// 5. TIMEOUT - deteccion ACTIVA: el padre sondea /health de cada hijo
// ============================================================================

setInterval(async () => {
    for (const entry of [...registry.values()]) {
        const t0 = Date.now()
        try {
            await axios.get(`${entry.url}/health`, {
                timeout: 4000,
                headers: { "ngrok-skip-browser-warning": "true" }
            })
            entry.latencyMs = Date.now() - t0
            if (entry.reachable === false) {
                log("success", "probe-recovered", { name: entry.display, latencyMs: entry.latencyMs })
            }
            entry.reachable = true
        } catch (error) {
            if (entry.reachable !== false) {
                metrics.probesFailed++
                log("warn", "probe-failed", { name: entry.display, url: entry.url, error: error.message })
            }
            entry.reachable = false
            entry.latencyMs = null
        }
    }
}, config.probeInterval)

// ============================================================================
// ARRANQUE
// ============================================================================

app.listen(config.port, config.host, () => {
    const nets = os.networkInterfaces()
    const lan = Object.values(nets).flat().filter(i => i && i.family === "IPv4" && !i.internal).map(i => i.address)

    console.log("========================================================")
    console.log(` SERVER (padre) escuchando en ${config.host}:${config.port}`)
    console.log(` Local     : http://localhost:${config.port}`)
    lan.forEach(ip => console.log(` LAN       : http://${ip}:${config.port}`))
    if (config.publicUrl) console.log(` Publica   : ${config.publicUrl}`)
    console.log(` Host      : ${HOST_INFO.hostname} (${HOST_INFO.platform}/${HOST_INFO.arch}) node ${HOST_INFO.node}`)
    console.log(` Timeout   : ${config.timeout} ms   Admin key: ${config.adminKey}`)
    console.log("========================================================")

    log("success", "server-up", { port: config.port, host: HOST_INFO.hostname, lan })
})

// Al cerrar el server se apagan los hijos que el mismo lanzo (sin huerfanos)
let closing = false

function shutdownChildren(signal) {
    if (closing) return
    closing = true

    const names = [...serverProcesses.values()].map(p => p.name)
    if (names.length) console.log(`\nApagando ${names.length} miniserver(s): ${names.join(", ")}`)

    for (const proc of serverProcesses.values()) {
        try { proc.child.kill() } catch { /* ya termino */ }
    }

    setTimeout(() => process.exit(0), names.length ? 800 : 0)
    if (signal) console.log(`Server detenido (${signal})`)
}

process.on("SIGINT", () => shutdownChildren("SIGINT"))
process.on("SIGTERM", () => shutdownChildren("SIGTERM"))
process.on("exit", () => {
    for (const proc of serverProcesses.values()) {
        try { proc.child.kill() } catch { /* ya termino */ }
    }
})
