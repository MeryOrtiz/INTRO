// ============================================================================
// RUTAS DE LA ELECCION BULLY (ELECTION_MODE=bully)
// ----------------------------------------------------------------------------
// Protocolo entre coordinadores (/election/*), foto del cluster (/cluster) y
// relay del panel (/relay). El stream y los fallos simulados estan en
// common-routes.js.
// ============================================================================

const express = require("express")
const axios = require("axios")

const engine = require("./engine")
const faults = require("./faults")
const strategies = require("./strategies")
const commonRoutes = require("./common-routes")
const peerUrl = require("./peer-url")

const router = express.Router()

router.use(commonRoutes(engine))

/* -------------------------------------------------------------- protocolo -- */

// Mensaje del protocolo de eleccion. Contesta siempre rapido: las respuestas
// del algoritmo viajan como mensajes nuevos, no como cuerpo de esta respuesta.
router.post("/election/message", (req, res) => {
    const result = engine.handleMessage(req.body)

    if (!result.ok) return res.status(503).json(result)
    res.json(result)
})

// Ping del detector de fallos. Sirve para tres cosas a la vez: probar que
// estoy vivo, decir quien soy, y contarnos los peers que conocemos cada uno.
router.post("/election/ping", (req, res) => {
    const snapshot = engine.handlePing(req.body)

    if (!snapshot) return res.status(503).json({ error: "unreachable" })
    res.json(snapshot)
})

/* ----------------------------------------------------------------- estado -- */

router.get("/election/state", (req, res) => {
    res.json(engine.snapshot())
})

router.get("/election/algorithms", (req, res) => {
    res.json({ active: engine.state.algo, available: strategies.list() })
})

router.post("/election/algorithm", (req, res) => {
    const { algo, propagate } = req.body || {}

    if (!algo) return res.status(400).json({ error: "algo required" })
    if (!strategies.has(algo)) {
        return res.status(400).json({ error: `Unknown algorithm: ${algo}`, available: strategies.list().map(s => s.name) })
    }

    // propagate por defecto true: al conmutar desde el panel queremos que
    // TODO el cluster cambie a la vez, o los nodos hablarian idiomas distintos.
    engine.setAlgorithm(algo, propagate !== false)

    res.json({ message: `Algoritmo activo: ${algo}`, state: engine.snapshot() })
})

router.post("/election/trigger", (req, res) => {
    engine.stepDown()
    res.json({ message: "Eleccion forzada", state: engine.snapshot() })
})

/* ------------------------------------------------------------------ peers -- */

router.get("/election/peers", (req, res) => {
    res.json(engine.snapshot().peers)
})

// Conectar con el server de un companero por su URL (su ngrok).
// Se comprueba ANTES de guardarlo: si no, una URL mal escrita se queda
// pingandose para siempre y el panel no sabe decir que pasa.
router.post("/election/peers", async (req, res) => {
    const url = peerUrl.clean(req.body?.url)
    if (!url) return res.status(400).json({ ok: false, error: "URL required" })

    const problem = peerUrl.problem(url)
    if (problem) return res.status(400).json({ ok: false, error: problem })

    if (url === peerUrl.clean(engine.state.url)) {
        return res.status(400).json({ ok: false, error: "Esa es la URL de este mismo server" })
    }

    const probe = await peerUrl.probe(url)
    if (!probe.ok) return res.status(400).json({ ok: false, error: `${url}: ${probe.why}` })

    // Vivo y sano, pero puede estar hablando el otro protocolo
    const mismatch = peerUrl.mismatch("bully", probe.state)
    if (mismatch) return res.status(409).json({ ok: false, error: mismatch })

    const peer = engine.addPeer(url)
    if (!peer) return res.status(400).json({ ok: false, error: "URL invalida o es la mia" })

    res.json({
        ok: true,
        id: probe.state.id,
        url: peer.url,
        message: `Conectado con ${probe.state.id}`,
        peers: engine.snapshot().peers
    })
})

router.delete("/election/peers", (req, res) => {
    const url = req.body?.url
    if (!url) return res.status(400).json({ error: "URL required" })
    if (!engine.removePeer(url)) return res.status(404).json({ error: "Peer no encontrado" })

    res.json({ message: `Peer eliminado: ${url}` })
})

/* ---------------------------------------------------------------- cluster -- */

// Vista agregada: la foto global del cluster. Es el detector de split-brain:
// si aqui salen dos lideres a la vez, el algoritmo no es seguro.
// No sale a la red: usa lo que trajo el ultimo ping del detector de fallos.
router.get("/cluster", async (req, res) => {
    // Congelado: mi foto no vale nada, se la pido a alguien vivo y solo
    // aporto mi propia tarjeta (para poder reanudarme desde el panel).
    if (faults.state.paused && !req.get("x-cluster-view")) {
        const remoto = await engine.peerCluster()

        if (remoto && remoto.nodes) {
            const yo = { ...engine.snapshot(), reachable: true, self: true, staleFor: 0 }
            const otros = remoto.nodes
                .filter(node => node.id !== yo.id)
                .map(node => ({ ...node, self: false }))

            return res.json({ ...remoto, clusterSize: otros.length + 1, nodes: [yo, ...otros] })
        }
    }

    const mine = engine.snapshot()

    const remotes = engine.cachedPeers().map(peer => {
        if (!peer.alive || !peer.snapshot) {
            // Aunque ya no conteste, se guarda el ultimo rol que se le vio: en
            // la demo lo que importa es justo eso, "el que cayo ERA el lider".
            const last = peer.snapshot || {}

            return {
                url: peer.url,
                id: peer.id,
                reachable: false,
                role: null,
                lastRole: last.role || null,
                leader: null,
                lastLeader: last.leader || null,
                lastWorkers: last.workers || [],
                term: null,
                lastTerm: last.term === undefined ? null : last.term,
                staleFor: peer.lastSeen ? Date.now() - peer.lastSeen : null
            }
        }

        return { ...peer.snapshot, reachable: true, staleFor: Date.now() - peer.lastSeen }
    })

    const nodes = [{ ...mine, reachable: true, self: true, staleFor: 0 }, ...remotes]

    // Un nodo congelado sigue contestando con la foto de antes de pausarse:
    // su opinion no cuenta para decidir si el cluster converge.
    const active = nodes.filter(node => node.reachable && !(node.faults && node.faults.paused))

    const leaders = active.filter(node => node.role === "leader")
    const distinctLeaders = [...new Set(active.filter(n => n.leader).map(n => String(n.leader)))]

    res.json({
        algorithm: mine.algo,
        quorum: mine.quorum,
        clusterSize: mine.clusterSize,
        leaders: leaders.map(node => ({ id: node.id, term: node.term, fencingToken: node.fencingToken })),
        splitBrain: leaders.length > 1,
        converged: leaders.length === 1 && distinctLeaders.length === 1,
        agreedLeader: distinctLeaders.length === 1 ? distinctLeaders[0] : null,
        nodes
    })
})

// Reenvia una orden a otro coordinador. El panel controla a TODOS, pero se
// sirve desde uno solo. Solo acepta peers conocidos: no es un proxy abierto.
router.post("/relay", async (req, res) => {
    const { url, path, body } = req.body || {}

    if (!url || !path) return res.status(400).json({ error: "url y path requeridos" })

    const target = String(url).replace(/\/+$/, "")
    const allowed = [...engine.peerUrls(), engine.state.url]

    if (!allowed.includes(target)) {
        return res.status(403).json({ error: "Ese nodo no es un peer conocido", allowed })
    }

    try {
        const response = await axios.post(`${target}${path}`, body || {}, {
            timeout: 5000,
            headers: { "ngrok-skip-browser-warning": "true" }
        })

        res.json(response.data)

    } catch (err) {
        const status = err.response ? err.response.status : 502
        res.status(status).json(err.response ? err.response.data : { error: `No pude alcanzar ${target}` })
    }
})

module.exports = router
