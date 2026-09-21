// ============================================================================
// RUTAS COMUNES DE LA ELECCION
// ----------------------------------------------------------------------------
// Stream de eventos de la eleccion (/election/stream) e inyeccion de fallos
// sobre ESTE server (/debug/*).
// ============================================================================

const express = require("express")

const events = require("./events")
const faults = require("./faults")
const log = require("./logger")

// motor: el de bully (engine.js); solo se usa su
// id y su snapshot()
module.exports = function commonRoutes(motor) {
    const router = express.Router()

    /* ------------------------------------------------------------------ feed -- */

    // /events ya lo usa el panel principal; la eleccion tiene su propio stream
    router.get("/election/stream", (req, res) => {
        res.writeHead(200, {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            Connection: "keep-alive",
            "X-Accel-Buffering": "no"
        })

        res.write(`data: ${JSON.stringify({ kind: "hello", node: motor.state.id })}\n\n`)
        events.history().slice(-60).forEach(entry => res.write(`data: ${JSON.stringify(entry)}\n\n`))

        const unsubscribe = events.subscribe(res)

        // Sin esto, un proxy con timeout corto (ngrok incluido) corta el stream.
        const keepAlive = setInterval(() => res.write(": ping\n\n"), 15000)

        req.on("close", () => {
            clearInterval(keepAlive)
            unsubscribe()
        })
    })

    router.get("/election/events", (req, res) => {
        res.json({ counters: events.counters, events: events.history() })
    })

    /* ------------------------------------------------------ fallos simulados -- */
    // Nada de esto mata el proceso: el nodo sigue vivo y se le puede seguir
    // preguntando por su estado, que es justo lo que hace falta para ver la
    // diferencia entre "caido" y "incomunicado".

    // Congela el nodo: deja de enviar y de responder al protocolo (crash simulado)
    router.post("/debug/pause", (req, res) => {
        faults.state.paused = true

        log("WARN", "PAUSA: este nodo deja de participar en la eleccion")
        events.emit({ kind: "fault", fault: "pause" })

        res.json({ message: "Nodo pausado", faults: faults.snapshot() })
    })

    router.post("/debug/resume", (req, res) => {
        faults.state.paused = false

        log("INFO", "REANUDADO: el nodo vuelve a la eleccion")
        events.emit({ kind: "fault", fault: "resume" })

        res.json({ message: "Nodo reanudado", faults: faults.snapshot() })
    })

    // Particion de red: corta la comunicacion con los ids indicados
    router.post("/debug/partition", (req, res) => {
        const block = req.body?.block

        if (!Array.isArray(block)) return res.status(400).json({ error: "block debe ser un array de ids" })

        faults.partition(block)

        log("WARN", `PARTICION: incomunicado de [${block.join(", ")}]`)
        events.emit({ kind: "fault", fault: "partition", blocked: block })

        res.json({ message: `Particionado de ${block.join(", ")}`, faults: faults.snapshot() })
    })

    router.post("/debug/heal", (req, res) => {
        faults.heal()

        log("INFO", "RED SANADA: se restablecen todas las comunicaciones")
        events.emit({ kind: "fault", fault: "heal" })

        res.json({ message: "Red sanada", faults: faults.snapshot() })
    })

    router.post("/debug/latency", (req, res) => {
        faults.state.latency = Math.max(0, Number(req.body?.ms) || 0)
        faults.state.jitter = Math.max(0, Number(req.body?.jitter) || 0)

        events.emit({ kind: "fault", fault: "latency", ms: faults.state.latency })

        res.json({ message: `Latencia ${faults.state.latency}ms (+${faults.state.jitter})`, faults: faults.snapshot() })
    })

    router.post("/debug/drop", (req, res) => {
        faults.state.drop = Math.min(1, Math.max(0, Number(req.body?.probability) || 0))

        events.emit({ kind: "fault", fault: "drop", probability: faults.state.drop })

        res.json({ message: `Perdida de mensajes al ${faults.state.drop * 100}%`, faults: faults.snapshot() })
    })

    router.post("/debug/clock-skew", (req, res) => {
        faults.state.clockSkew = Number(req.body?.ms) || 0

        log("WARN", `RELOJ DESFASADO ${faults.state.clockSkew}ms`)
        events.emit({ kind: "fault", fault: "clock-skew", ms: faults.state.clockSkew })

        res.json({ message: `Reloj desfasado ${faults.state.clockSkew}ms`, faults: faults.snapshot() })
    })

    router.get("/debug/state", (req, res) => {
        res.json({ faults: faults.snapshot(), election: motor.snapshot() })
    })

    router.post("/debug/reset-counters", (req, res) => {
        events.resetCounters()
        res.json({ message: "Contadores a cero", counters: events.counters })
    })

    return router
}
