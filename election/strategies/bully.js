const { isGreater } = require("../ids")

// Cada equipo escribe el rol a su manera ("leader", "lider", "LEADER"...).
// Para decidir si alguien se cree el jefe hay que aceptar todas.
function seCreeLider(role) {
    return /^l[ií]der$|^leader$/i.test(String(role || "").trim())
}

// BULLY (Garcia-Molina, 1982) -- familia extremum, malla completa.
//
// "El maton manda": gana siempre el ID mas alto que siga vivo. Cuando alguien
// nota que el lider cayo, reta a todos los que tienen ID mayor que el suyo.
// Si ninguno contesta, se proclama coordinador.
//
// NO usa quorum: le basta con el silencio de los mayores. Por eso, bajo
// particion, cada lado elige su propio lider -> split-brain garantizado.
// Coste: O(n^2) mensajes en el peor caso, y se nota en el contador del panel.

function startElection(ctx) {
    const now = Date.now()

    // Antirrebote: sin esto, cada tick de 250 ms lanzaria otra eleccion.
    if (ctx.state.electing && now - ctx.state.startedAt < ctx.timing.electionMax) return

    ctx.state.electing = true
    ctx.state.startedAt = now
    ctx.state.gotAnswer = false

    ctx.becomeCandidate()

    const higher = ctx.allNodes().filter(node => !node.self && node.alive && isGreater(node.id, ctx.self.id))

    ctx.log("election-start", { higher: higher.map(node => node.id) })

    // Nadie por encima de mi vivo: soy el mayor, me proclamo.
    if (higher.length === 0) return win(ctx)

    higher.forEach(node => ctx.send(node.id, "ELECTION", {}))

    // Si ninguno de los mayores contesta a tiempo, es que estan todos muertos.
    ctx.timer("answer", ctx.timing.electionMin, () => {
        if (ctx.state.gotAnswer) return
        win(ctx)
    })
}

function win(ctx) {
    ctx.state.electing = false

    ctx.clearTimer("answer")
    ctx.clearTimer("coordinator")

    ctx.becomeLeader()
    ctx.broadcast("COORDINATOR", { leader: ctx.self.id })
    ctx.log("election-won", { by: ctx.self.id })
}

module.exports = {
    name: "bully",
    family: "extremum",
    topology: "full-mesh",
    usesQuorum: false,
    summary: "Gana el ID mas alto vivo. Sin quorum: se parte bajo particion.",

    init(ctx) {
        ctx.state.electing = false
        ctx.state.startedAt = 0
        ctx.state.gotAnswer = false

        // Margen para que el detector de fallos descubra a los peers antes
        // de lanzar la primera eleccion; si no, cada nodo se cree solo.
        ctx.state.settleUntil = Date.now() + ctx.timing.suspect
    },

    onTick(ctx) {
        const now = Date.now()

        if (now < ctx.state.settleUntil) return

        if (ctx.role === "leader") {
            // EL MANDO ES DE LA LETRA MAYOR VIVA.
            //
            // La regla de bully dice que gana el ID mas alto, pero hasta ahora
            // solo se cedia al RECIBIR un COORDINATOR suyo. Si ese mensaje no
            // llega -- porque el otro equipo lo implemento distinto, o se
            // perdio en el tunel -- quedan dos lideres a la vez para siempre.
            //
            // Asi que no se espera a que avise: si veo vivo a alguien con letra
            // mayor que ya se cree el jefe, le cedo el puesto y se acabo el
            // split-brain.
            const mayor = ctx.peers()
                .filter(peer => peer.alive && peer.id && isGreater(peer.id, ctx.self.id))
                .filter(peer => peer.snapshot && seCreeLider(peer.snapshot.role))
                .sort((a, b) => (isGreater(a.id, b.id) ? -1 : 1))[0]

            if (mayor) {
                ctx.log("yield", { to: mayor.id, from: ctx.self.id })
                ctx.state.electing = false
                ctx.clearTimer("answer")
                ctx.clearTimer("coordinator")
                ctx.becomeFollower(mayor.id)
                return
            }

            // Reafirmacion periodica del mando. Sin esto, un lider que vuelve
            // de una pausa o de una particion se queda callado creyendose el
            // jefe mientras otro nodo tambien lo cree: dos lideres para
            // siempre. El anuncio es lo que obliga al peor de los dos a ceder.
            if (now - (ctx.state.lastAnnounce || 0) < ctx.timing.heartbeat * 3) return

            ctx.state.lastAnnounce = now
            ctx.broadcast("COORDINATOR", { leader: ctx.self.id })
            return
        }

        const leaderAlive = ctx.leader && ctx.alive(ctx.leader)
        if (leaderAlive) return

        startElection(ctx)
    },

    onMessage(ctx, message) {
        const from = message.from.id

        if (message.type === "ELECTION") {
            // Le contesto para que sepa que hay alguien mayor vivo, y me
            // lanzo yo tambien: la eleccion se propaga hacia arriba.
            ctx.send(from, "ANSWER", {})
            if (!ctx.state.electing) startElection(ctx)
            return
        }

        if (message.type === "ANSWER") {
            ctx.state.gotAnswer = true
            ctx.clearTimer("answer")

            // Hay alguien mayor vivo: le doy tiempo a que se proclame.
            // Si no lo hace, vuelvo a empezar.
            ctx.timer("coordinator", ctx.timing.electionMax, () => {
                if (ctx.leader) return
                ctx.state.electing = false
                startElection(ctx)
            })
            return
        }

        if (message.type === "COORDINATOR") {
            const leader = message.payload.leader || from

            // Ya tengo un lider vivo y mejor que el que se anuncia: paso.
            // Esto es lo que evita el baile de anuncios al sanar una particion.
            if (ctx.leader && ctx.alive(ctx.leader) && isGreater(ctx.leader, leader)) return

            // Si el que se proclama es menor que yo, no lo acepto: le disputo
            // el puesto. Esta es literalmente la parte "matona" del algoritmo.
            if (isGreater(ctx.self.id, leader)) {
                ctx.state.electing = false
                startElection(ctx)
                return
            }

            ctx.state.electing = false
            ctx.clearTimer("answer")
            ctx.clearTimer("coordinator")
            ctx.becomeFollower(leader)
        }
    },

    onPeerSuspected(ctx, peerId) {
        if (!peerId) return
        if (ctx.leader && String(peerId) === String(ctx.leader)) startElection(ctx)
    },

    describe(ctx) {
        return { electing: Boolean(ctx.state.electing) }
    }
}
