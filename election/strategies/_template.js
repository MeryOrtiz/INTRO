// PLANTILLA PARA TU ALGORITMO DE ELECCION
//
// 1. Copia este archivo a  strategies/<tu-algoritmo>.js
// 2. Implementalo.
// 3. Registralo en  strategies/index.js
// 4. Compruebalo:   node scripts/verify-election.js --algo <tu-algoritmo>
//
// El motor no sabe nada de tu algoritmo: solo llama a estos metodos. Todo lo
// que necesitas llega en ctx. NO guardes estado en variables del modulo (el
// archivo se comparte entre reinicios del algoritmo): usa ctx.state, que se
// vacia cada vez que se conmuta de algoritmo.
//
// ----------------------------------------------------------------- ctx ----
//
// IDENTIDAD Y VECINOS
//   ctx.self              -> { id, url } quien soy
//   ctx.peers()           -> todos los peers conocidos [{ id, url, alive }]
//   ctx.alivePeers()      -> solo los que responden ahora mismo
//   ctx.allNodes()        -> yo + los peers, ORDENADOS por id
//   ctx.alive(id)         -> true si ese nodo responde
//   ctx.clusterSize()     -> nodos configurados (vivos o no)
//   ctx.quorum()          -> mayoria: floor(n/2) + 1
//   ctx.ring()            -> { nodes, successor(), predecessor() } saltando muertos
//
// MENSAJES (asincronos: la respuesta NO es el return, llega por onMessage)
//   ctx.send(peerId, "TIPO", { ...datos })
//   ctx.broadcast("TIPO", { ...datos })
//
// CAMBIOS DE ROL
//   ctx.becomeLeader()          me proclamo lider
//   ctx.becomeFollower(id)      acepto a id como lider
//   ctx.becomeCandidate()       estoy compitiendo
//   ctx.stepDown()              dejo de reconocer lider
//   ctx.role / ctx.leader / ctx.term
//   ctx.setTerm(n)              term, epoch, ballot... como lo llame tu algoritmo
//
// TIEMPO
//   ctx.timing                  { heartbeat, suspect, electionMin, electionMax, leaseTtl }
//   ctx.now()                   reloj (afectado por /debug/clock-skew)
//   ctx.randomTimeout(min, max) timeout aleatorio -- clave si todos compiten a la vez
//   ctx.timer(nombre, ms, fn)   timer con nombre (se reemplaza si ya existia)
//   ctx.clearTimer(nombre)
//
// OTROS
//   ctx.dataVersion()           cuantos datos tiene este coordinador (para ZAB)
//   ctx.log("mi-evento", {...}) manda el evento al panel en vivo
//
// -------------------------------------------------------------------------

module.exports = {
    name: "template",
    family: "extremum",              // extremum | quorum | lease | especial
    topology: "full-mesh",           // full-mesh | ring | epidemic
    usesQuorum: false,               // true si exige mayoria (y por tanto NO se parte)
    summary: "Describe tu algoritmo en una linea.",

    // Se llama una vez al activar el algoritmo. Inicializa aqui ctx.state.
    init(ctx) {
        ctx.state.ejemplo = 0

        // Casi siempre querras esperar a que el detector de fallos descubra a
        // los peers antes de convocar nada; si no, cada nodo se cree solo en
        // el mundo y se autoproclama.
        ctx.state.settleUntil = Date.now() + ctx.timing.suspect
    },

    // Se llama cada 250 ms. Aqui van los timeouts y los heartbeats.
    // Ojo: es MUY frecuente. Pon antirrebote a lo que no deba repetirse.
    onTick(ctx) {
        if (Date.now() < ctx.state.settleUntil) return
        if (ctx.role === "leader") return

        const leaderAlive = ctx.leader && ctx.alive(ctx.leader)
        if (leaderAlive) return

        // TODO: convocar eleccion
    },

    // Llega un mensaje de otro nodo.
    // message = { type, from: { id, url }, term, payload }
    onMessage(ctx, message) {
        // TODO: implementar el protocolo
    },

    // El detector de fallos dejo de ver a un peer (opcional).
    onPeerSuspected(ctx, peerId) {},

    // El peer volvio (opcional).
    onPeerRecovered(ctx, peerId) {},

    // Se llama al cambiar a otro algoritmo (opcional). Los timers se limpian solos.
    teardown(ctx) {},

    // Lo que devuelvas sale en el panel, dentro de la tarjeta del nodo.
    // Saca lo que ayude a entender por que tu algoritmo hace lo que hace.
    describe(ctx) {
        return { ejemplo: ctx.state.ejemplo }
    }
}
