// PRUEBA DE CONFORMIDAD DE UN ALGORITMO DE ELECCION
//
// Corre contra cualquier implementacion que respete el contrato de
// strategies/_template.js, no solo contra la de referencia.
//
//   node scripts/verify-election.js
//   node scripts/verify-election.js --algo bully
//   node scripts/verify-election.js --nodes 3
//   node scripts/verify-election.js --urls https://a.ngrok.app,https://b.ngrok.app
//
// Comprueba tres cosas distintas:
//
//   SAFETY    nunca hay dos lideres a la vez.
//             Solo se considera fallo si el algoritmo dice usar quorum.
//
//   LIVENESS  tras matar al lider, el cluster vuelve a converger en uno solo
//             dentro del plazo. Si esto falla, el sistema se queda colgado.
//
//   PARTICION al partir la red, solo el lado con mayoria deberia tener lider.
//             bully FALLA esta a proposito: elige dos lideres, uno por lado.
//             No es un bug del test: es la limitacion que toca arreglar en la
//             siguiente clase, y aqui queda documentada.

const axios = require("axios")

const HEADERS = { "ngrok-skip-browser-warning": "true" }

/* ------------------------------------------------------------ argumentos -- */

function arg(name, fallback) {
    const index = process.argv.indexOf(`--${name}`)
    return index > -1 && process.argv[index + 1] ? process.argv[index + 1] : fallback
}

const ALGO = arg("algo", null)
const NODES = Number(arg("nodes", 3))
const TIMEOUT = Number(arg("timeout", 20000))
const BASE_PORT = Number(arg("base-port", 3000))

const URLS = arg("urls", null)
    ? arg("urls").split(",").map(url => url.trim().replace(/\/+$/, ""))
    : Array.from({ length: NODES }, (unused, i) => `http://localhost:${BASE_PORT + i}`)

/* ---------------------------------------------------------------- utiles -- */

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))

async function state(url) {
    try {
        const res = await axios.get(`${url}/election/state`, { timeout: 3000, headers: HEADERS })
        // OJO: el spread va PRIMERO y "url" despues, a proposito: "url" tiene
        // que seguir siendo la direccion por la que acabamos de preguntar, no
        // la que el server diga de si mismo.
        return { ...res.data, url, reachable: true }
    } catch {
        return { url, reachable: false, role: null, leader: null, term: null }
    }
}

async function snapshot() {
    return Promise.all(URLS.map(state))
}

async function post(url, path, body) {
    try {
        const res = await axios.post(`${url}${path}`, body || {}, { timeout: 4000, headers: HEADERS })
        return res.data
    } catch {
        return null
    }
}

function leadersIn(nodes) {
    // Un nodo congelado sigue diciendo que era lider, pero no puede ejercer.
    // Contarlo daria falsos split-brain en cuanto pausamos a alguien.
    return nodes.filter(node =>
        node.reachable &&
        node.role === "leader" &&
        !(node.faults && node.faults.paused))
}

// Espera a que se cumpla una condicion, vigilando la seguridad por el camino.
async function waitFor(predicate, timeout, onSample) {
    const deadline = Date.now() + timeout

    while (Date.now() < deadline) {
        const nodes = await snapshot()

        if (onSample) onSample(nodes)
        if (predicate(nodes)) return { ok: true, nodes, ms: timeout - (deadline - Date.now()) }

        await sleep(250)
    }

    return { ok: false, nodes: await snapshot(), ms: timeout }
}

/* ------------------------------------------------------- comprobaciones -- */

const results = []
const safetyViolations = []

// A nivel de modulo porque finish() tambien se llama en la salida temprana.
let algorithmName = "?"
let algorithmUsesQuorum = false

function record(name, passed, detail, expected) {
    results.push({ name, passed, detail, expected })

    const mark = passed ? "PASA" : (expected ? "FALLA (esperado)" : "FALLA")
    console.log(`  ${passed ? "[OK]  " : "[X]   "}${name}: ${mark}`)
    if (detail) console.log(`        ${detail}`)
}

// Dos lideres a la vez es violacion de seguridad. Si ademas comparten term,
// el algoritmo miente sobre su propio quorum.
function checkSafety(nodes) {
    const leaders = leadersIn(nodes)
    if (leaders.length <= 1) return

    const terms = leaders.map(leader => leader.term)
    const sameTerm = new Set(terms).size < terms.length

    safetyViolations.push({
        at: new Date().toISOString(),
        leaders: leaders.map(leader => `${leader.id}@term${leader.term}`),
        sameTerm
    })
}

async function run() {
    console.log(`\nVerificando ${URLS.length} nodos: ${URLS.join(", ")}`)

    if (ALGO) {
        console.log(`Conmutando el cluster a "${ALGO}"...`)
        await post(URLS[0], "/election/algorithm", { algo: ALGO, propagate: true })
        await sleep(1500)
    }

    const initial = await snapshot()
    const online = initial.filter(node => node.reachable)

    if (online.length === 0) {
        console.log("\nNingun nodo responde. Levanta el cluster:  node scripts/cluster.js\n")
        process.exit(2)
    }

    const algorithm = online[0].algo
    const quorum = online[0].quorum
    const clusterSize = online[0].clusterSize
    // Se le pregunta al propio cluster en vez de tener una lista quemada aqui:
    // asi, si alguien registra su propia estrategia, el veredicto de la prueba
    // de particion se ajusta solo a lo que esa estrategia dice que hace.
    let usesQuorum = false

    try {
        const info = await axios.get(`${online[0].url}/election/algorithms`, { timeout: 3000, headers: HEADERS })
        const found = (info.data.available || []).find(entry => entry.name === algorithm)
        usesQuorum = Boolean(found && found.quorum)
    } catch {
        // Si no contesta, asumimos que no usa quorum: es lo mas comun.
    }

    algorithmName = algorithm
    algorithmUsesQuorum = usesQuorum

    console.log(`Algoritmo: ${algorithm} | cluster ${clusterSize}${usesQuorum ? ` | quorum ${quorum}` : " | sin quorum"}\n`)

    /* -- 1. Convergencia inicial ------------------------------------------ */

    const converged = await waitFor(
        nodes => leadersIn(nodes).length === 1,
        TIMEOUT,
        checkSafety
    )

    const firstLeader = leadersIn(converged.nodes)[0]

    record(
        "Convergencia inicial",
        converged.ok,
        converged.ok
            ? `lider = ${firstLeader.id}${firstLeader.term ? ` (term ${firstLeader.term})` : ""} en ~${converged.ms}ms`
            : `lideres encontrados: ${leadersIn(converged.nodes).length}`
    )

    if (!converged.ok) return finish()

    /* -- 2. Failover: se cae el lider ------------------------------------- */

    console.log(`\n  Pausando al lider (${firstLeader.id})...`)
    await post(firstLeader.url, "/debug/pause")

    const failover = await waitFor(
        nodes => {
            const alive = nodes.filter(node => node.url !== firstLeader.url)
            return leadersIn(alive).length === 1
        },
        TIMEOUT,
        nodes => checkSafety(nodes.filter(node => node.url !== firstLeader.url))
    )

    const newLeader = leadersIn(failover.nodes.filter(node => node.url !== firstLeader.url))[0]

    record(
        "Failover tras caida del lider",
        failover.ok,
        failover.ok
            ? `nuevo lider = ${newLeader.id}${newLeader.term ? ` (term ${newLeader.term})` : ""} en ~${failover.ms}ms`
            : "el cluster se quedo sin lider"
    )

    console.log(`  Reanudando ${firstLeader.id}...`)
    await post(firstLeader.url, "/debug/resume")
    await sleep(2000)

    /* -- 3. El lider viejo vuelve ----------------------------------------- */

    const rejoin = await waitFor(nodes => leadersIn(nodes).length === 1, TIMEOUT, checkSafety)

    record(
        "El lider caido vuelve sin duplicar liderazgo",
        rejoin.ok,
        rejoin.ok
            ? `sigue habiendo un unico lider: ${leadersIn(rejoin.nodes)[0].id}`
            : `${leadersIn(rejoin.nodes).length} lideres tras la reincorporacion`
    )

    /* -- 4. Particion de red ---------------------------------------------- */

    if (URLS.length >= 3) {
        const half = Math.floor(URLS.length / 2)
        const minority = (await snapshot()).filter(node => node.reachable).slice(0, half)
        const majority = (await snapshot()).filter(node => node.reachable).slice(half)

        const minorityIds = minority.map(node => node.id)
        const majorityIds = majority.map(node => node.id)

        console.log(`\n  Partiendo la red: [${minorityIds.join(",")}] | [${majorityIds.join(",")}]`)

        await Promise.all([
            ...minority.map(node => post(node.url, "/debug/partition", { block: majorityIds })),
            ...majority.map(node => post(node.url, "/debug/partition", { block: minorityIds }))
        ])

        await sleep(Math.min(TIMEOUT, 12000))

        const partitioned = await snapshot()
        const minorityLeaders = leadersIn(partitioned.filter(node => minorityIds.includes(node.id)))
        const majorityLeaders = leadersIn(partitioned.filter(node => majorityIds.includes(node.id)))

        const noSplitBrain = minorityLeaders.length === 0 && majorityLeaders.length === 1

        record(
            "Sin split-brain bajo particion",
            noSplitBrain,
            `minoria (${minorityIds.length}/${clusterSize}): ${minorityLeaders.length} lider(es) | ` +
            `mayoria (${majorityIds.length}/${clusterSize}): ${majorityLeaders.length} lider(es)`,
            !usesQuorum
        )

        if (!noSplitBrain && !usesQuorum) {
            console.log(`        "${algorithm}" no usa quorum, asi que esto es exactamente lo que tenia que pasar.`)
        }

        console.log("  Sanando la red...")
        await Promise.all(URLS.map(url => post(url, "/debug/heal")))

        const healed = await waitFor(nodes => leadersIn(nodes).length === 1, TIMEOUT, checkSafety)

        record(
            "Reconvergencia tras sanar la particion",
            healed.ok,
            healed.ok
                ? `un unico lider: ${leadersIn(healed.nodes)[0].id}`
                : `${leadersIn(healed.nodes).length} lideres siguen en pie`
        )
    }

    finish(algorithm, usesQuorum)
}

function finish(algorithm = algorithmName, usesQuorum = algorithmUsesQuorum) {
    console.log("\n" + "-".repeat(64))

    // Un algoritmo CON quorum que acaba con dos lideres esta roto: la
    // aritmetica de las mayorias lo prohibe. Uno SIN quorum que acaba con dos
    // lideres esta funcionando como se espera de el, y esa es justo la
    // diferencia que se quiere ensenar. Mismo sintoma, veredicto opuesto.
    let unsafe = false

    if (!safetyViolations.length) {
        console.log("\n  Seguridad: en ningun momento hubo dos lideres a la vez.")

    } else if (usesQuorum) {
        unsafe = true

        console.log(`\n  SEGURIDAD VIOLADA: hubo ${safetyViolations.length} momentos con dos lideres a la vez.`)
        safetyViolations.slice(0, 3).forEach(violation => console.log(`     ${violation.leaders.join("  y  ")}`))
        console.log(`\n  "${algorithm}" dice usar quorum, asi que esto es un fallo del algoritmo:`)
        console.log("  dos mayorias del mismo cluster siempre se solapan, y el nodo comun")
        console.log("  no puede haber votado a los dos. Revisa el conteo de votos y los terms.")

    } else {
        console.log(`\n  Hubo ${safetyViolations.length} momentos con dos lideres a la vez.`)
        console.log(`  "${algorithm}" no usa quorum: nada se lo impide. No es un fallo de la`)
        console.log("  implementacion, es la carencia del algoritmo. Arreglar esto es el tema")
        console.log("  de la siguiente clase.")
    }

    const real = results.filter(result => !result.expected)
    const failed = real.filter(result => !result.passed)

    console.log(`\n  ${real.length - failed.length}/${real.length} comprobaciones superadas` +
        (results.length !== real.length ? `  (+${results.length - real.length} fallo esperado por diseno)` : ""))
    console.log("-".repeat(64) + "\n")

    process.exit(failed.length || unsafe ? 1 : 0)
}

run().catch(err => {
    console.error("\nLa verificacion se rompio:", err.message, "\n")
    process.exit(2)
})
