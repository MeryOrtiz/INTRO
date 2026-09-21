// Levanta N servers (coordinadores) en local, para probar la eleccion sin
// necesitar varias maquinas.
//
//   node scripts/cluster.js        -> A, B y C en 3000, 3001 y 3002
//   node scripts/cluster.js 4      -> 4 coordinadores
//
// Gana la letra mas alta que siga viva: con A, B y C manda C.
//
// Para la demo de verdad entre laptops no hace falta esto. En cada PC:
//   node server.js
//   ngrok http 3000
// y se conectan entre ellas con la URL de ngrok de una companera (por
// argumento, o desde el panel de eleccion).

const path = require("path")
const { spawn } = require("child_process")

const COUNT = Math.max(1, Number(process.argv[2]) || 3)
const BASE_PORT = Number(process.env.COORD_BASE_PORT || 3000)

const NAMES = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".split("")

const serverPath = path.resolve(__dirname, "../server.js")
const children = []

const nodes = Array.from({ length: COUNT }, (unused, index) => ({
    id: NAMES[index] || `N${index}`,
    port: BASE_PORT + index,
    url: `http://localhost:${BASE_PORT + index}`
}))

console.log(`\nLevantando ${COUNT} coordinadores (bully)\n`)

function launch(prefix, args, env) {
    const child = spawn(process.execPath, args, {
        env: { ...process.env, ...env },
        stdio: ["ignore", "pipe", "pipe"]
    })

    child.stdout.on("data", chunk => process.stdout.write(prefixLines(prefix, chunk)))
    child.stderr.on("data", chunk => process.stderr.write(prefixLines(prefix, chunk)))
    child.on("close", code => console.log(`${prefix} termino con codigo ${code}`))

    children.push(child)
}

nodes.forEach((node, index) => {
    // Basta con darle UNA semilla: los demas se aprenden solos con los pings.
    const peers = nodes.filter(other => other.url !== node.url).map(other => other.url)

    const env = {
        PUBLIC_URL: node.url,
        ELECTION_TIMING: process.env.ELECTION_TIMING || "lan",   // en local no hay latencia real
        BASE_PORT: String(4000 + index * 100),                   // puertos para sus miniservers
        NO_PREGUNTAR: "1"                                        // el nombre va por argumento
    }

    launch(`[${node.id}:${node.port}]`, [serverPath, String(node.port), node.id, peers.join(",")], env)

    console.log(`  ${node.id}  ${node.url}   panel: ${node.url}/election.html`)
})

const mayor = nodes[nodes.length - 1]
console.log(`\nDeberia mandar ${mayor.id} (la letra mas alta viva)`)
console.log(`Hijo de prueba:  node miniserver.js 4500 alfa ${nodes[0].url}\n`)

function prefixLines(prefix, chunk) {
    return String(chunk)
        .split("\n")
        .map(line => (line.trim() ? `${prefix} ${line}` : line))
        .join("\n")
}

function shutdown() {
    console.log("\nApagando el cluster...")
    children.forEach(child => child.kill())
    process.exit(0)
}

process.on("SIGINT", shutdown)
process.on("SIGTERM", shutdown)
