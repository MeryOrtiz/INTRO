const banner = document.getElementById("banner")
const clusterMeta = document.getElementById("clusterMeta")
const nodesEl = document.getElementById("nodes")
const partitionPicker = document.getElementById("partitionPicker")

let lastCluster = null
let selected = new Set()

function escapeHtml(text) {
    return String(text ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
}

// Todas las ordenes a otros nodos van por el relay del coordinador local:
// asi el panel nunca hace peticiones cruzadas y funciona igual por ngrok.
async function control(url, path, body) {
    const res = await fetch("/relay", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ url, path, body: body || {} })
    })

    return res.json()
}

function allNodes() {
    return lastCluster ? lastCluster.nodes.filter(node => node.id) : []
}

/* --------------------------------------------------------------- cluster -- */

async function refresh() {
    try {
        const res = await fetch("/cluster")
        lastCluster = await res.json()
    } catch {
        banner.className = "banner danger"
        banner.innerText = "Sin conexion con el coordinador local"
        return
    }

    renderBanner(lastCluster)
    renderNodes(lastCluster)
    renderCounters(lastCluster)
    renderPicker(lastCluster)
}

function renderBanner(cluster) {
    const leaders = cluster.leaders || []

    clusterMeta.innerText = `${cluster.clusterSize} nodos · algoritmo ${cluster.algorithm}`

    // Si el server que sirve este panel esta pausado, el server ya trae la
    // vista del cluster desde otro server vivo: aqui solo se muestra el lider.
    if (leaders.length > 1) {
        banner.className = "banner danger"
        banner.innerText =
            `SPLIT-BRAIN: ${leaders.length} lideres a la vez (${leaders.map(l => l.id).join("  y  ")})`
        return
    }

    if (leaders.length === 0) {
        banner.className = "banner warn"
        banner.innerText = "Sin lider: eleccion en curso"
        return
    }

    const leader = leaders[0]
    const extra = leader.term ? ` (term ${leader.term})` : ""

    banner.className = "banner ok"
    banner.innerText = `Lider: ${mayus(leader.id)}${extra}`
}

// Los nombres de los coordinadores se muestran siempre en mayuscula.
// Este server ya los guarda asi, pero un companero puede haberse puesto
// minusculas: bully compara sin distinguir, asi que se ve igual de bien.
function mayus(id) {
    return String(id == null ? "" : id).toUpperCase()
}

/* ------------------------------------------------------ los roles -- */

// Los TRES roles del algoritmo, definidos en un solo sitio. Todo lo que
// el panel muestra sobre roles sale de aqui.
//
// No son etiquetas decorativas: cada uno dice que hace ese server con lo
// que le manden los workers, que es lo unico que se nota desde fuera.
const ROLES = {
    leader: {
        etiqueta: "LIDER",
        que: "Es el que manda. Atiende los registros, los pulsos y los mensajes de los workers, y avisa cada pocos segundos que sigue vivo."
    },
    candidate: {
        etiqueta: "CANDIDATO",
        que: "Convoco una eleccion y esta esperando respuesta de los de letra mayor. Es un estado de paso: dura segundos."
    },
    follower: {
        etiqueta: "SEGUIDOR",
        que: "No manda. Si un worker le escribe, le contesta 409 con la URL del lider para que se vaya para alla."
    }
}

function rolInfo(role) {
    return ROLES[role] || { etiqueta: String(role || "?").toUpperCase(), que: "Rol desconocido" }
}

// Insignia del rol.
//
// Que un nodo no conteste NO quiere decir que no sepamos su papel: los que
// siguen vivos siguen diciendo a quien obedecen. Si el caido es justo ese,
// lo que hay delante es "el lider se cayo" (y viene eleccion); si no, es
// un coordinador mas del grupo. Distinguirlo es lo unico que importa aqui.
function insigniaRol(node, liderAcordado) {
    if (node.reachable) {
        const info = rolInfo(node.role)
        return `<span class="role ${node.role}" title="${escapeHtml(info.que)}">${info.etiqueta}</span>`
    }

    const esLider = Boolean(liderAcordado && node.id && String(node.id) === String(liderAcordado))

    if (esLider) {
        return `<span class="role gone lider" title="${escapeHtml(ROLES.leader.que)}">LIDER</span>`
    }

    return `<span class="role gone" title="${escapeHtml(ROLES.follower.que)}">COORDINADOR</span>`
}

function renderNodes(cluster) {
    nodesEl.innerHTML = ""

    cluster.nodes.forEach(node => {
        const card = document.createElement("div")
        const faults = node.faults || {}

        const classes = ["node"]
        if (!node.reachable) classes.push("unreachable")
        if (faults.paused) classes.push("paused")
        if (node.role) classes.push(node.role)

        card.className = classes.join(" ")

        // Un coordinador que no contesta lleva la MISMA tarjeta que el resto:
        // mismo rol, mismo lider, mismos workers. Lo unico que cambia es que
        // los datos son los ultimos que se le conocieron, no los de ahora, y
        // eso se dice sin rodeos en vez de dejar la tarjeta medio vacia.
        if (!node.reachable) {
            const rolPrevio = (cluster.agreedLeader && String(node.id) === String(cluster.agreedLeader))
                ? "leader"
                : node.lastRole

            card.innerHTML = `
                <div class="node-head">
                    <strong>${escapeHtml(node.id ? mayus(node.id) : node.url)}</strong>
                    ${insigniaRol(node, cluster.agreedLeader)}
                </div>
                ${rolPrevio ? `<div class="role-what">${escapeHtml(rolInfo(rolPrevio).que)}</div>` : ""}
                <div class="node-body">
                    lider: <code>${escapeHtml(node.lastLeader ? mayus(node.lastLeader) : "ninguno")}</code>
                    ${node.lastTerm ? `<br />term: <code>${escapeHtml(node.lastTerm)}</code>` : ""}
                    <br /><span class="dim">${escapeHtml(node.url)}</span>
                </div>
                ${renderWorkers({ workers: node.lastWorkers }, { paused: true })}
                <div class="node-actions">
                    <button class="ghost" disabled title="No se le puede mandar nada: no contesta">Matar</button>
                    <button class="ghost" disabled title="No se le puede mandar nada: no contesta">Sanar</button>
                </div>`

            nodesEl.appendChild(card)
            return
        }

        const flags = []
        if (faults.paused) flags.push('<span class="flag">PAUSADO</span>')
        if ((faults.blocked || []).length) flags.push(`<span class="flag">AISLADO DE ${escapeHtml(faults.blocked.join(","))}</span>`)
        if (faults.clockSkew) flags.push(`<span class="flag skew">RELOJ ${faults.clockSkew > 0 ? "+" : ""}${faults.clockSkew}ms</span>`)
        if (faults.latency) flags.push(`<span class="flag slow">+${faults.latency}ms</span>`)
        if (faults.drop) flags.push(`<span class="flag slow">PIERDE ${Math.round(faults.drop * 100)}%</span>`)

        card.innerHTML = `
            <div class="node-head">
                <strong>${escapeHtml(mayus(node.id))}</strong>
                ${insigniaRol(node, cluster.agreedLeader)}
            </div>
            <div class="role-what">${escapeHtml(rolInfo(node.role).que)}</div>
            <div class="node-flags">${flags.join("")}</div>
            <div class="node-body">
                lider: <code>${escapeHtml(node.leader ? mayus(node.leader) : "ninguno")}</code>
                ${node.term ? `<br />term: <code>${escapeHtml(node.term)}</code>` : ""}
                ${describeStrategy(node)}
            </div>
            ${renderWorkers(node, faults)}
            <div class="node-actions">
                <button class="${faults.paused ? "" : "danger"}" data-action="${faults.paused ? "resume" : "pause"}" data-url="${escapeHtml(node.url)}">
                    ${faults.paused ? "Reanudar" : "Matar"}
                </button>
                <button class="ghost" data-action="heal" data-url="${escapeHtml(node.url)}">Sanar</button>
            </div>`

        nodesEl.appendChild(card)
    })

    nodesEl.querySelectorAll("button[data-action]").forEach(button => {
        button.onclick = async () => {
            button.disabled = true

            const target = cluster.nodes.find(node => node.url === button.dataset.url)

            // Matar a un follower no dispara nada, y sin este aviso parece que
            // el panel se ha colgado. La eleccion solo salta si cae EL LIDER.
            if (button.dataset.action === "pause" && target && target.role !== "leader") {
                note(`${target.id} no era el lider: no habra eleccion. Mata al lider para ver el failover.`)
            }

            await control(button.dataset.url, `/debug/${button.dataset.action}`)
            refresh()
        }
    })
}

// Cada algoritmo saca aqui lo suyo: lo que devuelva su describe().
function describeStrategy(node) {
    const s = node.strategyState || {}
    const parts = []

    if (s.electing !== undefined) parts.push(`eligiendo: <code>${s.electing}</code>`)

    if (s.host) parts.push(`PC: <code>${escapeHtml(s.host)}</code>`)
    if (s.via) parts.push(`se le oye por: <code>${escapeHtml(s.via)}</code>`)
    if (s.eligible) parts.push(`puede ser líder: <code>${escapeHtml(s.eligible)}</code>`)

    // Campos de cualquier estrategia que anadan despues.
    if (s.votes !== undefined) parts.push(`votos: <code>${s.votes}/${s.needs}</code>`)
    if (s.msToTimeout !== undefined) parts.push(`timeout en <code>${(s.msToTimeout / 1000).toFixed(1)}s</code>`)

    return parts.length ? `<br />${parts.join("<br />")}` : ""
}

// Los workers enganchados a este coordinador. Esta es LA parte que hay que
// mirar en clase: al matar al lider, estas etiquetas saltan solas a otra
// tarjeta. Nadie toca nada; los workers se reenganchan ellos.
function renderWorkers(node, faults) {
    const workers = node.workers || []

    // Un nodo congelado conserva la lista que tenia al morir. Si no se avisa,
    // parece que los mismos workers estan en dos coordinadores a la vez.
    const stale = faults.paused

    if (!workers.length) {
        return `<div class="workers empty">sin workers</div>`
    }

    const badges = workers
        .map(worker => `<span class="worker ${worker.online ? "" : "off"}">${escapeHtml(worker.name)}</span>`)
        .join("")

    return `
        <div class="workers ${stale ? "stale" : ""}">
            ${stale ? '<span class="stale-note">ultima lista conocida</span>' : ""}
            ${badges}
        </div>`
}

function renderCounters(cluster) {
    const totals = cluster.nodes.reduce((acc, node) => {
        const counters = node.counters || {}

        acc.protocol += counters.protocol || 0
        acc.ping += counters.ping || 0

        Object.entries(counters.byType || {}).forEach(([type, count]) => {
            acc.byType[type] = (acc.byType[type] || 0) + count
        })

        return acc
    }, { protocol: 0, ping: 0, byType: {} })

    document.getElementById("msgProtocol").innerText = totals.protocol
    document.getElementById("msgPing").innerText = totals.ping

    const types = Object.entries(totals.byType)
        .sort((a, b) => b[1] - a[1])
        .map(([type, count]) => `${type} ${count}`)
        .join("  ·  ")

    document.getElementById("msgTypes").innerText = types || "-"
}

/* ---------------------------------------------------------- herramientas -- */

function renderPicker(cluster) {
    const ids = allNodes().map(node => node.id)
    const signature = ids.join(",")

    if (partitionPicker.dataset.signature === signature) return
    partitionPicker.dataset.signature = signature

    partitionPicker.innerHTML = ""

    cluster.nodes.filter(node => node.id).forEach(node => {
        const label = document.createElement("label")

        label.className = selected.has(node.id) ? "on" : ""
        label.innerHTML = `<input type="checkbox" ${selected.has(node.id) ? "checked" : ""} /> ${escapeHtml(mayus(node.id))}`

        label.querySelector("input").onchange = event => {
            if (event.target.checked) selected.add(node.id)
            else selected.delete(node.id)

            label.className = event.target.checked ? "on" : ""
        }

        partitionPicker.appendChild(label)
    })
}

document.getElementById("applyPartition").onclick = async () => {
    const side = [...selected]
    if (!side.length) return

    const others = allNodes().map(node => node.id).filter(id => !selected.has(id))

    // Cada lado bloquea al otro. La particion tiene que ser simetrica: si solo
    // corta un lado, el otro sigue enviandole y no seria una particion real.
    await Promise.all(allNodes().map(node =>
        control(node.url, "/debug/partition", { block: side.includes(node.id) ? others : side })
    ))

    refresh()
}

document.getElementById("healNetwork").onclick = async () => {
    await Promise.all(allNodes().map(node => control(node.url, "/debug/heal")))
    selected.clear()
    partitionPicker.dataset.signature = ""
    refresh()
}

document.getElementById("applyNetwork").onclick = async () => {
    const ms = Number(document.getElementById("latency").value) || 0
    const drop = (Number(document.getElementById("drop").value) || 0) / 100

    await Promise.all(allNodes().flatMap(node => [
        control(node.url, "/debug/latency", { ms, jitter: Math.round(ms / 2) }),
        control(node.url, "/debug/drop", { probability: drop })
    ]))

    refresh()
}

document.getElementById("resetCounters").onclick = async () => {
    await Promise.all(allNodes().map(node => control(node.url, "/debug/reset-counters")))
    refresh()
}

// Aviso del propio panel (no viene del servidor). Sale flotando un rato y
// se va solo: son mensajes de una linea, no hacen falta en ningun historial.
let toastHandle = null

function note(text) {
    const toast = document.getElementById("toast")
    if (!toast) return

    toast.textContent = text
    toast.hidden = false

    clearTimeout(toastHandle)
    toastHandle = setTimeout(() => { toast.hidden = true }, 6000)
}

/* ------------------------------------------ conectar por internet -- */

const connectBox = document.getElementById("connectBox")
const peerResult = document.getElementById("peerResult")

const peerApi = "/election/peers"

async function loadPeers() {
    let data

    {
        try {
            const [stateRes, peersRes] = await Promise.all([
                fetch("/election/state"),
                fetch("/election/peers")
            ])
            if (!stateRes.ok) return

            const state = await stateRes.json()
            const peers = peersRes.ok ? await peersRes.json() : []

            data = {
                myUrl: state.url,
                id: state.id,
                peers: peers.map(p => ({ url: p.url, id: p.id, alive: p.alive, error: null }))
            }
        } catch {
            return
        }
    }

    connectBox.hidden = false
    document.getElementById("myUrl").textContent = data.myUrl || "sin URL todavía (arranca ngrok en esta PC)"

    document.getElementById("peerList").innerHTML = data.peers.map(peer => {
        const state = peer.alive
            ? `<span class="ok">conectado${peer.id ? ` con ${escapeHtml(peer.id)}` : ""}</span>`
            : `<span class="bad">${escapeHtml(peer.error || "esperando respuesta")}</span>`
        // Una URL mal escrita se reintenta en cada ronda: hay que poder quitarla
        const quitar = `<button class="ghost small" data-drop-peer="${escapeHtml(peer.url)}" type="button">quitar</button>`
        return `<li>${escapeHtml(peer.url)} · ${state} ${quitar}</li>`
    }).join("")

    document.getElementById("peerList").querySelectorAll("[data-drop-peer]").forEach(button => {
        button.onclick = async () => {
            button.disabled = true
            try {
                await fetch(peerApi, {
                    method: "DELETE",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ url: button.dataset.dropPeer })
                })
                peerResult.textContent = ""
            } catch {
                button.disabled = false
                return
            }
            loadPeers()
        }
    })
}

document.getElementById("copyUrl").onclick = async () => {
    const url = document.getElementById("myUrl").textContent
    if (!/^https?:/.test(url)) return
    try {
        await navigator.clipboard.writeText(url)
        note("URL copiada: pásasela a tus compañeros")
    } catch {
        note(`Tu URL: ${url}`)
    }
}

document.getElementById("peerForm").onsubmit = async event => {
    event.preventDefault()
    const input = document.getElementById("peerUrl")
    const button = event.target.querySelector("button")

    button.disabled = true
    peerResult.textContent = "Conectando…"

    try {
        const res = await fetch(peerApi, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ url: input.value })
        })
        const data = await res.json()
        peerResult.textContent = data.ok
            ? `Conectado con ${data.id}. Ya forma parte del grupo y puede ser elegido líder.`
            : (data.error || "No se pudo conectar")
        if (data.ok) input.value = ""
    } catch {
        peerResult.textContent = "Sin conexión con este server"
    } finally {
        button.disabled = false
        loadPeers()
        refresh()
    }
}

loadPeers()
setInterval(loadPeers, 5000)

refresh()
setInterval(refresh, 1000)
