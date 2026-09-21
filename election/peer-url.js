// ============================================================================
// COMPROBAR LA URL DEL SERVER DE UN COMPANERO
// ----------------------------------------------------------------------------
// Sirve para avisar con criterio cuando una URL no es la de un coordinador.
//
// La clave: se pregunta por /election/state, que es el unico endpoint que
// TODOS los servers del cluster tienen, hablen el protocolo que hablen. La
// raiz "/" no sirve para esto: un server sano devuelve ahi el panel (200
// text/html), asi que mirarla no distingue un server de una pagina cualquiera.
// ============================================================================

const axios = require("axios")

const HEADERS = { "ngrok-skip-browser-warning": "true" }

// Sitios que la gente pega por error en vez de la URL de un companero.
const NOT_A_SERVER = {
    "github.com": "un repositorio de codigo",
    "gitlab.com": "un repositorio de codigo",
    "bitbucket.org": "un repositorio de codigo",
    "drive.google.com": "una carpeta de archivos",
    "docs.google.com": "un documento",
    "youtube.com": "un video"
}

function clean(url) {
    return String(url || "").trim().replace(/[/]+$/, "")
}

// Revisa la FORMA de la URL antes de gastar una peticion.
// Devuelve el motivo del rechazo, o null si tiene pinta de server.
function problem(url) {
    let parsed
    try {
        parsed = new URL(url)
    } catch {
        return "Esa no es una URL valida. Tiene que ser algo como https://xxxx.ngrok-free.dev"
    }

    if (!/^https?:$/.test(parsed.protocol)) return "La URL debe empezar por http:// o https://"

    const host = parsed.hostname.replace(/^www[.]/, "")
    if (NOT_A_SERVER[host]) {
        return `${host} es ${NOT_A_SERVER[host]}, no el server de un companero. ` +
            "Aqui va la URL que le sale a EL en su panel (su ngrok, algo como https://xxxx.ngrok-free.dev)"
    }

    // El codigo le pega /election/ping detras: si la URL trae
    // una ruta, esa peticion va a dar a un 404.
    if (parsed.pathname && parsed.pathname !== "/") {
        return `Pega solo la direccion base (${parsed.origin}), sin la parte que va despues del dominio`
    }

    if (parsed.search || parsed.hash) {
        return `Pega solo la direccion base (${parsed.origin}), sin parametros`
    }

    return null
}

// Que algoritmo habla el server que hay al otro lado.
function modeOf(state) {
    return state && state.algo ? String(state.algo) : null
}

// ngrok no devuelve el error del server: devuelve SU PROPIA pagina, con un
// codigo ERR_NGROK_xxxx dentro. Traducirlo es la diferencia entre "algo
// fallo" y "tu companera tiene el ngrok abierto pero el server apagado".
function ngrokError(body) {
    const html = typeof body === "string" ? body : ""
    const match = html.match(/ERR_NGROK_(\d+)/)
    if (!match) return null

    const code = match[1]

    // El tunel llega hasta su PC, pero ahi no hay nada escuchando.
    if (code === "8012" || code === "502") {
        return "su ngrok esta abierto, pero del otro lado no hay ningun server escuchando " +
            "(ERR_NGROK_8012). Que arranque su server con 'node server.js', o que revise que " +
            "su ngrok apunte al mismo puerto ('ngrok http 3000')"
    }

    // Nadie tiene ese tunel levantado ahora mismo.
    if (code === "3200" || code === "3004") {
        return "esa URL de ngrok no esta abierta ahora mismo (ERR_NGROK_" + code + "): " +
            "que arranque su ngrok, o pasate la URL nueva (cambia cada vez, salvo dominio fijo)"
    }

    // La pagina de aviso del plan gratuito.
    if (code === "6024" || code === "6023") {
        return "ngrok esta mostrando su pagina de aviso en vez de pasar la peticion (ERR_NGROK_" + code + ")"
    }

    if (code === "3202" || code === "3201") {
        return "ngrok rechazo la peticion por limite de trafico del plan gratuito (ERR_NGROK_" + code + ")"
    }

    return "ngrok devolvio el error ERR_NGROK_" + code + " en vez de la respuesta de su server"
}

// Pregunta por /election/state. Devuelve { ok:true, state } o { ok:false, why }.
async function probe(url, timeout = 10000) {
    try {
        const res = await axios.get(`${url}/election/state`, {
            timeout,
            headers: HEADERS,
            validateStatus: () => true
        })

        if (res.status === 200 && res.data && typeof res.data === "object" && res.data.id) {
            return { ok: true, state: res.data }
        }

        // Antes que nada: ¿es ngrok quejandose, o el server del companero?
        const ngrok = ngrokError(res.data)
        if (ngrok) return { ok: false, why: ngrok }

        if (res.status === 404) {
            return {
                ok: false,
                why: "contesta, pero no tiene /election/state: o no es un server del cluster, " +
                    "o esta corriendo con la eleccion apagada (ELECTION=off)"
            }
        }

        // 502/503/504 sin codigo de ngrok: hay un proxy delante y el server
        // de atras no contesta. Es el mismo cuadro que el 8012.
        if (res.status === 502 || res.status === 503 || res.status === 504) {
            return {
                ok: false,
                why: "el tunel/proxy responde pero el server de atras no (HTTP " + res.status + "): " +
                    "lo mas probable es que su server este apagado"
            }
        }

        const type = String(res.headers["content-type"] || "").split(";")[0]
        return {
            ok: false,
            why: `contesta HTTP ${res.status}${type ? ` (${type})` : ""} en /election/state, que no es lo que devuelve un server del cluster`
        }

    } catch (err) {
        if (err.code === "ENOTFOUND" || err.code === "EAI_AGAIN") return { ok: false, why: "esa direccion no existe: revisa que este bien escrita" }
        if (err.code === "ECONNREFUSED") return { ok: false, why: "nadie escucha ahi: el server esta apagado o el puerto es otro" }
        if (err.code === "ECONNABORTED" || /timeout/i.test(err.message || "")) return { ok: false, why: "no contesta a tiempo: el tunel esta caido, o hay un firewall en medio" }
        if (/certificate/i.test(err.message || "")) return { ok: false, why: "el certificado https de esa direccion no es valido" }
        return { ok: false, why: `no se pudo contactar (${err.code || err.message})` }
    }
}

// El error que mas cuesta ver: el otro server esta vivo y sano, pero habla
// otro protocolo, asi que por mucho que se le hable no va a entender nada.
function mismatch(myMode, theirState) {
    const theirs = modeOf(theirState)
    if (!theirs || theirs === myMode) return null

    return `Ese server usa el algoritmo "${theirs}" y este usa "${myMode}": no hablan el mismo ` +
        `protocolo, asi que no pueden elegir lider juntos (su id es "${theirState.id}")`
}

module.exports = { clean, problem, probe, modeOf, mismatch, HEADERS }
