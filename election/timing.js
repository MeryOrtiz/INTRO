// Los timeouts SON la suposicion del algoritmo sobre la red.
// En LAN un nodo que tarda 1s esta muerto; a traves de un tunel es normal.
//
// Y ojo con el trafico: cada nodo pinga a todos los demas cada "heartbeat".
// Con N coordinadores eso son N*(N-1) peticiones por cada heartbeat. Con 8
// nodos y heartbeat de 300ms son ~190 peticiones/segundo cruzando los tuneles,
// que es lo que acaba con la cuota de ngrok en un rato. El preset de clase
// baja eso a ~14/s sin que el failover deje de verse rapido.
const PRESETS = {
    lan: {
        tick: 250,
        heartbeat: 300,
        suspect: 1000,
        electionMin: 800,
        electionMax: 1600,
        rpcTimeout: 1000
    },

    // Por defecto para trabajar entre laptops. Failover en ~5-8 segundos:
    // suficientemente rapido para que se vea en clase, suficientemente lento
    // para no reventar la cuota del tunel ni provocar falsas caidas.
    classroom: {
        tick: 500,
        heartbeat: 2000,
        suspect: 7000,
        electionMin: 4000,
        electionMax: 9000,
        rpcTimeout: 4000
    }
}

// "wan" es alias de "classroom": es el mismo caso de uso.
PRESETS.wan = PRESETS.classroom

function get(name) {
    const preset = PRESETS[name] || PRESETS.classroom
    const label = PRESETS[name] ? (name === "wan" ? "classroom" : name) : "classroom"

    // Perillas por entorno para poder bajar el trafico en clase sin tocar
    // codigo. Subir ELECTION_HEARTBEAT es la forma directa de gastar menos
    // cuota de tunel: el failover tarda mas, pero se sigue viendo.
    const heartbeat = Number(process.env.ELECTION_HEARTBEAT) || preset.heartbeat
    const suspect = Number(process.env.ELECTION_SUSPECT) || Math.max(preset.suspect, heartbeat * 3)

    // El timeout de eleccion NO se ata a "suspect": en Raft ese timeout es su
    // propio detector de fallos. Solo tiene que ser bastante mayor que el
    // heartbeat (para no convocar elecciones por un latido que llego tarde) y
    // aleatorio entre min y max (para que no salgan todos a la vez).
    const electionMin = Math.max(preset.electionMin, heartbeat * 2)

    return {
        ...preset,
        heartbeat,
        suspect,
        electionMin,
        electionMax: Math.max(preset.electionMax, electionMin * 2),
        preset: label
    }
}

module.exports = { get, PRESETS }
