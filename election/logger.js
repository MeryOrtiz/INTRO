// Salida de los mensajes del motor de eleccion.
// Por defecto va a consola; server.js la redirige a su propio log para que
// las elecciones aparezcan tambien en el registro de eventos del panel.

let sink = (level, message) => console.log(`[${new Date().toISOString()}] ${level.padEnd(7)} ${message}`)

function log(level, message) {
    sink(level, message)
}

log.use = fn => { sink = fn }

module.exports = log
