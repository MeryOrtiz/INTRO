// Registro de algoritmos de eleccion.
//
// Ahora mismo solo hay uno: BULLY. Manda el ID mas alto que siga vivo.
//
// Es el mas simple que resuelve el problema de verdad, y tiene un fallo
// conocido y a proposito: bajo particion de red elige DOS lideres, uno por
// cada lado. Arreglar eso es el tema de la proxima clase, no de esta.
//
// Para anadir otro: copia _template.js, implementalo y registralo aqui.
const catalog = {
    bully: require("./bully")
}

function has(name) {
    return Object.prototype.hasOwnProperty.call(catalog, name)
}

function get(name) {
    return catalog[name]
}

function list() {
    return Object.keys(catalog).map(name => ({
        name,
        family: catalog[name].family,
        topology: catalog[name].topology,
        quorum: Boolean(catalog[name].usesQuorum),
        summary: catalog[name].summary
    }))
}

module.exports = { has, get, list, catalog }
