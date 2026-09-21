// Los IDs son strings, pero "10" tiene que ser mayor que "9".
function compare(a, b) {
    return String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: "base" })
}

function isGreater(a, b) {
    return compare(a, b) > 0
}

function max(a, b) {
    if (a === null || a === undefined) return b
    if (b === null || b === undefined) return a
    return isGreater(a, b) ? a : b
}

module.exports = { compare, isGreater, max }
