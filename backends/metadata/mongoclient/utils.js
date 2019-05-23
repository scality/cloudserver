
function escape(obj) {
    return JSON.parse(JSON.stringify(obj).
                      replace(/\$/g, '\uFF04').
                      replace(/\./g, '\uFF0E'));
}

function unescape(obj) {
    return JSON.parse(JSON.stringify(obj).
                      replace(/\uFF04/g, '$').
                      replace(/\uFF0E/g, '.'));
}

function serialize(objMD) {
    // Tags require special handling since dot and dollar are accepted
    if (objMD.tags) {
        // eslint-disable-next-line
        objMD.tags = escape(objMD.tags);
    }
}

function unserialize(objMD) {
    // Tags require special handling
    if (objMD.tags) {
        // eslint-disable-next-line
        objMD.tags = unescape(objMD.tags);
    }
}

module.exports = { escape, unescape, serialize, unserialize };
