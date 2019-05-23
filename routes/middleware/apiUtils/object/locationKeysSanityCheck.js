/**
* Check keys that exist in the current list which will be used in composing
* object. This method checks against accidentally removing data keys due to
* instability from the metadata layer. The check returns true if there was no
* match and false if at least one key from the previous list exists in the
* current list
* @param {array|string} prev - list of keys from the object being overwritten
* @param {array} curr - list of keys to be used in composing current object
* @returns {array} list of keys that can be deleted
*/
function locationKeysSanityCheck(prev, curr) {
    if (!prev || prev.length === 0) {
        return true;
    }
    // backwards compatibility check if object is of model version 2
    if (typeof prev === 'string') {
        return curr.every(v => v.key !== prev);
    }
    const keysMap = {};
    prev.forEach(v => { keysMap[v.key] = true; });
    return curr.every(v => !keysMap[v.key]);
}

module.exports = locationKeysSanityCheck;
