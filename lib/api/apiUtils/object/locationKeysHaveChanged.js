/**
* Check if all keys that exist in the current list which will be used
* in composing object are not present in the old object's list.
*
* This method can be used to check against accidentally removing data
* keys due to instability from the metadata layer, or for replay
* detection in general.
*
* @param {array|string|null} prev - list of keys from the object being
* overwritten
* @param {array} curr - list of keys to be used in composing current object
* @returns {boolean} true if no key in `curr` is present in `prev`,
* false otherwise
*/
function locationKeysHaveChanged(prev, curr) {
    if (!prev || prev.length === 0) {
        return true;
    }
    // backwards compatibility check if object is of model version 2
    if (typeof prev === 'string') {
        return curr.every(v => v.key !== prev);
    }
    const keysMap = {};
    prev.forEach(v => {
        if (!keysMap[v.dataStoreType]) {
            keysMap[v.dataStoreType] = {};
        }
        keysMap[v.dataStoreType][v.key] = true;
    });
    return curr.every(v => !(keysMap[v.dataStoreType] && keysMap[v.dataStoreType][v.key]));
}

module.exports = locationKeysHaveChanged;
