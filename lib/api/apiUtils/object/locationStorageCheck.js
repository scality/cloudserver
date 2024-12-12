const { config } = require('../../../Config');

/**
 * locationStorageCheck - will ensure there is enough space left for object on
 * PUT operations, or will update metric on DELETE
 * NOTE: storage limit may not be exactly enforced in the case of concurrent
 * requests when near limit
 * @param {string} location - name of location to check quota
 * @param {number} updateSize - new size to check against quota in bytes
 * @param {object} log - werelogs logger
 * @param {function} cb - callback function
 * @return {undefined}
 */
function locationStorageCheck(location, updateSize, log, cb) {
    const lc = config.locationConstraints;
    const sizeLimitGB = lc[location] ? lc[location].sizeLimitGB : undefined;
    if (updateSize === 0 || sizeLimitGB === undefined || sizeLimitGB === null) {
        return cb();
    }
    // no need to list location metric, since it should be decreased
    if (updateSize < 0) {
        return cb();
    }

    return cb();
}

module.exports = locationStorageCheck;
