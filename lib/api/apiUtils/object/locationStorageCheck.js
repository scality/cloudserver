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
    // Starting 9.0, cloudserver does not support UTAPI metrics
    // FIXME: rely on scuba backend
    return cb();
}

module.exports = locationStorageCheck;
