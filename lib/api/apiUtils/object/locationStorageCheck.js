const { errors } = require('arsenal');

const { config } = require('../../../Config');
const { getLocationMetric, pushLocationMetric } =
    require('../../../utapi/utilities');

function _gbToBytes(gb) {
    return gb * 1024 * 1024 * 1024;
}

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
    const sizeLimitGB = lc[location] && lc[location].details ?
        lc[location].details.sizeLimitGB : undefined;
    if (updateSize === 0 || sizeLimitGB === undefined) {
        return cb();
    }
    // no need to list location metric, since it should be decreased
    if (updateSize < 0) {
        return pushLocationMetric(location, updateSize, log, cb);
    }
    return getLocationMetric(location, log, (err, bytesStored) => {
        if (err) {
            log.error(`Error listing metrics from Utapi: ${err.message}`);
            return cb(err);
        }
        const newStorageSize = bytesStored + updateSize;
        const sizeLimitBytes = _gbToBytes(sizeLimitGB);
        if (sizeLimitBytes < newStorageSize) {
            return cb(errors.AccessDenied.customizeDescription(
                `The assigned storage space limit for location ${location} ` +
                'will be exceeded'));
        }
        return pushLocationMetric(location, updateSize, log, cb);
    });
}

module.exports = locationStorageCheck;
