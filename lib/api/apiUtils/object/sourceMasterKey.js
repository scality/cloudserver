const async = require('async');
const crypto = require('crypto');
const { jsutil } = require('arsenal');

const kms = require('../../../kms/wrapper');
const { buildSourcePartsStream } = require('./sourceChecksum');

// `<part number>:<MD5 of the part's plaintext>`
const partETagPattern = /^\d+:([0-9a-f]{32})$/;

/**
 * Find the smallest encrypted part of the data that has an MD5. A part is an
 * MPU part, or the whole object if it was not uploaded in parts.
 *
 * An MPU part written by UploadPartCopy can span several locations, and each
 * of them carries the MD5 of the whole part in its dataStoreETag. So a part is
 * the run of consecutive locations that share a dataStoreETag.
 *
 * An empty part decrypts to the same MD5 under any key, so it is picked only if
 * every other part is empty too.
 *
 * @param {Array} dataLocator - source data locations
 * @returns {object|null} the part's locations and MD5, or null if no
 * encrypted part has an MD5
 */
function _smallestPart(dataLocator) {
    const parts = [];
    dataLocator.forEach(location => {
        const part = parts[parts.length - 1];
        if (part && location.dataStoreETag === part.dataStoreETag) {
            part.locations.push(location);
            part.size += Number(location.size);
        } else {
            parts.push({
                dataStoreETag: location.dataStoreETag,
                locations: [location],
                size: Number(location.size),
            });
        }
    });
    const rank = part => (part.size === 0 ? Infinity : part.size);
    return parts.reduce((smallest, part) => {
        const match = partETagPattern.exec(part.dataStoreETag);
        if (!match || !part.locations.every(location => location.cipheredDataKey)) {
            return smallest;
        }
        if (smallest && rank(smallest) <= rank(part)) {
            return smallest;
        }
        return { locations: part.locations, md5: match[1], size: part.size };
    }, null);
}

/**
 * Compute the MD5 of the locations' data, decrypted with masterKeyId.
 *
 * @param {Array} locations - data locations, in order
 * @param {string} masterKeyId - master key to decrypt the data with
 * @param {object} log - request logger
 * @param {function} cb - cb(err, md5): md5 is the hex digest
 * @returns {undefined}
 */
function _md5WithKey(locations, masterKeyId, log, cb) {
    const done = jsutil.once(cb);
    const hash = crypto.createHash('md5');
    const stream = buildSourcePartsStream(
        locations.map(location => ({ ...location, masterKeyId })),
        log,
    );
    stream.on('data', chunk => hash.update(chunk));
    stream.once('error', done);
    stream.once('end', () => done(null, hash.digest('hex')));
}

/**
 * Find which master key decrypts the source data correctly: read one part,
 * decrypted with each candidate key in turn, and compare its MD5 with the
 * MD5 recorded for the part.
 *
 * @param {Array} dataLocator - source data locations
 * @param {Array} candidateKeyIds - master key IDs, in the order to try
 * @param {object} log - request logger
 * @param {function} cb - cb(err, masterKeyId, reason): masterKeyId is the
 * first candidate that decrypts the data, or null with the reason when none
 * does or the data has no MD5 to check; err is the error of the read with the
 * first candidate, returned when no other candidate decrypts the data
 * @returns {undefined}
 */
function findSourceMasterKey(dataLocator, candidateKeyIds, log, cb) {
    const part = _smallestPart(dataLocator);
    if (!part) {
        return process.nextTick(cb, null, null, 'no MD5 recorded for the encrypted data');
    }
    const keyIds = candidateKeyIds.filter(
        (keyId, i) => keyId && !candidateKeyIds.slice(0, i).some(other => kms.isSameMasterKey(other, keyId, log)),
    );
    let firstReadError = null;
    return async.detectSeries(
        keyIds,
        (keyId, next) =>
            _md5WithKey(part.locations, keyId, log, (err, md5) => {
                if (err) {
                    log.debug('cannot read the source data with a candidate master key', {
                        masterKeyId: keyId,
                        error: err,
                    });
                    if (keyId === keyIds[0]) {
                        firstReadError = err;
                    }
                    return next(null, false);
                }
                return next(null, md5 === part.md5);
            }),
        (err, keyId) => {
            if (keyId) {
                return cb(null, keyId);
            }
            if (firstReadError) {
                return cb(firstReadError);
            }
            return cb(null, null, 'no candidate master key decrypts the data');
        },
    );
}

module.exports = { findSourceMasterKey, _smallestPart };
