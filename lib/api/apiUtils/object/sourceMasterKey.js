const async = require('async');
const crypto = require('crypto');
const { jsutil } = require('arsenal');

const kms = require('../../../kms/wrapper');
const { buildSourcePartsStream } = require('./sourceChecksum');

// `<part number>:<MD5 of the part's plaintext>`
const partETagPattern = /^\d+:([0-9a-f]{32})$/;
// Size of the MD5: a wrong key matches a part of this size once in 2^128
const minCheckedPartSize = 16;

/**
 * Find the encrypted part of the data, with an MD5, to decrypt with each
 * candidate key. A part is an MPU part, or the whole object if it was not
 * uploaded in parts.
 *
 * An MPU part written by UploadPartCopy can span several locations, and each
 * of them carries the MD5 of the whole part in its dataStoreETag. So a part is
 * the run of consecutive locations that share a dataStoreETag.
 *
 * A wrong key can, by chance, decrypt a small part to its original content: one
 * key in 256 does it for a part of one byte, and any key for an empty part. So
 * the smallest part of at least minCheckedPartSize bytes is picked, or the
 * largest part if there is none. Every part of a multipart upload but the last
 * is at least 5 MiB, so only an object of one part has none, and checking it
 * checks the whole content.
 *
 * @param {Array} dataLocator - source data locations
 * @returns {object|null} the part's locations, MD5 and size, or null if no
 * encrypted part has an MD5
 */
function _partToCheck(dataLocator) {
    const parts = [];
    for (const location of dataLocator) {
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
    }
    const hasEncryptedMD5 = part =>
        partETagPattern.test(part.dataStoreETag) && part.locations.every(location => location.cipheredDataKey);
    const isPreferred = (part, other) =>
        part.size >= minCheckedPartSize && other.size >= minCheckedPartSize
            ? part.size < other.size
            : part.size > other.size;
    const chosen = parts
        .filter(hasEncryptedMD5)
        .reduce((best, part) => (!best || isPreferred(part, best) ? part : best), null);
    if (!chosen) {
        return null;
    }
    return { locations: chosen.locations, md5: partETagPattern.exec(chosen.dataStoreETag)[1], size: chosen.size };
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
 * does or the data has no MD5 to check; err is the first read error, returned
 * when no candidate decrypts the data
 * @returns {undefined}
 */
function findSourceMasterKey(dataLocator, candidateKeyIds, log, cb) {
    const part = _partToCheck(dataLocator);
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
                    if (!firstReadError) {
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

module.exports = { findSourceMasterKey, _partToCheck };
