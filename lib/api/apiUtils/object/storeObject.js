const { errors, jsutil } = require('arsenal');

const { data } = require('../../../data/wrapper');
const { prepareStream } = require('./prepareStream');
const { arsenalErrorFromChecksumError } = require('../../apiUtils/integrity/validateChecksums');

/**
 * Check that `hashedStream.completedHash` matches header `stream.contentMD5`
 * and delete old data or remove 'hashed' listeners, if applicable.
 * @param {object} stream - stream containing the data
 * @param {object} hashedStream - instance of MD5Sum
 * @param {object} dataRetrievalInfo - object containing the keys of stored data
 * @param {number} dataRetrievalInfo.key - key of the stored data
 * @param {string} dataRetrievalInfo.dataStoreName - the implName of the data
 * @param {object} checksumStream - checksum transform stream with digest/algoName properties
 * @param {object} log - request logger instance
 * @param {function} cb - callback to send error or move to next task
 * @return {function} - calls callback with arguments:
 * error, dataRetrievalInfo, and completedHash (if any)
 */
function checkHashMatchMD5(stream, hashedStream, dataRetrievalInfo, checksumStream, log, cb) {
    const contentMD5 = stream.contentMD5;
    const completedHash = hashedStream.completedHash;
    if (contentMD5 && completedHash && contentMD5 !== completedHash) {
        log.debug('contentMD5 and completedHash do not match, deleting data', {
            method: 'storeObject::dataStore',
            completedHash,
            contentMD5,
        });
        const dataToDelete = [];
        dataToDelete.push(dataRetrievalInfo);
        return data.batchDelete(dataToDelete, null, null, log, err => {
            if (err) {
                // failure of batch delete is only logged, client gets the
                // error code about the md mismatch
                log.error('error deleting old data', { error: err });
            }
            return cb(errors.BadDigest);
        });
    }
    const checksum = { algorithm: checksumStream.algoName, value: checksumStream.digest };
    return cb(null, dataRetrievalInfo, completedHash, checksum);
}

/**
 * Stores object and responds back with key and storage type
 * @param {object} objectContext - object's keyContext for sproxyd Key
 * computation (put API)
 * @param {object} cipherBundle - cipher bundle that encrypt the data
 * @param {object} stream - the stream containing the data
 * @param {number} size - data size in the stream
 * @param {object | null } streamingV4Params - if v4 auth, object containing
 * accessKey, signatureFromRequest, region, scopeDate, timestamp, and
 * credentialScope (to be used for streaming v4 auth if applicable)
 * @param {BackendInfo} backendInfo - info to determine which data
 * backend to use
 * @param {object} checksums - checksum configuration
 * @param {object} checksums.primary - primary checksum data
 * @param {object|null} checksums.secondary - secondary checksum data
 * @param {RequestLogger} log - the current stream logger
 * @param {function} cb - callback containing result for the next task
 * @return {undefined}
 */
function dataStore(objectContext, cipherBundle, stream, size, streamingV4Params, backendInfo, checksums, log, cb) {
    const cbOnce = jsutil.once(cb);

    // errCb is delegated through a mutable reference so it can be upgraded to
    // include batchDelete once data.put has actually stored data.
    let onStreamError = cbOnce;
    const errCb = err => onStreamError(err);

    const checksumedStream = prepareStream(stream, streamingV4Params, checksums, log, errCb);
    if (checksumedStream.error) {
        log.debug('dataStore failed to prepare stream', checksumedStream);
        return process.nextTick(() => cbOnce(checksumedStream.error));
    }
    return data.put(
        cipherBundle, checksumedStream.stream, size, objectContext, backendInfo, log,
        (err, dataRetrievalInfo, hashedStream) => {
            if (err) {
                log.error('error in datastore', { error: err });
                return cbOnce(err);
            }
            if (!dataRetrievalInfo) {
                log.fatal('data put returned neither an error nor a key', { method: 'storeObject::dataStore' });
                return cbOnce(errors.InternalError);
            }
            log.trace('dataStore: backend stored key', { dataRetrievalInfo });

            // Data is now stored. Upgrade the error handler so that any stream
            // error from this point on cleans up the stored data first.
            onStreamError = streamErr => {
                log.error('checksum stream error after data.put', { error: streamErr });
                return data.batchDelete([dataRetrievalInfo], null, null, log, deleteErr => {
                    if (deleteErr) {
                        log.error('dataStore failed to delete old data', { error: deleteErr });
                    }
                    cbOnce(streamErr);
                });
            };

            // stream is always the primary (end of pipe, stored checksum).
            // secondaryChecksumStream is upstream and only validated.
            const { secondaryChecksumStream } = checksumedStream;

            const doValidate = () => {
                // Validate the secondary (checked-only) checksum first.
                if (secondaryChecksumStream) {
                    const secondaryErr = secondaryChecksumStream.validateChecksum();
                    if (secondaryErr) {
                        log.debug('failed secondary checksum validation', { error: secondaryErr });
                        return data.batchDelete([dataRetrievalInfo], null, null, log, deleteErr => {
                            if (deleteErr) {
                                log.error('dataStore failed to delete old data', { error: deleteErr });
                            }
                            return cbOnce(arsenalErrorFromChecksumError(secondaryErr));
                        });
                    }
                }
                // Validate the primary (stored) checksum.
                const primaryErr = checksumedStream.stream.validateChecksum();
                if (primaryErr) {
                    log.debug('failed primary checksum validation', { error: primaryErr });
                    return data.batchDelete([dataRetrievalInfo], null, null, log, deleteErr => {
                        if (deleteErr) {
                            log.error('dataStore failed to delete old data', { error: deleteErr });
                        }
                        return cbOnce(arsenalErrorFromChecksumError(primaryErr));
                    });
                }
                if (!secondaryChecksumStream) {
                    return checkHashMatchMD5(stream, hashedStream, dataRetrievalInfo,
                        checksumedStream.stream, log, cbOnce);
                }
                // Dual-checksum: checkHashMatchMD5 returns the primary
                // (storage) checksum. Swap it to the client-facing one
                // from the secondary stream and attach the primary as
                // storageChecksum.
                return checkHashMatchMD5(stream, hashedStream, dataRetrievalInfo,
                    checksumedStream.stream, log, (err, dataInfo, hash, primaryChecksum) => {
                        if (err) {
                            return cbOnce(err);
                        }
                        const checksum = {
                            algorithm: secondaryChecksumStream.algoName, // Used for the response headers.
                            value: secondaryChecksumStream.digest,
                            storageChecksum: primaryChecksum,
                        };
                        return cbOnce(null, dataInfo, hash, checksum);
                    });
            };

            // ChecksumTransform._flush computes the digest asynchronously for
            // some algorithms (e.g. crc64nvme). writableFinished is true once
            // _flush has called its callback, guaranteeing this.digest is set.
            // stream is the primary (end of pipe) — when it finishes all
            // upstream transforms (including the secondary) have flushed.
            if (checksumedStream.stream.writableFinished) {
                return doValidate();
            }
            checksumedStream.stream.once('finish', doValidate);
            return undefined;
        });
}

module.exports = {
    dataStore,
};
