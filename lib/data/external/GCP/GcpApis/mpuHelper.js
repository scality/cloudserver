const async = require('async');
const { eachSlice, getRandomInt, createMpuKey, logger } =
    require('../GcpUtils');
const { logHelper } = require('../../utils');

// TO-DO: Make a helper class for MPU

/**
 * createDelSlices - creates a list of lists of objects to be deleted via
 * the a batch operation for MPU. Becase batch operation has a limit of 1000 op
 * per batch, this function creates the list of lists to be process.
 * @param {object[]} list - a list of objects given to be sliced
 * @return {object[]} - a list of lists of object to be deleted
 */
function createDelSlices(list) {
    const retSlice = [];
    for (let ind = 0; ind < list.length; ind += 1000) {
        retSlice.push(list.slice(ind, ind + 1000));
    }
    return retSlice;
}

/**
 * retryCompose - exponential backoff retry implementation for the compose
 * operation
 * @param {object} params - compose object params
 * @param {number} retry - the retry count
 * @param {function} callback - callback function to call with the result of the
 * compose operation
 * @return {undefined}
 */
function _retryCompose(params, retry, callback) {
    // retries up each request to a maximum of 5 times before
    // declaring as a failed completeMPU
    const timeout = Math.pow(2, retry) * 1000 + getRandomInt(100, 500);
    return setTimeout((params, callback) =>
    this.composeObject(params, callback), timeout, params, (err, res) => {
        if (err) {
            if (retry <= this._maxRetries && err.statusCode === 429) {
                logger.trace('retryCompose: slow down request',
                    { retryCount: retry, timeout });
                return _retryCompose.call(this, params, retry + 1, callback);
            }
            logHelper(logger, 'error', 'retryCompose: failed to compose', err);
            return callback(err);
        }
        return callback(null, res);
    });
}

/**
 * retryCopy - exponential backoff retry implementation for the copy operation
 * @param {object} params - copy object params
 * @param {number} retry - the retry count
 * @param {function} callback - callback function to call with the result of the
 * copy operation
 * @return {undefined}
 */
function _retryCopy(params, retry, callback) {
    const timeout = Math.pow(2, retry) * 1000 + getRandomInt(100, 500);
    return setTimeout((params, callback) =>
    this.copyObject(params, callback), timeout, params, (err, res) => {
        if (err) {
            if (retry <= this._maxRetries && err.code === 429) {
                logger.trace('retryCopy: slow down request',
                    { retryCount: retry, timeout });
                return _retryCopy.call(this, params, retry + 1, callback);
            }
            logHelper(logger, 'error', 'retryCopy: failed to copy', err);
            return callback(err);
        }
        return callback(null, res);
    });
}

/**
 * splitMerge - breaks down the MPU list of parts to be compose on GCP; splits
 * partList into chunks of 32 objects, the limit of each compose operation.
 * @param {object} params - complete MPU params
 * @param {string} params.Bucket - bucket name
 * @param {string} params.MPU - mpu bucket name
 * @param {string} params.Overflow - overflow bucket name
 * @param {string} params.Key - object key
 * @param {string} params.UploadId - MPU upload id
 * @param {object[]} partList - list of parts for complete multipart upload
 * @param {string} level - the phase name of the MPU process
 * @param {function} callback - the callback function to call
 * @return {undefined}
 */
function _splitMerge(params, partList, level, callback) {
    // create composition of slices from the partList array
    return async.mapLimit(eachSlice.call(partList, 32), this._maxConcurrent,
    (infoParts, cb) => {
        const mpuPartList = infoParts.Parts.map(item =>
            ({ PartName: item.PartName }));
        const partNumber = infoParts.PartNumber;
        const tmpKey =
            createMpuKey(params.Key, params.UploadId, partNumber, level);
        const mergedObject = { PartName: tmpKey };
        if (mpuPartList.length < 2) {
            logger.trace('splitMerge: parts are fewer than 2, copy instead');
            // else just perform a copy
            const copyParams = {
                Bucket: params.MPU,
                Key: tmpKey,
                CopySource: `${params.MPU}/${mpuPartList[0].PartName}`,
            };
            return this.copyObject(copyParams, (err, res) => {
                if (err) {
                    logHelper(logger, 'error',
                        'error in splitMerge - copyObject', err);
                    return cb(err);
                }
                mergedObject.VersionId = res.VersionId;
                mergedObject.ETag = res.ETag;
                return cb(null, mergedObject);
            });
        }
        const composeParams = {
            Bucket: params.MPU,
            Key: tmpKey,
            MultipartUpload: { Parts: mpuPartList },
        };
        return _retryCompose.call(this, composeParams, 0, (err, res) => {
            if (err) {
                return cb(err);
            }
            mergedObject.VersionId = res.VersionId;
            mergedObject.ETag = res.ETag;
            return cb(null, mergedObject);
        });
    }, (err, res) => {
        if (err) {
            return callback(err);
        }
        return callback(null, res.length);
    });
}

/**
 * removeParts - remove all objects created to perform a multipart upload
 * @param {object} params - remove parts params
 * @param {string} params.Bucket - bucket name
 * @param {string} params.MPU - mpu bucket name
 * @param {string} params.Overflow - overflow bucket name
 * @param {string} params.Key - object key
 * @param {string} params.UploadId - MPU upload id
 * @param {function} callback - callback function to call
 * @return {undefined}
 */
function _removeParts(params, callback) {
    const _getObjectVersions = (bucketType, callback) => {
        logger.trace(`remove all parts ${bucketType} bucket`);
        let partList = [];
        let isTruncated = true;
        let nextMarker;
        const bucket = params[bucketType];
        return async.whilst(() => isTruncated, next => {
            const listParams = {
                Bucket: bucket,
                Prefix: params.Prefix,
                Marker: nextMarker,
            };
            return this.listVersions(listParams, (err, res) => {
                if (err) {
                    logHelper(logger, 'error', 'error in ' +
                        `removeParts - listVersions ${bucketType}`, err);
                    return next(err);
                }
                nextMarker = res.NextMarker;
                isTruncated = res.IsTruncated;
                partList = partList.concat(res.Versions);
                return next();
            });
        }, err => callback(err, partList));
    };

    const _deleteObjects = (bucketType, partsList, callback) => {
        logger.trace(`successfully listed ${bucketType} parts`, {
            objectCount: partsList.length,
        });
        const delSlices = createDelSlices(partsList);
        const bucket = params[bucketType];
        return async.eachLimit(delSlices, 10, (list, next) => {
            const delParams = {
                Bucket: bucket,
                Delete: { Objects: list },
            };
            return this.deleteObjects(delParams, err => {
                if (err) {
                    logHelper(logger, 'error',
                        `error deleting ${bucketType} object`, err);
                }
                return next(err);
            });
        }, err => callback(err));
    };

    return async.parallel([
        done => async.waterfall([
            next => _getObjectVersions('MPU', next),
            (parts, next) => _deleteObjects('MPU', parts, next),
        ], err => done(err)),
        done => async.waterfall([
            next => _getObjectVersions('Overflow', next),
            (parts, next) => _deleteObjects('Overflow', parts, next),
        ], err => done(err)),
    ], err => callback(err));
}

module.exports = {
    _retryCompose,
    _retryCopy,
    _splitMerge,
    _removeParts,
};
