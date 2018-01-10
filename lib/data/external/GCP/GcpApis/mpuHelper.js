const async = require('async');
const Backoff = require('backo');
const { errors } = require('arsenal');
const { eachSlice, createMpuKey, createMpuList, logger } =
    require('../GcpUtils');
const { logHelper } = require('../../utils');
const { createAggregateETag } =
    require('../../../../api/apiUtils/object/processMpuParts');

const BACKOFF_PARAMS = { min: 1000, max: 300000, jitter: 0.1, factor: 1.5 };

class MpuHelper {
    constructor(service, options = {}) {
        this.service = service;
        this.backoffParams = {
            min: options.min || BACKOFF_PARAMS.min,
            max: options.max || BACKOFF_PARAMS.max,
            jitter: options.jitter || BACKOFF_PARAMS.jitter,
            factor: options.factor || BACKOFF_PARAMS.factor,
        };
    }

    /**
     * createDelSlices - creates a list of lists of objects to be deleted via
     * the a batch operation for MPU. Because batch operation has a limit of
     * 1000 op per batch, this function creates the list of lists to be process.
     * @param {object[]} list - a list of objects given to be sliced
     * @return {object[]} - a list of lists of object to be deleted
     */
    createDelSlices(list) {
        const retSlice = [];
        for (let ind = 0; ind < list.length; ind += 1000) {
            retSlice.push(list.slice(ind, ind + 1000));
        }
        return retSlice;
    }

    _retry(fnName, params, callback) {
        const backoff = new Backoff(this.backoffParams);
        const handleFunc = (fnName, params, retry, callback) => {
            const timeout = backoff.duration();
            return setTimeout((params, cb) =>
            this.service[fnName](params, cb), timeout, params,
            (err, res) => {
                if (err) {
                    if (err.statusCode === 429 || err.code === 429) {
                        if (fnName === 'composeObject') {
                            logger.trace('composeObject: slow down request',
                                { retryCount: retry, timeout });
                        } else if (fnName === 'copyObject') {
                            logger.trace('copyObject: slow down request',
                                { retryCount: retry, timeout });
                        }
                        return handleFunc(
                            fnName, params, retry + 1, callback);
                    }
                    logHelper(logger, 'error', `${fnName} failed`, err);
                    return callback(err);
                }
                backoff.reset();
                return callback(null, res);
            });
        };
        handleFunc(fnName, params, 0, callback);
    }

    /**
     * retryCompose - exponential backoff retry implementation for the compose
     * operation
     * @param {object} params - compose object params
     * @param {function} callback - callback function to call with the result
     * of the compose operation
     * @return {undefined}
     */
    retryCompose(params, callback) {
        this._retry('composeObject', params, callback);
    }


    /**
     * retryCopy - exponential backoff retry implementation for the copy
     * operation
     * @param {object} params - copy object params
     * @param {function} callback - callback function to call with the result
     * of the copy operation
     * @return {undefined}
     */
    retryCopy(params, callback) {
        this._retry('copyObject', params, callback);
    }

    /**
     * splitMerge - breaks down the MPU list of parts to be compose on GCP;
     * splits partList into chunks of 32 objects, the limit of each compose
     * operation.
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
    splitMerge(params, partList, level, callback) {
        // create composition of slices from the partList array
        return async.mapLimit(eachSlice.call(partList, 32),
        this.service._maxConcurrent,
        (infoParts, cb) => {
            const mpuPartList = infoParts.Parts.map(item =>
                ({ PartName: item.PartName }));
            const partNumber = infoParts.PartNumber;
            const tmpKey =
                createMpuKey(params.Key, params.UploadId, partNumber, level);
            const mergedObject = { PartName: tmpKey };
            if (mpuPartList.length < 2) {
                logger.trace(
                    'splitMerge: parts are fewer than 2, copy instead');
                // else just perform a copy
                const copyParams = {
                    Bucket: params.MPU,
                    Key: tmpKey,
                    CopySource: `${params.MPU}/${mpuPartList[0].PartName}`,
                };
                return this.service.copyObject(copyParams, (err, res) => {
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
            return this.retryCompose(composeParams, (err, res) => {
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
    removeParts(params, callback) {
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
                return this.service.listVersions(listParams, (err, res) => {
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
            const delSlices = this.createDelSlices(partsList);
            const bucket = params[bucketType];
            return async.each(delSlices, (list, next) => {
                const delParams = {
                    Bucket: bucket,
                    Delete: { Objects: list },
                };
                return this.service.deleteObjects(delParams, err => {
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

    copyToOverflow(numParts, params, callback) {
        // copy phase: in overflow bucket
        // resetting component count by moving item between
        // different region/class buckets
        logger.trace('completeMultipartUpload: copy to overflow',
            { partCount: numParts });
        const parts = createMpuList(params, 'mpu2', numParts);
        if (parts.length !== numParts) {
            return callback(errors.InternalError);
        }
        return async.eachLimit(parts, 10, (infoParts, cb) => {
            const partName = infoParts.PartName;
            const partNumber = infoParts.PartNumber;
            const overflowKey = createMpuKey(
                params.Key, params.UploadId, partNumber, 'overflow');
            const rewriteParams = {
                Bucket: params.Overflow,
                Key: overflowKey,
                CopySource: `${params.MPU}/${partName}`,
            };
            logger.trace('rewrite object', { rewriteParams });
            this.service.rewriteObject(rewriteParams, cb);
        }, err => {
            if (err) {
                logHelper(logger, 'error', 'error in ' +
                    'createMultipartUpload - rewriteObject', err);
                return callback(err);
            }
            return callback(null, numParts);
        });
    }

    composeOverflow(numParts, params, callback) {
        // final compose: in overflow bucket
        // number of parts to compose <= 10
        // perform final compose in overflow bucket
        logger.trace('completeMultipartUpload: overflow compose');
        const parts = createMpuList(params, 'overflow', numParts);
        const partList = parts.map(item => (
            { PartName: item.PartName }));
        if (partList.length < 2) {
            logger.trace(
                'fewer than 2 parts in overflow, skip to copy phase');
            return callback(null, partList[0].PartName);
        }
        const composeParams = {
            Bucket: params.Overflow,
            Key: createMpuKey(params.Key, params.UploadId, 'final'),
            MultipartUpload: { Parts: partList },
        };
        return this.retryCompose(composeParams, err => {
            if (err) {
                return callback(err);
            }
            return callback(null, null);
        });
    }

    /*
     * Create MPU Aggregate ETag
     */
    generateMpuResult(res, partList, callback) {
        const concatETag = partList.reduce((prev, curr) =>
            prev + curr.ETag.substring(1, curr.ETag.length - 1), '');
        const aggregateETag = createAggregateETag(concatETag, partList);
        return callback(null, res, aggregateETag);
    }

    copyToMain(res, aggregateETag, params, callback) {
        // move object from overflow bucket into the main bucket
        // retrieve initial metadata then compose the object
        const copySource = res ||
            createMpuKey(params.Key, params.UploadId, 'final');
        return async.waterfall([
            next => {
                // retrieve metadata from init object in mpu bucket
                const headParams = {
                    Bucket: params.MPU,
                    Key: createMpuKey(params.Key, params.UploadId,
                        'init'),
                };
                logger.trace('retrieving object metadata');
                return this.service.headObject(headParams, (err, res) => {
                    if (err) {
                        logHelper(logger, 'error',
                            'error in createMultipartUpload - headObject',
                            err);
                        return next(err);
                    }
                    return next(null, res.Metadata);
                });
            },
            (metadata, next) => {
                // copy the final object into the main bucket
                const copyMetadata = Object.assign({}, metadata);
                copyMetadata['scal-ETag'] = aggregateETag;
                const copyParams = {
                    Bucket: params.Bucket,
                    Key: params.Key,
                    Metadata: copyMetadata,
                    MetadataDirective: 'REPLACE',
                    CopySource: `${params.Overflow}/${copySource}`,
                };
                logger.trace('copyParams', { copyParams });
                this.retryCopy(copyParams, (err, res) => {
                    if (err) {
                        logHelper(logger, 'error', 'error in ' +
                            'createMultipartUpload - final copyObject',
                            err);
                        return next(err);
                    }
                    const mpuResult = {
                        Bucket: params.Bucket,
                        Key: params.Key,
                        VersionId: res.VersionId,
                        ContentLength: res.ContentLength,
                        ETag: `"${aggregateETag}"`,
                    };
                    return next(null, mpuResult);
                });
            },
        ], callback);
    }
}

module.exports = MpuHelper;
