const async = require('async');
const { errors } = require('arsenal');
const { _removeParts, _splitMerge,
    _retryCompose, _retryCopy } = require('./mpuHelper');
const { createMpuList, createMpuKey, logger } = require('../GcpUtils');
const { logHelper } = require('../../utils');
const { createAggregateETag } =
    require('../../../../api/apiUtils/object/processMpuParts');

/**
 * completeMPU - merges a list of parts into a single object
 * @param {object} params - completeMPU params
 * @param {string} params.Bucket - bucket name
 * @param {string} params.MPU - mpu bucket name
 * @param {string} params.Overflow - overflow bucket name
 * @param {string} params.Key - object key
 * @param {number} params.UploadId - MPU upload id
 * @param {Object} params.MultipartUpload - MPU upload object
 * @param {Object[]} param.MultipartUpload.Parts - a list of parts to merge
 * @param {function} callback - callback function to call with MPU result
 * @return {undefined}
 */
function completeMPU(params, callback) {
    if (!params || !params.MultipartUpload ||
        !params.MultipartUpload.Parts || !params.UploadId ||
        !params.Bucket || !params.Key) {
        const error = errors.InvalidRequest
            .customizeDescription('Missing required parameter');
        logHelper(logger, 'error', 'error in completeMultipartUpload', error);
        return callback(error);
    }
    const partList = params.MultipartUpload.Parts;
    // verify that the part list is in order
    if (params.MultipartUpload.Parts.length === 0) {
        const error = errors.InvalidRequest
            .customizeDescription('You must specify at least one part');
        logHelper(logger, 'error', 'error in completeMultipartUpload', error);
        return callback(error);
    }
    for (let ind = 1; ind < partList.length; ++ind) {
        if (partList[ind - 1].PartNumber >= partList[ind].PartNumber) {
            logHelper(logger, 'error', 'error in completeMultipartUpload',
                errors.InvalidPartOrder);
            return callback(errors.InvalidPartOrder);
        }
    }

    const _copyToOverflow = (numParts, callback) => {
        // copy phase: in overflow bucket
        // resetting component count by moving item between
        // different region/class buckets
        logger.trace('completeMultipartUpload: copy to overflow',
            { partCount: numParts });
        const parts = createMpuList(params, 'mpu2', numParts);
        let doneCount = 0;
        if (parts.length !== numParts) {
            return callback(errors.InternalError);
        }
        return async.eachLimit(parts, 10, (infoParts, cb) => {
            const partName = infoParts.PartName;
            const partNumber = infoParts.PartNumber;
            const overflowKey = createMpuKey(
                params.Key, params.UploadId, partNumber, 'overflow');
            const rewriteParams = {
                SourceBucket: params.MPU,
                SourceObject: partName,
                DestinationBucket: params.Overflow,
                DestinationObject: overflowKey,
            };
            logger.trace('rewrite object', { rewriteParams });
            let rewriteDone = false;
            async.whilst(() => !rewriteDone, next => {
                this.rewriteObject(rewriteParams, (err, res) => {
                    if (err) {
                        logHelper(logger, 'error', 'error in ' +
                            'createMultipartUpload - rewriteObject', err);
                    } else {
                        rewriteDone = res.done;
                        rewriteParams.RewriteToken = res.rewriteToken;
                    }
                    return next(err);
                });
            }, err => {
                if (!err) {
                    doneCount++;
                }
                return cb(err);
            });
        }, err => {
            if (err) {
                return callback(err);
            }
            return callback(null, doneCount);
        });
    };

    const _composeOverflow = (numParts, callback) => {
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
        return _retryCompose.call(this, composeParams, 0, err => {
            if (err) {
                return callback(err);
            }
            return callback(null, null);
        });
    };

    /*
     * Create MPU Aggregate ETag
     */
    const _generateMpuResult = (res, callback) => {
        const concatETag = partList.reduce((prev, curr) =>
            prev + curr.ETag.substring(1, curr.ETag.length - 1), '');
        const aggregateETag = createAggregateETag(concatETag, partList);
        return callback(null, res, aggregateETag);
    };

    const _copyToMain = (res, aggregateETag, callback) => {
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
                return this.headObject(headParams, (err, res) => {
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
                _retryCopy.call(this, copyParams, 0, (err, res) => {
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
    };

    return async.waterfall([
        next => {
            // first compose: in mpu bucket
            // max 10,000 => 313 parts
            // max component count per object 32
            logger.trace('completeMultipartUpload: compose round 1',
                { partCount: partList.length });
            _splitMerge.call(this, params, partList, 'mpu1', next);
        },
        (numParts, next) => {
            // second compose: in mpu bucket
            // max 313 => 10 parts
            // max component count per object 1024
            logger.trace('completeMultipartUpload: compose round 2',
                { partCount: numParts });
            const parts = createMpuList(params, 'mpu1', numParts);
            if (parts.length !== numParts) {
                return next(errors.InternalError);
            }
            return _splitMerge.call(this, params, parts, 'mpu2', next);
        },
        _copyToOverflow,
        _composeOverflow,
        _generateMpuResult,
        _copyToMain,
        (mpuResult, next) => {
            const delParams = {
                Bucket: params.Bucket,
                MPU: params.MPU,
                Overflow: params.Overflow,
                Prefix: createMpuKey(params.Key, params.UploadId),
            };
            return _removeParts.call(this, delParams, err => {
                if (err) {
                    return next(err);
                }
                return next(null, mpuResult);
            });
        },
    ], (err, mpuResult) => {
        if (err) {
            return callback(err);
        }
        return callback(null, mpuResult);
    });
}

module.exports = completeMPU;
