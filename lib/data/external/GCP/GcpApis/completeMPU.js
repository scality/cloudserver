const async = require('async');
const { errors } = require('arsenal');
const MpuHelper = require('./mpuHelper');
const { createMpuKey, logger } = require('../GcpUtils');
const { logHelper } = require('../../utils');

/**
 * completeMPU - merges a list of parts into a single object
 * @param {object} params - completeMPU params
 * @param {string} params.Bucket - bucket name
 * @param {string} params.MPU - mpu bucket name
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
    const mpuHelper = new MpuHelper(this); // this === GcpClient
    return async.waterfall([
        next => {
            // first compose: in mpu bucket
            // max 10,000 => 313 parts
            // max component count per object 32
            logger.trace('completeMultipartUpload: compose',
                { partCount: partList.length });
            mpuHelper.splitMerge(params, partList, 'compose', next);
        },
        (numParts, next) => mpuHelper.composeFinal(numParts, params, next),
        (result, next) => mpuHelper.generateMpuResult(result, partList, next),
        (result, aggregateETag, next) =>
            mpuHelper.copyToMain(result, aggregateETag, params, next),
        (mpuResult, next) => {
            const delParams = {
                Bucket: params.Bucket,
                MPU: params.MPU,
                Prefix: createMpuKey(params.Key, params.UploadId),
            };
            return mpuHelper.removeParts(delParams,
                err => next(err, mpuResult));
        },
    ], callback);
}

module.exports = completeMPU;
