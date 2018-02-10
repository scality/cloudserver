const { errors } = require('arsenal');
const { _removeParts } = require('./mpuHelper');
const { createMpuKey, logger } = require('../GcpUtils');
const { logHelper } = require('../../utils');

/**
 * abortMPU - remove all objects of a GCP Multipart Upload
 * @param {object} params - abortMPU params
 * @param {string} params.Bucket - bucket name
 * @param {string} params.MPU - mpu bucket name
 * @param {string} params.Overflow - overflow bucket name
 * @param {string} params.Key - object key
 * @param {number} params.UploadId - MPU upload id
 * @param {function} callback - callback function to call
 * @return {undefined}
 */
function abortMPU(params, callback) {
    if (!params || !params.Key || !params.UploadId ||
        !params.Bucket || !params.MPU || !params.Overflow) {
        const error = errors.InvalidRequest
            .customizeDescription('Missing required parameter');
        logHelper(logger, 'error', 'error in abortMultipartUpload', error);
        return callback(error);
    }
    const delParams = {
        Bucket: params.Bucket,
        MPU: params.MPU,
        Overflow: params.Overflow,
        Prefix: createMpuKey(params.Key, params.UploadId),
    };
    return _removeParts.call(this, delParams, callback);
}

module.exports = abortMPU;
