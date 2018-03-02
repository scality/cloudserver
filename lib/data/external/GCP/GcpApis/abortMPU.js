const { errors } = require('arsenal');
const MpuHelper = require('./mpuHelper');
const { createMpuKey, logger } = require('../GcpUtils');
const { logHelper } = require('../../utils');

/**
 * abortMPU - remove all objects of a GCP Multipart Upload
 * @param {object} params - abortMPU params
 * @param {string} params.Bucket - bucket name
 * @param {string} params.MPU - mpu bucket name
 * @param {string} params.Key - object key
 * @param {number} params.UploadId - MPU upload id
 * @param {function} callback - callback function to call
 * @return {undefined}
 */
function abortMPU(params, callback) {
    if (!params || !params.Key || !params.UploadId || !params.Bucket) {
        const error = errors.InvalidRequest
            .customizeDescription('Missing required parameter');
        logHelper(logger, 'error', 'error in abortMultipartUpload', error);
        return callback(error);
    }
    const mpuHelper = new MpuHelper(this);
    const delParams = {
        Bucket: params.Bucket,
        MPU: params.MPU,
        Prefix: createMpuKey(params.Key, params.UploadId),
    };
    return mpuHelper.removeParts(delParams, callback);
}

module.exports = abortMPU;
