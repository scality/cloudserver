const { errors } = require('arsenal');
const { getPartNumber, createMpuKey, logger } = require('../GcpUtils');
const { logHelper } = require('../../utils');

/**
 * uploadPartCopy - upload part copy
 * @param {object} params - upload part copy params
 * @param {string} params.Bucket - bucket name
 * @param {string} params.Key - object key
 * @param {string} params.CopySource - source object to copy
 * @param {function} callback - callback function to call
 * @return {undefined}
 */
function uploadPartCopy(params, callback) {
    if (!params || !params.UploadId || !params.Bucket || !params.Key ||
        !params.CopySource) {
        const error = errors.InvalidRequest
            .customizeDescription('Missing required parameter');
        logHelper(logger, 'error', 'error in uploadPartCopy', error);
        return callback(error);
    }
    const partNumber = getPartNumber(params.PartNumber);
    if (!partNumber) {
        const error = errors.InvalidArgument
            .customizeDescription('PartNumber is not a number');
        logHelper(logger, 'debug', 'error in uploadPartCopy', error);
        return callback(error);
    }
    const mpuParams = {
        Bucket: params.Bucket,
        Key: createMpuKey(params.Key, params.UploadId, partNumber),
        CopySource: params.CopySource,
    };
    return this.copyObject(mpuParams, callback);
}

module.exports = uploadPartCopy;
