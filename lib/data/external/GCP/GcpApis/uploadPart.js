const { errors } = require('arsenal');
const { createMpuKey, logger } = require('../GcpUtils');
const { logHelper } = require('../../utils');

/**
 * uploadPart - upload part
 * @param {object} params - upload part params
 * @param {string} params.Bucket - bucket name
 * @param {string} params.Key - object key
 * @param {function} callback - callback function to call
 * @return {undefined}
 */
function uploadPart(params, callback) {
    if (!params || !params.UploadId || !params.Bucket || !params.Key) {
        const error = errors.InvalidRequest
            .customizeDescription('Missing required parameter');
        logHelper(logger, 'error', 'error in uploadPart', error);
        return callback(error);
    }
    const mpuParams = {
        Bucket: params.Bucket,
        Key: createMpuKey(params.Key, params.UploadId, params.PartNumber),
        Body: params.Body,
        ContentLength: params.ContentLength,
    };
    return this.putObjectReq(mpuParams, (err, res) => {
        if (err) {
            logHelper(logger, 'error',
                'error in uploadPart - putObject', err);
            return callback(err);
        }
        return callback(null, res);
    });
}

module.exports = uploadPart;
