const { errors } = require('arsenal');
const { createMpuKey, logger } = require('../GcpUtils');
const { logHelper } = require('../../utils');

/**
 * listParts - list uploaded MPU parts
 * @param {object} params - listParts param
 * @param {string} params.Bucket - bucket name
 * @param {string} params.Key - object key
 * @param {string} params.UploadId - MPU upload id
 * @param {function} callback - callback function to call with the list of parts
 * @return {undefined}
 */
function listParts(params, callback) {
    if (!params || !params.UploadId || !params.Bucket || !params.Key) {
        const error = errors.InvalidRequest
            .customizeDescription('Missing required parameter');
        logHelper(logger, 'error', 'error in listParts', error);
        return callback(error);
    }
    if (params.PartNumberMarker && params.PartNumberMarker < 0) {
        return callback(errors.InvalidArgument);
    }
    const mpuParams = {
        Bucket: params.Bucket,
        Prefix: createMpuKey(params.Key, params.UploadId, 'parts'),
        Marker: createMpuKey(params.Key, params.UploadId,
            params.PartNumberMarker, 'parts'),
        MaxKeys: params.MaxParts,
    };
    return this.listObjects(mpuParams, (err, res) => {
        if (err) {
            logHelper(logger, 'error',
                'error in listParts - listObjects', err);
            return callback(err);
        }
        return callback(null, res);
    });
}

module.exports = listParts;
