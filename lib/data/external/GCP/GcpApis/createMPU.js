const uuid = require('uuid/v4');
const { errors } = require('arsenal');
const { createMpuKey, logger, getPutTagsMetadata } = require('../GcpUtils');
const { logHelper } = require('../../utils');

/**
 * createMPU - creates a MPU upload on GCP (sets a 0-byte object placeholder
 * with for the final composed object)
 * @param {object} params - createMPU param
 * @param {string} params.Bucket - bucket name
 * @param {string} params.Key - object key
 * @param {string} params.Metadata - object Metadata
 * @param {string} params.ContentType - Content-Type header
 * @param {string} params.CacheControl - Cache-Control header
 * @param {string} params.ContentDisposition - Content-Disposition header
 * @param {string} params.ContentEncoding - Content-Encoding header
 * @param {function} callback - callback function to call with the generated
 * upload-id for MPU operations
 * @return {undefined}
 */
function createMPU(params, callback) {
    // As google cloud does not have a create MPU function,
    // create an empty 'init' object that will temporarily store the
    // object metadata and return an upload ID to mimic an AWS MPU
    if (!params || !params.Bucket || !params.Key) {
        const error = errors.InvalidRequest
            .customizeDescription('Missing required parameter');
        logHelper(logger, 'error', 'error in createMultipartUpload', error);
        return callback(error);
    }
    const uploadId = uuid().replace(/-/g, '');
    const mpuParams = {
        Bucket: params.Bucket,
        Key: createMpuKey(params.Key, uploadId, 'init'),
        Metadata: params.Metadata,
        ContentType: params.ContentType,
        CacheControl: params.CacheControl,
        ContentDisposition: params.ContentDisposition,
        ContentEncoding: params.ContentEncoding,
    };
    mpuParams.Metadata = getPutTagsMetadata(mpuParams.Metadata, params.Tagging);
    return this.putObject(mpuParams, err => {
        if (err) {
            logHelper(logger, 'error', 'error in createMPU - putObject', err);
            return callback(err);
        }
        return callback(null, { UploadId: uploadId });
    });
}

module.exports = createMPU;
