const async = require('async');
const request = require('request');
const { errors } = require('arsenal');

const { getSourceInfo, jsonRespCheck } = require('../GcpUtils');

/**
 * copyObject - minimum required functionality to perform object copy
 * for GCP Backend
 * @param {object} params - update metadata params
 * @param {string} params.Bucket - bucket name
 * @param {string} params.Key - object key
 * @param {string} param.CopySource - source object
 * @param {function} callback - callback function to call with the copy object
 * result
 * @return {undefined}
 */
function copyObject(params, callback) {
    const { CopySource } = params;
    if (!CopySource || typeof CopySource !== 'string') {
        return callback(errors.InvalidArgument);
    }
    const { sourceBucket, sourceObject } = getSourceInfo(CopySource);
    if (!sourceBucket || !sourceObject) {
        return callback(errors.InvalidArgument);
    }
    return async.waterfall([
        next => this.getToken((err, res) => next(err, res)),
        (token, next) => {
            const uri = '/storage/v1' +
                        `/b/${encodeURIComponent(sourceBucket)}` +
                        `/o/${encodeURIComponent(sourceObject)}` +
                        '/copyTo' +
                        `/b/${encodeURIComponent(params.Bucket)}` +
                        `/o/${encodeURIComponent(params.Key)}`;
            request({
                method: 'POST',
                baseUrl: this.config.jsonEndpoint,
                proxy: this.config.proxy,
                uri,
                auth: { bearer: token } },
            (err, resp, body) =>
                jsonRespCheck(err, resp, body, 'copyObject', next));
        },
        (result, next) => {
            // if metadata directive is REPLACE then perform a metadata update
            // otherwise default to COPY
            const hasEtagMpu = result.metadata && result.metadata['scal-ETag'];
            if (params.MetadataDirective === 'REPLACE' || hasEtagMpu) {
                const cleanMetadata = Object.assign({}, result.metadata);
                delete cleanMetadata['scal-ETag'];
                const updateParams = {
                    Bucket: params.Bucket,
                    Key: params.Key,
                    Metadata: params.Metadata || cleanMetadata || {},
                    VersionId: result.generation,
                };
                return this.updateMetadata(updateParams, next);
            }
            return next(null, result);
        },
    ], (err, result) => {
        if (err) {
            return callback(err);
        }
        const md5Hash = result.md5Hash ?
            Buffer.from(result.md5Hash, 'base64').toString('hex') : null;
        const resObj = {
            CopyObjectResult: {
                ETag: md5Hash,
                LastModified: result.updated,
            },
            ContentLength: result.size,
            VersionId: result.generation,
        };
        return callback(null, resObj);
    });
}

module.exports = copyObject;
