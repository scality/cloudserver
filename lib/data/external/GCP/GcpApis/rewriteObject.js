const async = require('async');
const request = require('request');
const { errors } = require('arsenal');

const { getSourceInfo, jsonRespCheck } = require('../GcpUtils');

/**
 * rewriteObject - copy object between buckets of different storage class or
 * regions. As copyObject has inconsistent results when performed on large
 * objects across different buckets
 * @param {object} params - JSON request parameters
 * @param {string} params.SourceBucket - copy source bucket
 * @param {string} params.SourceObject - copy source object
 * @param {string} params.SourceVersionId - specify source version
 * @param {string} params.DestinationBucket - copy destination bucket
 * @param {string} params.DestinationObject - copy destination object
 * @param {string} param.RewriteToken - token to pick up where previous rewrite
 * had left off
 * @param {function} callback - callback function to call with object rewrite
 * results
 * @return {undefined}
 */
function rewriteObject(params, callback) {
    const { CopySource } = params;
    if (!CopySource) {
        return callback(errors.MissingParameter);
    }
    if (typeof CopySource !== 'string') {
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
                        '/rewriteTo' +
                        `/b/${encodeURIComponent(params.Bucket)}` +
                        `/o/${encodeURIComponent(params.Key)}`;
            const qs = {
                sourceGeneration: params.SourceVersionId,
                rewriteToken: params.RewriteToken,
            };
            let rewriteDone = false;
            return async.whilst(() => !rewriteDone, done => {
                request({
                    method: 'POST',
                    baseUrl: this.config.jsonEndpoint,
                    proxy: this.config.proxy,
                    uri,
                    qs,
                    auth: { bearer: token } },
                (err, resp, body) =>
                jsonRespCheck(err, resp, body, 'rewriteObject',
                (err, res) => {
                    if (err) {
                        return done(err);
                    }
                    rewriteDone = res.done;
                    qs.rewriteToken = res.rewriteToken;
                    return done(null, res);
                }));
            }, (err, result) => {
                if (err) {
                    return next(err);
                }
                return next(null, result.resource);
            });
        },
    ], callback);
}

module.exports = rewriteObject;
