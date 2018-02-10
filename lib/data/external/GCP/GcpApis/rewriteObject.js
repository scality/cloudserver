const async = require('async');
const request = require('request');

const { jsonRespCheck } = require('../GcpUtils');

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
    async.waterfall([
        next => this.getToken((err, res) => next(err, res)),
        (token, next) => {
            const uri = '/storage/v1' +
                        `/b/${encodeURIComponent(params.SourceBucket)}` +
                        `/o/${encodeURIComponent(params.SourceObject)}` +
                        '/rewriteTo' +
                        `/b/${encodeURIComponent(params.DestinationBucket)}` +
                        `/o/${encodeURIComponent(params.DestinationObject)}`;
            const qs = {
                sourceGeneration: params.SourceVersionId,
                rewriteToken: params.RewriteToken,
            };
            request({
                method: 'POST',
                baseUrl: this.config.jsonEndpoint,
                proxy: this.config.proxy,
                uri,
                qs,
                auth: { bearer: token } },
            (err, resp, body) =>
                jsonRespCheck(err, resp, body, 'rewriteObject', next));
        },
    ], callback);
}

module.exports = rewriteObject;
