const async = require('async');
const request = require('request');

const { jsonRespCheck } = require('../GcpUtils');

/**
 * updateMetadata - update the metadata of an object. Only used when
 * changes to an object metadata should not affect the version id. Example:
 * objectTagging, in which creation/deletion of medatadata is required for GCP,
 * and copyObject.
 * @param {object} params - update metadata params
 * @param {string} params.Bucket - bucket name
 * @param {string} params.Key - object key
 * @param {string} params.VersionId - object version id
 * @param {function} callback - callback function to call with the object result
 * @return {undefined}
 */
function updateMetadata(params, callback) {
    async.waterfall([
        next => this.getToken((err, res) => next(err, res)),
        (token, next) => {
            const uri = '/storage/v1' +
                        `/b/${encodeURIComponent(params.Bucket)}` +
                        `/o/${encodeURIComponent(params.Key)}`;
            const body = {
                acl: {},
                metadata: params.Metadata,
                generation: params.VersionId,
            };
            request({
                method: 'PUT',
                baseUrl: this.config.jsonEndpoint,
                proxy: this.config.proxy,
                uri,
                body,
                json: true,
                auth: { bearer: token } },
            (err, resp, body) =>
                jsonRespCheck(err, resp, body, 'updateMetadata', next));
        },
    ], callback);
}

module.exports = updateMetadata;
