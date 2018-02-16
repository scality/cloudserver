const async = require('async');
const request = require('request');
const uuid = require('uuid/v4');
const { errors } = require('arsenal');

const { jsonRespCheck } = require('../GcpUtils');

function formBatchRequest(bucket, deleteList) {
    let retBody = '';
    const boundary = uuid().replace(/-/g, '');

    deleteList.forEach(object => {
        // add boundary
        retBody += `--${boundary}\r\n`;
        // add req headers
        retBody += `Content-Type: application/http\r\n`;
        retBody += '\r\n';
        const key = object.Key;
        const versionId = object.VersionId;
        let path = `/storage/v1/b/${bucket}/o/${encodeURIComponent(key)}`;
        if (versionId) path += `?generation=${versionId}`;
        retBody += `DELETE ${path} HTTP/1.1\r\n`;
        retBody += '\r\n';
    });
    retBody += `--${boundary}\r\n`;
    return { body: retBody, boundary };
}

/**
 * deleteObjects - delete a list of objects
 * @param {object} params - deleteObjects parameters
 * @param {string} params.Bucket - bucket location
 * @param {object} params.Delete - delete config object
 * @param {object[]} params.Delete.Objects - a list of objects to be deleted
 * @param {string} params.Delete.Objects[].Key - object key
 * @param {string} params.Delete.Objects[].VersionId - object version Id, if
 * not given the master version will be archived
 * @param {function} callback - callback function to call when a batch response
 * is returned
 * @return {undefined}
 */
function deleteObjects(params, callback) {
    if (!params || !params.Delete || !params.Delete.Objects) {
        return callback(errors.MalformedXML);
    }
    return async.waterfall([
        next => this.getToken((err, res) => next(err, res)),
        (token, next) => {
            const { body, boundary } =
                formBatchRequest(params.Bucket, params.Delete.Objects, token);
            request({
                method: 'POST',
                baseUrl: this.config.jsonEndpoint,
                proxy: this.config.proxy,
                uri: '/batch',
                headers: {
                    'Content-Type': `multipart/mixed; boundary=${boundary}`,
                },
                body,
                auth: { bearer: token },
            },
            // batch response is a string of http bodies
            // attempt to parse response body
            // if body element can be transformed into an object
            // there then check if the response is a error object
            // TO-DO: maybe, check individual batch op response
            (err, resp, body) =>
                jsonRespCheck(err, resp, body, 'deleteObjects', next));
        },
    ], callback);
}

module.exports = deleteObjects;
