const { makeRequest } = require('../raw-node/utils/makeRequest');

const ipAddress = process.env.IP ? process.env.IP : '127.0.0.1';
const { models: { ObjectMD } } = require('arsenal');

// NOTE: The routes "getMetadata" and "putMetadata" are utilized for modifying the metadata of an object.
// This approach is preferred over directly updating the metadata in MongoDB,
// as it allows the tests to be compatible with S3C metadata.
function updateMetadata(params, toUpdate, cb) {
    const { bucket, objectKey, versionId, authCredentials } = params;
    const { dataStoreName } = toUpdate;
    const options = {
        authCredentials,
        hostname: ipAddress,
        port: 8000,
        method: 'GET',
        path: `/_/backbeat/metadata/${bucket}/${objectKey}`,
        jsonResponse: true,
    };
    if (versionId) {
        options.queryObj = { versionId };
    }
    return makeRequest(options, (err, data) => {
        if (err) {
            return cb(err);
        }
        let parsedBody;
        try {
            parsedBody = JSON.parse(data.body);
        } catch (err) {
            return cb(err);
        }
        const { result, error } = ObjectMD.createFromBlob(parsedBody.Body);
        if (error) {
            return cb(error);
        }

        if (dataStoreName) {
            result.setDataStoreName(dataStoreName);
        }
        const options = {
            authCredentials,
            hostname: ipAddress,
            port: 8000,
            method: 'PUT',
            path: `/_/backbeat/metadata/${bucket}/${objectKey}`,
            requestBody: result.getSerialized(),
            jsonResponse: true,
        };
        if (versionId) {
            options.queryObj = { versionId };
        }
        return makeRequest(options, err => cb(err));
    });
}

/** makeBackbeatRequest - utility function to generate a request going
 * through backbeat route
 * @param {object} params - params for making request
 * @param {string} params.method - request method
 * @param {string} params.bucket - bucket name
 * @param {string} params.subCommand - subcommand to backbeat
 * @param {object} [params.headers] - headers and their string values
 * @param {object} [params.authCredentials] - authentication credentials
 * @param {object} params.authCredentials.accessKey - access key
 * @param {object} params.authCredentials.secretKey - secret key
 * @param {string} [params.requestBody] - request body contents
 * @param {function} callback - with error and response parameters
 * @return {undefined} - and call callback
 */
function makeBackbeatRequest(params, callback) {
    const { method, headers, bucket, authCredentials, queryObj } = params;
    const options = {
        hostname: ipAddress,
        port: 8000,
        method,
        headers,
        authCredentials,
        path: `/_/backbeat/lifecycle/${bucket}`,
        jsonResponse: true,
        queryObj,
    };
    makeRequest(options, callback);
}

module.exports = {
    makeBackbeatRequest,
    updateMetadata,
};
