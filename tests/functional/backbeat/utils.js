const { makeRequest } = require('../raw-node/utils/makeRequest');

const ipAddress = process.env.IP ? process.env.IP : '127.0.0.1';

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

const runIfMongoV1 = process.env.S3METADATA === 'mongodb' && process.env.DEFAULT_BUCKET_KEY_FORMAT === 'v1' ?
    describe : describe.skip;

module.exports = {
    makeBackbeatRequest,
    runIfMongoV1,
};
