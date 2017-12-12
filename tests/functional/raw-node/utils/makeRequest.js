const { auth } = require('arsenal');

const http = require('http');
const https = require('https');
const querystring = require('querystring');

const conf = require('../../../../lib/Config').config;
const { GcpSigner } = require('../../../../lib/data/external/GCP');

const transport = conf.https ? https : http;
const ipAddress = process.env.IP ? process.env.IP : '127.0.0.1';

function _parseError(responseBody, statusCode, jsonResponse) {
    if (jsonResponse && statusCode !== 200) {
        return JSON.parse(responseBody);
    }
    if (responseBody.indexOf('<Error>') > -1) {
        const error = {};
        const codeStartIndex = responseBody.indexOf('<Code>') + 6;
        const codeEndIndex = responseBody.indexOf('</Code>');
        error.code = responseBody.slice(codeStartIndex, codeEndIndex);
        const msgStartIndex = responseBody.indexOf('<Message>') + 9;
        const msgEndIndex = responseBody.indexOf('</Message>');
        error.message = responseBody.slice(msgStartIndex, msgEndIndex);
        return error;
    }
    return null;
}

function _decodeURI(uri) {
    // do the same decoding than in S3 server
    return decodeURIComponent(uri.replace(/\+/g, ' '));
}

/** makeRequest - utility function to generate a request
 * @param {object} params - params for making request
 * @param {string} params.hostname - request hostname
 * @param {number} [params.port] - request port
 * @param {string} params.method - request method
 * @param {object} [params.queryObj] - query fields and their string values
 * @param {object} [params.headers] - headers and their string values
 * @param {string} [params.path] - URL-encoded request path
 * @param {object} [params.authCredentials] - authentication credentials
 * @param {object} params.authCredentials.accessKey - access key
 * @param {object} params.authCredentials.secretKey - secret key
 * @param {object} params.GCP - flag to setup for GCP request
 * @param {string} [params.requestBody] - request body contents
 * @param {boolean} [params.jsonResponse] - if true, response is
 *   expected to be received in JSON format (including errors)
 * @param {function} callback - with error and response parameters
 * @return {undefined} - and call callback
 */
function makeRequest(params, callback) {
    const { hostname, port, method, queryObj, headers, path,
            authCredentials, requestBody, jsonResponse } = params;
    const options = {
        hostname,
        port,
        method,
        headers,
        path: path || '/',
        rejectUnauthorized: false,
    };

    if (queryObj) {
        const qs = querystring.stringify(queryObj);
        options.path = `${options.path}?${qs}`;
    }

    if (params.GCP && authCredentials) {
        const getAuthObject = {
            endpoint: { host: hostname },
            method,
            path: options.path || '/',
            headers,
        };
        const signer = new GcpSigner(getAuthObject);
        signer.addAuthorization(authCredentials, new Date());
        Object.assign(options.headers, {
            Authorization: getAuthObject.headers.Authorization,
            Date: getAuthObject.headers['x-goog-date'],
        });
    }

    const req = transport.request(options, res => {
        const body = [];
        res.on('data', chunk => {
            body.push(chunk);
        });
        res.on('error', err => {
            process.stdout.write('err receiving response');
            return callback(err);
        });
        res.on('end', () => {
            const total = body.join('');
            const data = {
                headers: res.headers,
                statusCode: res.statusCode,
                body: total,
            };
            const err = _parseError(total, res.statusCode, jsonResponse);
            if (err) {
                err.statusCode = res.statusCode;
            }
            return callback(err, data);
        });
    });
    req.on('error', err => {
        process.stdout.write('err sending request');
        return callback(err);
    });
    // generate v4 headers if authentication credentials are provided
    const encodedPath = req.path;
    // decode path because signing code re-encodes it
    req.path = _decodeURI(encodedPath);
    if (authCredentials && !params.GCP) {
        if (queryObj) {
            auth.client.generateV4Headers(req, queryObj,
                authCredentials.accessKey, authCredentials.secretKey, 's3');
        // may update later if request may contain POST body
        } else {
            auth.client.generateV4Headers(req, '', authCredentials.accessKey,
                authCredentials.secretKey, 's3');
        }
    }
    // restore original URL-encoded path
    req.path = encodedPath;
    if (requestBody) {
        req.write(requestBody);
    }
    req.end();
}

/** makeS3Request - utility function to generate a request against S3
 * @param {object} params - params for making request
 * @param {string} params.method - request method
 * @param {object} [params.queryObj] - query fields and their string values
 * @param {object} [params.headers] - headers and their string values
 * @param {string} [params.bucket] - bucket name
 * @param {string} [params.objectKey] - object key name
 * @param {object} [params.authCredentials] - authentication credentials
 * @param {object} params.authCredentials.accessKey - access key
 * @param {object} params.authCredentials.secretKey - secret key
 * @param {function} callback - with error and response parameters
 * @return {undefined} - and call callback
 */
function makeS3Request(params, callback) {
    const { method, queryObj, headers, bucket, objectKey, authCredentials }
        = params;
    const options = {
        authCredentials,
        hostname: process.env.AWS_ON_AIR ? 's3.amazonaws.com' : ipAddress,
        port: process.env.AWS_ON_AIR ? 80 : 8000,
        method,
        queryObj,
        headers: headers || {},
        path: bucket ? `/${bucket}/` : '/',
    };
    if (objectKey) {
        options.path = `${options.path}${objectKey}`;
    }
    makeRequest(options, callback);
}

function makeGcpRequest(params, callback) {
    const { method, queryObj, headers, bucket, objectKey, authCredentials,
        requestBody } = params;
    const options = {
        authCredentials,
        requestBody,
        hostname: 'storage.googleapis.com',
        port: 80,
        method,
        queryObj,
        headers: headers || {},
        path: bucket ? `/${bucket}/` : '/',
        GCP: true,
    };
    if (objectKey) {
        options.path = `${options.path}${objectKey}`;
    }
    makeRequest(options, callback);
}

module.exports = {
    makeRequest,
    makeS3Request,
    makeGcpRequest,
};
