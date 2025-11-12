const nodeFetch = require('node-fetch');
const AWS = require('aws-sdk');
const xml2js = require('xml2js');
const { getCredentials } = require('../support/credentials');

const { config } = require('../../../../../lib/Config');

const skipIfRateLimitDisabled = config.rateLimiting.enabled ? describe : describe.skip;

async function sendRateLimitRequest(method, host, path, body = '') {
    const service = 's3';
    const endpoint = new AWS.Endpoint(host);

    const request = new AWS.HttpRequest(endpoint);
    request.method = method.toUpperCase();
    request.path = path;
    request.body = body;
    request.headers.Host = host;
    request.headers['X-Amz-Date'] = new Date().toISOString().replace(/[:\-]|\.\d{3}/g, '');
    const sha256hash = AWS.util.crypto.sha256(request.body || '', 'hex');
    request.headers['X-Amz-Content-SHA256'] = sha256hash;
    request.region = 'us-east-1';

    const signer = new AWS.Signers.V4(request, service);
    const credentials = getCredentials('lisa');
    const awsCredentials = new AWS.Credentials(
        credentials.accessKeyId,
        credentials.secretAccessKey
    );
    signer.addAuthorization(awsCredentials, new Date());

    const url = `http://${host}${path}`;
    const options = {
        method: request.method,
        headers: request.headers,
    };

    if (method !== 'GET' && method !== 'DELETE') {
        options.body = request.body;
    }

    const response = await nodeFetch(url, options);
    const text = await response.text();

    // Check if response is successful
    if (!response.ok) {
        // Try to parse as XML error first (S3 errors are typically XML)
        let xmlResult;
        try {
            xmlResult = await xml2js.parseStringPromise(text);
        } catch {
            // XML parsing failed, will try JSON below
        }

        if (xmlResult && xmlResult.Error) {
            throw xmlResult;
        }

        // If XML parsing failed or no Error in XML, try JSON
        try {
            const json = JSON.parse(text);
            if (json.error) {
                throw json;
            }
        } catch {
            // If both fail, throw the original error
            throw new Error(`Request failed with status ${response.status}: ${text}`);
        }
    }

    if (!text.trim()) {
        return null;
    }

    return JSON.parse(text);
}

module.exports = {
    sendRateLimitRequest,
    skipIfRateLimitDisabled,
};
