const nodeFetch = require('node-fetch');
const { HttpRequest } = require('@aws-sdk/protocol-http');
const { SignatureV4 } = require('@aws-sdk/signature-v4');
const { Sha256 } = require('@aws-crypto/sha256-js');
const xml2js = require('xml2js');

const sendRequest = async (method, host, path, body = '', config = null, signingDate = new Date()) => {
    const service = 's3';
    const region = 'us-east-1';

    // Ensure host includes port for canonical request
    const hostname = host.split(':')[0]; // Extract 127.0.0.1
    const port = parseInt(host.split(':')[1] || '8000', 10); // Default to 8000
    const [pathBase, queryString] = path.split('?');
    const query = queryString ? Object.fromEntries(new URLSearchParams(queryString)) : {};

    // Create HTTP request (mimics AWS.HttpRequest with v2-like endpoint structure)
    const request = new HttpRequest({
        protocol: 'http:', // Match Scality CloudServer
        hostname, // 127.0.0.1
        port, // 8000
        method: method.toUpperCase(),
        path: pathBase,
        query,
        body,
        headers: {
            Host: host, // Explicitly set Host: 127.0.0.1:8000
            'X-Amz-Date': signingDate.toISOString().replace(/[:\-]|\.\d{3}/g, ''),
        },
    });

    // Compute SHA256 hash for body
    const sha256 = new Sha256();
    sha256.update(request.body || '');
    const hash = await sha256.digest();
    request.headers['X-Amz-Content-SHA256'] = Buffer.from(hash).toString('hex');
    request.region = region;

    // Get credentials
    const accessKeyId = config?.accessKey || config?.accessKeyId || 'accessKey1';
    const secretAccessKey = config?.secretKey || config?.secretAccessKey || 'verySecretKey1';
    if (!accessKeyId || !secretAccessKey) {
        throw new Error('Missing accessKeyId or secretAccessKey in config');
    }
    const credentials = { accessKeyId, secretAccessKey };

    // Create signer
    const signer = new SignatureV4({
        credentials,
        region,
        service,
        sha256: Sha256,
        uriEscapePath: true,
        applyChecksum: true,
    });

    // Sign request
    const signedRequest = await signer.sign(request, { signingDate });

    // Rename 'authorization' to 'Authorization'
    if (signedRequest.headers.authorization) {
        signedRequest.headers.Authorization = signedRequest.headers.authorization;
        delete signedRequest.headers.authorization;
    }

    // Send HTTP request
    const url = `http://${host}${path}`; // Match Scality CloudServer
    const options = {
        method: signedRequest.method,
        headers: signedRequest.headers,
    };

    if (method.toUpperCase() !== 'GET') {
        options.body = signedRequest.body;
    }

    let response;
    try {
        response = await (nodeFetch.default || nodeFetch)(url, options);
    } catch (error) {
        throw new Error(`HTTP request failed: ${error.message}`);
    }
    const text = await response.text();

    let result;
    try {
        result = await xml2js.parseStringPromise(text);
    } catch {
        result = { Error: { Message: text } };
    }
    if (result && result.Error) {
        throw result;
    }

    return {
        result,
        status: response.status,
        ok: response.ok,
        error: result?.Error ? text : null,
        request: signedRequest,
    };
};

module.exports = {
    sendRequest,
};
