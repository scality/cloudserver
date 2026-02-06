const nodeFetch = require('node-fetch');
const { SignatureV4 } = require('@aws-sdk/signature-v4');
const { HttpRequest } = require('@aws-sdk/protocol-http');
const { Sha256 } = require('@aws-crypto/sha256-js');
const xml2js = require('xml2js');
const { URL } = require('url');
const { getCredentials } = require('../support/credentials');

const { config } = require('../../../../../lib/Config');

const skipIfRateLimitDisabled = config.rateLimiting.enabled ? describe : describe.skip;

function buildHttpRequest(method, host, rawPath, body = '') {
    const endpoint = new URL(`http://${host}`);
    const target = new URL(rawPath, `http://${host}`);

    const query = {};
    target.searchParams.forEach((value, key) => {
        if (Object.prototype.hasOwnProperty.call(query, key)) {
            const current = query[key];
            query[key] = Array.isArray(current) ? current.concat(value) : [current, value];
        } else {
            query[key] = value ?? '';
        }
    });

    const request = new HttpRequest({
        method: method.toUpperCase(),
        protocol: endpoint.protocol,
        hostname: endpoint.hostname,
        port: endpoint.port ? Number(endpoint.port) : undefined,
        path: target.pathname,
        query: Object.keys(query).length ? query : undefined,
        headers: {
            host,
        },
        body: body || undefined,
    });

    return { request, target };
}

async function sendRateLimitRequest(method, host, path, body = '') {
    const { request, target } = buildHttpRequest(method, host, path, body);

    const credentials = getCredentials('lisa');
    const signer = new SignatureV4({
        credentials,
        region: 'us-east-1',
        service: 's3',
        sha256: Sha256,
    });
    const signedRequest = await signer.sign(request);

    const url = target.href;
    const options = {
        method: signedRequest.method,
        headers: signedRequest.headers,
    };

    if (signedRequest.body && method !== 'GET' && method !== 'DELETE') {
        options.body = signedRequest.body;
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
