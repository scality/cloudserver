const nodeFetch = require('node-fetch');
const { SignatureV4 } = require('@aws-sdk/signature-v4');
const { Sha256 } = require('@aws-crypto/sha256-js');
const { fromIni } = require('@aws-sdk/credential-provider-ini');
const { parseStringPromise } = require('xml2js');

const sendRequest = async (method, host, path, body = '', config = null) => {
    const service = 's3';
    const region = 'us-east-1';
    const endpoint = `http://${host}`;
    const url = `${endpoint}${path}`;
    const headers = {
        Host: host,
        'X-Amz-Date': new Date().toISOString().replace(/[:\-]|\.\d{3}/g, ''),
        'X-Amz-Content-SHA256': '', // will be set by signer
    };
    const credentials = config?.accessKey && config?.secretKey
        ? { accessKeyId: config.accessKey, secretAccessKey: config.secretKey }
        : await fromIni()();

    const signer = new SignatureV4({
        credentials,
        service,
        region,
        sha256: Sha256,
    });

    const signedRequest = await signer.sign({
        method: method.toUpperCase(),
        protocol: 'http:',
        hostname: host,
        path,
        headers,
        body,
    });

    const options = {
        method: signedRequest.method,
        headers: signedRequest.headers,
    };
    if (method !== 'GET') {
        options.body = body;
    }

    const response = await nodeFetch(url, options);
    const text = await response.text();
    const result = await parseStringPromise(text);
    if (result && result.Error) {
        throw result;
    }
    return result;
};

module.exports = {
    sendRequest,
};
