const fetch = require('node-fetch');
const AWS = require('aws-sdk');
const xml2js = require('xml2js');

const sendRequest = async (method, host, path, body = '') =>
    new Promise(async (resolve, reject) => {
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
        signer.addAuthorization(AWS.config.credentials, new Date());

        const url = `http://${host}${path}`;
        const options = {
            method: request.method,
            headers: request.headers,
        };

        if (method !== 'GET') {
            options.body = request.body;
        }

        try {
            const response = await fetch(url, options);
            const text = await response.text();
            const result = await xml2js.parseStringPromise(text);
            if (result && result.Error) {
                reject(result);
            } else {
                resolve(result);
            }
        } catch (error) {
            reject(error);
        }
    });

module.exports = {
    sendRequest,
};
