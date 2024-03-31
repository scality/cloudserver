// const fetch = require('node-fetch');
const AWS = require('aws-sdk');
const commander = require('commander');

const sendRequest = async (method, host, path, body = '') => {
    const service = 's3';
    const endpoint = new AWS.Endpoint(host);

    const request = new AWS.HttpRequest(endpoint);
    request.method = method.toUpperCase();
    request.path = path;
    request.body = body;
    request.headers.Host = host; // Use dot notation
    request.headers['X-Amz-Date'] = new Date().toISOString().replace(/[:\-]|\.\d{3}/g, '');
    const sha256hash = AWS.util.crypto.sha256(request.body || '', 'hex');
    request.headers['X-Amz-Content-SHA256'] = sha256hash;
    request.region = AWS.config.region;

    const signer = new AWS.Signers.V4(request, service);
    signer.addAuthorization(AWS.config.credentials, new Date());

    // const url = `http://${host}${path}`;
    const options = {
        method: request.method,
        headers: request.headers
    };

    if (method !== 'GET' && method !== 'HEAD') {
        options.body = request.body;
    }

    // try {
    //     const response = await fetch(url, options);
    //     const data = await response.text();
    // } catch (error) {
    //     // Handle errors if needed
    // }
};

commander
    .version('0.0.1')
    .arguments('<method> <host> <path> [body]')
    .action((method, host, path, body) => {
        sendRequest(method, host, path, body);
    })
    .parse(process.argv);
