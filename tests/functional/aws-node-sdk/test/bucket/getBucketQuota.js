const { S3 , AWS } = require('aws-sdk');

const async = require('async');
const assert = require('assert');
const getConfig = require('../support/config');

const bucket = 'getquotatestbucket';

const sendRequest = async (method, host, path, body = '') => {
    const service = 's3';
    const endpoint = new AWS.Endpoint(host);

    const request = new AWS.HttpRequest(endpoint);
    request.method = method.toUpperCase();
    request.path = path;
    request.body = body;
    request.headers.Host = "127.0.0.1:8000"
    request.headers['X-Amz-Date'] = new Date().toISOString().replace(/[:\-]|\.\d{3}/g, '');
    const sha256hash = AWS.util.crypto.sha256(request.body || '', 'hex');
    request.headers['X-Amz-Content-SHA256'] = sha256hash;
    request.region = AWS.config.region;

    const signer = new AWS.Signers.V4(request, service);
    signer.addAuthorization(AWS.config.credentials, new Date());

    const options = {
        method: request.method,
        headers: request.headers
    };

    if (method !== 'GET' && method !== 'HEAD') {
        options.body = request.body;
    }

    try {
        const response = await fetch(url, options);
        const data = await response.text();
    } catch (error) {
       assert.ifError(error);
    }
};

describe('aws-sdk test get bucket quota', () => {
    let s3;

    before(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
    });

    beforeEach(done => s3.createBucket({ Bucket: bucket }, done));

    afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

    it('should return the Quota', done => {
        const quota = { quota: 1000 };
        async.series([
            next => sendRequest('PUT', '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota), next),
            next => sendRequest('GET', '127.0.0.1:8000', `/${bucket}/?quota=true`, {}, next),
        ], (err, data) => {
            assert.ifError(err);
            assert.deepStrictEqual(data, quota);
            done();
        });
    });
});
