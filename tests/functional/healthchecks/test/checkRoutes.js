const assert = require('assert');
const http = require('http');
const https = require('https');
const conf = require('../../config.json');

const transportStr = conf.transport;
const transport = transportStr === 'http' ? http : https;
const options = {
    host: conf.ipAddress,
    path: '/_/healthcheck',
    port: 8000,
};
if (transportStr === 'https') {
    options.rejectUnauthorized = false;
    options.agent = new https.Agent(options);
}

function checkResult(expectedStatus, res) {
    const actualStatus = res.statusCode;
    assert.strictEqual(actualStatus, expectedStatus);
}

function makeChecker(expectedStatus, done) {
    return res => {
        checkResult(expectedStatus, res);
        done();
    };
}

function deepCopy(options) {
    return JSON.parse(JSON.stringify(options));
}

describe('Healthcheck routes', () => {
    it('should return 200 OK on GET request', done => {
        const getOptions = deepCopy(options);
        getOptions.method = 'GET';
        const req = transport.request(getOptions, makeChecker(200, done));
        req.end();
    });
    it('should return 200 OK on POST request', done => {
        const postOptions = deepCopy(options);
        postOptions.method = 'POST';
        const req = transport.request(postOptions, makeChecker(200, done));
        req.end();
    });
    it('should return 400 on other requests', done => {
        const putOptions = deepCopy(options);
        putOptions.method = 'PUT';
        const req = transport.request(putOptions, makeChecker(400, done));
        req.end();
    });
});
