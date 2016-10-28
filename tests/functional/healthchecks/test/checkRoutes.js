const assert = require('assert');
const http = require('http');
const https = require('https');
const conf = require('../../config.json');
const fs = require('fs');

const transportStr = conf.transport;
const transport = transportStr === 'http' ? http : https;
const options = {
    host: conf.ipAddress,
    path: '/_/healthcheck',
    port: 8000,
};

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

function makeAgent() {
    if (transportStr === 'https') {
        const newAgent = new https.Agent({
            ca: fs.readFileSync(conf.caCertPath),
        });
        return newAgent;
    }
    return undefined;
}

describe('Healthcheck routes', () => {
    it('should return 200 OK on GET request', done => {
        const getOptions = deepCopy(options);
        getOptions.method = 'GET';
        getOptions.agent = makeAgent();
        const req = transport.request(getOptions, makeChecker(200, done));
        req.end();
    });
    it('should return 200 OK on POST request', done => {
        const postOptions = deepCopy(options);
        postOptions.method = 'POST';
        postOptions.agent = makeAgent();
        const req = transport.request(postOptions, makeChecker(200, done));
        req.end();
    });
    it('should return 400 on other requests', done => {
        const putOptions = deepCopy(options);
        putOptions.method = 'PUT';
        putOptions.agent = makeAgent();
        const req = transport.request(putOptions, makeChecker(400, done));
        req.end();
    });
});
