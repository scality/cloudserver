'use strict'; // eslint-disable-line strict
const assert = require('assert');
const http = require('http');
const https = require('https');
const fs = require('fs');
const async = require('async');
const Redis = require('ioredis');

const conf = require('../../config.json');

const redis = new Redis({
    host: conf.localCache.host,
    port: conf.localCache.port,
    // disable offline queue
    enableOfflineQueue: false,
});

redis.on('error', () => {});

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
        return new https.Agent({
            ca: fs.readFileSync(conf.caCertPath),
        });
    }
    return undefined;
}

function makeDummyS3Request(cb) {
    const getOptions = deepCopy(options);
    getOptions.path = '/foo/bar';
    getOptions.method = 'GET';
    getOptions.agent = makeAgent();
    const req = transport.request(getOptions);
    req.end(() => cb());
}

function makeStatsRequest(cb) {
    const getOptions = deepCopy(options);
    getOptions.method = 'GET';
    getOptions.agent = makeAgent();
    const req = transport.request(getOptions, res => {
        const chunks = [];
        res.on('data', c => chunks.push(c));
        res.on('end', () => cb(null, Buffer.concat(chunks).toString()));
    });
    req.on('error', err => cb(err));
    req.end();
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
    it('should return 200 on deep GET request', done => {
        const deepOptions = deepCopy(options);
        deepOptions.method = 'GET';
        deepOptions.path = '/_/healthcheck/deep';
        deepOptions.agent = makeAgent();
        const req = transport.request(deepOptions, makeChecker(200, done));
        req.end();
    });
});

describe('Healthcheck stats', () => {
    const totalReqs = 5;
    beforeEach(done => {
        redis.flushdb(() => {
            async.timesSeries(totalReqs,
                (n, next) => makeDummyS3Request(next), done);
        });
    });

    afterEach(() => redis.flushdb());

    it('should respond back with total requests', done =>
        makeStatsRequest((err, res) => {
            if (err) {
                return done(err);
            }
            const expectedStatsRes = { 'requests': totalReqs, '500s': 0,
                'sampleDuration': 30 };
            assert.deepStrictEqual(JSON.parse(res), expectedStatsRes);
            return done();
        })
    );
});
