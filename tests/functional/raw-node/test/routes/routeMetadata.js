const assert = require('assert');
const http = require('http');

const { makeRequest } = require('../../utils/makeRequest');
const MetadataMock = require('../../utils/MetadataMock');

const ipAddress = process.env.IP ? process.env.IP : '127.0.0.1';
const metadataMock = new MetadataMock();

const metadataAuthCredentials = {
    accessKey: 'accessKey1',
    secretKey: 'verySecretKey1',
};

function makeMetadataRequest(params, callback) {
    const { method, headers, authCredentials,
        requestBody, queryObj, path } = params;
    const options = {
        authCredentials,
        hostname: ipAddress,
        port: 8000,
        method,
        headers,
        path,
        requestBody,
        jsonResponse: true,
        queryObj,
    };
    makeRequest(options, callback);
}

describe('metadata routes with metadata mock backend', () => {
    let httpServer;

    before(done => {
        httpServer = http.createServer(
            (req, res) => metadataMock.onRequest(req, res)).listen(9000);
        done();
    });

    after(done => {
        httpServer.close();
        done();
    });

    it('should retrieve list of buckets', done => {
        makeMetadataRequest({
            method: 'GET',
            authCredentials: metadataAuthCredentials,
            path: '/_/metadata/listbuckets/1',
        }, (err, res) => {
            assert.ifError(err);
            assert.strictEqual(res.statusCode, 200);
            assert(res.body);
            assert.strictEqual(res.body, '["bucket1","bucket2"]');
            return done();
        });
    });

    it('should retrieve list of objects from bucket', done => {
        makeMetadataRequest({
            method: 'GET',
            authCredentials: metadataAuthCredentials,
            path: '/_/metadata/listobjects/bucket1',
        }, (err, res) => {
            assert.ifError(err);
            assert.strictEqual(res.statusCode, 200);
            const body = JSON.parse(res.body);
            assert.strictEqual(body.Contents[0].key, 'testobject1');
            return done();
        });
    });

    it('should retrieve metadata of bucket', done => {
        makeMetadataRequest({
            method: 'GET',
            authCredentials: metadataAuthCredentials,
            path: '/_/metadata/getbucket/bucket1',
        }, (err, res) => {
            assert.ifError(err);
            assert.strictEqual(res.statusCode, 200);
            assert(res.body);
            return done();
        });
    });

    it('should retrieve metadata of object', done => {
        makeMetadataRequest({
            method: 'GET',
            authCredentials: metadataAuthCredentials,
            path: '/_/metadata/getobject/bucket1/testobject1',
        }, (err, res) => {
            assert.ifError(err);
            assert(res.body);
            assert.strictEqual(res.statusCode, 200);
            const body = JSON.parse(res.body);
            assert.strictEqual(body.metadata, 'dogsAreGood');
            return done();
        });
    });
});
