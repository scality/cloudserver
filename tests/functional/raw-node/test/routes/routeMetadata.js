const assert = require('assert');
const http = require('http');

const { makeRequest } = require('../../utils/makeRequest');
const MetadataMock = require('../../utils/MetadataMock');

const ipAddress = process.env.IP ? process.env.IP : 'localhost';
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

    beforeAll(done => {
        httpServer = http.createServer(
            (req, res) => metadataMock.onRequest(req, res)).listen(9000, done);
    });

    afterAll(() => httpServer.close());

    test('should retrieve list of buckets', done => {
        makeMetadataRequest({
            method: 'GET',
            authCredentials: metadataAuthCredentials,
            path: '/_/metadata/admin/raft_sessions/1/bucket',
        }, (err, res) => {
            assert.ifError(err);
            expect(res.statusCode).toBe(200);
            expect(res.body).toBeTruthy();
            expect(res.body).toBe('["bucket1","bucket2"]');
            return done();
        });
    });

    test('should retrieve list of objects from bucket', done => {
        makeMetadataRequest({
            method: 'GET',
            authCredentials: metadataAuthCredentials,
            path: '/_/metadata/default/bucket/bucket1',
            queryObj: { listingType: 'Delimiter' },
        }, (err, res) => {
            assert.ifError(err);
            expect(res.statusCode).toBe(200);
            const body = JSON.parse(res.body);
            expect(body.Contents[0].key).toBe('testobject1');
            return done();
        });
    });

    test('should retrieve metadata of bucket', done => {
        makeMetadataRequest({
            method: 'GET',
            authCredentials: metadataAuthCredentials,
            path: '/_/metadata/default/attributes/bucket1',
        }, (err, res) => {
            assert.ifError(err);
            expect(res.statusCode).toBe(200);
            expect(res.body).toBeTruthy();
            return done();
        });
    });

    test('should retrieve metadata of object', done => {
        makeMetadataRequest({
            method: 'GET',
            authCredentials: metadataAuthCredentials,
            path: '/_/metadata/default/bucket/bucket1/testobject1',
        }, (err, res) => {
            assert.ifError(err);
            expect(res.body).toBeTruthy();
            expect(res.statusCode).toBe(200);
            const body = JSON.parse(res.body);
            expect(body.metadata).toBe('dogsAreGood');
            return done();
        });
    });

    test('should get an error for accessing invalid routes', done => {
        makeMetadataRequest({
            method: 'GET',
            authCredentials: metadataAuthCredentials,
            path: '/_/metadata/admin/raft_sessions',
        }, err => {
            expect(err.code).toBe('NotImplemented');
            return done();
        });
    });
});
