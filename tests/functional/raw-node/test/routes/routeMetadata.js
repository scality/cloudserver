const assert = require('assert');
const http = require('http');

const { makeRequest } = require('../../utils/makeRequest');
const MetadataMock = require('../../utils/MetadataMock');
const { getCredentials } = require('../../../aws-node-sdk/test/support/credentials');
const BucketUtility = require('../../../aws-node-sdk/lib/utility/bucket-util');
const metadataMock = new MetadataMock();

const ipAddress = process.env.IP ? process.env.IP : 'localhost';

const { accessKeyId, secretAccessKey } = getCredentials();

const metadataAuthCredentials = {
    accessKey: accessKeyId,
    secretKey: secretAccessKey,
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

describe('metadata routes with metadata', () => {
    const bucketUtil = new BucketUtility(
        'default', { signatureVersion: 'v4' });
    const s3 = bucketUtil.s3;

    const bucket1 = 'bucket1';
    const bucket2 = 'bucket2';
    const keyName = 'testobject1';

    // E2E tests use S3C metadata, whereas functional tests use mocked metadata.
    if (process.env.S3_END_TO_END) {
        before(done => s3.createBucket({ Bucket: bucket1 }).promise()
            .then(() => s3.putObject({ Bucket: bucket1, Key: keyName, Body: '' }).promise())
            .then(() => s3.createBucket({ Bucket: bucket2 }).promise())
            .then(() => done(), err => done(err))
        );

        after(done => bucketUtil.empty(bucket1)
            .then(() => s3.deleteBucket({ Bucket: bucket1 }).promise())
            .then(() => bucketUtil.empty(bucket2))
            .then(() => s3.deleteBucket({ Bucket: bucket2 }).promise())
            .then(() => done(), err => done(err))
        );
    } else {
        let httpServer;

        before(done => {
            httpServer = http.createServer(
                (req, res) => metadataMock.onRequest(req, res)).listen(9000, done);
        });

        after(() => httpServer.close());
    }

    it('should retrieve list of buckets', done => {
        makeMetadataRequest({
            method: 'GET',
            authCredentials: metadataAuthCredentials,
            path: '/_/metadata/admin/raft_sessions/1/bucket',
        }, (err, res) => {
            assert.ifError(err);
            assert.strictEqual(res.statusCode, 200);
            assert(res.body);
            const expectedArray = [bucket1, 'users..bucket', bucket2];
            const responseArray = JSON.parse(res.body);

            expectedArray.sort();
            responseArray.sort();

            assert.deepStrictEqual(responseArray, expectedArray);
            return done();
        });
    });

    it('should retrieve list of objects from bucket', done => {
        makeMetadataRequest({
            method: 'GET',
            authCredentials: metadataAuthCredentials,
            path: `/_/metadata/default/bucket/${bucket1}`,
            queryObj: { listingType: 'Delimiter' },
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
            path: `/_/metadata/default/attributes/${bucket1}`,
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
            path: `/_/metadata/default/bucket/${bucket1}/${keyName}`,
        }, (err, res) => {
            assert.ifError(err);
            assert(res.body);
            assert.strictEqual(res.statusCode, 200);
            const body = JSON.parse(res.body);
            assert(body['owner-id']);
            return done();
        });
    });

    it('should get an error for accessing invalid routes', done => {
        makeMetadataRequest({
            method: 'GET',
            authCredentials: metadataAuthCredentials,
            path: '/_/metadata/admin/raft_sessions',
        }, err => {
            assert.strictEqual(err.code, 'NotImplemented');
            return done();
        });
    });
});
