const AWS = require('aws-sdk');
const S3 = AWS.S3;
const fetch = require('node-fetch');

const async = require('async');
const assert = require('assert');
const getConfig = require('../support/config');
const sendRequest = require('../quota/tooling').sendRequest;

const bucket = 'updatequotatestbucket';   
const nonExistantBucket = 'updatequotatestnonexistantbucket';
const quota = { quota: 2000 };
const negativeQuota = { quota: -1000 };
const wrongquotaFromat = '1000';

describe('Test update bucket quota', () => {
    let s3;

    before(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
    });

    beforeEach(done => s3.createBucket({ Bucket: bucket }, done));

    afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

    it('should update the quota', (done) => {
        sendRequest('POST', '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota), err => {
            assert.strictEqual(err,null);
            done();     
        });
    });

    it('should return no such bucket error', (done) => {
        sendRequest('POST', '127.0.0.1:8000', `/${nonExistantBucket}/?quota=true`, JSON.stringify(quota), err => {
            assert.strictEqual(err.Error.Code[0], 'NoSuchBucket');
            done();     
        });
    });

    it('should return error when quota is negative', (done) => {
        sendRequest('POST', '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(negativeQuota), err => {
            assert.strictEqual(err.Error.Code[0], 'InvalidArgument');
            assert.strictEqual(err.Error.Message[0], 'Quota Value should be a positive number');
            done();
        });
    });

    it('should return error when quota is not in correct format', (done) => {
        sendRequest('POST', '127.0.0.1:8000', `/${bucket}/?quota=true`, wrongquotaFromat, err => {
            assert.strictEqual(err.Error.Code[0], 'InvalidArgument');
            assert.strictEqual(err.Error.Message[0], 'Request body must be a JSON object');
            done();     
        });
    });
});
