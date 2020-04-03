const assert = require('assert');
const async = require('async');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutPolicy = require('../../../lib/api/bucketPutPolicy');
const bucketDeletePolicy = require('../../../lib/api/bucketDeletePolicy');
const { cleanup,
    DummyRequestLogger,
    makeAuthInfo }
    = require('../helpers');
const metadata = require('../../../lib/metadata/wrapper');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketname';

function _makeRequest(includePolicy) {
    const request = {
        bucketName,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: '/',
    };
    if (includePolicy) {
        const examplePolicy = {
            Version: '2012-10-17',
            Statement: [
                {
                    Effect: 'Allow',
                    Resource: `arn:aws:s3:::${bucketName}`,
                    Principal: '*',
                    Action: ['s3:GetBucketLocation'],
                },
            ],
        };
        request.post = JSON.stringify(examplePolicy);
    }
    return request;
}

describe('deleteBucketPolicy API', () => {
    before(() => cleanup());
    beforeEach(done => bucketPut(authInfo, _makeRequest(), log, done));
    afterEach(() => cleanup());

    it('should not return an error even if no policy put', done => {
        bucketDeletePolicy(authInfo, _makeRequest(), log, err => {
            assert.equal(err, null, `Err deleting policy: ${err}`);
            done();
        });
    });
    it('should delete bucket policy', done => {
        async.series([
            next => bucketPutPolicy(authInfo, _makeRequest(true), log, next),
            next => bucketDeletePolicy(authInfo, _makeRequest(), log, next),
            next => metadata.getBucket(bucketName, log, next),
        ], (err, results) => {
            assert.equal(err, null, `Expected success, got error: ${err}`);
            const bucket = results[2];
            const bucketPolicy = bucket.getBucketPolicy();
            assert.equal(bucketPolicy, null);
            done();
        });
    });
});
