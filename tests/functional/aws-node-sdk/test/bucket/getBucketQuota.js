const { S3 } = require('aws-sdk');
const async = require('async');
const assert = require('assert');
const getConfig = require('../support/config');

const bucket = 'getquotatestbucket';

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
            next => s3.updateBucketQuota({
                AccountId: s3.AccountId,
                quota,
                Bucket: bucket
            }, next),
            next => s3.bucketGetQuota({
                AccountId: s3.AccountId,
                Bucket: bucket,
            }, next),
        ], (err, data) => {
            assert.ifError(err);
            assert.deepStrictEqual(data, quota);
            done();
        });
    });
});
