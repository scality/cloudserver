const assert = require('assert');
const { S3 } = require('aws-sdk');
const async = require('async');
const assertError = require('../../../../utilities/bucketTagging-util');

const getConfig = require('../support/config');

const bucket = 'policyputtaggingtestbucket';

const validTagging = {
    TagSet: [
        {
            Key: 'key1',
            Value: 'value1',
        },
        {
            Key: 'key2',
            Value: 'value2',
        },
    ],
};

describe('aws-sdk test delete bucket tagging', () => {
    let s3;

    before(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
    });

    beforeEach(done => s3.createBucket({ Bucket: bucket }, done));

    afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

    it('should delete tag', done => {
        async.series([
            next => s3.putBucketTagging({
                AccountId: s3.AccountId,
                Tagging: validTagging, Bucket: bucket,
            }, (err, res) => next(err, res)),
            next => s3.getBucketTagging({
                AccountId: s3.AccountId,
                Bucket: bucket,
            }, (err, res) => {
                assert.deepStrictEqual(res, validTagging);
                next(err, res);
            }),
            next => s3.deleteBucketTagging({
                AccountId: s3.AccountId,
                Bucket: bucket,
            }, (err, res) => next(err, res)),
            next => s3.getBucketTagging({
                AccountId: s3.AccountId,
                Bucket: bucket,
            }, next),
        ], (err) => {
            assertError(err, 'NoSuchTagSet');
            done();
        });
    });

    it('should make no change when deleting tags on bucket with no tags', done => {
        async.series([
            next => s3.getBucketTagging({
                AccountId: s3.AccountId,
                Bucket: bucket,
            }, (err) => {
                assertError(err, 'NoSuchTagSet');
                next();
            }),
            next => s3.deleteBucketTagging({
                AccountId: s3.AccountId,
                Bucket: bucket,
            }, (err, res) => next(err, res)),
            next => s3.getBucketTagging({
                AccountId: s3.AccountId,
                Bucket: bucket,
            }, (err) => {
                assertError(err, 'NoSuchTagSet');
                next();
            }),
        ], done);
    });
});
