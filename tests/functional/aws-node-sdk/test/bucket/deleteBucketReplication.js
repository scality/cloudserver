const assert = require('assert');
const { S3 } = require('aws-sdk');
const { series } = require('async');
const { errors } = require('arsenal');

const getConfig = require('../support/config');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = 'source-bucket';
const replicationConfig = {
    Role: 'arn:aws:iam::account-id:role/src-resource,' +
        'arn:aws:iam::account-id:role/dest-resource',
    Rules: [
        {
            Destination: { Bucket: 'arn:aws:s3:::destination-bucket' },
            Prefix: 'test-prefix',
            Status: 'Enabled',
            ID: 'test-id',
        },
    ],
};

describe('aws-node-sdk test deleteBucketReplication', () => {
    let s3;
    let otherAccountS3;
    const config = getConfig('default', { signatureVersion: 'v4' });

    function putVersioningOnBucket(bucket, cb) {
        return s3.putBucketVersioning({
            Bucket: bucket,
            VersioningConfiguration: { Status: 'Enabled' },
        }, cb);
    }

    function putReplicationOnBucket(bucket, cb) {
        return s3.putBucketReplication({
            Bucket: bucket,
            ReplicationConfiguration: replicationConfig,
        }, cb);
    }

    function deleteReplicationAndCheckResponse(bucket, cb) {
        return s3.deleteBucketReplication({ Bucket: bucket }, (err, data) => {
            expect(err).toBe(null);
            assert.deepStrictEqual(data, {});
            return cb();
        });
    }

    beforeEach(done => {
        s3 = new S3(config);
        otherAccountS3 = new BucketUtility('lisa', {}).s3;
        return s3.createBucket({ Bucket: bucket }, done);
    });

    afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

    test('should return empty object if bucket has no replication config', done =>
        deleteReplicationAndCheckResponse(bucket, done));

    test('should delete a bucket replication config when it has one', done =>
        series([
            next => putVersioningOnBucket(bucket, next),
            next => putReplicationOnBucket(bucket, next),
            next => deleteReplicationAndCheckResponse(bucket, next),
        ], done));

    test('should return ReplicationConfigurationNotFoundError if getting ' +
    'replication config after it has been deleted', done =>
        series([
            next => putVersioningOnBucket(bucket, next),
            next => putReplicationOnBucket(bucket, next),
            next => s3.getBucketReplication({ Bucket: bucket }, (err, data) => {
                if (err) {
                    return next(err);
                }
                assert.deepStrictEqual(data, {
                    ReplicationConfiguration: replicationConfig,
                });
                return next();
            }),
            next => deleteReplicationAndCheckResponse(bucket, next),
            next => s3.getBucketReplication({ Bucket: bucket }, err => {
                expect(errors.ReplicationConfigurationNotFoundError[err.code]).toBeTruthy();
                return next();
            }),
        ], done));

    test('should return AccessDenied if user is not bucket owner', done =>
        otherAccountS3.deleteBucketReplication({ Bucket: bucket }, err => {
            expect(err).toBeTruthy();
            expect(err.code).toBe('AccessDenied');
            expect(err.statusCode).toBe(403);
            return done();
        }));
});
