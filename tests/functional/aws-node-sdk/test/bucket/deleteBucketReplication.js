const assert = require('assert');
const { S3 } = require('aws-sdk');
const { series } = require('async');
const { errors } = require('arsenal');

const getConfig = require('../support/config');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = 'source-bucket';
const replicationConfig = {
    Role: 'arn:partition:service::account-id:resourcetype/resource',
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
            assert.strictEqual(err, null);
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

    it('should return empty object if bucket has no replication config', done =>
        deleteReplicationAndCheckResponse(bucket, done));

    it('should delete a bucket replication config when it has one', done =>
        series([
            next => putVersioningOnBucket(bucket, next),
            next => putReplicationOnBucket(bucket, next),
            next => deleteReplicationAndCheckResponse(bucket, next),
        ], done));

    it('should return ReplicationConfigurationNotFoundError if getting ' +
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
                assert(errors.ReplicationConfigurationNotFoundError[err.code]);
                return next();
            }),
        ], done));

    it('should return AccessDenied if user is not bucket owner', done =>
        otherAccountS3.deleteBucketReplication({ Bucket: bucket }, err => {
            assert(err);
            assert.strictEqual(err.code, 'AccessDenied');
            assert.strictEqual(err.statusCode, 403);
            return done();
        }));
});
