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

describe('aws-node-sdk test getBucketReplication', () => {
    let s3;
    let otherAccountS3;

    beforeEach(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        otherAccountS3 = new BucketUtility('lisa', {}).s3;
        return series([
            next => s3.createBucket({ Bucket: bucket }, next),
            next => s3.putBucketVersioning({
                Bucket: bucket,
                VersioningConfiguration: {
                    Status: 'Enabled',
                },
            }, next),
        ], done);
    });

    afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

    test("should return 'ReplicationConfigurationNotFoundError' if bucket does " +
    'not have a replication configuration', done =>
        s3.getBucketReplication({ Bucket: bucket }, err => {
            expect(errors.ReplicationConfigurationNotFoundError[err.code]).toBeTruthy();
            return done();
        }));

    test(
        'should get the replication configuration that was put on a bucket',
        done => s3.putBucketReplication({
            Bucket: bucket,
            ReplicationConfiguration: replicationConfig,
        }, err => {
            if (err) {
                return done(err);
            }
            return s3.getBucketReplication({ Bucket: bucket }, (err, data) => {
                if (err) {
                    return done(err);
                }
                const expectedObj = {
                    ReplicationConfiguration: replicationConfig,
                };
                assert.deepStrictEqual(data, expectedObj);
                return done();
            });
        })
    );

    test('should return AccessDenied if user is not bucket owner', done =>
        otherAccountS3.getBucketReplication({ Bucket: bucket }, err => {
            expect(err).toBeTruthy();
            expect(err.code).toBe('AccessDenied');
            expect(err.statusCode).toBe(403);
            return done();
        }));
});
