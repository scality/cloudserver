const assert = require('assert');
const async = require('async');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { removeAllVersions, versioningEnabled } =
    require('../../lib/utility/versioning-util');
const { PutObjectCommand, 
    HeadObjectCommand,
    CreateBucketCommand, 
    DeleteBucketCommand,
    PutBucketVersioningCommand, 
    PutBucketReplicationCommand } = require('@aws-sdk/client-s3');
const sourceBucket = 'source-bucket';
const keyPrefix = 'test-prefix';

describe("Head object 'ReplicationStatus' value", () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        function checkHeadObj(key, expectedStatus, cb) {
            const params = { Bucket: sourceBucket, Key: key };
            return async.series([
                next => s3.send(new PutObjectCommand(params)).then(() => next()).catch(err => next(err)),
                next => s3.send(new HeadObjectCommand(params)).then(res => {
                    assert.strictEqual(res.ReplicationStatus, expectedStatus);
                    return next();
                }).catch(err => next(err)),
            ], cb);
        }

        beforeEach(done => async.series([
            next => s3.send(new CreateBucketCommand({ Bucket: sourceBucket }))
            .then(() => next()).catch(err => next(err)),
            next => s3.send(new PutBucketVersioningCommand({
                Bucket: sourceBucket,
                VersioningConfiguration: versioningEnabled,
            })).then(() => next()).catch(err => next(err)),
        ], done));

        afterEach(async () => {
            await removeAllVersions({ Bucket: sourceBucket });
            await s3.send(new DeleteBucketCommand({ Bucket: sourceBucket }));
        });

        it('should be `undefined` when there is no bucket replication config',
            done => checkHeadObj(`${keyPrefix}-foobar`, undefined, done));

        describe('With bucket replication config', () => {
            const role = process.env.S3_END_TO_END
            ? 'arn:aws:iam::123456789012:role/src-resource,arn:aws:iam::123456789012:role/dest-resource'
            : 'arn:aws:iam::123456789012:role/src-resource';
            beforeEach(done => s3.send(new PutBucketReplicationCommand({
                Bucket: sourceBucket,
                ReplicationConfiguration: {
                    Role: role,
                    Rules: [
                        {
                            Destination: { StorageClass: 'us-east-2',
                            Bucket: 'arn:aws:s3:::dest-bucket' },
                            Prefix: keyPrefix,
                            Status: 'Enabled',
                        },
                    ],
                },
            })).then(() => done()).catch(err => done(err)));

            it("should be 'PENDING' when object key prefix applies",
                done => checkHeadObj(`${keyPrefix}-foobar`, 'PENDING', done));

            it('should be `undefined` when object key prefix does not apply',
                done => checkHeadObj(`foobar-${keyPrefix}`, undefined, done));
        });
    });
});
