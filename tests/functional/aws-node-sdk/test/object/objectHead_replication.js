const assert = require('assert');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { removeAllVersions, versioningEnabled } = require('../../lib/utility/versioning-util');
const {
    PutObjectCommand,
    HeadObjectCommand,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketVersioningCommand,
    PutBucketReplicationCommand,
} = require('@aws-sdk/client-s3');
const sourceBucket = 'source-bucket';
const keyPrefix = 'test-prefix';

describe("Head object 'ReplicationStatus' value", () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        async function checkHeadObj(key, expectedStatus) {
            const params = { Bucket: sourceBucket, Key: key };
            await s3.send(new PutObjectCommand(params));
            const res = await s3.send(new HeadObjectCommand(params));
            assert.strictEqual(res.ReplicationStatus, expectedStatus);
        }

        beforeEach(async () => {
            await s3.send(new CreateBucketCommand({ Bucket: sourceBucket }));
            await s3.send(
                new PutBucketVersioningCommand({
                    Bucket: sourceBucket,
                    VersioningConfiguration: versioningEnabled,
                }),
            );
        });

        afterEach(done => {
            removeAllVersions({ Bucket: sourceBucket }, err => {
                if (err) {
                    return done(err);
                }
                return s3
                    .send(new DeleteBucketCommand({ Bucket: sourceBucket }))
                    .then(() => done())
                    .catch(done);
            });
        });

        it('should be `undefined` when there is no bucket replication config', async () =>
            await checkHeadObj(`${keyPrefix}-foobar`, undefined));

        describe('With bucket replication config', () => {
            const role = process.env.S3_END_TO_END
                ? 'arn:aws:iam::123456789012:role/src-resource,arn:aws:iam::123456789012:role/dest-resource'
                : 'arn:aws:iam::123456789012:role/src-resource';
            beforeEach(async () => {
                await s3.send(
                    new PutBucketReplicationCommand({
                        Bucket: sourceBucket,
                        ReplicationConfiguration: {
                            Role: role,
                            Rules: [
                                {
                                    Destination: { StorageClass: 'us-east-2', Bucket: 'arn:aws:s3:::dest-bucket' },
                                    Prefix: keyPrefix,
                                    Status: 'Enabled',
                                },
                            ],
                        },
                    }),
                );
            });

            it("should be 'PENDING' when object key prefix applies", async () =>
                await checkHeadObj(`${keyPrefix}-foobar`, 'PENDING'));

            it('should be `undefined` when object key prefix does not apply', async () =>
                await checkHeadObj(`foobar-${keyPrefix}`, undefined));
        });
    });
});
