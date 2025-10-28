const assert = require('assert');
const { S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    GetBucketReplicationCommand,
    PutBucketReplicationCommand,
    PutBucketVersioningCommand } = require('@aws-sdk/client-s3');
const { errorInstances } = require('arsenal');

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

    beforeEach(async () => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
        otherAccountS3 = new BucketUtility('lisa', {}).s3;        
        await s3.send(new CreateBucketCommand({ Bucket: bucket }));
        await s3.send(new PutBucketVersioningCommand({
            Bucket: bucket,
            VersioningConfiguration: {
                Status: 'Enabled',
            },
        }));
    });

    afterEach(() => s3.send(new DeleteBucketCommand({ Bucket: bucket })));

    it("should return 'ReplicationConfigurationNotFoundError' if bucket does " +
    'not have a replication configuration', async () => {
        try {
            await s3.send(new GetBucketReplicationCommand({ Bucket: bucket }));
            throw new Error('Expected ReplicationConfigurationNotFoundError');
        } catch (err) {
            assert(errorInstances.ReplicationConfigurationNotFoundError.is[err.Code]);
        }
    });

    it('should get the replication configuration that was put on a bucket', async () => {
        await s3.send(new PutBucketReplicationCommand({
            Bucket: bucket,
            ReplicationConfiguration: replicationConfig,
        }));
        const data = await s3.send(new GetBucketReplicationCommand({ Bucket: bucket }));
        const expectedObj = {
            ReplicationConfiguration: replicationConfig,
        };
        assert.deepStrictEqual(data.ReplicationConfiguration, expectedObj.ReplicationConfiguration);
    });

    it('should return AccessDenied if user is not bucket owner', async () => {
        try {
            await otherAccountS3.send(new GetBucketReplicationCommand({ Bucket: bucket }));
            throw new Error('Expected AccessDenied error');
        } catch (err) {
            assert.strictEqual(err.name, 'AccessDenied');
            assert.strictEqual(err.$metadata.httpStatusCode, 403);
        }
    });
});
