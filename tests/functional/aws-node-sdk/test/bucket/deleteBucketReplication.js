const assert = require('assert');
const { S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketVersioningCommand,
    PutBucketReplicationCommand,
    DeleteBucketReplicationCommand,
    GetBucketReplicationCommand } = require('@aws-sdk/client-s3');
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

describe('aws-node-sdk test deleteBucketReplication', () => {
    let s3;
    let otherAccountS3;
    const config = getConfig('default', { signatureVersion: 'v4' });

    async function putVersioningOnBucket(bucket) {
        await s3.send(new PutBucketVersioningCommand({
            Bucket: bucket,
            VersioningConfiguration: { Status: 'Enabled' },
        }));
    }

    async function putReplicationOnBucket(bucket) {
        await s3.send(new PutBucketReplicationCommand({
            Bucket: bucket,
            ReplicationConfiguration: replicationConfig,
        }));
    }

    async function deleteReplicationAndCheckResponse(bucket) {
        const data = await s3.send(new DeleteBucketReplicationCommand({ Bucket: bucket }));
        // eslint-disable-next-line no-console
        console.log('delete replication response: ', data);
        assert.deepStrictEqual(data.$metadata.httpStatusCode, 204);
    }

    beforeEach(async () => {
        s3 = new S3Client(config);
        otherAccountS3 = new BucketUtility('lisa', {}).s3;
        await s3.send(new CreateBucketCommand({ Bucket: bucket }));
    });

    afterEach(async () => {
        await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
    });

    it('should return empty object if bucket has no replication config', async () => {
        await deleteReplicationAndCheckResponse(bucket);
    });

    it('should delete a bucket replication config when it has one', async () => {
        // Put versioning on bucket
        await putVersioningOnBucket(bucket);
        
        // Put replication on bucket
        await putReplicationOnBucket(bucket);
        
        // Delete replication and check response
        await deleteReplicationAndCheckResponse(bucket);
    });

    it('should return ReplicationConfigurationNotFoundError if getting ' +
    'replication config after it has been deleted', async () => {
        // Put versioning on bucket
        await putVersioningOnBucket(bucket);
        
        // Put replication on bucket
        await putReplicationOnBucket(bucket);
        
        // Get bucket replication to verify it exists
        const data = await s3.send(new GetBucketReplicationCommand({ Bucket: bucket }));
        assert.deepStrictEqual(data.ReplicationConfiguration, replicationConfig);

        // Delete replication and check response
        await deleteReplicationAndCheckResponse(bucket);
        
        // Try to get replication config again (should fail)
        try {
            await s3.send(new GetBucketReplicationCommand({ Bucket: bucket }));
            throw new Error('Expected ReplicationConfigurationNotFoundError');
        } catch (err) {
            assert(errorInstances.ReplicationConfigurationNotFoundError.is[err.Code]);
        }
    });

    it('should return AccessDenied if user is not bucket owner', async () => {
        try {
            await otherAccountS3.send(new DeleteBucketReplicationCommand({ Bucket: bucket }));
            throw new Error('Expected AccessDenied error');
        } catch (err) {
            assert(err);
            assert.strictEqual(err.Code, 'AccessDenied');
            assert.strictEqual(err.$metadata.httpStatusCode, 403);
        }
    });
});
