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

    function putVersioningOnBucket(bucket) {
        return s3.send(new PutBucketVersioningCommand({
            Bucket: bucket,
            VersioningConfiguration: { Status: 'Enabled' },
        }));
    }

    function putReplicationOnBucket(bucket) {
        return s3.send(new PutBucketReplicationCommand({
            Bucket: bucket,
            ReplicationConfiguration: replicationConfig,
        }));
    }

    function deleteReplicationAndCheckResponse(bucket) {
        return s3.send(new DeleteBucketReplicationCommand({ Bucket: bucket }))
            .then(data => {
                assert.deepStrictEqual(data.$metadata.httpStatusCode, 204);
            });
    }

    beforeEach(() => {
        s3 = new S3Client(config);
        otherAccountS3 = new BucketUtility('lisa', {}).s3;
        return s3.send(new CreateBucketCommand({ Bucket: bucket }));
    });

    afterEach(() => s3.send(new DeleteBucketCommand({ Bucket: bucket })));

    it('should return empty object if bucket has no replication config', 
        () => deleteReplicationAndCheckResponse(bucket));

    it('should delete a bucket replication config when it has one', async () => {
        await putVersioningOnBucket(bucket);
        await putReplicationOnBucket(bucket);
        await deleteReplicationAndCheckResponse(bucket);
    });

    it('should return ReplicationConfigurationNotFoundError if getting ' +
    'replication config after it has been deleted', async () => {
        await putVersioningOnBucket(bucket);
        await putReplicationOnBucket(bucket);
        
        const data = await s3.send(new GetBucketReplicationCommand({ Bucket: bucket }));
        assert.deepStrictEqual(data.ReplicationConfiguration, replicationConfig);

        await deleteReplicationAndCheckResponse(bucket);
        
        try {
            await s3.send(new GetBucketReplicationCommand({ Bucket: bucket }));
            assert.fail('Expected ReplicationConfigurationNotFoundError');
        } catch (err) {
            assert(errorInstances.ReplicationConfigurationNotFoundError.is[err.name]);
        }
    });

    it('should return AccessDenied if user is not bucket owner', async () => {
        try {
            await otherAccountS3.send(new DeleteBucketReplicationCommand({ Bucket: bucket }));
            assert.fail('Expected AccessDenied error');
        } catch (err) {
            assert.strictEqual(err.name, 'AccessDenied');
            assert.strictEqual(err.$metadata.httpStatusCode, 403);
        }
    });
});
