const assert = require('assert');
const {
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketVersioningCommand,
    PutBucketReplicationCommand,
    DeleteBucketReplicationCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const getConfig = require('../support/config');

const bucketName = `versioning-bucket-${Date.now()}`;

function checkError(err, code) {
    assert.notEqual(err, null, 'Expected failure but got success');
    assert.strictEqual(err.code, code);
}

function checkNoError(err) {
    assert.ifError(err, `Expected success, got error ${JSON.stringify(err)}`);
}

async function testVersioning(s3, versioningStatus, replicationStatus, removeReplication) {
    const versioningParams = { Bucket: bucketName,
        VersioningConfiguration: { Status: versioningStatus } };
    const replicationParams = {
        Bucket: bucketName,
        ReplicationConfiguration: {
            Role: 'arn:aws:iam::123456789012:role/examplerole,' +
            'arn:aws:iam::123456789012:role/examplerole',
            Rules: [
                {
                    Destination: {
                        Bucket: 'arn:aws:s3:::destinationbucket',
                        StorageClass: 'STANDARD',
                    },
                    Prefix: '',
                    Status: replicationStatus,
                },
            ],
        },
    };
    
    await s3.send(new PutBucketReplicationCommand(replicationParams));
    
    if (removeReplication) {
        await s3.send(new DeleteBucketReplicationCommand({ Bucket: bucketName }));
    }
    
    await s3.send(new PutBucketVersioningCommand(versioningParams));
}

describe('Versioning on a replication source bucket', () => {
    withV4(sigCfg => {
        let s3;

        beforeEach(async () => {
            const config = getConfig('default', sigCfg);
            s3 = new S3Client(config);
            
            await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
            await s3.send(new PutBucketVersioningCommand({
                Bucket: bucketName,
                VersioningConfiguration: {
                    Status: 'Enabled',
                },
            }));
        });

        afterEach(async () => {
            await s3.send(new DeleteBucketCommand({ Bucket: bucketName }));
        });

        it('should not be able to disable versioning if replication enabled', async () => {
            try {
                await testVersioning(s3, 'Suspended', 'Enabled', false);
                throw new Error('Expected InvalidBucketState error');
            } catch (err) {
                checkError(err, 'InvalidBucketState');
            }
        });

        it('should be able to suspend versioning if replication disabled', async () => {
            try {
                await testVersioning(s3, 'Suspended', 'Disabled', false);
                checkNoError(null); // Success case
            } catch (err) {
                checkNoError(err);
            }
        });

        it('should be able to suspend versioning after removed replication', async () => {
            try {
                await testVersioning(s3, 'Suspended', 'Disabled', true);
                checkNoError(null); // Success case
            } catch (err) {
                checkNoError(err);
            }
        });
    });
});
