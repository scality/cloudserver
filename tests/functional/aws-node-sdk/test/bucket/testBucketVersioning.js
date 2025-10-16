const assert = require('assert');
const { S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketVersioningCommand,
    GetBucketVersioningCommand } = require('@aws-sdk/client-s3');

const getConfig = require('../support/config');

const bucket = `versioning-bucket-${Date.now()}`;
const config = getConfig('default', { signatureVersion: 'v4' });
const configReplication = getConfig('replication',
    { signatureVersion: 'v4' });
const s3 = new S3Client(config);
describe('aws-node-sdk test bucket versioning', function testSuite() {
    this.timeout(60000);
    let replicationAccountS3;

    // setup test
    before(async () => {
        replicationAccountS3 = new S3Client(configReplication);
        await s3.send(new CreateBucketCommand({ Bucket: bucket }));
    });

    // delete bucket after testing
    after(async () => {
        await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
    });

    it('should not accept empty versioning configuration', async () => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {},
        };
        try {
            await s3.send(new PutBucketVersioningCommand(params));
            throw new Error('accepted empty versioning configuration');
        } catch (error) {
            assert.strictEqual(error.$metadata.httpStatusCode, 400);
            assert.strictEqual(
                error.Code, 'IllegalVersioningConfigurationException');
        }
    });

    it('should retrieve an empty versioning configuration', async () => {
        const params = { Bucket: bucket };
        const data = await s3.send(new GetBucketVersioningCommand(params));
        assert.strictEqual(data.$metadata.httpStatusCode, 200);
        assert.strictEqual(data.Status, undefined);
    });

    it('should not accept versioning configuration w/o "Status"', async () => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {
                MFADelete: 'Enabled',
            },
        };
        try {
            await s3.send(new PutBucketVersioningCommand(params));
            throw new Error('accepted empty versioning configuration');
        } catch (error) {
            assert.strictEqual(error.$metadata.httpStatusCode, 400);
            assert.strictEqual(
                error.Code, 'IllegalVersioningConfigurationException');
        }
    });

    it('should retrieve an empty versioning configuration', async () => {
        const params = { Bucket: bucket };
        const data = await s3.send(new GetBucketVersioningCommand(params));
        assert.strictEqual(data.$metadata.httpStatusCode, 200);
        assert.deepStrictEqual(data.Status, undefined);
    });

    it('should not accept versioning configuration w/ invalid value', async () => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {
                MFADelete: 'fun',
                Status: 'let\'s do it',
            },
        };
        try {
            await s3.send(new PutBucketVersioningCommand(params));
            throw new Error('accepted empty versioning configuration');
        } catch (error) {
            assert.strictEqual(error.$metadata.httpStatusCode, 400);
            assert.strictEqual(
                error.Code, 'IllegalVersioningConfigurationException');
        }
    });

    it('should not accept versioning with MFA Delete enabled', async () => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {
                MFADelete: 'Enabled',
                Status: 'Enabled',
            },
        };
        try {
            await s3.send(new PutBucketVersioningCommand(params));
            throw new Error('Expected failure but got success');
        } catch (error) {
            assert.notEqual(error, null, 'Expected failure but got success');
            assert.strictEqual(error.$metadata.httpStatusCode, 501);
            assert.strictEqual(error.Code, 'NotImplemented');
        }
    });

    it('should accept versioning with MFA Delete disabled', async () => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {
                MFADelete: 'Disabled',
                Status: 'Enabled',
            },
        };
        try {
            await s3.send(new PutBucketVersioningCommand(params));
        } catch (error) {
            throw new Error(`Expected success but got failure: ${error.message}`);
        }
    });

    it('should retrieve the valid versioning configuration', async () => {
        const params = { Bucket: bucket };
        try {
            const response = await s3.send(new GetBucketVersioningCommand(params));
            assert.strictEqual(response.$metadata.httpStatusCode, 200);
        } catch (error) {
            throw new Error(`Expected success but got failure: ${error.message}`);
        }
    });

    it('should accept valid versioning configuration', async () => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {
                Status: 'Enabled',
            },
        };
        await s3.send(new PutBucketVersioningCommand(params));
    });

    // S3C doesn't support service account. There is no cross account access for replication account.
    // (canonicalId looking like http://acs.zenko.io/accounts/service/replication)
    const itSkipS3C = process.env.S3_END_TO_END ? it.skip : it;
    itSkipS3C('should accept valid versioning configuration if user is a ' +
    'replication user', async () => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {
                Status: 'Enabled',
            },
        };
        await replicationAccountS3.send(new PutBucketVersioningCommand(params));
    });

    it('should retrieve the valid versioning configuration', async () => {
        const params = { Bucket: bucket };
        const data = await s3.send(new GetBucketVersioningCommand(params));
        assert.deepStrictEqual(data.Status, 'Enabled');
    });
});


describe('bucket versioning for ingestion buckets', () => {
    const Bucket = `ingestion-bucket-${Date.now()}`;
    before(async () => {
        await s3.send(new CreateBucketCommand({
            Bucket,
            CreateBucketConfiguration: {
                LocationConstraint: 'us-east-2:ingest',
            },
        }));
    });

    after(async () => {
        await s3.send(new DeleteBucketCommand({ Bucket }));
    });

    it('should not allow suspending versioning for ingestion buckets', async () => {
        try {
            await s3.send(new PutBucketVersioningCommand({ 
                Bucket, 
                VersioningConfiguration: {
                    Status: 'Suspended'
                } 
            }));
            throw new Error('Expected error but got success');
        } catch (err) {
            assert(err, 'Expected error but got success');
            assert.strictEqual(err.Code, 'InvalidBucketState');
        }
    });
});

describe('aws-node-sdk test bucket versioning with object lock', () => {
    let s3ObjectLock;

    // setup test
    before(async () => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3ObjectLock = new S3Client(config);
        await s3ObjectLock.send(new CreateBucketCommand({
            Bucket: bucket,
            ObjectLockEnabledForBucket: true,
        }));
    });

    // delete bucket after testing
    after(async () => {
        await s3ObjectLock.send(new DeleteBucketCommand({ Bucket: bucket }));
    });

    it('should not accept suspending version when object lock is enabled', async () => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {
                Status: 'Suspended',
            },
        };
        try {
            await s3ObjectLock.send(new PutBucketVersioningCommand(params));
            throw new Error('Expected error but got success');
        } catch (error) {
            assert.strictEqual(error.Code, 'InvalidBucketState');
        }
    });
});

