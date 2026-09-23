const assert = require('assert');
const {
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    GetBucketEncryptionCommand,
} = require('@aws-sdk/client-s3');

const checkError = require('../../lib/utility/checkError');
const getConfig = require('../support/config');
const metadata = require('../../../../../lib/metadata/wrapper');
const { DummyRequestLogger } = require('../../../../unit/helpers');

const bucketName = 'encrypted-bucket';
const log = new DummyRequestLogger();

function setEncryptionInfo(info) {
    return new Promise((resolve, reject) => {
        metadata.getBucket(bucketName, log, (err, bucket) => {
            if (err) {
                reject(err);
                return;
            }
            bucket.setServerSideEncryption(info);
            metadata.updateBucket(bucket.getName(), bucket, log, (err, result) => {
                if (err) {
                    reject(err);
                    return;
                }
                resolve(result);
            });
        });
    });
}

describe('aws-sdk test get bucket encryption', () => {
    let s3;

    before(async () => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
        await new Promise((resolve, reject) => {
            metadata.setup(err => (err ? reject(err) : resolve()));
        });
    });

    beforeEach(() => s3.send(new CreateBucketCommand({ Bucket: bucketName })));

    afterEach(() => s3.send(new DeleteBucketCommand({ Bucket: bucketName })));

    it('should return NoSuchBucket error if bucket does not exist', async () => {
        try {
            await s3.send(new GetBucketEncryptionCommand({ Bucket: 'invalid' }));
            throw new Error('Expected NoSuchBucket error');
        } catch (err) {
            checkError(err, 'NoSuchBucket', 404);
        }
    });

    it('should return ServerSideEncryptionConfigurationNotFoundError if no sse configured', async () => {
        try {
            await s3.send(new GetBucketEncryptionCommand({ Bucket: bucketName }));
            throw new Error('Expected ServerSideEncryptionConfigurationNotFoundError');
        } catch (err) {
            checkError(err, 'ServerSideEncryptionConfigurationNotFoundError', 404);
        }
    });

    it('should return ServerSideEncryptionConfigurationNotFoundError if `mandatory` flag not set', async () => {
        await setEncryptionInfo({ cryptoScheme: 1, algorithm: 'AES256', masterKeyId: '12345', mandatory: false });
        try {
            await s3.send(new GetBucketEncryptionCommand({ Bucket: bucketName }));
            throw new Error('Expected ServerSideEncryptionConfigurationNotFoundError');
        } catch (err) {
            checkError(err, 'ServerSideEncryptionConfigurationNotFoundError', 404);
        }
    });

    it('should include KMSMasterKeyID if user has configured a custom master key', async () => {
        await setEncryptionInfo({
            cryptoScheme: 1,
            algorithm: 'aws:kms',
            masterKeyId: '12345',
            configuredMasterKeyId: '54321',
            mandatory: true,
        });
        const { $metadata, ...res } = await s3.send(new GetBucketEncryptionCommand({ Bucket: bucketName }));
        assert.deepStrictEqual(res, {
            ServerSideEncryptionConfiguration: {
                Rules: [
                    {
                        ApplyServerSideEncryptionByDefault: {
                            SSEAlgorithm: 'aws:kms',
                            KMSMasterKeyID: '54321',
                        },
                        BucketKeyEnabled: false,
                    },
                ],
            },
        });
        assert.strictEqual($metadata.httpStatusCode, 200);
    });

    it('should not include KMSMasterKeyID if no user configured master key', async () => {
        await setEncryptionInfo({ cryptoScheme: 1, algorithm: 'AES256', masterKeyId: '12345', mandatory: true });
        const { $metadata, ...res } = await s3.send(new GetBucketEncryptionCommand({ Bucket: bucketName }));
        assert.deepStrictEqual(res, {
            ServerSideEncryptionConfiguration: {
                Rules: [
                    {
                        ApplyServerSideEncryptionByDefault: {
                            SSEAlgorithm: 'AES256',
                        },
                        BucketKeyEnabled: false,
                    },
                ],
            },
        });
        assert.strictEqual($metadata.httpStatusCode, 200);
    });
});
