const assert = require('assert');
const { S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    GetBucketEncryptionCommand } = require('@aws-sdk/client-s3');

const checkError = require('../../lib/utility/checkError');
const getConfig = require('../support/config');
const metadata = require('../../../../../lib/metadata/wrapper');
const { DummyRequestLogger } = require('../../../../unit/helpers');

const bucketName = 'encrypted-bucket';
const log = new DummyRequestLogger();

function setEncryptionInfo(info, cb) {
    metadata.getBucket(bucketName, log, (err, bucket) => {
        if (err) {
            return cb(err);
        }
        bucket.setServerSideEncryption(info);
        return metadata.updateBucket(bucket.getName(), bucket, log, cb);
    });
}

describe('aws-sdk test get bucket encryption', () => {
    let s3;

    before(async () => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
        await new Promise((resolve, reject) => {
            metadata.setup(err => err ? reject(err) : resolve());
        });
    }); 

    beforeEach(async () => {
        await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
    });

    afterEach(async () => {
        await s3.send(new DeleteBucketCommand({ Bucket: bucketName }));
    });

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
        await new Promise((resolve, reject) => {
            setEncryptionInfo({ cryptoScheme: 1, algorithm: 'AES256', masterKeyId: '12345', mandatory: false }, err => {
                if (err) {return reject(err);}
                return resolve();
            });
        });

        try {
            await s3.send(new GetBucketEncryptionCommand({ Bucket: bucketName }));
            throw new Error('Expected ServerSideEncryptionConfigurationNotFoundError');
        } catch (err) {
            checkError(err, 'ServerSideEncryptionConfigurationNotFoundError', 404);
        }
    });

    it('should include KMSMasterKeyID if user has configured a custom master key', async () => {
        await new Promise((resolve, reject) => {
            setEncryptionInfo({ cryptoScheme: 1, algorithm: 'aws:kms', masterKeyId: '12345',
                configuredMasterKeyId: '54321', mandatory: true }, err => {
                if (err) {return reject(err);}
                return resolve();
            });
        });

        const { $metadata, ...res } = await s3.send(new GetBucketEncryptionCommand({ Bucket: bucketName }));
        // eslint-disable-next-line no-console
        console.log('GetBucketEncryption response:', res);
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
        await new Promise((resolve, reject) => {
            setEncryptionInfo({ cryptoScheme: 1, algorithm: 'AES256', masterKeyId: '12345', mandatory: true }, err => {
                if (err) {return reject(err);}
                return resolve();
            });
        });

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
