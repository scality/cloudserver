const assert = require('assert');
const { S3 } = require('aws-sdk');

const checkError = require('../../lib/utility/checkError');
const getConfig = require('../support/config');
const BucketUtility = require('../../lib/utility/bucket-util');
const metadata = require('../../../../../lib/metadata/wrapper');
const { DummyRequestLogger, makeAuthInfo } = require('../../../../unit/helpers');

const bucketName = 'encrypted-bucket';
const log = new DummyRequestLogger();

function setEncryptionInfo(info, cb) {
    metadata.getBucket(bucketName, log, (err, bucket) => {
        if (err) return cb(err);
        bucket.setServerSideEncryption(info);
        return metadata.updateBucket(bucket.getName(), bucket, log, cb);
    });
}

describe('aws-sdk test get bucket encryption', () => {
    let s3;

    before(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        return done();
    });

    beforeEach(done => s3.createBucket({ Bucket: bucketName }, done));

    afterEach(done => s3.deleteBucket({ Bucket: bucketName }, done));

    it('should return NoSuchBucket error if bucket does not exist', done => {
        s3.getBucketEncryption({ Bucket: 'invalid' }, err => {
            checkError(err, 'NoSuchBucket', 404);
            done();
        });
    });

    it('should return ServerSideEncryptionConfigurationNotFoundError if no sse configured', done => {
        s3.getBucketEncryption({Bucket: bucketName}, err => {
            checkError(err, 'ServerSideEncryptionConfigurationNotFoundError', 404);
            done();
        })
    });

    it('should return ServerSideEncryptionConfigurationNotFoundError `mandatory` flag not set', done => {
        setEncryptionInfo({ cryptoScheme: 1, algorithm: 'AES256', masterKeyId: '12345', mandatory: false }, err => {
            assert.ifError(err);
            s3.getBucketEncryption({Bucket: bucketName}, err => {
                checkError(err, 'ServerSideEncryptionConfigurationNotFoundError', 404);
                done();
            })
        });
    });

    it('should include KMSMasterKeyID if algo is aws:kms', done => {
        setEncryptionInfo({ cryptoScheme: 1, algorithm: 'aws:kms', masterKeyId: '12345', mandatory: true }, err => {
            assert.ifError(err);
            s3.getBucketEncryption({Bucket: bucketName}, (err, res) => {
                assert.ifError(err);
                assert.deepStrictEqual(res, {
                    ServerSideEncryptionConfiguration: {
                        Rules: [
                            {
                                ApplyServerSideEncryptionByDefault: {
                                    SSEAlgorithm: 'aws:kms',
                                    KMSMasterKeyID: '12345'
                                },
                                BucketKeyEnabled: false
                            }
                        ]
                    }
                });
                done();
            })
        });
    });

    it('should not include KMSMasterKeyID if algo is AES256', done => {
        setEncryptionInfo({ cryptoScheme: 1, algorithm: 'AES256', masterKeyId: '12345', mandatory: true }, err => {
            assert.ifError(err);
            s3.getBucketEncryption({Bucket: bucketName}, (err, res) => {
                assert.ifError(err);
                assert.deepStrictEqual(res, {
                    ServerSideEncryptionConfiguration: {
                        Rules: [
                            {
                                ApplyServerSideEncryptionByDefault: {
                                    SSEAlgorithm: 'AES256',
                                },
                                BucketKeyEnabled: false
                            }
                        ]
                    }
                });
                done();
            })
        });
    });
});
