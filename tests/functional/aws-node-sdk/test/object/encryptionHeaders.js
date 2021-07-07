const assert = require('assert');
const async = require('async');
const uuid = require('uuid');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const kms = require('../../../../../lib/kms/wrapper');
const { DummyRequestLogger } = require('../../../../unit/helpers');

const log = new DummyRequestLogger();

const testCases = [
    {},
    {
        algo: 'AES256',
    },
    {
        algo: 'aws:kms',
    },
    {
        algo: 'aws:kms',
        masterKeyId: true,
    },
];

function s3NoOp(_, cb) { cb(); }

function getSSEConfig(s3, Bucket, Key, cb) {
    return s3.headObject({ Bucket, Key }, (err, resp) => {
        if (err) {
            return cb(err);
        }
        return cb(null,
            JSON.parse(JSON.stringify({ algo: resp.ServerSideEncryption, masterKeyId: resp.SSEKMSKeyId })));
    });
}

function putEncryptedObject(s3, Bucket, Key, sseConfig, kmsKeyId, cb) {
    const params = {
        Bucket,
        Key,
        ServerSideEncryption: sseConfig.algo,
        Body: 'somedata',
    };
    if (sseConfig.masterKeyId) {
        params.SSEKMSKeyId = kmsKeyId;
    }
    return s3.putObject(params, cb);
}

function createExpected(sseConfig, kmsKeyId) {
    const expected = {};
    if (sseConfig.algo) {
        expected.algo = sseConfig.algo;
    }

    if (sseConfig.masterKeyId) {
        expected.masterKeyId = kmsKeyId;
    }
    return expected;
}

function hydrateSSEConfig({ algo: SSEAlgorithm, masterKeyId: KMSMasterKeyID }) {
    // stringify and parse to strip undefined values
    return JSON.parse(
        JSON.stringify({
            Rules: [
                {
                    ApplyServerSideEncryptionByDefault: {
                        SSEAlgorithm,
                        KMSMasterKeyID,
                    },
                },
            ],
        }
        )
    );
}

describe('per object encryption headers', () => {
    withV4(sigCfg => {
        let bucket;
        let bucket2;
        let object;
        let object2;
        let bucketUtil;
        let s3;
        let kmsKeyId;

        before(done => {
            kms.createBucketKey('enc-bucket-test', log,
                (err, keyId) => {
                    assert.ifError(err);
                    kmsKeyId = keyId;
                    done();
                }
            );
        });

        beforeEach(() => {
            bucket = `enc-bucket-${uuid.v4()}`;
            bucket2 = `enc-bucket-2-${uuid.v4()}`;
            object = `enc-object-${uuid.v4()}`;
            object2 = `enc-object-2-${uuid.v4()}`;
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucket({ Bucket: bucket }).promise()
                .then(() => s3.createBucket({ Bucket: bucket2 }).promise())
                .catch(err => {
                    process.stdout.write(`Error creating bucket: ${err}\n`);
                    throw err;
                });
        });

        afterEach(() => {
            const buckets = [bucket, bucket2];
            return bucketUtil.emptyMany(buckets).then(() => bucketUtil.deleteMany(buckets));
        });

        testCases.forEach(target => {
            const hasKey = target.masterKeyId ? 'a' : 'no';
            describe(`Test algorithm ${target.algo || 'none'} with ${hasKey} configuredMasterKeyId`, () => {
                it('should put an encrypted object in a unencrypted bucket', done =>
                    putEncryptedObject(s3, bucket, object, target, kmsKeyId, error => {
                        assert.ifError(error);
                        return getSSEConfig(
                            s3,
                            bucket,
                            object,
                            (error, sseConfig) => {
                                assert.ifError(error);
                                const expected = createExpected(target, kmsKeyId);
                                // We differ from aws behavior and always return a
                                // masterKeyId even when not explicitly configured.
                                if (expected.algo === 'aws:kms' && !expected.masterKeyId) {
                                    // eslint-disable-next-line no-param-reassign
                                    delete sseConfig.masterKeyId;
                                }
                                assert.deepStrictEqual(sseConfig, expected);
                                done();
                            }
                        );
                    }));

                it('should put two encrypted objects in a unencrypted bucket, reusing the generated config', done =>
                    async.mapSeries(
                        [object, object2],
                        (obj, cb) => putEncryptedObject(s3, bucket, obj, target, kmsKeyId, cb),
                        error => {
                            assert.ifError(error);
                            return async.map(
                                [object, object2],
                                (obj, cb) => getSSEConfig(s3, bucket, obj, cb),
                                (error, res) => {
                                    const [objConf1] = res;
                                    const expected = createExpected(target, kmsKeyId);
                                    // We differ from aws behavior and always return a
                                    // masterKeyId even when not explicitly configured.
                                    // We abuse this here to check if the same key is used for both objects
                                    if (objConf1.masterKeyId) {
                                        expected.masterKeyId = objConf1.masterKeyId;
                                    }
                                    res.forEach(sseConfig => assert.deepStrictEqual(sseConfig, expected));
                                    done();
                                }
                            );
                        }
                    ));

                testCases
                .forEach(existing => it('should override default bucket encryption settings', done => {
                    const _existing = Object.assign({}, existing);
                    if (existing.masterKeyId) {
                        _existing.masterKeyId = kmsKeyId;
                    }
                    const params = {
                        Bucket: bucket,
                        ServerSideEncryptionConfiguration: hydrateSSEConfig(_existing),
                    };
                    // no op putBucketNotification for the unencrypted case
                    const s3Op = existing.algo ? (...args) => s3.putBucketEncryption(...args) : s3NoOp;
                    s3Op(params, error => {
                        assert.ifError(error);
                        return putEncryptedObject(s3, bucket, object, target, kmsKeyId, error => {
                            assert.ifError(error);
                            return getSSEConfig(
                                s3,
                                bucket,
                                object,
                                (error, sseConfig) => {
                                    assert.ifError(error);
                                    let expected = createExpected(target, kmsKeyId);
                                    // In the null case the expected encryption config is
                                    // the buckets default policy
                                    if (!target.algo) {
                                        expected = createExpected(existing, kmsKeyId);
                                    }
                                    // We differ from aws behavior and always return a
                                    // masterKeyId even when not explicitly configured.
                                    if (expected.algo === 'aws:kms' && !expected.masterKeyId) {
                                        // eslint-disable-next-line no-param-reassign
                                        delete sseConfig.masterKeyId;
                                    }
                                    assert.deepStrictEqual(sseConfig, expected);
                                    done();
                                }
                            );
                        });
                    });
                }));

                testCases
                .forEach(existing => it('should copy an object to an encrypted key overriding bucket settings',
                    done => {
                        const _existing = Object.assign({}, existing);
                        if (existing.masterKeyId) {
                            _existing.masterKeyId = kmsKeyId;
                        }
                        const params = {
                            Bucket: bucket2,
                            ServerSideEncryptionConfiguration: hydrateSSEConfig(_existing),
                        };
                        // no op putBucketNotification for the unencrypted case
                        const s3Op = existing.algo ? (...args) => s3.putBucketEncryption(...args) : s3NoOp;
                        s3Op(params, error => {
                            assert.ifError(error);
                            return putEncryptedObject(s3, bucket, object, target, kmsKeyId, error => {
                                assert.ifError(error);
                                const copyParams = {
                                    Bucket: bucket2,
                                    Key: object2,
                                    CopySource: `/${bucket}/${object}`,
                                };
                                if (target.algo) {
                                    copyParams.ServerSideEncryption = target.algo;
                                }
                                if (target.masterKeyId) {
                                    copyParams.SSEKMSKeyId = kmsKeyId;
                                }
                                return s3.copyObject(copyParams, error => {
                                    assert.ifError(error);
                                    return getSSEConfig(
                                        s3,
                                        bucket2,
                                        object2,
                                        (error, sseConfig) => {
                                            assert.ifError(error);
                                            let expected = createExpected(target, kmsKeyId);
                                            // In the null case the expected encryption config is
                                            // the buckets default policy
                                            if (!target.algo) {
                                                expected = _existing;
                                            }
                                            // We differ from aws behavior and always return a
                                            // masterKeyId even when not explicitly configured.
                                            if (expected.algo === 'aws:kms' && !expected.masterKeyId) {
                                            // eslint-disable-next-line no-param-reassign
                                                delete sseConfig.masterKeyId;
                                            }
                                            assert.deepStrictEqual(sseConfig, expected);
                                            done();
                                        }
                                    );
                                });
                            });
                        });
                    }));

                it('should init an encrypted MPU and put an encrypted part', done => {
                    const params = {
                        Bucket: bucket,
                        Key: object,
                    };
                    if (target.algo) {
                        params.ServerSideEncryption = target.algo;
                    }
                    if (target.masterKeyId) {
                        params.SSEKMSKeyId = kmsKeyId;
                    }
                    s3.createMultipartUpload(params, (error, resp) => {
                        assert.ifError(error);
                        const { UploadId } = resp;
                        const partParams = {
                            UploadId,
                            Body: 'somedata',
                            Bucket: bucket,
                            Key: object,
                            PartNumber: 1,
                        };
                        s3.uploadPart(partParams, error => {
                            assert.ifError(error);
                            done();
                        });
                    });
                });

                it('should copy and encrypt a mpu part', done => {
                    const sourceParams = {
                        Bucket: bucket,
                        Key: object,
                    };
                    s3.createMultipartUpload(sourceParams, (error, resp) => {
                        assert.ifError(error);
                        const { UploadId: sourceUploadId } = resp;
                        const sourcePartParams = {
                            UploadId: sourceUploadId,
                            Body: 'somedata',
                            Bucket: bucket,
                            Key: object,
                            PartNumber: 1,
                        };
                        s3.uploadPart(sourcePartParams, error => {
                            assert.ifError(error);
                            const targetParams = {
                                Bucket: bucket,
                                Key: object2,
                            };
                            if (target.algo) {
                                targetParams.ServerSideEncryption = target.algo;
                            }
                            if (target.masterKeyId) {
                                targetParams.SSEKMSKeyId = kmsKeyId;
                            }
                            s3.createMultipartUpload(targetParams, (error, resp) => {
                                const { UploadId: targetUploadId } = resp;
                                const targetPartParams = {
                                    UploadId: targetUploadId,
                                    Body: 'somedata',
                                    Bucket: bucket,
                                    Key: object2,
                                    PartNumber: 1,
                                };
                                s3.uploadPart(targetPartParams, error => {
                                    assert.ifError(error);
                                    done();
                                });
                            });
                        });
                    });
                });
            });
        });
    });
});
