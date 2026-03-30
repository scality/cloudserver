const assert = require('assert');
const async = require('async');
const uuid = require('uuid');
const { 
    CreateBucketCommand,
    HeadObjectCommand,
    PutObjectCommand,
    PutBucketEncryptionCommand,
    CopyObjectCommand,
    CreateMultipartUploadCommand,
    UploadPartCommand,
} = require('@aws-sdk/client-s3');
const BucketInfo = require('arsenal').models.BucketInfo;
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const kms = require('../../../../../lib/kms/wrapper');
const { DummyRequestLogger } = require('../../../../unit/helpers');
const { config } = require('../../../../../lib/Config');
const { getKeyIdFromArn } = require('arsenal/build/lib/network/KMSInterface');

// For this test env S3_CONFIG_FILE should be the same as running cloudserver
// to have the same config.kmsHideScalityArn value

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
    const command = new HeadObjectCommand({ Bucket, Key });
    s3.send(command)
        .then(resp => {
            const sseConfig = JSON.parse(JSON.stringify({ 
                algo: resp.ServerSideEncryption, 
                masterKeyId: resp.SSEKMSKeyId 
            }));
            cb(null, sseConfig);
        })
        .catch(cb);
}

function putEncryptedObject(s3, Bucket, Key, sseConfig, kmsKeyId, cb) {
    const params = {
        Bucket,
        Key,
        Body: 'somedata',
    };
    
    if (sseConfig.algo) {
        params.ServerSideEncryption = sseConfig.algo;
    }
    
    if (sseConfig.masterKeyId) {
        params.SSEKMSKeyId = kmsKeyId;
    }
    
    const command = new PutObjectCommand(params);
    s3.send(command)
        .then(response => cb(null, response))
        .catch(cb);
}

function createExpected(sseConfig, kmsKeyId) {
    const expected = {};
    if (sseConfig.algo) {
        expected.algo = sseConfig.algo;
    }

    if (sseConfig.masterKeyId) {
        expected.masterKeyId = config.kmsHideScalityArn
            ? getKeyIdFromArn(kmsKeyId)
            : kmsKeyId;
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

function putBucketEncryption(s3, params, cb) {
    const command = new PutBucketEncryptionCommand(params);
    s3.send(command)
        .then(response => cb(null, response))
        .catch(cb);
}

function copyObject(s3, params, cb) {
    const command = new CopyObjectCommand(params);
    s3.send(command)
        .then(response => cb(null, response))
        .catch(cb);
}

function createMultipartUpload(s3, params, cb) {
    const command = new CreateMultipartUploadCommand(params);
    s3.send(command)
        .then(response => cb(null, response))
        .catch(cb);
}

function uploadPart(s3, params, cb) {
    const command = new UploadPartCommand(params);
    s3.send(command)
        .then(response => cb(null, response))
        .catch(cb);
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
            const bucket = new BucketInfo('enc-bucket-test', 'OwnerId',
                'OwnerDisplayName', new Date().toJSON());
            kms.createBucketKey(bucket, log,
                (err, { masterKeyArn: keyId }) => {
                    assert.ifError(err);
                    kmsKeyId = keyId;
                    done();
                }
            );
        });

        beforeEach(async () => {
            bucket = `enc-bucket-${uuid.v4()}`;
            bucket2 = `enc-bucket-2-${uuid.v4()}`;
            object = `enc-object-${uuid.v4()}`;
            object2 = `enc-object-2-${uuid.v4()}`;
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));
            await s3.send(new CreateBucketCommand({ Bucket: bucket2 }));
        });

        afterEach(() => {
            const buckets = [bucket, bucket2];
            return bucketUtil.emptyMany(buckets).then(() => bucketUtil.deleteMany(buckets));
        });

        testCases.forEach(target => {
            const hasKey = target.masterKeyId ? 'a' : 'no';
            describe(`Test algorithm ${target.algo || 'none'} with ${hasKey} configuredMasterKeyId`, () => {
                it('should put an encrypted object in a unencrypted bucket', done =>
                    putEncryptedObject(s3, bucket, object, target, kmsKeyId, (error, putResp) => {
                        assert.ifError(error);
                        if (target.algo) {
                            assert.strictEqual(putResp.ServerSideEncryption, target.algo,
                                'PutObject response should include ServerSideEncryption header');
                            if (target.algo === 'aws:kms') {
                                assert(putResp.SSEKMSKeyId,
                                    'PutObject response should include SSEKMSKeyId for aws:kms');
                            }
                        }
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
                .forEach(existing => {
                    const hasKey = target.masterKeyId ? 'a' : 'no';
                    const { algo } = target;
                    it('should override bucket encryption settings with '
                    + `algo ${algo || 'none'} with ${hasKey} key id`, done => {
                        const _existing = Object.assign({}, existing);
                        if (existing.masterKeyId) {
                            _existing.masterKeyId = kmsKeyId;
                        }
                        const params = {
                            Bucket: bucket,
                            ServerSideEncryptionConfiguration: hydrateSSEConfig(_existing),
                        };
                        // no op putBucketEncryption for the unencrypted case
                        const s3Op = existing.algo ? 
                            (params, cb) => putBucketEncryption(s3, params, cb) : s3NoOp;
                        s3Op(params, error => {
                            assert.ifError(error);
                            return putEncryptedObject(s3, bucket, object, target, kmsKeyId, (error, putResp) => {
                                assert.ifError(error);
                                if (target.algo) {
                                    assert.strictEqual(putResp.ServerSideEncryption, target.algo,
                                        'PutObject response should include ServerSideEncryption header');
                                    if (target.algo === 'aws:kms') {
                                        assert(putResp.SSEKMSKeyId,
                                            'PutObject response should include SSEKMSKeyId for aws:kms');
                                    }
                                } else if (existing.algo) {
                                    assert.strictEqual(putResp.ServerSideEncryption, existing.algo,
                                        'PutObject response should include ServerSideEncryption from bucket default');
                                }
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
                    });
                });

                testCases
                .forEach(existing => it('should copy an object to an encrypted key overriding bucket settings',
                    done => {
                        const _existing = Object.assign({}, existing);
                        if (existing.masterKeyId) {
                            _existing.masterKeyId = config.kmsHideScalityArn
                                ? getKeyIdFromArn(kmsKeyId)
                                : kmsKeyId;
                        }
                        const params = {
                            Bucket: bucket2,
                            ServerSideEncryptionConfiguration: hydrateSSEConfig(_existing),
                        };
                        // no op putBucketEncryption for the unencrypted case
                        const s3Op = existing.algo ? 
                            (params, cb) => putBucketEncryption(s3, params, cb) : s3NoOp;
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
                                return copyObject(s3, copyParams, error => {
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
                    createMultipartUpload(s3, params, (error, resp) => {
                        assert.ifError(error);
                        const { UploadId } = resp;
                        const partParams = {
                            UploadId,
                            Body: 'somedata',
                            Bucket: bucket,
                            Key: object,
                            PartNumber: 1,
                        };
                        uploadPart(s3, partParams, error => {
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
                    createMultipartUpload(s3, sourceParams, (error, resp) => {
                        assert.ifError(error);
                        const { UploadId: sourceUploadId } = resp;
                        const sourcePartParams = {
                            UploadId: sourceUploadId,
                            Body: 'somedata',
                            Bucket: bucket,
                            Key: object,
                            PartNumber: 1,
                        };
                        uploadPart(s3, sourcePartParams, error => {
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
                            createMultipartUpload(s3, targetParams, (error, resp) => {
                                assert.ifError(error);
                                const { UploadId: targetUploadId } = resp;
                                const targetPartParams = {
                                    UploadId: targetUploadId,
                                    Body: 'somedata',
                                    Bucket: bucket,
                                    Key: object2,
                                    PartNumber: 1,
                                };
                                uploadPart(s3, targetPartParams, error => {
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
