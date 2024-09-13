const assert = require('assert');
const sinon = require('sinon');
const { errors } = require('arsenal');
const inMemory = require('../../../lib/kms/in_memory/backend').backend;
const vault = require('../../../lib/auth/vault');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutEncryption = require('../../../lib/api/bucketPutEncryption');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const { templateSSEConfig, templateRequest, getSSEConfig } = require('../utils/bucketEncryption');


const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketname';

const bucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
    actionImplicitDenies: false,
};

describe('bucketPutEncryption API', () => {
    let createBucketKeySpy;

    beforeEach(done => {
        createBucketKeySpy = sinon.spy(inMemory, 'createBucketKey');
        bucketPut(authInfo, bucketPutRequest, log, done);
    });

    afterEach(() => {
        sinon.restore();
        cleanup();
    });

    describe('test invalid sse configs', () => {
        it('should reject an empty config', done => {
            bucketPutEncryption(authInfo, templateRequest(bucketName, { post: '' }), log, err => {
                assert.strictEqual(err.is.MalformedXML, true);
                done();
            });
        });

        it('should reject a config with no Rule', done => {
            bucketPutEncryption(authInfo, templateRequest(bucketName,
            { post: `<?xml version="1.0" encoding="UTF-8"?>
                <ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                </ServerSideEncryptionConfiguration>`,
            }), log, err => {
                assert.strictEqual(err.is.MalformedXML, true);
                done();
            });
        });

        it('should reject a config with no ApplyServerSideEncryptionByDefault section', done => {
            bucketPutEncryption(authInfo, templateRequest(bucketName,
            { post: `<?xml version="1.0" encoding="UTF-8"?>
                <ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Rule></Rule>
                </ServerSideEncryptionConfiguration>`,
            }), log, err => {
                assert.strictEqual(err.is.MalformedXML, true);
                done();
            });
        });

        it('should reject a config with no SSEAlgorithm', done => {
            const post = templateSSEConfig({});
            bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
                assert.strictEqual(err.is.MalformedXML, true);
                done();
            });
        });

        it('should reject a config with an invalid SSEAlgorithm', done => {
            const post = templateSSEConfig({ algorithm: 'InvalidAlgo' });
            bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
                assert.strictEqual(err.is.MalformedXML, true);
                done();
            });
        });

        it('should reject a config with SSEAlgorithm == AES256 and a provided KMSMasterKeyID', done => {
            const post = templateSSEConfig({ algorithm: 'AES256', keyId: '12345' });
            bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
                assert.strictEqual(err.is.InvalidArgument, true);
                done();
            });
        });
    });

    describe('test setting config without a previous one', () => {
        it('should apply a config with SSEAlgorithm == AES256', done => {
            const post = templateSSEConfig({ algorithm: 'AES256' });
            bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
                assert.ifError(err);
                sinon.assert.calledOnce(createBucketKeySpy);
                return getSSEConfig(bucketName, log, (err, sseInfo) => {
                    assert.ifError(err);
                    assert.deepStrictEqual(sseInfo, {
                        mandatory: true,
                        algorithm: 'AES256',
                        cryptoScheme: 1,
                        masterKeyId: sseInfo.masterKeyId,
                    });
                    done();
                });
            });
        });

        it('should apply a config with SSEAlgorithm == aws:kms', done => {
            const post = templateSSEConfig({ algorithm: 'aws:kms' });
            bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
                assert.ifError(err);
                sinon.assert.calledOnce(createBucketKeySpy);
                return getSSEConfig(bucketName, log, (err, sseInfo) => {
                    assert.ifError(err);
                    assert.deepStrictEqual(sseInfo, {
                        mandatory: true,
                        algorithm: 'aws:kms',
                        cryptoScheme: 1,
                        masterKeyId: sseInfo.masterKeyId,
                    });
                    done();
                });
            });
        });

        it('should apply a config with SSEAlgorithm == aws:kms and a KMSMasterKeyID', done => {
            const post = templateSSEConfig({ algorithm: 'aws:kms', keyId: '12345' });
            bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
                assert.ifError(err);
                sinon.assert.notCalled(createBucketKeySpy);
                return getSSEConfig(bucketName, log, (err, sseInfo) => {
                    assert.ifError(err);
                    assert.deepStrictEqual(sseInfo, {
                        mandatory: true,
                        algorithm: 'aws:kms',
                        cryptoScheme: 1,
                        configuredMasterKeyId: '12345',
                    });
                    done();
                });
            });
        });
    });

    describe('test overwriting an existing config', () => {
        it('should perform a no-op if SSEAlgorithm is already set to AES256', done => {
            const post = templateSSEConfig({ algorithm: 'AES256' });
            bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
                assert.ifError(err);
                return getSSEConfig(bucketName, log, (err, sseInfo) => {
                    assert.ifError(err);
                    const { masterKeyId } = sseInfo;
                    return bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
                        assert.ifError(err);
                        assert.deepStrictEqual(sseInfo, {
                            mandatory: true,
                            algorithm: 'AES256',
                            cryptoScheme: 1,
                            // master key should not be rolled
                            masterKeyId,
                        });
                        done();
                    });
                });
            });
        });

        it('should update SSEAlgorithm if existing SSEAlgorithm is AES256, ' +
            'new SSEAlgorithm is aws:kms and no KMSMasterKeyID is provided',
            done => {
                const post = templateSSEConfig({ algorithm: 'AES256' });
                bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
                    assert.ifError(err);
                    return getSSEConfig(bucketName, log, (err, sseInfo) => {
                        assert.ifError(err);
                        const { masterKeyId } = sseInfo;
                        const newConf = templateSSEConfig({ algorithm: 'aws:kms' });
                        return bucketPutEncryption(authInfo, templateRequest(bucketName, { post: newConf }), log,
                            err => {
                                assert.ifError(err);
                                return getSSEConfig(bucketName, log, (err, updatedSSEInfo) => {
                                    assert.deepStrictEqual(updatedSSEInfo, {
                                        mandatory: true,
                                        algorithm: 'aws:kms',
                                        cryptoScheme: 1,
                                        masterKeyId,
                                    });
                                    done();
                                });
                            }
                        );
                    });
                });
            });

        it('should update SSEAlgorithm to aws:kms and set KMSMasterKeyID', done => {
            const post = templateSSEConfig({ algorithm: 'AES256' });
            bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
                assert.ifError(err);
                return getSSEConfig(bucketName, log, (err, sseInfo) => {
                    assert.ifError(err);
                    const { masterKeyId } = sseInfo;
                    const newConf = templateSSEConfig({ algorithm: 'aws:kms', keyId: '12345' });
                    return bucketPutEncryption(authInfo, templateRequest(bucketName, { post: newConf }), log, err => {
                        assert.ifError(err);
                        return getSSEConfig(bucketName, log, (err, updatedSSEInfo) => {
                            assert.deepStrictEqual(updatedSSEInfo, {
                                mandatory: true,
                                algorithm: 'aws:kms',
                                cryptoScheme: 1,
                                masterKeyId,
                                configuredMasterKeyId: '12345',
                            });
                            done();
                        });
                    });
                });
            });
        });

        it('should update SSEAlgorithm to AES256', done => {
            const post = templateSSEConfig({ algorithm: 'aws:kms' });
            bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
                assert.ifError(err);
                return getSSEConfig(bucketName, log, (err, sseInfo) => {
                    assert.ifError(err);
                    const { masterKeyId } = sseInfo;
                    const newConf = templateSSEConfig({ algorithm: 'AES256' });
                    return bucketPutEncryption(authInfo, templateRequest(bucketName, { post: newConf }), log, err => {
                        assert.ifError(err);
                        return getSSEConfig(bucketName, log, (err, updatedSSEInfo) => {
                            assert.deepStrictEqual(updatedSSEInfo, {
                                mandatory: true,
                                algorithm: 'AES256',
                                cryptoScheme: 1,
                                masterKeyId,
                            });
                            done();
                        });
                    });
                });
            });
        });

        it('should update SSEAlgorithm to AES256 and remove KMSMasterKeyID', done => {
            const post = templateSSEConfig({ algorithm: 'aws:kms', keyId: '12345' });
            bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
                assert.ifError(err);
                return getSSEConfig(bucketName, log, err => {
                    assert.ifError(err);
                    const newConf = templateSSEConfig({ algorithm: 'AES256' });
                    return bucketPutEncryption(authInfo, templateRequest(bucketName, { post: newConf }), log, err => {
                        assert.ifError(err);
                        return getSSEConfig(bucketName, log, (err, updatedSSEInfo) => {
                            assert.strictEqual(updatedSSEInfo.mandatory, true);
                            assert.strictEqual(updatedSSEInfo.algorithm, 'AES256');
                            assert.strictEqual(updatedSSEInfo.cryptoScheme, 1);
                            assert(updatedSSEInfo.masterKeyId);
                            done();
                        });
                    });
                });
            });
        });
    });
});

describe('bucketPutEncryption API with failed encryption service', () => {
    beforeEach(done => {
        sinon.stub(inMemory, 'createBucketKey').callsFake((bucketName, log, cb) => cb(errors.InternalError));
        bucketPut(authInfo, bucketPutRequest, log, done);
    });

    afterEach(() => {
        sinon.restore();
        cleanup();
    });

    it('should fail putting bucket encryption', done => {
        const post = templateSSEConfig({ algorithm: 'AES256' });
        bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
            assert(err && err.InternalError);
            done();
        });
    });
});

describe('bucketPutEncryption API with account level encryption', () => {
    let getOrCreateEncryptionKeyIdSpy;
    const accountLevelMasterKeyId = 'account-level-master-encryption-key';

    beforeEach(done => {
        sinon.stub(inMemory, 'supportsDefaultKeyPerAccount').value(true);
        getOrCreateEncryptionKeyIdSpy = sinon.spy(vault, 'getOrCreateEncryptionKeyId');
        bucketPut(authInfo, bucketPutRequest, log, done);
    });

    afterEach(() => {
        sinon.restore();
        cleanup();
    });

    it('should create account level master encryption key with AES256 algorithm', done => {
        const post = templateSSEConfig({ algorithm: 'AES256' });
        bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
            assert.ifError(err);
            sinon.assert.calledOnce(getOrCreateEncryptionKeyIdSpy);
            return getSSEConfig(bucketName, log, (err, sseInfo) => {
                assert.ifError(err);
                assert.deepStrictEqual(sseInfo, {
                    cryptoScheme: 1,
                    algorithm: 'AES256',
                    mandatory: true,
                    masterKeyId: accountLevelMasterKeyId,
                    isAccountEncryptionEnabled: true,
                });
                done();
            });
        });
    });

    it('should create account level master encryption key with aws:kms algorithm', done => {
        const post = templateSSEConfig({ algorithm: 'aws:kms' });
        bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
            assert.ifError(err);
            sinon.assert.calledOnce(getOrCreateEncryptionKeyIdSpy);
            return getSSEConfig(bucketName, log, (err, sseInfo) => {
                assert.ifError(err);
                assert.deepStrictEqual(sseInfo, {
                    cryptoScheme: 1,
                    algorithm: 'aws:kms',
                    mandatory: true,
                    masterKeyId: accountLevelMasterKeyId,
                    isAccountEncryptionEnabled: true,
                });
                done();
            });
        });
    });

    it('should not create account level master key if custom master key id is specified', done => {
        const keyId = '12345';
        const post = templateSSEConfig({ algorithm: 'aws:kms', keyId });
        bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
            assert.ifError(err);
            sinon.assert.notCalled(getOrCreateEncryptionKeyIdSpy);
            return getSSEConfig(bucketName, log, (err, sseInfo) => {
                assert.ifError(err);
                assert.deepStrictEqual(sseInfo, {
                    cryptoScheme: 1,
                    algorithm: 'aws:kms',
                    mandatory: true,
                    configuredMasterKeyId: keyId,
                });
                done();
            });
        });
    });
});

describe('bucketPutEncryption API with failed vault service', () => {
    beforeEach(done => {
        sinon.stub(inMemory, 'supportsDefaultKeyPerAccount').value(true);
        sinon.stub(vault, 'getOrCreateEncryptionKeyId').callsFake((accountCanonicalId, log, cb) =>
            cb(errors.ServiceFailure));
        bucketPut(authInfo, bucketPutRequest, log, done);
    });

    afterEach(() => {
        sinon.restore();
        cleanup();
    });

    it('should fail putting bucket encryption', done => {
        const post = templateSSEConfig({ algorithm: 'AES256' });
        bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
            assert(err && err.ServiceFailure);
            done();
        });
    });
});
