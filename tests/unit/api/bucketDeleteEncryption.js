const assert = require('assert');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutEncryption = require('../../../lib/api/bucketPutEncryption');
const bucketDeleteEncryption = require('../../../lib/api/bucketDeleteEncryption');
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

describe('bucketDeleteEncryption API', () => {
    before(() => cleanup());

    beforeEach(done => bucketPut(authInfo, bucketPutRequest, log, done));
    afterEach(() => cleanup());

    it('should perform a no-op if no sse config exists', done => {
        bucketDeleteEncryption(authInfo, templateRequest(bucketName, {}), log, err => {
            assert.ifError(err);
            return getSSEConfig(bucketName, log, (err, sseInfo) => {
                assert.ifError(err);
                assert.strictEqual(sseInfo, null);
                done();
            });
        });
    });

    ['AES256', 'aws:kms'].forEach(algorithm =>
        it(`should disable mandatory sse for ${algorithm}`, done => {
            const post = templateSSEConfig({ algorithm });
            bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
                assert.ifError(err);
                bucketDeleteEncryption(authInfo, templateRequest(bucketName, {}), log, err => {
                    assert.ifError(err);
                    return getSSEConfig(bucketName, log, (err, sseInfo) => {
                        assert.ifError(err);
                        assert.strictEqual(sseInfo.mandatory, false);
                        done();
                    });
                });
            });
        }));

    it('should disable mandatory sse and clear key for aws:kms with a configured master key id', done => {
        const post = templateSSEConfig({ algorithm: 'aws:kms', keyId: '12345' });
        bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
            assert.ifError(err);
            bucketDeleteEncryption(authInfo, templateRequest(bucketName, {}), log, err => {
                assert.ifError(err);
                return getSSEConfig(bucketName, log, (err, sseInfo) => {
                    assert.ifError(err);
                    assert(!sseInfo.masterKeyId);
                    assert.strictEqual(sseInfo.mandatory, false);
                    assert.strictEqual(sseInfo.configuredMasterKeyId, '12345');
                    done();
                });
            });
        });
    });

    it('should generate a new master key and clear the configured key id', done => {
        const keyId = '12345';
        const post = templateSSEConfig({ algorithm: 'aws:kms', keyId });
        bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
            assert.ifError(err);
            bucketDeleteEncryption(authInfo, templateRequest(bucketName, {}), log, err => {
                assert.ifError(err);
                const post2 = templateSSEConfig({ algorithm: 'aws:kms' });
                bucketPutEncryption(authInfo, templateRequest(bucketName, { post: post2 }), log, err => {
                    assert.ifError(err);
                    return getSSEConfig(bucketName, log, (err, sseInfo) => {
                        assert.ifError(err);
                        assert.strictEqual(sseInfo.mandatory, true);
                        assert.strictEqual(sseInfo.algorithm, 'aws:kms');
                        assert(sseInfo.masterKeyId);
                        assert.notStrictEqual(sseInfo.masterKeyId, keyId, 'masterKeyId should be different from keyId');
                        assert(!sseInfo.configuredMasterKeyId);
                        done();
                    });
                });
            });
        });
    });

    it('should generate a new master key, update the algorithm and clear the configured key id', done => {
        const keyId = '12345';
        const post = templateSSEConfig({ algorithm: 'aws:kms', keyId });
        bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
            assert.ifError(err);
            bucketDeleteEncryption(authInfo, templateRequest(bucketName, {}), log, err => {
                assert.ifError(err);
                const post2 = templateSSEConfig({ algorithm: 'AES256' });
                bucketPutEncryption(authInfo, templateRequest(bucketName, { post: post2 }), log, err => {
                    assert.ifError(err);
                    return getSSEConfig(bucketName, log, (err, sseInfo) => {
                        assert.ifError(err);
                        assert.strictEqual(sseInfo.mandatory, true);
                        assert.strictEqual(sseInfo.algorithm, 'AES256');
                        assert(sseInfo.masterKeyId);
                        assert.notStrictEqual(sseInfo.masterKeyId, keyId, 'masterKeyId should be different from keyId');
                        assert(!sseInfo.configuredMasterKeyId);
                        done();
                    });
                });
            });
        });
    });

    it('should update the configured key id', done => {
        const keyId = '12345';
        const post = templateSSEConfig({ algorithm: 'aws:kms', keyId });
        bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
            assert.ifError(err);
            bucketDeleteEncryption(authInfo, templateRequest(bucketName, {}), log, err => {
                assert.ifError(err);
                const keyId2 = '12345';
                const post2 = templateSSEConfig({ algorithm: 'aws:kms', keyId: keyId2 });
                bucketPutEncryption(authInfo, templateRequest(bucketName, { post: post2 }), log, err => {
                    assert.ifError(err);
                    return getSSEConfig(bucketName, log, (err, sseInfo) => {
                        assert.ifError(err);
                        assert.strictEqual(sseInfo.mandatory, true);
                        assert.strictEqual(sseInfo.algorithm, 'aws:kms');
                        assert(!sseInfo.masterKeyId);
                        assert.strictEqual(sseInfo.configuredMasterKeyId, keyId2);
                        done();
                    });
                });
            });
        });
    });

    it('should add the configured key id and keep the default master key id', done => {
        const post = templateSSEConfig({ algorithm: 'AES256' });
        bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
            assert.ifError(err);
            return getSSEConfig(bucketName, log, (err, sseInfo) => {
                assert.ifError(err);
                const expectedMasterKeyId = sseInfo.masterKeyId;
                bucketDeleteEncryption(authInfo, templateRequest(bucketName, {}), log, err => {
                    assert.ifError(err);
                    const keyId = '12345';
                    const post2 = templateSSEConfig({ algorithm: 'aws:kms', keyId });
                    bucketPutEncryption(authInfo, templateRequest(bucketName, { post: post2 }), log, err => {
                        assert.ifError(err);
                        return getSSEConfig(bucketName, log, (err, sseInfo) => {
                            assert.ifError(err);
                            assert.strictEqual(sseInfo.mandatory, true);
                            assert.strictEqual(sseInfo.algorithm, 'aws:kms');
                            assert.strictEqual(sseInfo.masterKeyId, expectedMasterKeyId);
                            assert.strictEqual(sseInfo.configuredMasterKeyId, keyId);
                            done();
                        });
                    });
                });
            });
        });
    });

    it('should use the default master key id', done => {
        const post = templateSSEConfig({ algorithm: 'AES256' });
        bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
            assert.ifError(err);
            return getSSEConfig(bucketName, log, (err, sseInfo) => {
                assert.ifError(err);
                const expectedMasterKeyId = sseInfo.masterKeyId;
                bucketDeleteEncryption(authInfo, templateRequest(bucketName, {}), log, err => {
                    assert.ifError(err);
                    const post2 = templateSSEConfig({ algorithm: 'AES256' });
                    bucketPutEncryption(authInfo, templateRequest(bucketName, { post: post2 }), log, err => {
                        assert.ifError(err);
                        return getSSEConfig(bucketName, log, (err, sseInfo) => {
                            assert.ifError(err);
                            assert.strictEqual(sseInfo.mandatory, true);
                            assert.strictEqual(sseInfo.algorithm, 'AES256');
                            assert.strictEqual(sseInfo.masterKeyId, expectedMasterKeyId);
                            assert(!sseInfo.configuredMasterKeyId);
                            done();
                        });
                    });
                });
            });
        });
    });

    it('should use the default master key id with aws:kms algorithm', done => {
        const post = templateSSEConfig({ algorithm: 'AES256' });
        bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
            assert.ifError(err);
            return getSSEConfig(bucketName, log, (err, sseInfo) => {
                assert.ifError(err);
                const expectedMasterKeyId = sseInfo.masterKeyId;
                bucketDeleteEncryption(authInfo, templateRequest(bucketName, {}), log, err => {
                    assert.ifError(err);
                    const post2 = templateSSEConfig({ algorithm: 'aws:kms' });
                    bucketPutEncryption(authInfo, templateRequest(bucketName, { post: post2 }), log, err => {
                        assert.ifError(err);
                        return getSSEConfig(bucketName, log, (err, sseInfo) => {
                            assert.ifError(err);
                            assert.strictEqual(sseInfo.mandatory, true);
                            assert.strictEqual(sseInfo.algorithm, 'aws:kms');
                            assert.strictEqual(sseInfo.masterKeyId, expectedMasterKeyId);
                            assert(!sseInfo.configuredMasterKeyId);
                            done();
                        });
                    });
                });
            });
        });
    });
});
