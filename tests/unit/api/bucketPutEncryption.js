const assert = require('assert');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutEncryption = require('../../../lib/api/bucketPutEncryption');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const metadata = require('../../../lib/metadata/wrapper');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketname';

const bucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
};

function templateSSEConfig({ algorithm, keyId }) {
    const xml = [];
    xml.push(`
    <?xml version="1.0" encoding="UTF-8"?>
    <ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Rule>
            <ApplyServerSideEncryptionByDefault>`
    );

    if (algorithm) {
        xml.push(`<SSEAlgorithm>${algorithm}</SSEAlgorithm>`);
    }

    if (keyId) {
        xml.push(`<KMSMasterKeyID>${keyId}</KMSMasterKeyID>`);
    }

    xml.push(`</ApplyServerSideEncryptionByDefault>
        </Rule>
    </ServerSideEncryptionConfiguration>`);
    return xml.join('');
}

function templateRequest({ post }) {
    return {
        bucketName,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        post,
    };
}

function getSSEConfig(cb) {
    return metadata.getBucket(bucketName, log, (err, md) => {
        if (err) {
            return cb(err);
        }
        return cb(null, md.getServerSideEncryption());
    });
}

describe('bucketPutEncryption API', () => {
    before(() => cleanup());

    beforeEach(done => bucketPut(authInfo, bucketPutRequest, log, done));
    afterEach(() => cleanup());

    describe('test invalid sse configs', () => {
        it('should reject an empty config', done => {
            bucketPutEncryption(authInfo, templateRequest({ post: '' }), log, err => {
                assert.strictEqual(err.MalformedXML, true);
                done();
            });
        });

        it('should reject a config with no Rule', done => {
            bucketPutEncryption(authInfo, templateRequest(
            { post: `<?xml version="1.0" encoding="UTF-8"?>
                <ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                </ServerSideEncryptionConfiguration>`,
            }), log, err => {
                assert.strictEqual(err.MalformedXML, true);
                done();
            });
        });

        it('should reject a config with no ApplyServerSideEncryptionByDefault section', done => {
            bucketPutEncryption(authInfo, templateRequest(
            { post: `<?xml version="1.0" encoding="UTF-8"?>
                <ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Rule></Rule>
                </ServerSideEncryptionConfiguration>`,
            }), log, err => {
                assert.strictEqual(err.MalformedXML, true);
                done();
            });
        });

        it('should reject a config with no SSEAlgorithm', done => {
            const post = templateSSEConfig({});
            bucketPutEncryption(authInfo, templateRequest({ post }), log, err => {
                assert.strictEqual(err.MalformedXML, true);
                done();
            });
        });

        it('should reject a config with an invalid SSEAlgorithm', done => {
            const post = templateSSEConfig({ algorithm: 'InvalidAlgo' });
            bucketPutEncryption(authInfo, templateRequest({ post }), log, err => {
                assert.strictEqual(err.MalformedXML, true);
                done();
            });
        });

        it('should reject a config with SSEAlgorithm == AES256 and a provided KMSMasterKeyID', done => {
            const post = templateSSEConfig({ algorithm: 'AES256', keyId: '12345' });
            bucketPutEncryption(authInfo, templateRequest({ post }), log, err => {
                assert.strictEqual(err.InvalidArgument, true);
                done();
            });
        });
    });

    describe('test setting config without a previous one', () => {
        it('should apply a config with SSEAlgorithm == AES256', done => {
            const post = templateSSEConfig({ algorithm: 'AES256' });
            bucketPutEncryption(authInfo, templateRequest({ post }), log, err => {
                assert.ifError(err);
                return getSSEConfig((err, sseInfo) => {
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
            bucketPutEncryption(authInfo, templateRequest({ post }), log, err => {
                assert.ifError(err);
                return getSSEConfig((err, sseInfo) => {
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
            bucketPutEncryption(authInfo, templateRequest({ post }), log, err => {
                assert.ifError(err);
                return getSSEConfig((err, sseInfo) => {
                    assert.ifError(err);
                    assert.deepStrictEqual(sseInfo, {
                        mandatory: true,
                        algorithm: 'aws:kms',
                        cryptoScheme: 1,
                        masterKeyId: sseInfo.masterKeyId,
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
            bucketPutEncryption(authInfo, templateRequest({ post }), log, err => {
                assert.ifError(err);
                return getSSEConfig((err, sseInfo) => {
                    assert.ifError(err);
                    const { masterKeyId } = sseInfo;
                    return bucketPutEncryption(authInfo, templateRequest({ post }), log, err => {
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
                bucketPutEncryption(authInfo, templateRequest({ post }), log, err => {
                    assert.ifError(err);
                    return getSSEConfig((err, sseInfo) => {
                        assert.ifError(err);
                        const { masterKeyId } = sseInfo;
                        const newConf = templateSSEConfig({ algorithm: 'aws:kms' });
                        return bucketPutEncryption(authInfo, templateRequest({ post: newConf }), log, err => {
                            assert.ifError(err);
                            return getSSEConfig((err, updatedSSEInfo) => {
                                assert.deepStrictEqual(updatedSSEInfo, {
                                    mandatory: true,
                                    algorithm: 'aws:kms',
                                    cryptoScheme: 1,
                                    masterKeyId,
                                });
                                done();
                            });
                        });
                    });
                });
            });

        it('should update SSEAlgorithm to aws:kms and set KMSMasterKeyID', done => {
            const post = templateSSEConfig({ algorithm: 'AES256' });
            bucketPutEncryption(authInfo, templateRequest({ post }), log, err => {
                assert.ifError(err);
                return getSSEConfig((err, sseInfo) => {
                    assert.ifError(err);
                    const { masterKeyId } = sseInfo;
                    const newConf = templateSSEConfig({ algorithm: 'aws:kms', keyId: '12345' });
                    return bucketPutEncryption(authInfo, templateRequest({ post: newConf }), log, err => {
                        assert.ifError(err);
                        return getSSEConfig((err, updatedSSEInfo) => {
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
            bucketPutEncryption(authInfo, templateRequest({ post }), log, err => {
                assert.ifError(err);
                return getSSEConfig((err, sseInfo) => {
                    assert.ifError(err);
                    const { masterKeyId } = sseInfo;
                    const newConf = templateSSEConfig({ algorithm: 'AES256' });
                    return bucketPutEncryption(authInfo, templateRequest({ post: newConf }), log, err => {
                        assert.ifError(err);
                        return getSSEConfig((err, updatedSSEInfo) => {
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
            bucketPutEncryption(authInfo, templateRequest({ post }), log, err => {
                assert.ifError(err);
                return getSSEConfig((err, sseInfo) => {
                    assert.ifError(err);
                    const { masterKeyId } = sseInfo;
                    const newConf = templateSSEConfig({ algorithm: 'AES256' });
                    return bucketPutEncryption(authInfo, templateRequest({ post: newConf }), log, err => {
                        assert.ifError(err);
                        return getSSEConfig((err, updatedSSEInfo) => {
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
    });
});
