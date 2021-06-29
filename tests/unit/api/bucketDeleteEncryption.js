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
                    assert.strictEqual(sseInfo.mandatory, false);
                    assert.strictEqual(sseInfo.configuredMasterKeyId, undefined);
                    done();
                });
            });
        });
    });
});
