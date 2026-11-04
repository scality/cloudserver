const assert = require('assert');
const KMS = require('../../../lib/kms/wrapper');
const { DummyRequestLogger } = require('../helpers');

const log = new DummyRequestLogger();

describe('KMS.isSameMasterKey', () => {
    let keyId;
    let keyArn;
    let otherKeyArn;

    before(done => {
        KMS.client.createBucketKey('bucket', log, (err, id, arn) => {
            if (err) {
                return done(err);
            }
            keyId = id;
            keyArn = arn;
            return KMS.client.createBucketKey('bucket', log, (err, otherId, otherArn) => {
                otherKeyArn = otherArn;
                done(err);
            });
        });
    });

    it('should match a key ARN with itself', () => {
        assert.strictEqual(KMS.isSameMasterKey(keyArn, keyArn, log), true);
    });

    it('should match a key ARN with its key ID', () => {
        assert.strictEqual(KMS.isSameMasterKey(keyArn, keyId, log), true);
        assert.strictEqual(KMS.isSameMasterKey(keyId, keyArn, log), true);
    });

    it('should not match two different keys', () => {
        assert.strictEqual(KMS.isSameMasterKey(keyArn, otherKeyArn, log), false);
    });

    it('should not match a missing key', () => {
        assert.strictEqual(KMS.isSameMasterKey(undefined, undefined, log), false);
        assert.strictEqual(KMS.isSameMasterKey(keyArn, undefined, log), false);
    });

    it('should not match a key ID of an unconfigured KMS', () => {
        const otherKmsArn = `arn:scality:kms:external:kmip:thales:key/${keyId}`;
        assert.strictEqual(KMS.isSameMasterKey(keyArn, otherKmsArn, log), false);
    });
});
