import assert from 'assert';
import KMS from '../../../lib/kms/wrapper';
import Common from '../../../lib/kms/common';
import { cleanup, DummyRequestLogger } from '../helpers';

const log = new DummyRequestLogger();

describe('KMS unit tests', () => {
    beforeEach(() => {
        cleanup();
    });

    it('should construct a sse info object on AES256', done => {
        const algorithm = 'AES256';
        const headers = {
            'x-amz-scal-server-side-encryption': algorithm,
        };
        KMS.bucketLevelEncryption(
            'dummyBucket', headers, log,
            (err, sseInfo) => {
                assert.strictEqual(err, null);
                assert.strictEqual(sseInfo.cryptoScheme, 1);
                assert.strictEqual(sseInfo.mandatory, true);
                assert.strictEqual(sseInfo.algorithm, algorithm);
                assert.notEqual(sseInfo.masterKeyId, undefined);
                assert.notEqual(sseInfo.masterKeyId, null);
                done();
            });
    });

    it('should construct a sse info object on aws:kms', done => {
        const algorithm = 'aws:kms';
        const masterKeyId = 'foobarbaz';
        const headers = {
            'x-amz-scal-server-side-encryption': algorithm,
            'x-amz-scal-server-side-encryption-aws-kms-key-id': masterKeyId,
        };
        KMS.bucketLevelEncryption(
            'dummyBucket', headers, log,
            (err, sseInfo) => {
                assert.strictEqual(err, null);
                assert.strictEqual(sseInfo.cryptoScheme, 1);
                assert.strictEqual(sseInfo.mandatory, true);
                assert.strictEqual(sseInfo.algorithm, 'aws:kms');
                assert.strictEqual(sseInfo.masterKeyId, masterKeyId);
                done();
            });
    });

    it('should not construct a sse info object if ' +
        'x-amz-scal-server-side-encryption header contains invalid ' +
        'algorithm option', done => {
        const algorithm = 'garbage';
        const masterKeyId = 'foobarbaz';
        const headers = {
            'x-amz-scal-server-side-encryption': algorithm,
            'x-amz-scal-server-side-encryption-aws-kms-key-id': masterKeyId,
        };
        KMS.bucketLevelEncryption(
            'dummyBucket', headers, log,
            (err, sseInfo) => {
                assert.strictEqual(err, null);
                assert.strictEqual(sseInfo, null);
                done();
            });
    });

    it('should not construct a sse info object if no ' +
        'x-amz-scal-server-side-encryption header included with request',
        done => {
            KMS.bucketLevelEncryption(
                'dummyBucket', {}, log,
                (err, sseInfo) => {
                    assert.strictEqual(err, null);
                    assert.strictEqual(sseInfo, null);
                    done();
                });
        });

    it('should create a cipher bundle for AES256', done => {
        const algorithm = 'AES256';
        const headers = {
            'x-amz-scal-server-side-encryption': algorithm,
        };
        KMS.bucketLevelEncryption(
            'dummyBucket', headers, log,
            (err, sseInfo) => {
                KMS.createCipherBundle(
                    sseInfo, log, (err, cipherBundle) => {
                        assert.strictEqual(cipherBundle.algorithm,
                                           sseInfo.algorithm);
                        assert.strictEqual(cipherBundle.masterKeyId,
                                           sseInfo.masterKeyId);
                        assert.strictEqual(cipherBundle.cryptoScheme,
                                           sseInfo.cryptoScheme);
                        assert.notEqual(cipherBundle.cipheredDataKey, null);
                        assert.notEqual(cipherBundle.cipher, null);
                        done();
                    });
            });
    });

    it('should create a cipher bundle for aws:kms', done => {
        const headers = {
            'x-amz-scal-server-side-encryption': 'AES256',
        };
        let masterKeyId;
        KMS.bucketLevelEncryption(
            'dummyBucket', headers, log,
            (err, sseInfo) => {
                assert.strictEqual(err, null);
                masterKeyId = sseInfo.bucketKeyId;
            });

        headers['x-amz-scal-server-side-encryption'] = 'aws:kms';
        headers['x-amz-scal-server-side-encryption-aws-kms-key-id'] =
            masterKeyId;
        KMS.bucketLevelEncryption(
            'dummyBucket', headers, log,
            (err, sseInfo) => {
                KMS.createCipherBundle(
                    sseInfo, log, (err, cipherBundle) => {
                        assert.strictEqual(cipherBundle.algorithm,
                                           sseInfo.algorithm);
                        assert.strictEqual(cipherBundle.masterKeyId,
                                           sseInfo.masterKeyId);
                        assert.strictEqual(cipherBundle.cryptoScheme,
                                           sseInfo.cryptoScheme);
                        assert.notEqual(cipherBundle.cipheredDataKey, null);
                        assert.notEqual(cipherBundle.cipher, null);
                        done();
                    });
            });
    });

    /* cb(err, cipherBundle, decipherBundle)*/
    function _utestCreateBundlePair(log, cb) {
        const algorithm = 'AES256';
        const headers = {
            'x-amz-scal-server-side-encryption': algorithm,
        };
        KMS.bucketLevelEncryption(
            'dummyBucket', headers, log,
            (err, sseInfo) => {
                if (err) {
                    cb(err);
                    return;
                }
                KMS.createCipherBundle(
                    sseInfo, log, (err, cipherBundle) => {
                        if (err) {
                            cb(err);
                            return;
                        }
                        const creatingSseInfo = sseInfo;
                        creatingSseInfo.cipheredDataKey =
                            Buffer.from(cipherBundle.cipheredDataKey, 'base64');
                        KMS.createDecipherBundle(
                            sseInfo, 0, log, (err, decipherBundle) => {
                                if (err) {
                                    cb(err);
                                    return;
                                }
                                assert.strictEqual(typeof decipherBundle,
                                                   'object');
                                assert.strictEqual(decipherBundle.cryptoScheme,
                                                   cipherBundle.cryptoScheme);
                                assert.notEqual(decipherBundle.decipher, null);
                                cb(null, cipherBundle, decipherBundle);
                            });
                    });
            });
    }

    it('should cipher and decipher a datastream', done => {
        _utestCreateBundlePair(log, (err, cipherBundle, decipherBundle) => {
            assert.strictEqual(err, null);
            cipherBundle.cipher.pipe(decipherBundle.decipher);
            // note that node stream high water mark is 16kb
            // so this data will be written into and read from stream buffer
            // with just the write() and read() calls
            const target = Buffer.alloc(10000, 'e');
            cipherBundle.cipher.write(target);
            const result = decipherBundle.decipher.read();
            assert.deepEqual(result, target);
            done();
        });
    });

    it('should increment the IV by modifying the last two positions of ' +
        'the buffer', () => {
        const derivedIV = Buffer.from('aaaaaaff', 'hex');
        const counter = 6;
        const incrementedIV = Common._incrementIV(derivedIV, counter);
        const expected = Buffer.from('aaaaab05', 'hex');
        assert.deepStrictEqual(incrementedIV, expected);
    });

    it('should increment the IV by incrementing the last position of the ' +
        'buffer', () => {
        const derivedIV = Buffer.from('aaaaaaf0', 'hex');
        const counter = 6;
        const incrementedIV = Common._incrementIV(derivedIV, counter);
        const expected = Buffer.from('aaaaaaf6', 'hex');
        assert.deepStrictEqual(incrementedIV, expected);
    });

    it('should increment the IV by shifting each position in the ' +
        'buffer', () => {
        const derivedIV = Buffer.from('ffffffff', 'hex');
        const counter = 1;
        const incrementedIV = Common._incrementIV(derivedIV, counter);
        const expected = Buffer.from('00000001', 'hex');
        assert.deepStrictEqual(incrementedIV, expected);
    });
});
