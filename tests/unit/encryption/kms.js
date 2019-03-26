const assert = require('assert');
const KMS = require('../../../lib/kms/wrapper');
const Common = require('../../../lib/kms/common');
const { cleanup, DummyRequestLogger } = require('../helpers');

const log = new DummyRequestLogger();

describe('KMS unit tests', () => {
    beforeEach(() => {
        cleanup();
    });

    test('should construct a sse info object on AES256', done => {
        const algorithm = 'AES256';
        const headers = {
            'x-amz-scal-server-side-encryption': algorithm,
        };
        KMS.bucketLevelEncryption(
            'dummyBucket', headers, log,
            (err, sseInfo) => {
                expect(err).toBe(null);
                expect(sseInfo.cryptoScheme).toBe(1);
                expect(sseInfo.mandatory).toBe(true);
                expect(sseInfo.algorithm).toBe(algorithm);
                expect(sseInfo.masterKeyId).not.toEqual(undefined);
                expect(sseInfo.masterKeyId).not.toEqual(null);
                done();
            });
    });

    test('should construct a sse info object on aws:kms', done => {
        const algorithm = 'aws:kms';
        const masterKeyId = 'foobarbaz';
        const headers = {
            'x-amz-scal-server-side-encryption': algorithm,
            'x-amz-scal-server-side-encryption-aws-kms-key-id': masterKeyId,
        };
        KMS.bucketLevelEncryption(
            'dummyBucket', headers, log,
            (err, sseInfo) => {
                expect(err).toBe(null);
                expect(sseInfo.cryptoScheme).toBe(1);
                expect(sseInfo.mandatory).toBe(true);
                expect(sseInfo.algorithm).toBe('aws:kms');
                expect(sseInfo.masterKeyId).toBe(masterKeyId);
                done();
            });
    });

    test('should not construct a sse info object if ' +
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
                expect(err).toBe(null);
                expect(sseInfo).toBe(null);
                done();
            });
    });

    test('should not construct a sse info object if no ' +
        'x-amz-scal-server-side-encryption header included with request', done => {
        KMS.bucketLevelEncryption(
            'dummyBucket', {}, log,
            (err, sseInfo) => {
                expect(err).toBe(null);
                expect(sseInfo).toBe(null);
                done();
            });
    });

    test('should create a cipher bundle for AES256', done => {
        const algorithm = 'AES256';
        const headers = {
            'x-amz-scal-server-side-encryption': algorithm,
        };
        KMS.bucketLevelEncryption(
            'dummyBucket', headers, log,
            (err, sseInfo) => {
                KMS.createCipherBundle(
                    sseInfo, log, (err, cipherBundle) => {
                        expect(cipherBundle.algorithm).toBe(sseInfo.algorithm);
                        expect(cipherBundle.masterKeyId).toBe(sseInfo.masterKeyId);
                        expect(cipherBundle.cryptoScheme).toBe(sseInfo.cryptoScheme);
                        expect(cipherBundle.cipheredDataKey).not.toEqual(null);
                        expect(cipherBundle.cipher).not.toEqual(null);
                        done();
                    });
            });
    });

    test('should create a cipher bundle for aws:kms', done => {
        const headers = {
            'x-amz-scal-server-side-encryption': 'AES256',
        };
        let masterKeyId;
        KMS.bucketLevelEncryption(
            'dummyBucket', headers, log,
            (err, sseInfo) => {
                expect(err).toBe(null);
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
                        expect(cipherBundle.algorithm).toBe(sseInfo.algorithm);
                        expect(cipherBundle.masterKeyId).toBe(sseInfo.masterKeyId);
                        expect(cipherBundle.cryptoScheme).toBe(sseInfo.cryptoScheme);
                        expect(cipherBundle.cipheredDataKey).not.toEqual(null);
                        expect(cipherBundle.cipher).not.toEqual(null);
                        done();
                    });
            });
    });

    /* cb(err, cipherBundle, decipherBundle) */
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
                                expect(typeof decipherBundle).toBe('object');
                                expect(decipherBundle.cryptoScheme).toBe(cipherBundle.cryptoScheme);
                                expect(decipherBundle.decipher).not.toEqual(null);
                                cb(null, cipherBundle, decipherBundle);
                            });
                    });
            });
    }

    test('should cipher and decipher a datastream', done => {
        _utestCreateBundlePair(log, (err, cipherBundle, decipherBundle) => {
            expect(err).toBe(null);
            cipherBundle.cipher.pipe(decipherBundle.decipher);
            // note that node stream high water mark is 16kb
            // so this data will be written into and read from stream buffer
            // with just the write() and read() calls
            const target = Buffer.alloc(10000, 'e');
            cipherBundle.cipher.write(target);
            const result = decipherBundle.decipher.read();
            expect(result).toEqual(target);
            done();
        });
    });

    test('should increment the IV by modifying the last two positions of ' +
        'the buffer', () => {
        const derivedIV = Buffer.from('aaaaaaff', 'hex');
        const counter = 6;
        const incrementedIV = Common._incrementIV(derivedIV, counter);
        const expected = Buffer.from('aaaaab05', 'hex');
        assert.deepStrictEqual(incrementedIV, expected);
    });

    test('should increment the IV by incrementing the last position of the ' +
        'buffer', () => {
        const derivedIV = Buffer.from('aaaaaaf0', 'hex');
        const counter = 6;
        const incrementedIV = Common._incrementIV(derivedIV, counter);
        const expected = Buffer.from('aaaaaaf6', 'hex');
        assert.deepStrictEqual(incrementedIV, expected);
    });

    test('should increment the IV by shifting each position in the ' +
        'buffer', () => {
        const derivedIV = Buffer.from('ffffffff', 'hex');
        const counter = 1;
        const incrementedIV = Common._incrementIV(derivedIV, counter);
        const expected = Buffer.from('00000001', 'hex');
        assert.deepStrictEqual(incrementedIV, expected);
    });
});
