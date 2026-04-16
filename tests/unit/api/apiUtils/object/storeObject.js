const assert = require('assert');
const sinon = require('sinon');
const { errors } = require('arsenal');

const { dataStore } = require('../../../../../lib/api/apiUtils/object/storeObject');
const dataWrapper = require('../../../../../lib/data/wrapper');
const { DummyRequestLogger } = require('../../../helpers');
const DummyRequest = require('../../../DummyRequest');
const { defaultChecksumData } = require('../../../../../lib/api/apiUtils/integrity/validateChecksums');

const log = new DummyRequestLogger();
const defaultChecksums = { primary: defaultChecksumData, secondary: null };

const fakeDataRetrievalInfo = { key: 'test-key', dataStoreName: 'mem' };

function makeStream(headers = {}, body = '') {
    return new DummyRequest({ headers }, body ? Buffer.from(body) : undefined);
}

describe('dataStore', () => {
    let putStub, batchDeleteStub;

    beforeEach(() => {
        putStub = sinon.stub(dataWrapper.data, 'put');
        batchDeleteStub = sinon.stub(dataWrapper.data, 'batchDelete');
    });

    afterEach(() => {
        sinon.restore();
    });

    // Stub data.put to drain the readable side and succeed once the stream finishes.
    function putSucceeds(completedHash = null) {
        putStub.callsFake((cipher, stream, size, ctx, backend, log2, cb) => {
            stream.resume();
            stream.once('finish', () => cb(null, fakeDataRetrievalInfo, { completedHash }));
        });
    }

    // Stub data.batchDelete to succeed.
    function batchDeleteSucceeds() {
        batchDeleteStub.callsFake((keys, a, b, log2, cb) => cb(null));
    }

    describe('normal behaviour', () => {
        it('should call data.put with the stream returned by prepareStream', done => {
            putSucceeds();
            const request = makeStream({ 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' });
            dataStore({}, null, request, 0, null, {}, defaultChecksums, log, err => {
                assert.strictEqual(err, null);
                assert(putStub.calledOnce);
                done();
            });
        });

        it('should call cb with (null, dataRetrievalInfo, completedHash) on success', done => {
            putSucceeds('abc123');
            const request = makeStream({ 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' });
            dataStore({}, null, request, 0, null, {}, defaultChecksums, log, (err, dataInfo, completedHash) => {
                assert.strictEqual(err, null);
                assert.strictEqual(dataInfo, fakeDataRetrievalInfo);
                assert.strictEqual(completedHash, 'abc123');
                done();
            });
        });

        it('should call cb with the error from data.put when data.put fails', done => {
            putStub.callsFake((cipher, stream, size, ctx, backend, log2, cb) => {
                stream.resume();
                cb(errors.InternalError);
            });
            const request = makeStream({ 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' });
            dataStore({}, null, request, 0, null, {}, defaultChecksums, log, err => {
                assert.deepStrictEqual(err, errors.InternalError);
                done();
            });
        });

        it('should not delete stored data when data.put fails', done => {
            putStub.callsFake((cipher, stream, size, ctx, backend, log2, cb) => {
                stream.resume();
                cb(errors.InternalError);
            });
            const request = makeStream({ 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' });
            dataStore({}, null, request, 0, null, {}, defaultChecksums, log, () => {
                assert(batchDeleteStub.notCalled);
                done();
            });
        });

        it('should call cb with InternalError when data.put returns neither error nor dataRetrievalInfo', done => {
            putStub.callsFake((cipher, stream, size, ctx, backend, log2, cb) => {
                stream.resume();
                cb(null, null, null);
            });
            const request = makeStream({ 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' });
            dataStore({}, null, request, 0, null, {}, defaultChecksums, log, err => {
                assert.deepStrictEqual(err, errors.InternalError);
                done();
            });
        });

        it('should call cb with BadDigest and delete stored data when content-md5 does not match', done => {
            batchDeleteSucceeds();
            putSucceeds('correct-md5');
            const request = makeStream({ 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' });
            request.contentMD5 = 'wrong-md5';
            dataStore({}, null, request, 0, null, {}, defaultChecksums, log, err => {
                assert.deepStrictEqual(err, errors.BadDigest);
                assert(batchDeleteStub.calledOnce);
                done();
            });
        });

        it('should call cb with (null, ...) and not delete stored data when content-md5 matches', done => {
            putSucceeds('abc123');
            const request = makeStream({ 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' });
            request.contentMD5 = 'abc123';
            dataStore({}, null, request, 0, null, {}, defaultChecksums, log, err => {
                assert.strictEqual(err, null);
                assert(batchDeleteStub.notCalled);
                done();
            });
        });

        it('should call cb exactly once on success', done => {
            putSucceeds();
            let cbCount = 0;
            const request = makeStream({ 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' });
            dataStore({}, null, request, 0, null, {}, defaultChecksums, log, () => {
                cbCount++;
                setImmediate(() => {
                    assert.strictEqual(cbCount, 1);
                    done();
                });
            });
        });
    });

    describe('checksum behaviour', () => {
        it('should call cb with error from prepareStream when signature checksum is unsupported', done => {
            const request = makeStream({
                'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER',
            });
            dataStore({}, null, request, 0, null, {}, defaultChecksums, log, err => {
                assert.strictEqual(err.message, 'BadRequest');
                done();
            });
        });

        it('should not call data.put when prepareStream returns an error', done => {
            const request = makeStream({
                'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER',
            });
            dataStore({}, null, request, 0, null, {}, defaultChecksums, log, () => {
                assert(putStub.notCalled);
                done();
            });
        });

        it('should call cb with BadDigest and delete stored data when checksum validation fails', done => {
            batchDeleteSucceeds();
            putSucceeds();
            // CRC32 of 'hello world' is not 0x00000000 (AAAAAA==)
            const request = makeStream({
                'x-amz-content-sha256': 'UNSIGNED-PAYLOAD',
            }, 'hello world');
            const badChecksums = {
                primary: { algorithm: 'crc32', isTrailer: false, expected: 'AAAAAA==' },
                secondary: null,
            };
            dataStore({}, null, request, 0, null, {}, badChecksums, log, err => {
                assert.strictEqual(err.message, 'BadDigest');
                assert(batchDeleteStub.calledOnce);
                done();
            });
        });

        it('should not delete stored data when checksum validation passes', done => {
            putSucceeds();
            const request = makeStream({
                'x-amz-content-sha256': 'UNSIGNED-PAYLOAD',
            }, 'hello world');
            const goodChecksums = {
                primary: { algorithm: 'crc32', isTrailer: false, expected: 'DUoRhQ==' },
                secondary: null,
            };
            dataStore({}, null, request, 0, null, {}, goodChecksums, log, err => {
                assert.strictEqual(err, null);
                assert(batchDeleteStub.notCalled);
                done();
            });
        });

        it('should wait for finish before validating when checksumedStream is not yet writableFinished after data.put',
            done => {
                let capturedStream;
                putStub.callsFake((cipher, stream, size, ctx, backend, log2, cb) => {
                    capturedStream = stream;
                    stream.resume();
                    // Call cb synchronously — _flush uses Promise.resolve().then() so
                    // writableFinished is false here, exercising the finish-wait path.
                    cb(null, fakeDataRetrievalInfo, { completedHash: null });
                });
                const request = makeStream({
                    'x-amz-content-sha256': 'UNSIGNED-PAYLOAD',
                }, 'hello world');
                const goodChecksums = {
                    primary: { algorithm: 'crc32', isTrailer: false, expected: 'DUoRhQ==' },
                    secondary: null,
                };
                dataStore({}, null, request, 0, null, {}, goodChecksums, log, err => {
                    assert.strictEqual(err, null);
                    assert(capturedStream.writableFinished);
                    done();
                });
            });

        it('should delete stored data and call cb with the error when checksumedStream emits error after data.put',
            done => {
                batchDeleteSucceeds();
                let capturedStream;
                putStub.callsFake((cipher, stream, size, ctx, backend, log2, cb) => {
                    capturedStream = stream;
                    // Do not resume — keeps writableFinished false, so onError listener is registered.
                    cb(null, fakeDataRetrievalInfo, { completedHash: null });
                });
                const request = makeStream({ 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' });
                dataStore({}, null, request, 0, null, {}, defaultChecksums, log, err => {
                    assert.deepStrictEqual(err, errors.InternalError);
                    assert(batchDeleteStub.calledOnce);
                    done();
                });
                // process.nextTick fires before Promise microtasks, so the error arrives
                // before _flush resolves, ensuring onError fires rather than onFinish.
                process.nextTick(() => capturedStream.emit('error', errors.InternalError));
            });

        it('should call cb exactly once when finish fires (no double callback)', done => {
            let cbCount = 0;
            putStub.callsFake((cipher, stream, size, ctx, backend, log2, cb) => {
                stream.resume();
                // Synchronous cb → writableFinished is false → finish-wait path.
                cb(null, fakeDataRetrievalInfo, { completedHash: null });
            });
            const request = makeStream({ 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' });
            dataStore({}, null, request, 0, null, {}, defaultChecksums, log, () => {
                cbCount++;
                setImmediate(() => {
                    assert.strictEqual(cbCount, 1);
                    done();
                });
            });
        });

        it('should call cb exactly once when stream errors after data.put (no double callback)', done => {
            batchDeleteSucceeds();
            let cbCount = 0;
            let capturedStream;
            putStub.callsFake((cipher, stream, size, ctx, backend, log2, cb) => {
                capturedStream = stream;
                cb(null, fakeDataRetrievalInfo, { completedHash: null });
            });
            const request = makeStream({ 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' });
            dataStore({}, null, request, 0, null, {}, defaultChecksums, log, () => {
                cbCount++;
                setImmediate(() => {
                    assert.strictEqual(cbCount, 1);
                    done();
                });
            });
            process.nextTick(() => capturedStream.emit('error', errors.InternalError));
        });
    });

    describe('dual-checksum behaviour', () => {
        it('should return client-facing checksum from secondary and storageChecksum from primary', done => {
            putSucceeds();
            const request = makeStream({
                'x-amz-content-sha256': 'UNSIGNED-PAYLOAD',
            }, 'hello world');
            // Primary: crc64nvme (storage), Secondary: crc32 (client-facing)
            const dualChecksums = {
                primary: { algorithm: 'crc64nvme', isTrailer: false, expected: undefined },
                secondary: { algorithm: 'crc32', isTrailer: false, expected: 'DUoRhQ==' },
            };
            dataStore({}, null, request, 0, null, {}, dualChecksums, log, (err, dataInfo, hash, checksum) => {
                assert.strictEqual(err, null);
                assert.strictEqual(checksum.algorithm, 'crc32');
                assert.strictEqual(checksum.value, 'DUoRhQ==');
                assert(checksum.storageChecksum);
                assert.strictEqual(checksum.storageChecksum.algorithm, 'crc64nvme');
                assert(checksum.storageChecksum.value);
                done();
            });
        });

        it('should fail with BadDigest when secondary checksum does not match', done => {
            batchDeleteSucceeds();
            putSucceeds();
            const request = makeStream({
                'x-amz-content-sha256': 'UNSIGNED-PAYLOAD',
            }, 'hello world');
            const dualChecksums = {
                primary: { algorithm: 'crc64nvme', isTrailer: false, expected: undefined },
                secondary: { algorithm: 'crc32', isTrailer: false, expected: 'AAAAAA==' },
            };
            dataStore({}, null, request, 0, null, {}, dualChecksums, log, err => {
                assert.strictEqual(err.message, 'BadDigest');
                assert(batchDeleteStub.calledOnce);
                done();
            });
        });

        it('should return no storageChecksum when secondary is null', done => {
            putSucceeds();
            const request = makeStream({
                'x-amz-content-sha256': 'UNSIGNED-PAYLOAD',
            }, 'hello world');
            const singleChecksums = {
                primary: { algorithm: 'crc32', isTrailer: false, expected: 'DUoRhQ==' },
                secondary: null,
            };
            dataStore({}, null, request, 0, null, {}, singleChecksums, log, (err, dataInfo, hash, checksum) => {
                assert.strictEqual(err, null);
                assert.strictEqual(checksum.algorithm, 'crc32');
                assert.strictEqual(checksum.value, 'DUoRhQ==');
                assert.strictEqual(checksum.storageChecksum, undefined);
                done();
            });
        });
    });

    describe('batchDelete failure paths', () => {
        it('should call cb with checksum error when validateChecksum fails and batchDelete also fails', done => {
            batchDeleteStub.callsFake((keys, a, b, log2, cb) => cb(errors.InternalError));
            putSucceeds();
            const request = makeStream({
                'x-amz-content-sha256': 'UNSIGNED-PAYLOAD',
            }, 'hello world');
            const badChecksums = {
                primary: { algorithm: 'crc32', isTrailer: false, expected: 'AAAAAA==' },
                secondary: null,
            };
            dataStore({}, null, request, 0, null, {}, badChecksums, log, err => {
                assert.strictEqual(err.message, 'BadDigest');
                done();
            });
        });

        it('should call cb with BadDigest when content-md5 mismatches and batchDelete also fails', done => {
            batchDeleteStub.callsFake((keys, a, b, log2, cb) => cb(errors.InternalError));
            putSucceeds('correct-md5');
            const request = makeStream({ 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' });
            request.contentMD5 = 'wrong-md5';
            dataStore({}, null, request, 0, null, {}, defaultChecksums, log, err => {
                assert.deepStrictEqual(err, errors.BadDigest);
                done();
            });
        });

        it('should call cb with stream error when checksumedStream errors after data.put and batchDelete also fails',
            done => {
                batchDeleteStub.callsFake((keys, a, b, log2, cb) => cb(errors.BadRequest));
                let capturedStream;
                putStub.callsFake((cipher, stream, size, ctx, backend, log2, cb) => {
                    capturedStream = stream;
                    cb(null, fakeDataRetrievalInfo, { completedHash: null });
                });
                const request = makeStream({ 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' });
                dataStore({}, null, request, 0, null, {}, defaultChecksums, log, err => {
                    assert.deepStrictEqual(err, errors.InternalError);
                    done();
                });
                process.nextTick(() => capturedStream.emit('error', errors.InternalError));
            });
    });
});
