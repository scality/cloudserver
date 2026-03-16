const assert = require('assert');
const { errors } = require('arsenal');

const { algorithms, ChecksumError } = require('../../../lib/api/apiUtils/integrity/validateChecksums');
const ChecksumTransform = require('../../../lib/auth/streamingV4/ChecksumTransform');
const { DummyRequestLogger } = require('../helpers');

const log = new DummyRequestLogger();
const testData = Buffer.from('hello world');
const algos = ['crc32', 'crc32c', 'sha1', 'sha256', 'crc64nvme'];

// Helper: pipe chunks into a ChecksumTransform, collect output, resolve on finish
function runTransform(stream, chunks) {
    return new Promise((resolve, reject) => {
        const output = [];
        stream.on('data', chunk => output.push(chunk));
        stream.on('end', () => resolve(Buffer.concat(output)));
        stream.on('error', reject);
        for (const chunk of chunks) {
            stream.write(chunk);
        }
        stream.end();
    });
}

// Helper: pipe data through and wait for finish without collecting output
function drainTransform(stream, chunks) {
    return new Promise((resolve, reject) => {
        stream.resume();
        stream.on('finish', resolve);
        stream.on('error', reject);
        for (const chunk of chunks) {
            stream.write(chunk);
        }
        stream.end();
    });
}

describe('ChecksumTransform', () => {
    describe('basic behaviour', () => {
        let expectedDigests;

        before(async () => {
            expectedDigests = {};
            for (const algo of algos) {
                expectedDigests[algo] = await Promise.resolve(algorithms[algo].digest(testData));
            }
        });

        for (const algo of algos) {
            it(`should pass data through unchanged [${algo}]`, async () => {
                const stream = new ChecksumTransform(algo, undefined, false, log);
                const output = await runTransform(stream, [testData]);
                assert.deepStrictEqual(output, testData);
            });

            it(`should compute digest correctly after stream ends [${algo}]`, async () => {
                const stream = new ChecksumTransform(algo, undefined, false, log);
                await drainTransform(stream, [testData]);
                assert.strictEqual(stream.digest, expectedDigests[algo]);
            });

            it(`should handle multi-chunk input: digest matches single-chunk equivalent [${algo}]`, async () => {
                const half = Math.floor(testData.length / 2);
                const stream = new ChecksumTransform(algo, undefined, false, log);
                await drainTransform(stream, [testData.subarray(0, half), testData.subarray(half)]);
                assert.strictEqual(stream.digest, expectedDigests[algo]);
            });

            it(`should handle Buffer and string chunks equally [${algo}]`, async () => {
                const streamBuf = new ChecksumTransform(algo, undefined, false, log);
                const streamStr = new ChecksumTransform(algo, undefined, false, log);
                await drainTransform(streamBuf, [testData]);
                await drainTransform(streamStr, [testData.toString()]);
                assert.strictEqual(streamBuf.digest, streamStr.digest);
            });
        }

        it('should emit error via stream error event if digestFromHash fails', done => {
            const stream = new ChecksumTransform('crc32', undefined, false, log);
            // Replace digestFromHash to return a rejected Promise
            stream.algo = Object.assign({}, stream.algo, {
                digestFromHash: () => Promise.reject(new Error('simulated digest failure')),
            });
            stream.on('error', err => {
                assert.deepStrictEqual(err, errors.InternalError);
                done();
            });
            stream.write(testData);
            stream.end();
            stream.resume();
        });
    });

    describe('validateChecksum — non-trailer mode (isTrailer=false)', () => {
        let crc32Digest;

        before(async () => {
            crc32Digest = await Promise.resolve(algorithms.crc32.digest(testData));
        });

        it('should return null when no expectedDigest and no trailer received', async () => {
            const stream = new ChecksumTransform('crc32', undefined, false, log);
            await drainTransform(stream, [testData]);
            assert.strictEqual(stream.validateChecksum(), null);
        });

        it('should return null when expectedDigest matches computed digest', async () => {
            const stream = new ChecksumTransform('crc32', crc32Digest, false, log);
            await drainTransform(stream, [testData]);
            assert.strictEqual(stream.validateChecksum(), null);
        });

        it('should return XAmzMismatch when expectedDigest does not match computed digest', async () => {
            const stream = new ChecksumTransform('crc32', 'AAAAAA==', false, log);
            await drainTransform(stream, [testData]);
            const result = stream.validateChecksum();
            assert.strictEqual(result.error, ChecksumError.XAmzMismatch);
            assert.strictEqual(result.details.algorithm, 'crc32');
            assert.strictEqual(result.details.calculated, crc32Digest);
            assert.strictEqual(result.details.expected, 'AAAAAA==');
        });

        it('should return TrailerUnexpected when setExpectedChecksum was called but isTrailer=false', async () => {
            const stream = new ChecksumTransform('crc32', undefined, false, log);
            stream.setExpectedChecksum('x-amz-checksum-crc32', crc32Digest);
            await drainTransform(stream, [testData]);
            const result = stream.validateChecksum();
            assert.strictEqual(result.error, ChecksumError.TrailerUnexpected);
        });
    });

    describe('validateChecksum — trailer mode (isTrailer=true)', () => {
        let crc32Digest;

        before(async () => {
            crc32Digest = await Promise.resolve(algorithms.crc32.digest(testData));
        });

        it('should return TrailerMissing when setExpectedChecksum was never called', async () => {
            const stream = new ChecksumTransform('crc32', undefined, true, log);
            await drainTransform(stream, [testData]);
            const result = stream.validateChecksum();
            assert.strictEqual(result.error, ChecksumError.TrailerMissing);
            assert.strictEqual(result.details.expectedTrailer, 'x-amz-checksum-crc32');
        });

        it('should return TrailerAlgoMismatch when trailer name does not match algo', async () => {
            const stream = new ChecksumTransform('crc32', undefined, true, log);
            stream.setExpectedChecksum('x-amz-checksum-sha256', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=');
            await drainTransform(stream, [testData]);
            const result = stream.validateChecksum();
            assert.strictEqual(result.error, ChecksumError.TrailerAlgoMismatch);
            assert.strictEqual(result.details.algorithm, 'crc32');
        });

        it('should return TrailerChecksumMalformed when trailer value is not a valid digest for the algo',
            async () => {
                const stream = new ChecksumTransform('crc32', undefined, true, log);
                stream.setExpectedChecksum('x-amz-checksum-crc32', 'not-valid!');
                await drainTransform(stream, [testData]);
                const result = stream.validateChecksum();
                assert.strictEqual(result.error, ChecksumError.TrailerChecksumMalformed);
                assert.strictEqual(result.details.algorithm, 'crc32');
            });

        it('should return XAmzMismatch when trailer value is valid but does not match computed digest', async () => {
            const stream = new ChecksumTransform('crc32', undefined, true, log);
            stream.setExpectedChecksum('x-amz-checksum-crc32', 'AAAAAA==');
            await drainTransform(stream, [testData]);
            const result = stream.validateChecksum();
            assert.strictEqual(result.error, ChecksumError.XAmzMismatch);
            assert.strictEqual(result.details.algorithm, 'crc32');
            assert.strictEqual(result.details.calculated, crc32Digest);
            assert.strictEqual(result.details.expected, 'AAAAAA==');
        });

        it('should return null when trailer name and value match computed digest', async () => {
            const stream = new ChecksumTransform('crc32', undefined, true, log);
            stream.setExpectedChecksum('x-amz-checksum-crc32', crc32Digest);
            await drainTransform(stream, [testData]);
            assert.strictEqual(stream.validateChecksum(), null);
        });
    });
});
