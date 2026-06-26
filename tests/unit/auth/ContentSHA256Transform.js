const assert = require('assert');
const crypto = require('crypto');

const { ChecksumError } = require('../../../lib/api/apiUtils/integrity/validateChecksums');
const ContentSHA256Transform = require('../../../lib/auth/streamingV4/ContentSHA256Transform');
const { DummyRequestLogger } = require('../helpers');

const log = new DummyRequestLogger();
const testData = Buffer.from('hello world');
const testDigest = crypto.createHash('sha256').update(testData).digest('hex');
const emptyDigest = crypto.createHash('sha256').update(Buffer.alloc(0)).digest('hex');
const wrongDigest = 'a'.repeat(64);

// Pipe chunks through the transform, collect output, resolve on end.
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

// Drain the transform without collecting output, resolve on finish.
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

describe('ContentSHA256Transform', () => {
    describe('pass-through and digest', () => {
        it('should pass data through unchanged', async () => {
            const stream = new ContentSHA256Transform(testDigest, log);
            const output = await runTransform(stream, [testData]);
            assert.deepStrictEqual(output, testData);
        });

        it('should compute the sha256 hex digest after the stream ends', async () => {
            const stream = new ContentSHA256Transform(testDigest, log);
            await drainTransform(stream, [testData]);
            assert.strictEqual(stream.digest, testDigest);
        });

        it('should compute the same digest for multi-chunk input', async () => {
            const half = Math.floor(testData.length / 2);
            const stream = new ContentSHA256Transform(testDigest, log);
            await drainTransform(stream, [testData.subarray(0, half), testData.subarray(half)]);
            assert.strictEqual(stream.digest, testDigest);
        });

        it('should handle Buffer and string chunks equally', async () => {
            const streamBuf = new ContentSHA256Transform(testDigest, log);
            const streamStr = new ContentSHA256Transform(testDigest, log);
            await drainTransform(streamBuf, [testData]);
            await drainTransform(streamStr, [testData.toString()]);
            assert.strictEqual(streamBuf.digest, streamStr.digest);
        });

        it('should compute the sha256 of an empty body', async () => {
            const stream = new ContentSHA256Transform(emptyDigest, log);
            await drainTransform(stream, []);
            assert.strictEqual(stream.digest, emptyDigest);
        });
    });

    describe('validateChecksum', () => {
        it('should return null when the expected digest matches the body', async () => {
            const stream = new ContentSHA256Transform(testDigest, log);
            await drainTransform(stream, [testData]);
            assert.strictEqual(stream.validateChecksum(), null);
        });

        it('should normalize an uppercase expected digest before comparing', async () => {
            const stream = new ContentSHA256Transform(testDigest.toUpperCase(), log);
            await drainTransform(stream, [testData]);
            assert.strictEqual(stream.validateChecksum(), null);
        });

        it('should return ContentSHA256Mismatch with calculated/expected details on mismatch', async () => {
            const stream = new ContentSHA256Transform(wrongDigest, log);
            await drainTransform(stream, [testData]);
            const result = stream.validateChecksum();
            assert.strictEqual(result.error, ChecksumError.ContentSHA256Mismatch);
            assert.strictEqual(result.details.calculated, testDigest);
            assert.strictEqual(result.details.expected, wrongDigest);
        });

        it('should return ContentSHA256Mismatch for an empty body when a non-empty digest is expected', async () => {
            const stream = new ContentSHA256Transform(testDigest, log);
            await drainTransform(stream, []);
            const result = stream.validateChecksum();
            assert.strictEqual(result.error, ChecksumError.ContentSHA256Mismatch);
            assert.strictEqual(result.details.calculated, emptyDigest);
        });
    });
});
