const assert = require('assert');
const { errors } = require('arsenal');
const { Readable } = require('stream');

const { algorithms } = require('../../../lib/api/apiUtils/integrity/validateChecksums');
const ChecksumWritable = require('../../../lib/auth/streamingV4/ChecksumWritable');
const { DummyRequestLogger } = require('../helpers');

const log = new DummyRequestLogger();
const testData = Buffer.from('hello world');
const algos = Object.keys(algorithms);

// Helper: write chunks into the sink and resolve on finish (digest is ready by then)
function drainWritable(sink, chunks) {
    return new Promise((resolve, reject) => {
        sink.on('finish', resolve);
        sink.on('error', reject);
        for (const chunk of chunks) {
            sink.write(chunk);
        }
        sink.end();
    });
}

describe('ChecksumWritable', () => {
    let expectedDigests;

    before(async () => {
        expectedDigests = {};
        for (const algo of algos) {
            expectedDigests[algo] = await Promise.resolve(algorithms[algo].digest(testData));
        }
    });

    for (const algo of algos) {
        it(`should compute digest correctly after finish [${algo}]`, async () => {
            const sink = new ChecksumWritable(algo, log);
            await drainWritable(sink, [testData]);
            assert.strictEqual(sink.digest, expectedDigests[algo]);
        });

        it(`should leave digest undefined until finish [${algo}]`, () => {
            const sink = new ChecksumWritable(algo, log);
            assert.strictEqual(sink.digest, undefined);
            sink.write(testData);
            assert.strictEqual(sink.digest, undefined);
        });

        it(`should handle multi-chunk input: digest matches single-chunk equivalent [${algo}]`, async () => {
            const half = Math.floor(testData.length / 2);
            const sink = new ChecksumWritable(algo, log);
            await drainWritable(sink, [testData.subarray(0, half), testData.subarray(half)]);
            assert.strictEqual(sink.digest, expectedDigests[algo]);
        });

        it(`should handle Buffer and string chunks equally [${algo}]`, async () => {
            const sinkBuf = new ChecksumWritable(algo, log);
            const sinkStr = new ChecksumWritable(algo, log);
            await drainWritable(sinkBuf, [testData]);
            await drainWritable(sinkStr, [testData.toString()]);
            assert.strictEqual(sinkBuf.digest, sinkStr.digest);
        });

        it(`should compute the digest when piped from a Readable [${algo}]`, done => {
            const sink = new ChecksumWritable(algo, log);
            sink.once('error', done);
            sink.once('finish', () => {
                assert.strictEqual(sink.digest, expectedDigests[algo]);
                done();
            });
            Readable.from([testData]).pipe(sink);
        });
    }

    it('should hash an empty body to the zero-length digest', async () => {
        const algo = 'crc32';
        const expected = await Promise.resolve(algorithms[algo].digest(Buffer.alloc(0)));
        const sink = new ChecksumWritable(algo, log);
        await drainWritable(sink, []);
        assert.strictEqual(sink.digest, expected);
    });

    it('should emit error via stream error event if digestFromHash fails', done => {
        const sink = new ChecksumWritable('crc32', log);
        // Replace digestFromHash to return a rejected Promise
        sink.algo = Object.assign({}, sink.algo, {
            digestFromHash: () => Promise.reject(new Error('simulated digest failure')),
        });
        sink.on('error', err => {
            assert.deepStrictEqual(err, errors.InternalError);
            done();
        });
        sink.write(testData);
        sink.end();
    });
});
