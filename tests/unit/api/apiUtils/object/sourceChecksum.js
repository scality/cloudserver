const assert = require('assert');
const sinon = require('sinon');
const { Readable } = require('stream');

const {
    buildSourcePartsStream,
    computeChecksumFromDataLocator,
} = require('../../../../../lib/api/apiUtils/object/sourceChecksum');
const dataWrapper = require('../../../../../lib/data/wrapper');
const { algorithms } = require('../../../../../lib/api/apiUtils/integrity/validateChecksums');
const { DummyRequestLogger } = require('../../../helpers');

const log = new DummyRequestLogger();

// Build a source-part descriptor carrying its bytes (via `value`) for the
// stubbed data.get below to serve. `getError` makes data.get fail on that part.
function part(key, value, opts = {}) {
    return {
        key,
        value,
        dataStoreName: 'mem',
        dataStoreType: 'mem',
        ...opts,
    };
}

// Drain a readable and hand back the fully concatenated bytes (or the error).
function collect(stream, cb) {
    const chunks = [];
    stream.on('data', chunk => chunks.push(chunk));
    stream.once('error', err => cb(err));
    stream.once('end', () => cb(null, Buffer.concat(chunks)));
}

// Promisified computeChecksumFromDataLocator for the async digest assertions.
function computeChecksum(dataLocator, algorithm) {
    return new Promise((resolve, reject) => {
        computeChecksumFromDataLocator(dataLocator, algorithm, log, (err, res) =>
            (err ? reject(err) : resolve(res)));
    });
}

describe('sourceChecksum util', () => {
    beforeEach(() => {
        // Emulate the two data.get shapes buildSourcePartsStream relies on:
        // azure writes part bytes into the provided writable; every other
        // backend returns a Readable through the callback.
        sinon.stub(dataWrapper.data, 'get').callsFake((p, writable, log2, cb) => {
            if (p.getError) {
                return cb(p.getError);
            }
            const bytes = Buffer.from(p.value || '');
            if (p.dataStoreType === 'azure') {
                process.nextTick(() => writable.end(bytes));
                return cb(null);
            }
            const rs = new Readable({ read() {} });
            // Push after the caller has wired up listeners + pipe in its callback.
            process.nextTick(() => {
                if (bytes.length) {
                    rs.push(bytes);
                }
                rs.push(null);
            });
            return cb(null, rs);
        });
    });

    afterEach(() => sinon.restore());

    describe('buildSourcePartsStream', () => {
        it('should concatenate parts in order', done => {
            const locator = [part('a', 'Hello, '), part('b', 'world'), part('c', '!')];
            collect(buildSourcePartsStream(locator, log), (err, buf) => {
                assert.ifError(err);
                assert.strictEqual(buf.toString(), 'Hello, world!');
                done();
            });
        });

        it('should serve azure parts (data.get writes into the provided writable)', done => {
            const locator = [part('a', 'azure-bytes', { dataStoreType: 'azure', dataStoreName: 'azurebackend' })];
            collect(buildSourcePartsStream(locator, log), (err, buf) => {
                assert.ifError(err);
                assert.strictEqual(buf.toString(), 'azure-bytes');
                done();
            });
        });

        it('should concatenate mixed azure and regular parts in order', done => {
            const locator = [
                part('a', 'one-'),
                part('b', 'two-', { dataStoreType: 'azure', dataStoreName: 'azurebackend' }),
                part('c', 'three'),
            ];
            collect(buildSourcePartsStream(locator, log), (err, buf) => {
                assert.ifError(err);
                assert.strictEqual(buf.toString(), 'one-two-three');
                done();
            });
        });

        it('should emit an empty stream for an empty dataLocator', done => {
            collect(buildSourcePartsStream([], log), (err, buf) => {
                assert.ifError(err);
                assert.strictEqual(buf.length, 0);
                done();
            });
        });

        it('should propagate a mid-stream read error wrapped with copyPart metadata', done => {
            const boom = new Error('read failed');
            const locator = [
                part('ok', 'good'),
                part('bad', '', { dataStoreName: 'ring0', dataStoreType: 'scality', getError: boom }),
            ];
            const stream = buildSourcePartsStream(locator, log);
            stream.on('data', () => {});
            stream.once('error', err => {
                assert.strictEqual(err, boom);
                assert.deepStrictEqual(err.copyPart, {
                    key: 'bad',
                    dataStoreName: 'ring0',
                    dataStoreType: 'scality',
                });
                done();
            });
        });
    });

    describe('computeChecksumFromDataLocator', () => {
        const locator = [part('a', 'Hello, '), part('b', 'world'), part('c', '!')];
        const fullBytes = Buffer.from('Hello, world!');

        // Derived from the algorithms map so a newly added algorithm is covered
        // automatically.
        Object.keys(algorithms).forEach(algo => {
            it(`should compute the ${algo} digest over the concatenated source bytes`, async () => {
                const expected = await algorithms[algo].digest(fullBytes);
                const result = await computeChecksum(locator, algo);
                assert.strictEqual(result.algorithm, algo);
                assert.strictEqual(result.value, expected);
            });

            it(`should compute the empty-input ${algo} digest for an empty dataLocator`, async () => {
                const expected = await algorithms[algo].digest(Buffer.alloc(0));
                const result = await computeChecksum([], algo);
                assert.strictEqual(result.value, expected);
            });
        });

        it('should surface a read error wrapped with copyPart metadata', done => {
            const boom = new Error('read failed');
            const locator2 = [part('bad', '', { dataStoreName: 'ring0', dataStoreType: 'scality', getError: boom })];
            computeChecksumFromDataLocator(locator2, 'crc32', log, err => {
                assert.strictEqual(err, boom);
                assert.deepStrictEqual(err.copyPart, {
                    key: 'bad',
                    dataStoreName: 'ring0',
                    dataStoreType: 'scality',
                });
                done();
            });
        });
    });
});
