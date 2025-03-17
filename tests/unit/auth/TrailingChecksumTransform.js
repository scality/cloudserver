const { errors } = require('arsenal');
const assert = require('assert');
const async = require('async');
const { Readable } = require('stream');

const TrailingChecksumTransform = require('../../../lib/auth/streamingV4/trailingChecksumTransform');
const { DummyRequestLogger } = require('../helpers');

const log = new DummyRequestLogger();

// note this is not the correct checksum in objDataWithTrailingChecksum
const objDataWithTrailingChecksum = '10\r\n01234\r6789abcd\r\n\r\n' +
                                    '2\r\n01\r\n' +
                                    '1\r\n2\r\n' +
                                    'd\r\n3456789abcdef\r\n' +
                                    '0\r\nchecksum:xyz=\r\n';
const objDataWithoutTrailingChecksum = '01234\r6789abcd\r\n0123456789abcdef';

class ChunkedReader extends Readable {
    constructor(chunks) {
        super();
        this._parts = chunks;
        this._index = 0;
    }

    _read() {
        if (this._index >= this._parts.length) {
            this.push(null);
            return;
        }
        this.push(this._parts[this._index]);
        this._index++;
    }

    getIndex() {
        return this._index;
    }
}

describe('TrailingChecksumTransform class', () => {
    it('should correctly remove checksums', done => {
        const trailingChecksumTransform = new TrailingChecksumTransform(log);
        trailingChecksumTransform.on('error', err => {
            assert.strictEqual(err, null);
        });
        const chunks = [
            Buffer.from(objDataWithTrailingChecksum),
        ];
        const chunkedReader = new ChunkedReader(chunks);
        chunkedReader.pipe(trailingChecksumTransform);
        const outputChunks = [];
        trailingChecksumTransform.on('data', chunk => outputChunks.push(Buffer.from(chunk)));
        trailingChecksumTransform.on('finish', () => {
            const data = Buffer.concat(outputChunks).toString();
            assert.strictEqual(data, objDataWithoutTrailingChecksum);
            done();
        });
        trailingChecksumTransform.on('error', err => {
            assert.ifError(err);
        });
    });

    // test all bisection of the input string
    async.forEach([...Array(objDataWithTrailingChecksum.length).keys()], i => {
        it(`should correctly remove checksums, cut at ${i}`, done => {
            const trailingChecksumTransform = new TrailingChecksumTransform(log);
            trailingChecksumTransform.on('error', err => {
                assert.strictEqual(err, null);
            });
            const chunks = [
                Buffer.from(objDataWithTrailingChecksum.substring(0, i)),
                Buffer.from(objDataWithTrailingChecksum.substring(i)),
            ];
            const chunkedReader = new ChunkedReader(chunks);
            chunkedReader.pipe(trailingChecksumTransform);
            const outputChunks = [];
            trailingChecksumTransform.on('data', chunk => outputChunks.push(Buffer.from(chunk)));
            trailingChecksumTransform.on('finish', () => {
                const data = Buffer.concat(outputChunks).toString();
                assert.strictEqual(data, objDataWithoutTrailingChecksum);
                done();
            });
            trailingChecksumTransform.on('error', err => {
                assert.ifError(err);
            });
        });
    });

    // test all trisection of the input string
    async.forEach([...Array(objDataWithTrailingChecksum.length - 2).keys()], i => {
        async.forEach([...Array(objDataWithTrailingChecksum.length - i - 2).keys()], j => {
            it(`should correctly remove checksums, cut at ${i} and ${i + j + 1}`, done => {
                const trailingChecksumTransform = new TrailingChecksumTransform(log);
                trailingChecksumTransform.on('error', err => {
                    assert.strictEqual(err, null);
                });
                const chunks = [
                    Buffer.from(objDataWithTrailingChecksum.substring(0, i + 1)),
                    Buffer.from(objDataWithTrailingChecksum.substring(i + 1, i + j + 2)),
                    Buffer.from(objDataWithTrailingChecksum.substring(i + j + 2)),
                ];
                const chunkedReader = new ChunkedReader(chunks);
                chunkedReader.pipe(trailingChecksumTransform);
                const outputChunks = [];
                trailingChecksumTransform.on('data', chunk => outputChunks.push(Buffer.from(chunk)));
                trailingChecksumTransform.on('finish', () => {
                    const data = Buffer.concat(outputChunks).toString();
                    assert.strictEqual(data, objDataWithoutTrailingChecksum);
                    done();
                });
                trailingChecksumTransform.on('error', err => {
                    assert.ifError(err);
                });
            });
        });
    });

    it('should correctly remove checksums, cut at each individual byte', done => {
        const trailingChecksumTransform = new TrailingChecksumTransform(log);
        trailingChecksumTransform.on('error', err => {
            assert.strictEqual(err, null);
        });
        const chunks = [];
        for (let i = 0; i < objDataWithTrailingChecksum.length; i++) {
            chunks.push(objDataWithTrailingChecksum.substring(i, i + 1));
        }
        const chunkedReader = new ChunkedReader(chunks);
        chunkedReader.pipe(trailingChecksumTransform);
        const outputChunks = [];
        trailingChecksumTransform.on('data', chunk => outputChunks.push(Buffer.from(chunk)));
        trailingChecksumTransform.on('finish', () => {
            const data = Buffer.concat(outputChunks).toString();
            assert.strictEqual(data, objDataWithoutTrailingChecksum);
            done();
        });
        trailingChecksumTransform.on('error', err => {
            assert.ifError(err);
        });
    });

    it('should return an error if the format does not follow trailing checksum specification', done => {
        const trailingChecksumTransform = new TrailingChecksumTransform(log);
        const chunks = [
            Buffer.from('11\r\n'),
            Buffer.alloc(1000000),
            Buffer.alloc(1000000),
            Buffer.alloc(1000000),
            Buffer.alloc(1000000),
            Buffer.alloc(1000000),
            Buffer.alloc(1000000),
            Buffer.alloc(1000000),
            Buffer.alloc(1000000),
        ];
        const chunkedReader = new ChunkedReader(chunks);
        trailingChecksumTransform.on('error', err => {
            assert.deepStrictEqual(err, errors.InvalidArgument);
            trailingChecksumTransform.end();
        });
        let bytesWritten = 0;
        trailingChecksumTransform.on('data', chunk => {
            bytesWritten += chunk.length;
        });
        trailingChecksumTransform.on('close', () => {
            assert.equal(bytesWritten, 17);
            // 2 is the minimum but it looks like buffering will fetch additional chunks before the error is emitted
            // as long as we do not read the full input stream, this is fine
            assert.ok(chunkedReader.getIndex() <= 4);
            done();
        });
        chunkedReader.pipe(trailingChecksumTransform);
    });

    it('should return early if supplied with an out of specification chunk size', done => {
        const trailingChecksumTransform = new TrailingChecksumTransform(log);
        const chunks = [
            Buffer.from('500000'),
            Buffer.from('000000\r\n'),
            Buffer.alloc(1000000),
            Buffer.alloc(1000000),
            Buffer.alloc(1000000),
            Buffer.alloc(1000000),
            Buffer.alloc(1000000),
            Buffer.alloc(1000000),
            Buffer.alloc(1000000),
            Buffer.alloc(1000000),
            Buffer.alloc(1000000),
        ];
        const chunkedReader = new ChunkedReader(chunks);
        trailingChecksumTransform.on('error', err => {
            assert.deepStrictEqual(err, errors.InvalidArgument);
            trailingChecksumTransform.end();
        });
        let bytesWritten = 0;
        trailingChecksumTransform.on('data', chunk => {
            bytesWritten += chunk.length;
        });
        trailingChecksumTransform.on('close', () => {
            assert.equal(bytesWritten, 0);
            // 2 is the minimum but it looks like buffering will fetch additional chunks before the error is emitted
            // as long as we do not read the full input stream, this is fine
            assert.ok(chunkedReader.getIndex() <= 4);
            done();
        });
        chunkedReader.pipe(trailingChecksumTransform);
    });
});
