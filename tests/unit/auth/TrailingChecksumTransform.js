const { errors } = require('arsenal');
const assert = require('assert');
const async = require('async');
const { Readable } = require('stream');

const TrailingChecksumTransform = require('../../../lib/auth/streamingV4/trailingChecksumTransform');
const { DummyRequestLogger } = require('../helpers');

// Helper: pipe input chunks through TrailingChecksumTransform, collect output and trailer events
function runTransform(inputChunks) {
    const stream = new TrailingChecksumTransform(new DummyRequestLogger());
    return new Promise((resolve, reject) => {
        const output = [];
        const trailerEvents = [];
        stream.on('data', chunk => output.push(Buffer.from(chunk)));
        stream.on('trailer', (name, value) => trailerEvents.push({ name, value }));
        stream.on('finish', () => resolve({ data: Buffer.concat(output), trailers: trailerEvents, stream }));
        stream.on('error', reject);
        for (const chunk of inputChunks) {
            stream.write(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
        }
        stream.end();
    });
}

// Helper: expect a stream error from the given input chunks
function expectError(inputChunks) {
    const stream = new TrailingChecksumTransform(new DummyRequestLogger());
    return new Promise((resolve, reject) => {
        stream.on('error', resolve);
        stream.on('finish', () => reject(new Error('expected error but stream finished cleanly')));
        for (const chunk of inputChunks) {
            stream.write(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
        }
        stream.end();
        stream.resume();
    });
}

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

    it('should propagate _flush error via errCb when stream closes without chunked encoding', done => {
        const incompleteData = '10\r\n01234\r6789abcd\r\n\r\n';
        const source = new ChunkedReader([Buffer.from(incompleteData)]);
        const stream = new TrailingChecksumTransform(log);
        stream.on('error', err => {
            assert.deepStrictEqual(err, errors.IncompleteBody);
            done();
        });
        source.pipe(stream);
        stream.resume();
    });

    it('should propagate _transform error via errCb for invalid chunk size', done => {
        const badData = '500000000000\r\n';
        const source = new ChunkedReader([Buffer.from(badData)]);
        const stream = new TrailingChecksumTransform(log);
        stream.on('error', err => {
            assert.deepStrictEqual(err, errors.InvalidArgument);
            done();
        });
        source.pipe(stream);
        stream.resume();
    });

    it('should return early if supplied with an out-of-specification chunk size', done => {
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

describe('TrailingChecksumTransform trailer parsing and emitting', () => {
    describe('happy path', () => {
        it('single chunk with data and trailer: forwards data, emits trailer name and value', async () => {
            const input = '5\r\nhello\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n';
            const { data, trailers } = await runTransform([input]);
            assert.strictEqual(data.toString(), 'hello');
            assert.strictEqual(trailers.length, 1);
            assert.strictEqual(trailers[0].name, 'x-amz-checksum-crc32');
            assert.strictEqual(trailers[0].value, 'AAAAAA==');
        });

        it('multiple data chunks followed by trailer: forwards all data, emits trailer once', async () => {
            const input = '5\r\nhello\r\n5\r\nworld\r\n0\r\nx-amz-checksum-sha256:AAAAAA==\r\n';
            const { data, trailers } = await runTransform([input]);
            assert.strictEqual(data.toString(), 'helloworld');
            assert.strictEqual(trailers.length, 1);
            assert.strictEqual(trailers[0].name, 'x-amz-checksum-sha256');
        });

        it('data chunk containing \\r\\n in payload is forwarded correctly', async () => {
            // 7 bytes: h e l \r \n l o
            const input = '7\r\nhel\r\nlo\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n';
            const { data } = await runTransform([input]);
            assert.strictEqual(data.toString(), 'hel\r\nlo');
        });

        it('trailer with whitespace around name and value: name and value are trimmed', async () => {
            const input = '0\r\n x-amz-checksum-crc32 : AAAAAA== \r\n';
            const { trailers } = await runTransform([input]);
            assert.strictEqual(trailers.length, 1);
            assert.strictEqual(trailers[0].name, 'x-amz-checksum-crc32');
            assert.strictEqual(trailers[0].value, 'AAAAAA==');
        });

        it('trailer terminated with \\n\\r\\n: trailing \\n stripped from parsed line', async () => {
            const input = '0\r\nx-amz-checksum-crc32:AAAAAA==\n\r\n';
            const { trailers } = await runTransform([input]);
            assert.strictEqual(trailers.length, 1);
            assert.strictEqual(trailers[0].name, 'x-amz-checksum-crc32');
            assert.strictEqual(trailers[0].value, 'AAAAAA==');
        });

        it('trailer value containing a colon: only first colon used as separator', async () => {
            const input = '0\r\nx-amz-checksum-crc32:AA:BB==\r\n';
            const { trailers } = await runTransform([input]);
            assert.strictEqual(trailers.length, 1);
            assert.strictEqual(trailers[0].name, 'x-amz-checksum-crc32');
            assert.strictEqual(trailers[0].value, 'AA:BB==');
        });

        it('bytes after trailer \\r\\n are silently discarded', async () => {
            const input = '5\r\nhello\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\nextra bytes ignored';
            const { data, trailers } = await runTransform([input]);
            assert.strictEqual(data.toString(), 'hello');
            assert.strictEqual(trailers.length, 1);
        });
    });

    describe('chunk boundary edge cases', () => {
        it('chunk size field split across two input chunks: parsed correctly', async () => {
            // size 'a' (hex) = 10 bytes; split the size field across two chunks
            const c1 = 'a';
            const c2 = '\r\nAAAAAAAAAA\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n';
            const { data, trailers } = await runTransform([c1, c2]);
            assert.strictEqual(data.toString(), 'AAAAAAAAAA');
            assert.strictEqual(trailers.length, 1);
            assert.strictEqual(trailers[0].name, 'x-amz-checksum-crc32');
        });

        it('data bytes split across two input chunks: all forwarded', async () => {
            // 5 bytes 'hello', split after 'hel'
            const c1 = '5\r\nhel';
            const c2 = 'lo\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n';
            const { data, trailers } = await runTransform([c1, c2]);
            assert.strictEqual(data.toString(), 'hello');
            assert.strictEqual(trailers.length, 1);
        });

        it('\\r\\n delimiter after chunk size split across two input chunks: parsed correctly', async () => {
            // '5\r' in chunk1, '\nhello\r\n0\r\n...' in chunk2
            const c1 = '5\r';
            const c2 = '\nhello\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n';
            const { data, trailers } = await runTransform([c1, c2]);
            assert.strictEqual(data.toString(), 'hello');
            assert.strictEqual(trailers.length, 1);
        });

        it('trailer line split across two input chunks: emits trailer correctly', async () => {
            const c1 = '5\r\nhello\r\n0\r\nx-amz-checksum-';
            const c2 = 'crc32:AAAAAA==\r\n';
            const { data, trailers } = await runTransform([c1, c2]);
            assert.strictEqual(data.toString(), 'hello');
            assert.strictEqual(trailers.length, 1);
            assert.strictEqual(trailers[0].name, 'x-amz-checksum-crc32');
            assert.strictEqual(trailers[0].value, 'AAAAAA==');
        });

        it('trailer \\r\\n split across two input chunks: emits trailer correctly', async () => {
            const c1 = '5\r\nhello\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r';
            const c2 = '\n';
            const { data, trailers } = await runTransform([c1, c2]);
            assert.strictEqual(data.toString(), 'hello');
            assert.strictEqual(trailers.length, 1);
            assert.strictEqual(trailers[0].name, 'x-amz-checksum-crc32');
            assert.strictEqual(trailers[0].value, 'AAAAAA==');
        });
    });

    describe('zero-size terminator and trailer', () => {
        it('empty trailer line (0\\r\\n\\r\\n): no trailer event emitted, stream closes cleanly', async () => {
            const input = '5\r\nhello\r\n0\r\n\r\n';
            const { data, trailers } = await runTransform([input]);
            assert.strictEqual(data.toString(), 'hello');
            assert.strictEqual(trailers.length, 0);
        });

        it('zero data chunks (only terminator + trailer): no data forwarded, trailer emitted', async () => {
            const input = '0\r\nx-amz-checksum-crc32:AAAAAA==\r\n';
            const { data, trailers } = await runTransform([input]);
            assert.strictEqual(data.length, 0);
            assert.strictEqual(trailers.length, 1);
            assert.strictEqual(trailers[0].name, 'x-amz-checksum-crc32');
            assert.strictEqual(trailers[0].value, 'AAAAAA==');
        });
    });

    describe('_flush error cases', () => {
        it('stream ends mid-data (no zero-chunk): IncompleteBody error', async () => {
            // 5 bytes declared but stream ends after only 3
            const err = await expectError(['5\r\nhel']);
            assert.deepStrictEqual(err, errors.IncompleteBody);
        });

        it('stream ends after zero-chunk with partial trailer content: IncompleteBody error', async () => {
            // zero-chunk received, trailer starts but no \r\n terminator
            const err = await expectError(['0\r\nx-amz-checksum-crc32:AAAAAA==']);
            assert.deepStrictEqual(err, errors.IncompleteBody);
        });

        it('stream ends after zero-chunk with no trailer content: no error', async () => {
            // only '0\r\n' — readingTrailer=true, trailerBuffer empty → no error
            const { data, trailers } = await runTransform(['0\r\n']);
            assert.strictEqual(data.length, 0);
            assert.strictEqual(trailers.length, 0);
        });
    });

    describe('_transform error cases', () => {
        it('chunk size field larger than 10 bytes: InvalidArgument error', async () => {
            // 11 hex digits — exceeds the 10-byte field size limit
            const err = await expectError(['12345678901\r\n']);
            assert.deepStrictEqual(err, errors.InvalidArgument);
        });

        it('chunk size is not valid hex: InvalidArgument error', async () => {
            // 2 chars, short enough to pass size check, but not valid hex
            const err = await expectError(['zz\r\n']);
            assert.deepStrictEqual(err, errors.InvalidArgument);
        });

        it('chunk size exceeds maximumAllowedPartSize: EntityTooLarge error', async () => {
            // 0x200000000 = 8589934592 > maximumAllowedPartSize (5GB = 0x140000000)
            const err = await expectError(['200000000\r\n']);
            assert.deepStrictEqual(err, errors.EntityTooLarge);
        });

        it('trailer line longer than 1024 bytes: MalformedTrailerError', async () => {
            // send zero-chunk then a trailer line > 1024 bytes with no \r\n
            const longTrailer = 'x'.repeat(1025);
            const err = await expectError([`0\r\n${longTrailer}`]);
            assert.deepStrictEqual(err, errors.MalformedTrailerError);
        });

        it('trailer line missing colon: IncompleteBody error', async () => {
            const err = await expectError(['0\r\nnocolon\r\n']);
            assert.deepStrictEqual(err, errors.IncompleteBody);
        });

        it('trailer line with colon at position 0 (empty name): IncompleteBody error', async () => {
            const err = await expectError(['0\r\n:value\r\n']);
            assert.deepStrictEqual(err, errors.IncompleteBody);
        });
    });
});
