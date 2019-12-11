const http = require('http');
const async = require('async');
const assert = require('assert');

const HttpRequestAuthV4 = require('../utils/HttpRequestAuthV4');
const config = require('../../config.json');

const DUMMY_SIGNATURE =
      'baadc0debaadc0debaadc0debaadc0debaadc0debaadc0debaadc0debaadc0de';

http.globalAgent.keepAlive = true;

const PORT = 8000;
const BUCKET = 'bad-chunk-signature-v4';

const N_PUTS = 100;
const N_DATA_CHUNKS = 20;
const DATA_CHUNK_SIZE = 128 * 1024;
const ALTER_CHUNK_SIGNATURE = true;

const CHUNK_DATA = Buffer.alloc(DATA_CHUNK_SIZE).fill('0').toString();

function createBucket(cb) {
    const req = new HttpRequestAuthV4(
        `http://${config.ipAddress}:${PORT}/${BUCKET}`, {
            accessKey: config.accessKey,
            secretKey: config.secretKey,
            method: 'PUT',
            headers: {
                connection: 'keep-alive',
            },
        }, res => {
            assert.strictEqual(res.statusCode, 200);
            res.on('data', () => {});
            res.on('end', cb);
        });

    req.on('error', err => {
        assert.ifError(err);
    });
    req.end();
}

function deleteBucket(cb) {
    const req = new HttpRequestAuthV4(
        `http://${config.ipAddress}:${PORT}/${BUCKET}`, {
            accessKey: config.accessKey,
            secretKey: config.secretKey,
            method: 'DELETE',
            headers: {
                connection: 'keep-alive',
            },
        }, res => {
            assert.strictEqual(res.statusCode, 204);
            res.on('data', () => {});
            res.on('end', cb);
        });

    req.on('error', err => {
        assert.ifError(err);
    });
    req.end();
}

class HttpChunkedUploadWithBadSignature extends HttpRequestAuthV4 {
    constructor(url, params, callback) {
        super(url, params, callback);
        this._chunkId = 0;
        this._alterSignatureChunkId = params.alterSignatureChunkId;
    }

    getChunkSignature(chunkData) {
        let signature;
        if (this._chunkId === this._alterSignatureChunkId) {
            // console.log(
            //     `ALTERING SIGNATURE OF DATA CHUNK #${this._chunkId}`);
            signature = DUMMY_SIGNATURE;
        } else {
            signature = super.getChunkSignature(chunkData);
        }
        ++this._chunkId;
        return signature;
    }
}

function testChunkedPutWithBadSignature(n, alterSignatureChunkId, cb) {
    const req = new HttpChunkedUploadWithBadSignature(
        `http://${config.ipAddress}:${PORT}/${BUCKET}/obj-${n}`, {
            accessKey: config.accessKey,
            secretKey: config.secretKey,
            method: 'PUT',
            headers: {
                'content-length': N_DATA_CHUNKS * DATA_CHUNK_SIZE,
                'connection': 'keep-alive',
            },
            alterSignatureChunkId,
        }, res => {
            if (alterSignatureChunkId >= 0 &&
                alterSignatureChunkId <= N_DATA_CHUNKS) {
                assert.strictEqual(res.statusCode, 403);
            } else {
                assert.strictEqual(res.statusCode, 200);
            }
            res.on('data', () => {});
            res.on('end', cb);
        });

    req.on('error', err => {
        assert.ifError(err);
    });
    async.timesSeries(N_DATA_CHUNKS, (chunkIndex, done) => {
        // console.log(`SENDING NEXT CHUNK OF LENGTH ${CHUNK_DATA.length}`);
        if (req.write(CHUNK_DATA)) {
            process.nextTick(done);
        } else {
            req.once('drain', done);
        }
    }, () => {
        req.end();
    });
}

describe('streaming V4 signature with bad chunk signature', () => {
    before(done => createBucket(done));
    after(done => deleteBucket(done));
    it('Cloudserver should be robust against bad signature in streaming ' +
    'payload', function badSignatureInStreamingPayload(cb) {
        this.timeout(120000);
        async.timesLimit(N_PUTS, 10, (n, done) => {
            // multiple test cases depend on the value of
            // alterSignatureChunkId:
            // alterSignatureChunkId >= 0 &&
            // alterSignatureChunkId < N_DATA_CHUNKS
            //    <=> alter the signature of the target data chunk
            // alterSignatureChunkId == N_DATA_CHUNKS
            //    <=> alter the signature of the last empty chunk that
            //        carries the last payload signature
            // alterSignatureChunkId > N_DATA_CHUNKS
            //    <=> no signature is altered (regular test case)
            // By making n go from 0 to nDatachunks+1, we cover all
            // above cases.

            const alterSignatureChunkId = ALTER_CHUNK_SIGNATURE ?
                  (n % (N_DATA_CHUNKS + 2)) : null;
            testChunkedPutWithBadSignature(n, alterSignatureChunkId, done);
        }, err => cb(err));
    });
});
