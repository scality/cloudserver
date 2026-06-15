const assert = require('assert');
const async = require('async');
const crypto = require('crypto');
const { storage } = require('arsenal');
const { parseString } = require('xml2js');

const { bucketPut } = require('../../../lib/api/bucketPut');
const initiateMultipartUpload = require('../../../lib/api/initiateMultipartUpload');
const objectPutPart = require('../../../lib/api/objectPutPart');
const constants = require('../../../constants');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const DummyRequest = require('../DummyRequest');
const { algorithms } = require('../../../lib/api/apiUtils/integrity/validateChecksums');

const { metadata } = storage.metadata.inMemory.metadata;

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'checksum-test-bucket';
const objectKey = 'testObject';
const mpuBucket = `${constants.mpuBucketPrefix}${bucketName}`;
const partBody = Buffer.from('I am a part body for checksum testing', 'utf8');

const bucketPutRequest = {
    bucketName,
    namespace,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
    actionImplicitDenies: false,
};

function makeInitiateRequest(extraHeaders = {}) {
    return {
        socket: { remoteAddress: '1.1.1.1' },
        bucketName,
        namespace,
        objectKey,
        headers: {
            host: `${bucketName}.s3.amazonaws.com`,
            ...extraHeaders,
        },
        url: `/${objectKey}?uploads`,
        actionImplicitDenies: false,
    };
}

function makePutPartRequest(uploadId, partNumber, body, extraHeaders = {}) {
    const md5Hash = crypto.createHash('md5').update(body);
    return new DummyRequest({
        bucketName,
        namespace,
        objectKey,
        headers: {
            host: `${bucketName}.s3.amazonaws.com`,
            ...extraHeaders,
        },
        url: `/${objectKey}?partNumber=${partNumber}&uploadId=${uploadId}`,
        query: { partNumber, uploadId },
        partHash: md5Hash.digest('hex'),
        actionImplicitDenies: false,
    }, body);
}

function initiateMPU(initiateHeaders, cb) {
    async.waterfall([
        next => bucketPut(authInfo, bucketPutRequest, log, next),
        (corsHeaders, next) => {
            const req = makeInitiateRequest(initiateHeaders);
            initiateMultipartUpload(authInfo, req, log, next);
        },
        (result, corsHeaders, next) => parseString(result, next),
    ], (err, json) => {
        if (err) {return cb(err);}
        return cb(null, json.InitiateMultipartUploadResult.UploadId[0]);
    });
}

function getPartMetadata(uploadId) {
    const mpuKeys = metadata.keyMaps.get(mpuBucket);
    if (!mpuKeys) {return null;}
    for (const [key, val] of mpuKeys) {
        if (key.startsWith(uploadId) && !key.startsWith('overview')) {
            return val;
        }
    }
    return null;
}

describe('objectPutPart checksum validation', () => {
    beforeEach(() => cleanup());

    describe('algo match validation', () => {
        it('should accept part with matching checksum algo', done => {
            initiateMPU({ 'x-amz-checksum-algorithm': 'crc32' }, (err, uploadId) => {
                assert.ifError(err);
                const request = makePutPartRequest(uploadId, 1, partBody, {
                    'x-amz-checksum-crc32': 'AAAAAA==',
                });
                objectPutPart(authInfo, request, undefined, log, err => {
                    // BadDigest is expected since the checksum value won't
                    // match the body, but NOT InvalidRequest — the algo is accepted.
                    if (err) {
                        assert.notStrictEqual(err.message, 'InvalidRequest');
                    }
                    done();
                });
            });
        });

        it('should reject part with mismatching checksum algo', done => {
            initiateMPU({ 'x-amz-checksum-algorithm': 'sha256' }, (err, uploadId) => {
                assert.ifError(err);
                const request = makePutPartRequest(uploadId, 1, partBody, {
                    'x-amz-checksum-crc32': 'AAAAAA==',
                });
                objectPutPart(authInfo, request, undefined, log, err => {
                    assert(err, 'Expected an error');
                    assert.strictEqual(err.message, 'InvalidRequest');
                    done();
                });
            });
        });

        it('should reject part with no checksum on a COMPOSITE MPU', done => {
            // sha256 is COMPOSITE-only; a COMPOSITE MPU's final checksum is
            // composed from the per-part checksums, so every part must carry
            // one and AWS rejects a part sent without it.
            initiateMPU({ 'x-amz-checksum-algorithm': 'sha256' }, (err, uploadId) => {
                assert.ifError(err);
                // No checksum header sent
                const request = makePutPartRequest(uploadId, 1, partBody);
                objectPutPart(authInfo, request, undefined, log, err => {
                    assert(err, 'Expected an error');
                    assert.strictEqual(err.message, 'InvalidRequest');
                    done();
                });
            });
        });

        it('should accept part with no checksum on a FULL_OBJECT MPU', done => {
            // crc64nvme is FULL_OBJECT-only; the server computes the
            // full-object checksum, so a missing per-part checksum is allowed.
            initiateMPU({ 'x-amz-checksum-algorithm': 'crc64nvme' }, (err, uploadId) => {
                assert.ifError(err);
                const request = makePutPartRequest(uploadId, 1, partBody);
                objectPutPart(authInfo, request, undefined, log, err => {
                    assert.ifError(err);
                    done();
                });
            });
        });

        it('should return BadDigest when matching algo but wrong digest', done => {
            initiateMPU({ 'x-amz-checksum-algorithm': 'sha256' }, (err, uploadId) => {
                assert.ifError(err);
                // Algo matches MPU (sha256) but digest is wrong
                const request = makePutPartRequest(uploadId, 1, partBody, {
                    'x-amz-checksum-sha256': 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=',
                });
                objectPutPart(authInfo, request, undefined, log, err => {
                    assert(err, 'Expected an error');
                    assert.strictEqual(err.message, 'BadDigest');
                    done();
                });
            });
        });

        it('should return InvalidRequest when MPU algo is sha256 and part sends crc32', done => {
            initiateMPU({ 'x-amz-checksum-algorithm': 'sha256' }, (err, uploadId) => {
                assert.ifError(err);
                const request = makePutPartRequest(uploadId, 1, partBody, {
                    'x-amz-checksum-crc32': 'DUoRhQ==',
                });
                objectPutPart(authInfo, request, undefined, log, err => {
                    assert(err, 'Expected an error');
                    assert.strictEqual(err.message, 'InvalidRequest');
                    done();
                });
            });
        });

        it('should accept any checksum algo on default (no algo specified) MPU', done => {
            initiateMPU({}, (err, uploadId) => {
                assert.ifError(err);
                // Send sha256 checksum even though MPU is default crc64nvme
                const request = makePutPartRequest(uploadId, 1, partBody, {
                    'x-amz-checksum-sha256': 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=',
                });
                objectPutPart(authInfo, request, undefined, log, err => {
                    // BadDigest (wrong value) is fine; InvalidRequest (wrong algo) is not
                    if (err) {
                        assert.notStrictEqual(err.message, 'InvalidRequest');
                    }
                    done();
                });
            });
        });
    });

    describe('checksum stored in part metadata', () => {
        it('should store checksumValue and checksumAlgorithm in part metadata', done => {
            initiateMPU({}, (err, uploadId) => {
                assert.ifError(err);
                const request = makePutPartRequest(uploadId, 1, partBody);
                objectPutPart(authInfo, request, undefined, log, err => {
                    assert.ifError(err);
                    const partMD = getPartMetadata(uploadId);
                    assert(partMD, 'Part metadata should exist');
                    assert(partMD.checksumValue, 'checksumValue should be stored');
                    assert.strictEqual(partMD.checksumAlgorithm, 'crc64nvme');
                    done();
                });
            });
        });

        it('should store the MPU algo checksum when client sends matching algo', done => {
            initiateMPU({ 'x-amz-checksum-algorithm': 'crc64nvme' }, (err, uploadId) => {
                assert.ifError(err);
                const request = makePutPartRequest(uploadId, 1, partBody);
                objectPutPart(authInfo, request, undefined, log, err => {
                    assert.ifError(err);
                    const partMD = getPartMetadata(uploadId);
                    assert(partMD);
                    assert.strictEqual(partMD.checksumAlgorithm, 'crc64nvme');
                    assert(partMD.checksumValue);
                    done();
                });
            });
        });
    });

    describe('dual-checksum', () => {
        it('should store crc64nvme when default MPU and client sends different algo', done => {
            initiateMPU({}, (err, uploadId) => {
                assert.ifError(err);
                const crc32Hash = algorithms.crc32.createHash();
                crc32Hash.update(partBody);
                const crc64Hash = algorithms.crc64nvme.createHash();
                crc64Hash.update(partBody);
                Promise.all([
                    algorithms.crc32.digestFromHash(crc32Hash),
                    algorithms.crc64nvme.digestFromHash(crc64Hash),
                ]).then(([crc32Digest, crc64Digest]) => {
                    const request = makePutPartRequest(uploadId, 1, partBody, {
                        'x-amz-checksum-crc32': crc32Digest,
                    });
                    objectPutPart(authInfo, request, undefined, log, (err, hexDigest, corsHeaders) => {
                        assert.ifError(err);
                        // Response header should be the client's algo (crc32)
                        assert.strictEqual(corsHeaders['x-amz-checksum-crc32'], crc32Digest);
                        // Stored metadata should be crc64nvme with correct value
                        const partMD = getPartMetadata(uploadId);
                        assert(partMD);
                        assert.strictEqual(partMD.checksumAlgorithm, 'crc64nvme');
                        assert.strictEqual(partMD.checksumValue, crc64Digest);
                        done();
                    });
                }).catch(done);
            });
        });

        it('should handle dual-checksum with trailer (STREAMING-UNSIGNED-PAYLOAD-TRAILER)', done => {
            initiateMPU({}, (err, uploadId) => {
                assert.ifError(err);
                const hash = algorithms.sha256.createHash();
                hash.update(partBody);
                const crc64Hash = algorithms.crc64nvme.createHash();
                crc64Hash.update(partBody);
                Promise.all([
                    algorithms.sha256.digestFromHash(hash),
                    algorithms.crc64nvme.digestFromHash(crc64Hash),
                ]).then(([sha256Digest, crc64Digest]) => {
                    // Build chunked body with trailing checksum
                    const hexLen = partBody.length.toString(16);
                    const chunkedBody = `${hexLen}\r\n${partBody.toString()}\r\n` +
                        `0\r\nx-amz-checksum-sha256:${sha256Digest}\r\n`;
                    const request = makePutPartRequest(uploadId, 1, Buffer.from(chunkedBody), {
                        'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                        'x-amz-trailer': 'x-amz-checksum-sha256',
                    });
                    request.parsedContentLength = partBody.length;
                    objectPutPart(authInfo, request, undefined, log, (err, hexDigest, corsHeaders) => {
                        assert.ifError(err);
                        // Response should echo the client's sha256
                        assert.strictEqual(corsHeaders['x-amz-checksum-sha256'], sha256Digest);
                        // Stored metadata should be crc64nvme with correct value
                        const partMD = getPartMetadata(uploadId);
                        assert(partMD);
                        assert.strictEqual(partMD.checksumAlgorithm, 'crc64nvme');
                        assert.strictEqual(partMD.checksumValue, crc64Digest);
                        done();
                    });
                }).catch(done);
            });
        });

        it('should return client-facing checksum in response header for dual-checksum', done => {
            initiateMPU({}, (err, uploadId) => {
                assert.ifError(err);
                const hash = algorithms.sha256.createHash();
                hash.update(partBody);
                Promise.resolve(algorithms.sha256.digestFromHash(hash)).then(digest => {
                    const request = makePutPartRequest(uploadId, 1, partBody, {
                        'x-amz-checksum-sha256': digest,
                    });
                    objectPutPart(authInfo, request, undefined, log, (err, hexDigest, corsHeaders) => {
                        assert.ifError(err);
                        assert.strictEqual(corsHeaders['x-amz-checksum-sha256'], digest);
                        assert.strictEqual(corsHeaders['x-amz-checksum-crc64nvme'], undefined);
                        done();
                    });
                }).catch(done);
            });
        });
    });

    describe('response checksum header', () => {
        const algos = Object.keys(algorithms);

        it('should not return a checksum header on a default MPU when none is sent', done => {
            initiateMPU({}, (err, uploadId) => {
                assert.ifError(err);
                const request = makePutPartRequest(uploadId, 1, partBody);
                objectPutPart(authInfo, request, undefined, log, (err, hexDigest, corsHeaders) => {
                    assert.ifError(err);
                    algos.forEach(algo => {
                        assert.strictEqual(corsHeaders[`x-amz-checksum-${algo}`], undefined);
                    });
                    // The part checksum is still stored so CompleteMPU can
                    // compute the final object checksum.
                    const partMD = getPartMetadata(uploadId);
                    assert(partMD);
                    assert.strictEqual(partMD.checksumAlgorithm, 'crc64nvme');
                    assert(partMD.checksumValue);
                    done();
                });
            });
        });

        algos.forEach(algo => {
            it(`should echo a client-supplied ${algo} checksum on a default MPU`, done => {
                initiateMPU({}, (err, uploadId) => {
                    assert.ifError(err);
                    Promise.resolve(algorithms[algo].digest(partBody)).then(digest => {
                        const request = makePutPartRequest(uploadId, 1, partBody, {
                            [`x-amz-checksum-${algo}`]: digest,
                        });
                        objectPutPart(authInfo, request, undefined, log, (err, hexDigest, corsHeaders) => {
                            assert.ifError(err);
                            assert.strictEqual(corsHeaders[`x-amz-checksum-${algo}`], digest);
                            done();
                        });
                    }).catch(done);
                });
            });

            it(`should echo the ${algo} checksum on an explicit ${algo} MPU`, done => {
                initiateMPU({ 'x-amz-checksum-algorithm': algo }, (err, uploadId) => {
                    assert.ifError(err);
                    Promise.resolve(algorithms[algo].digest(partBody)).then(digest => {
                        const request = makePutPartRequest(uploadId, 1, partBody, {
                            [`x-amz-checksum-${algo}`]: digest,
                        });
                        objectPutPart(authInfo, request, undefined, log, (err, hexDigest, corsHeaders) => {
                            assert.ifError(err);
                            assert.strictEqual(corsHeaders[`x-amz-checksum-${algo}`], digest);
                            done();
                        });
                    }).catch(done);
                });
            });
        });
    });
});
