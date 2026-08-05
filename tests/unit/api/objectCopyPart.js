const assert = require('assert');
const async = require('async');
const sinon = require('sinon');
const { parseString } = require('xml2js');
const { storage } = require('arsenal');
const { bucketPut } = require('../../../lib/api/bucketPut');
const objectPut = require('../../../lib/api/objectPut');
const objectPutCopyPart = require('../../../lib/api/objectPutCopyPart');
const initiateMultipartUpload = require('../../../lib/api/initiateMultipartUpload');
const { metadata } = storage.metadata.inMemory.metadata;
const metadataswitch = require('../metadataswitch');
const DummyRequest = require('../DummyRequest');
const { cleanup, DummyRequestLogger, makeAuthInfo, versioningTestUtils } = require('../helpers');
const { Readable } = require('stream');
const { algorithms } = require('../../../lib/api/apiUtils/integrity/validateChecksums');
const { data } = require('../../../lib/data/wrapper');
const { config } = require('../../../lib/Config');
const kms = require('../../../lib/kms/wrapper');

const checksumAlgos = Object.keys(algorithms);

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const destBucketName = 'destbucketname';
const sourceBucketName = 'sourcebucketname';
const objectKey = 'objectName';

function _createBucketPutRequest(bucketName) {
    return new DummyRequest({
        bucketName,
        namespace,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: '/',
    });
}

function _createInitiateRequest(bucketName, extraHeaders) {
    const params = {
        bucketName,
        namespace,
        objectKey,
        headers: { host: `${bucketName}.s3.amazonaws.com`, ...extraHeaders },
        url: `/${objectKey}?uploads`,
    };
    return new DummyRequest(params);
}

function _createObjectCopyPartRequest(destBucketName, uploadId, headers) {
    const params = {
        bucketName: destBucketName,
        namespace,
        objectKey,
        headers: headers || {},
        url: `/${destBucketName}/${objectKey}?partNumber=1`,
        query: {
            partNumber: 1,
            uploadId,
        },
    };
    return new DummyRequest(params);
}

const putDestBucketRequest = _createBucketPutRequest(destBucketName);
const putSourceBucketRequest = _createBucketPutRequest(sourceBucketName);
const initiateRequest = _createInitiateRequest(destBucketName);

describe('objectCopyPart', () => {
    let uploadId;
    const objData = Buffer.from('foo', 'utf8');
    const testPutObjectRequest = versioningTestUtils.createPutObjectRequest(sourceBucketName, objectKey, objData);
    before(done => {
        cleanup();
        sinon.spy(metadataswitch, 'putObjectMD');
        async.waterfall(
            [
                callback => bucketPut(authInfo, putDestBucketRequest, log, err => callback(err)),
                callback => bucketPut(authInfo, putSourceBucketRequest, log, err => callback(err)),
                callback => objectPut(authInfo, testPutObjectRequest, undefined, log, err => callback(err)),
                callback => initiateMultipartUpload(authInfo, initiateRequest, log, (err, res) => callback(err, res)),
            ],
            (err, res) => {
                if (err) {
                    return done(err);
                }
                return parseString(res, (err, json) => {
                    uploadId = json.InitiateMultipartUploadResult.UploadId[0];
                    return done();
                });
            },
        );
    });

    after(() => {
        metadataswitch.putObjectMD.restore();
        cleanup();
    });

    it('should copy part even if legacy metadata without dataStoreName', done => {
        // force metadata for dataStoreName to be undefined
        metadata.keyMaps.get(sourceBucketName).get(objectKey).dataStoreName = undefined;
        const testObjectCopyRequest = _createObjectCopyPartRequest(destBucketName, uploadId);
        objectPutCopyPart(authInfo, testObjectCopyRequest, sourceBucketName, objectKey, undefined, log, err => {
            assert.ifError(err);
            done();
        });
    });

    it('should return InvalidArgument error given invalid range', done => {
        const headers = { 'x-amz-copy-source-range': 'bad-range-parameter' };
        const req = _createObjectCopyPartRequest(destBucketName, uploadId, headers);
        objectPutCopyPart(authInfo, req, sourceBucketName, objectKey, undefined, log, err => {
            assert(err.is.InvalidArgument);
            assert.strictEqual(
                err.description,
                'The x-amz-copy-source-range value must be of the form ' +
                    'bytes=first-last where first and last are the ' +
                    'zero-based offsets of the first and last bytes to copy',
            );
            done();
        });
    });

    it('should pass overheadField', done => {
        const testObjectCopyRequest = _createObjectCopyPartRequest(destBucketName, uploadId);
        objectPutCopyPart(authInfo, testObjectCopyRequest, sourceBucketName, objectKey, undefined, log, err => {
            assert.ifError(err);
            sinon.assert.calledWith(
                metadataswitch.putObjectMD,
                sinon.match.string, // MPU shadow bucket
                objectKey,
                sinon.match.any,
                sinon.match({ overheadField: sinon.match.array }),
                sinon.match.any,
                sinon.match.any,
            );
            done();
        });
    });

    it('should set owner-id to the canonicalId of the dest bucket owner', done => {
        const testObjectCopyRequest = _createObjectCopyPartRequest(destBucketName, uploadId);
        objectPutCopyPart(authInfo, testObjectCopyRequest, sourceBucketName, objectKey, undefined, log, err => {
            assert.ifError(err);
            sinon.assert.calledWith(
                metadataswitch.putObjectMD.lastCall,
                sinon.match.string, // MPU shadow bucket
                `${uploadId}..|..00001`,
                sinon.match({ 'owner-id': authInfo.canonicalID }),
                sinon.match.any,
                sinon.match.any,
                sinon.match.any,
            );
            done();
        });
    });
});

describe('objectPutCopyPart._shouldRecomputeChecksum', () => {
    const { _shouldRecomputeChecksum } = objectPutCopyPart;
    const noRange = { headers: {} };
    const withRange = { headers: { 'x-amz-copy-source-range': 'bytes=0-1' } };

    it('should return true when a copy-source-range is requested', () => {
        assert.strictEqual(
            _shouldRecomputeChecksum(withRange, { checksumType: 'FULL_OBJECT', checksumAlgorithm: 'crc32' }, 'crc32'),
            true,
        );
    });

    it('should return true when the source has no checksum', () => {
        assert.strictEqual(_shouldRecomputeChecksum(noRange, undefined, 'crc32'), true);
    });

    checksumAlgos.forEach(algo => {
        const otherAlgo = algo === 'sha256' ? 'crc32' : 'sha256';

        it(`should return false when the source is FULL_OBJECT ${algo} matching the MPU`, () => {
            assert.strictEqual(
                _shouldRecomputeChecksum(noRange, { checksumType: 'FULL_OBJECT', checksumAlgorithm: algo }, algo),
                false,
            );
        });

        it(`should return true when the source ${otherAlgo} differs from the MPU ${algo}`, () => {
            assert.strictEqual(
                _shouldRecomputeChecksum(noRange, { checksumType: 'FULL_OBJECT', checksumAlgorithm: otherAlgo }, algo),
                true,
            );
        });

        it(`should return true when the source ${algo} checksum is COMPOSITE`, () => {
            assert.strictEqual(
                _shouldRecomputeChecksum(noRange, { checksumType: 'COMPOSITE', checksumAlgorithm: algo }, algo),
                true,
            );
        });
    });
});

describe('objectPutCopyPart checksum storage', () => {
    const objData = Buffer.from('foo', 'utf8');

    function _initiateWithHeaders(headers, cb) {
        const req = _createInitiateRequest(destBucketName, headers);
        return initiateMultipartUpload(authInfo, req, log, (err, res) => {
            if (err) {
                return cb(err);
            }
            return parseString(res, (parseErr, json) => cb(parseErr, json.InitiateMultipartUploadResult.UploadId[0]));
        });
    }

    // The part metadata is the last object written; pull its checksum fields.
    function _storedPartChecksum() {
        const omVal = metadataswitch.putObjectMD.lastCall.args[2];
        return { algorithm: omVal.checksumAlgorithm, value: omVal.checksumValue };
    }

    // Initiate an MPU for `algo`, optionally inject the source object's stored
    // checksum (to drive the reuse-vs-recompute decision), then copy a part and
    // resolve with the checksum persisted in the part metadata.
    function _copyPart(algo, { sourceKey = objectKey, sourceChecksum, headers } = {}) {
        return new Promise((resolve, reject) => {
            _initiateWithHeaders({ 'x-amz-checksum-algorithm': algo.toUpperCase() }, (err, uploadId) => {
                if (err) {
                    return reject(err);
                }
                if (sourceChecksum) {
                    metadata.keyMaps.get(sourceBucketName).get(sourceKey).checksum = sourceChecksum;
                }
                const req = _createObjectCopyPartRequest(destBucketName, uploadId, headers);
                return objectPutCopyPart(authInfo, req, sourceBucketName, sourceKey, undefined, log, copyErr =>
                    copyErr ? reject(copyErr) : resolve(_storedPartChecksum()),
                );
            });
        });
    }

    beforeEach(done => {
        cleanup();
        sinon.spy(metadataswitch, 'putObjectMD');
        async.waterfall(
            [
                cb => bucketPut(authInfo, putDestBucketRequest, log, e => cb(e)),
                cb => bucketPut(authInfo, putSourceBucketRequest, log, e => cb(e)),
                cb =>
                    objectPut(
                        authInfo,
                        versioningTestUtils.createPutObjectRequest(sourceBucketName, objectKey, objData),
                        undefined,
                        log,
                        e => cb(e),
                    ),
            ],
            done,
        );
    });

    afterEach(() => {
        sinon.restore();
        cleanup();
    });

    checksumAlgos.forEach(algo => {
        const otherAlgo = algo === 'sha256' ? 'crc32' : 'sha256';
        const mismatch = { checksumType: 'FULL_OBJECT', checksumAlgorithm: otherAlgo, checksumValue: 'unused' };

        it(`should recompute the part checksum (${algo}) when the source algorithm differs`, async () => {
            const expected = await algorithms[algo].digest(objData);
            assert.deepStrictEqual(await _copyPart(algo, { sourceChecksum: mismatch }), {
                algorithm: algo,
                value: expected,
            });
        });

        it(`should reuse the source checksum (${algo}) when the algorithm matches`, async () => {
            const sourceValue = await algorithms[algo].digest(objData);
            assert.deepStrictEqual(
                await _copyPart(algo, {
                    sourceChecksum: {
                        checksumType: 'FULL_OBJECT',
                        checksumAlgorithm: algo,
                        checksumValue: sourceValue,
                    },
                }),
                { algorithm: algo, value: sourceValue },
            );
        });

        it(`should recompute over the ranged bytes (${algo}) when a copy-source-range is set`, async () => {
            const expected = await algorithms[algo].digest(Buffer.from('fo'));
            assert.deepStrictEqual(
                await _copyPart(algo, {
                    sourceChecksum: {
                        checksumType: 'FULL_OBJECT',
                        checksumAlgorithm: algo,
                        checksumValue: await algorithms[algo].digest(objData),
                    },
                    headers: { 'x-amz-copy-source-range': 'bytes=0-1' },
                }),
                { algorithm: algo, value: expected },
            );
        });

        it(`should store the empty-bytes digest (${algo}) for a 0-byte source`, async () => {
            const expected = await algorithms[algo].digest(Buffer.alloc(0));
            const emptyKey = `empty-source-${algo}`;
            await new Promise((resolve, reject) =>
                objectPut(
                    authInfo,
                    versioningTestUtils.createPutObjectRequest(sourceBucketName, emptyKey, Buffer.alloc(0)),
                    undefined,
                    log,
                    e => (e ? reject(e) : resolve()),
                ),
            );
            assert.deepStrictEqual(await _copyPart(algo, { sourceKey: emptyKey, sourceChecksum: mismatch }), {
                algorithm: algo,
                value: expected,
            });
        });
    });

    it('should use the one-pass stream (not data.uploadPartCopy) for a local destination', done => {
        _initiateWithHeaders({ 'x-amz-checksum-algorithm': 'CRC32' }, (err, uploadId) => {
            assert.ifError(err);
            metadata.keyMaps.get(sourceBucketName).get(objectKey).checksum = {
                checksumType: 'FULL_OBJECT',
                checksumAlgorithm: 'sha256',
                checksumValue: 'unused',
            };
            const uploadPartCopySpy = sinon.spy(data, 'uploadPartCopy');
            const req = _createObjectCopyPartRequest(destBucketName, uploadId);
            objectPutCopyPart(authInfo, req, sourceBucketName, objectKey, undefined, log, copyErr => {
                assert.ifError(copyErr);
                sinon.assert.notCalled(uploadPartCopySpy);
                done();
            });
        });
    });

    it('should route an external-backend destination through data.uploadPartCopy and return no checksum', done => {
        _initiateWithHeaders({ 'x-amz-checksum-algorithm': 'CRC32' }, (err, uploadId) => {
            assert.ifError(err);
            metadata.keyMaps.get(sourceBucketName).get(objectKey).checksum = {
                checksumType: 'FULL_OBJECT',
                checksumAlgorithm: 'sha256',
                checksumValue: 'unused',
            };
            // Make the destination look like an external backend (data.put can't store its parts)...
            sinon.stub(config, 'getLocationConstraintType').returns('aws_s3');
            // ...and simulate the backend's native part copy returning the skip sentinel.
            const uploadPartCopyStub = sinon.stub(data, 'uploadPartCopy').callsFake((...args) => {
                const cb = args[args.length - 1];
                return cb(new Error('skip'), 'etag', '2020-01-01T00:00:00.000Z', null, [{ dataStoreETag: 'etag' }]);
            });
            const req = _createObjectCopyPartRequest(destBucketName, uploadId);
            objectPutCopyPart(authInfo, req, sourceBucketName, objectKey, undefined, log, (copyErr, xml) => {
                assert.ifError(copyErr);
                sinon.assert.calledOnce(uploadPartCopyStub);
                // external backends get no cloudserver checksum (matches UploadPart)
                assert.doesNotMatch(xml, /Checksum/);
                done();
            });
        });
    });
});

describe('objectPutCopyPart._copyPartStreamingWithChecksum', () => {
    const { _copyPartStreamingWithChecksum } = objectPutCopyPart;
    const srcBytes = Buffer.from('hello-copy-part', 'utf8');
    const dataLocator = [{ key: 'srckey', dataStoreType: 'mem', dataStoreName: 'mem' }];

    beforeEach(() => {
        // Serve the source bytes for buildSourcePartsStream.
        sinon.stub(data, 'get').callsFake((part, writable, log2, cb) => {
            const rs = new Readable({ read() {} });
            process.nextTick(() => {
                rs.push(srcBytes);
                rs.push(null);
            });
            return cb(null, rs);
        });
        // Drain the checksum stream (so it flushes its digest) and report an md5.
        sinon.stub(data, 'put').callsFake((cipherBundle, stream, size, ctx, backendInfo, log2, cb) => {
            stream.on('data', () => {});
            stream.once('end', () => cb(null, { key: 'destkey', dataStoreName: 'mem' }, { completedHash: 'fakemd5' }));
        });
    });

    afterEach(() => sinon.restore());

    function _run(sse, algo) {
        return new Promise((resolve, reject) =>
            _copyPartStreamingWithChecksum(
                dataLocator,
                srcBytes.length,
                sse,
                'us-east-1',
                {},
                algo,
                log,
                (err, result) => (err ? reject(err) : resolve(result)),
            ),
        );
    }

    checksumAlgos.forEach(algo => {
        it(`should return the part location, eTag and ${algo} checksum for an unencrypted copy`, async () => {
            const result = await _run(null, algo);
            assert.deepStrictEqual(result.locations, [
                {
                    key: 'destkey',
                    dataStoreName: 'mem',
                    dataStoreETag: 'fakemd5',
                    size: srcBytes.length,
                },
            ]);
            assert.strictEqual(result.totalHash, 'fakemd5');
            assert.deepStrictEqual(result.checksum, {
                algorithm: algo,
                value: await algorithms[algo].digest(srcBytes),
            });
        });

        it(`should add the SSE cipher fields with a ${algo} checksum when the MPU is encrypted`, async () => {
            const cipherBundle = { cryptoScheme: 1, cipheredDataKey: 'dk', algorithm: 'AES256', masterKeyId: 'mk' };
            sinon.stub(kms, 'createCipherBundle').callsFake((sse, log2, cb) => cb(null, cipherBundle));
            const result = await _run({ algorithm: 'AES256' }, algo);
            assert.deepStrictEqual(result.locations[0], {
                key: 'destkey',
                dataStoreName: 'mem',
                dataStoreETag: 'fakemd5',
                size: srcBytes.length,
                sseCryptoScheme: 1,
                sseCipheredDataKey: 'dk',
                sseAlgorithm: 'AES256',
                sseMasterKeyId: 'mk',
            });
            assert.deepStrictEqual(result.checksum, {
                algorithm: algo,
                value: await algorithms[algo].digest(srcBytes),
            });
        });
    });

    it('should surface a source read error wrapped with copyPart metadata', done => {
        data.get.restore();
        const boom = new Error('read failed');
        sinon.stub(data, 'get').callsFake((part, writable, log2, cb) => cb(boom));
        _copyPartStreamingWithChecksum(dataLocator, srcBytes.length, null, 'us-east-1', {}, 'crc32', log, err => {
            assert.strictEqual(err, boom);
            assert.strictEqual(err.copyPart.key, 'srckey');
            done();
        });
    });
});

describe('objectPutCopyPart with checksums disabled', () => {
    const { _shouldRecomputeChecksum } = objectPutCopyPart;
    const objData = Buffer.from('foo', 'utf8');
    let originalIntegrityChecks;

    beforeEach(done => {
        originalIntegrityChecks = config.integrityChecks;
        cleanup();
        sinon.spy(metadataswitch, 'putObjectMD');
        async.waterfall(
            [
                cb => bucketPut(authInfo, putDestBucketRequest, log, e => cb(e)),
                cb => bucketPut(authInfo, putSourceBucketRequest, log, e => cb(e)),
                cb =>
                    objectPut(
                        authInfo,
                        versioningTestUtils.createPutObjectRequest(sourceBucketName, objectKey, objData),
                        undefined,
                        log,
                        e => cb(e),
                    ),
            ],
            done,
        );
    });

    afterEach(() => {
        config.integrityChecks = originalIntegrityChecks;
        sinon.restore();
        cleanup();
    });

    describe('_shouldRecomputeChecksum', () => {
        it('should never recompute, whatever the source or range', () => {
            config.integrityChecks = { enabled: false };
            const withRange = { headers: { 'x-amz-copy-source-range': 'bytes=0-1' } };
            const noRange = { headers: {} };
            const composite = { checksumType: 'COMPOSITE', checksumAlgorithm: 'crc32' };
            // Each of these returns true when enabled.
            assert.strictEqual(_shouldRecomputeChecksum(withRange, composite, 'crc32'), false);
            assert.strictEqual(_shouldRecomputeChecksum(noRange, undefined, 'crc32'), false);
            assert.strictEqual(_shouldRecomputeChecksum(noRange, composite, 'crc32'), false);
        });

        it('should recompute again once re-enabled', () => {
            config.integrityChecks = { enabled: true };
            assert.strictEqual(_shouldRecomputeChecksum({ headers: {} }, undefined, 'crc32'), true);
        });
    });

    describe('part metadata', () => {
        function copyPartDisabled({ sourceChecksum, headers } = {}) {
            return new Promise((resolve, reject) => {
                const initReq = _createInitiateRequest(destBucketName, {
                    'x-amz-checksum-algorithm': 'CRC32',
                });
                // Create the MPU with checksums on so an algorithm is recorded,
                // then disable: the flag must be honoured at copy time.
                config.integrityChecks = { enabled: true };
                return initiateMultipartUpload(authInfo, initReq, log, (err, res) => {
                    if (err) {
                        return reject(err);
                    }
                    return parseString(res, (parseErr, json) => {
                        if (parseErr) {
                            return reject(parseErr);
                        }
                        const uploadId = json.InitiateMultipartUploadResult.UploadId[0];
                        if (sourceChecksum) {
                            metadata.keyMaps.get(sourceBucketName).get(objectKey).checksum = sourceChecksum;
                        } else {
                            delete metadata.keyMaps.get(sourceBucketName).get(objectKey).checksum;
                        }
                        config.integrityChecks = { enabled: false };
                        const req = _createObjectCopyPartRequest(destBucketName, uploadId, headers);
                        return objectPutCopyPart(authInfo, req, sourceBucketName, objectKey, undefined, log, copyErr =>
                            copyErr ? reject(copyErr) : resolve(metadataswitch.putObjectMD.lastCall.args[2]),
                        );
                    });
                });
            });
        }

        it('should store no checksum when the source has one to reuse', async () => {
            const omVal = await copyPartDisabled({
                sourceChecksum: { checksumType: 'FULL_OBJECT', checksumAlgorithm: 'crc32', checksumValue: 'AAAAAA==' },
            });
            assert.strictEqual(omVal.checksumAlgorithm, undefined);
            assert.strictEqual(omVal.checksumValue, undefined);
        });

        it('should store no checksum when the source has none', async () => {
            // Would previously recompute; must not dereference the absent source checksum.
            const omVal = await copyPartDisabled();
            assert.strictEqual(omVal.checksumAlgorithm, undefined);
            assert.strictEqual(omVal.checksumValue, undefined);
        });

        it('should store no checksum for a ranged copy', async () => {
            // A range always forces a recompute when enabled.
            const omVal = await copyPartDisabled({ headers: { 'x-amz-copy-source-range': 'bytes=0-1' } });
            assert.strictEqual(omVal.checksumAlgorithm, undefined);
            assert.strictEqual(omVal.checksumValue, undefined);
        });
    });
});
