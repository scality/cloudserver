const assert = require('assert');
const crypto = require('crypto');
const async = require('async');
const { parseString } = require('xml2js');

const { bucketPut } = require('../../../lib/api/bucketPut');
const initiateMultipartUpload = require('../../../lib/api/initiateMultipartUpload');
const objectPutPart = require('../../../lib/api/objectPutPart');
const completeMultipartUpload = require('../../../lib/api/completeMultipartUpload');
const { validatePerPartChecksums } = completeMultipartUpload;
const { validateMethodChecksumNoChunking } = require('../../../lib/api/apiUtils/integrity/validateChecksums');
const DummyRequest = require('../DummyRequest');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');

const SPLITTER = '..|..';
const UPLOAD_ID = 'upload-id-1';

// XML element name AWS uses for each algorithm in CompleteMultipartUpload's
// per-part body.
const TAG_BY_ALGO = {
    crc32: 'ChecksumCRC32',
    crc32c: 'ChecksumCRC32C',
    crc64nvme: 'ChecksumCRC64NVME',
    sha1: 'ChecksumSHA1',
    sha256: 'ChecksumSHA256',
};

// Two distinct base64 placeholder digests per algorithm. Sized to the real
// digest lengths so the test data looks realistic, though the validator
// itself doesn't enforce length.
const SAMPLE_DIGESTS = {
    crc32: ['AQIDBA==', 'BQYHCA=='],
    crc32c: ['CQoLDA==', 'DQ4PEA=='],
    crc64nvme: ['AQIDBAUGBwg=', 'CQoLDA0ODxA='],
    sha1: ['YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWE=', 'YmJiYmJiYmJiYmJiYmJiYmJiYmJiYmI='],
    sha256: ['YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWE=', 'YmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmI='],
};

// Every AWS-valid (algorithm, type) combination, plus the implicit default.
// See validateChecksums.getChecksumDataFromMPUHeaders for the source of truth.
const MATRIX = [
    { algorithm: 'crc32', type: 'COMPOSITE', isDefault: false },
    { algorithm: 'crc32', type: 'FULL_OBJECT', isDefault: false },
    { algorithm: 'crc32c', type: 'COMPOSITE', isDefault: false },
    { algorithm: 'crc32c', type: 'FULL_OBJECT', isDefault: false },
    { algorithm: 'crc64nvme', type: 'FULL_OBJECT', isDefault: false },
    { algorithm: 'crc64nvme', type: 'FULL_OBJECT', isDefault: true },
    { algorithm: 'sha1', type: 'COMPOSITE', isDefault: false },
    { algorithm: 'sha256', type: 'COMPOSITE', isDefault: false },
];

function makeStoredPart(partNumber, checksum) {
    const value = {
        ETag: 'd41d8cd98f00b204e9800998ecf8427e',
        Size: 5242880,
        partLocations: [{ key: `data-${partNumber}`, dataStoreName: 'us-east-1' }],
    };
    if (checksum) {
        value.ChecksumAlgorithm = checksum.algorithm;
        value.ChecksumValue = checksum.value;
    }
    return {
        key: `${UPLOAD_ID}${SPLITTER}${partNumber}`,
        value,
    };
}

function makeJsonPart(partNumber, eTag, checksums) {
    const part = {
        PartNumber: [String(partNumber)],
        ETag: [`"${eTag}"`],
    };
    if (checksums) {
        Object.entries(checksums).forEach(([tag, value]) => {
            part[tag] = [value];
        });
    }
    return part;
}

function pickWrongAlgo(algo) {
    return Object.keys(TAG_BY_ALGO).find(a => a !== algo);
}

describe('validatePerPartChecksums', () => {
    describe('AWS combination matrix', () => {
        MATRIX.forEach(({ algorithm, type, isDefault }) => {
            const label = `${algorithm}/${type}${isDefault ? ' (default)' : ''}`;
            const tag = TAG_BY_ALGO[algorithm];
            const [d1, d2] = SAMPLE_DIGESTS[algorithm];
            const mpuChecksum = { algorithm, type, isDefault };

            const stored = [makeStoredPart(1, { algorithm, value: d1 }), makeStoredPart(2, { algorithm, value: d2 })];

            describe(label, () => {
                it('should accept when every part includes the matching checksum', () => {
                    const jsonList = {
                        Part: [makeJsonPart(1, 'etag1', { [tag]: d1 }), makeJsonPart(2, 'etag2', { [tag]: d2 })],
                    };
                    const err = validatePerPartChecksums(jsonList, stored, SPLITTER, mpuChecksum);
                    assert.strictEqual(err, null);
                });

                it('should return BadDigest when a part uses the wrong checksum field', () => {
                    const wrongAlgo = pickWrongAlgo(algorithm);
                    const wrongTag = TAG_BY_ALGO[wrongAlgo];
                    const wrongDigest = SAMPLE_DIGESTS[wrongAlgo][0];
                    const jsonList = {
                        Part: [
                            makeJsonPart(1, 'etag1', { [wrongTag]: wrongDigest }),
                            makeJsonPart(2, 'etag2', { [tag]: d2 }),
                        ],
                    };
                    const err = validatePerPartChecksums(jsonList, stored, SPLITTER, mpuChecksum);
                    assert(err);
                    assert.strictEqual(err.is.BadDigest, true);
                    // AWS-style message: "The {algo} you specified for part {N} did not match what we received."
                    assert.strictEqual(
                        err.description,
                        `The ${wrongAlgo} you specified for part 1 did ` + 'not match what we received.',
                    );
                });

                it('should return InvalidPart when the matching field has the wrong value', () => {
                    const jsonList = {
                        Part: [makeJsonPart(1, 'etag1', { [tag]: d1 }), makeJsonPart(2, 'etag2', { [tag]: d1 })],
                    };
                    const err = validatePerPartChecksums(jsonList, stored, SPLITTER, mpuChecksum);
                    assert(err);
                    assert.strictEqual(err.is.InvalidPart, true);
                    // AWS reuses its generic InvalidPart message — no algorithm
                    // or part number in the wording.
                    assert.strictEqual(
                        err.description,
                        'One or more of the specified parts could not be ' +
                            'found.  The part may not have been uploaded, or ' +
                            'the specified entity tag may not match the ' +
                            "part's entity tag.",
                    );
                });

                const requiresPerPart = type === 'COMPOSITE' && !isDefault;
                const missingLabel = requiresPerPart
                    ? 'should return InvalidRequest when a part is missing its checksum'
                    : 'should accept a parts list missing per-part checksums';
                it(missingLabel, () => {
                    const jsonList = {
                        Part: [makeJsonPart(1, 'etag1', { [tag]: d1 }), makeJsonPart(2, 'etag2')],
                    };
                    const err = validatePerPartChecksums(jsonList, stored, SPLITTER, mpuChecksum);
                    if (requiresPerPart) {
                        assert(err);
                        assert.strictEqual(err.is.InvalidRequest, true);
                        assert(err.description.includes(algorithm));
                        assert(err.description.includes('part 2 in the request'));
                    } else {
                        assert.strictEqual(err, null);
                    }
                });
            });
        });
    });

    describe('legacy MPU (no algorithm configured)', () => {
        // Pre-feature MPUs have storedMetadata.checksumAlgorithm === undefined.
        // Pre-PR CompleteMPU silently ignored any per-part Checksum<X> body
        // elements; preserve that so in-flight uploads across the upgrade
        // boundary don't start failing with BadDigest.
        const mpuChecksum = { algorithm: undefined, type: undefined, isDefault: undefined };
        const stored = [makeStoredPart(1), makeStoredPart(2)];

        it('should accept when no parts include a checksum field', () => {
            const jsonList = { Part: [makeJsonPart(1, 'etag1'), makeJsonPart(2, 'etag2')] };
            const err = validatePerPartChecksums(jsonList, stored, SPLITTER, mpuChecksum);
            assert.strictEqual(err, null);
        });

        it('should accept when a part includes a single Checksum<X> field', () => {
            const jsonList = {
                Part: [
                    makeJsonPart(1, 'etag1', { ChecksumSHA256: SAMPLE_DIGESTS.sha256[0] }),
                    makeJsonPart(2, 'etag2'),
                ],
            };
            const err = validatePerPartChecksums(jsonList, stored, SPLITTER, mpuChecksum);
            assert.strictEqual(err, null);
        });

        it('should accept when parts include multiple Checksum<X> fields', () => {
            const jsonList = {
                Part: [
                    makeJsonPart(1, 'etag1', {
                        ChecksumSHA256: SAMPLE_DIGESTS.sha256[0],
                        ChecksumCRC32: SAMPLE_DIGESTS.crc32[0],
                    }),
                    makeJsonPart(2, 'etag2', { ChecksumCRC64NVME: SAMPLE_DIGESTS.crc64nvme[1] }),
                ],
            };
            const err = validatePerPartChecksums(jsonList, stored, SPLITTER, mpuChecksum);
            assert.strictEqual(err, null);
        });
    });
    describe('edge cases', () => {
        it('should accept an empty parts list', () => {
            const mpuChecksum = {
                algorithm: 'sha256',
                type: 'COMPOSITE',
                isDefault: false,
            };
            const err = validatePerPartChecksums({ Part: [] }, [], SPLITTER, mpuChecksum);
            assert.strictEqual(err, null);
        });

        it('should accept a parts list with no Part array (treated as empty)', () => {
            const mpuChecksum = {
                algorithm: 'crc64nvme',
                type: 'FULL_OBJECT',
                isDefault: true,
            };
            const err = validatePerPartChecksums({}, [], SPLITTER, mpuChecksum);
            assert.strictEqual(err, null);
        });

        it('should accept a FULL_OBJECT mixed list (one part with checksum, one without)', () => {
            const mpuChecksum = {
                algorithm: 'crc32',
                type: 'FULL_OBJECT',
                isDefault: false,
            };
            const [d1, d2] = SAMPLE_DIGESTS.crc32;
            const stored = [
                makeStoredPart(1, { algorithm: 'crc32', value: d1 }),
                makeStoredPart(2, { algorithm: 'crc32', value: d2 }),
            ];
            const jsonList = {
                Part: [makeJsonPart(1, 'etag1', { ChecksumCRC32: d1 }), makeJsonPart(2, 'etag2')],
            };
            const err = validatePerPartChecksums(jsonList, stored, SPLITTER, mpuChecksum);
            assert.strictEqual(err, null);
        });

        it('should not enforce per-part presence when MPU algorithm is unknown', () => {
            // CreateMPU should never let this state through, but guard against
            // an "InvalidRequest: using a undefined checksum" error if it did.
            const mpuChecksum = {
                algorithm: undefined,
                type: 'COMPOSITE',
                isDefault: false,
            };
            const stored = [makeStoredPart(1, null), makeStoredPart(2, null)];
            const jsonList = {
                Part: [makeJsonPart(1, 'etag1'), makeJsonPart(2, 'etag2')],
            };
            const err = validatePerPartChecksums(jsonList, stored, SPLITTER, mpuChecksum);
            assert.strictEqual(err, null);
        });

        it('should return InvalidPart when stored part has no checksum but request does', () => {
            const mpuChecksum = {
                algorithm: 'sha256',
                type: 'COMPOSITE',
                isDefault: false,
            };
            const stored = [makeStoredPart(1, null)];
            const jsonList = {
                Part: [
                    makeJsonPart(1, 'etag1', {
                        ChecksumSHA256: SAMPLE_DIGESTS.sha256[0],
                    }),
                ],
            };
            const err = validatePerPartChecksums(jsonList, stored, SPLITTER, mpuChecksum);
            assert(err);
            assert.strictEqual(err.is.InvalidPart, true);
        });
    });
});

describe('CompleteMultipartUpload x-amz-checksum-type header', () => {
    const log = new DummyRequestLogger();
    const authInfo = makeAuthInfo('accessKey1');
    const namespace = 'default';
    const bucketName = 'bucketname-checksum-type';
    const objectKey = 'testObject';

    const bucketPutRequest = {
        bucketName,
        namespace,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: '/',
        post:
            '<CreateBucketConfiguration ' +
            'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
            '<LocationConstraint>scality-internal-mem</LocationConstraint>' +
            '</CreateBucketConfiguration >',
        actionImplicitDenies: false,
    };

    function setupMpu(initiateHeaders, cb) {
        async.waterfall(
            [
                next => bucketPut(authInfo, bucketPutRequest, log, next),
                (corsHeaders, next) => {
                    const initiateRequest = {
                        bucketName,
                        namespace,
                        objectKey,
                        headers: {
                            host: `${bucketName}.s3.amazonaws.com`,
                            ...initiateHeaders,
                        },
                        url: `/${objectKey}?uploads`,
                        actionImplicitDenies: false,
                    };
                    initiateMultipartUpload(authInfo, initiateRequest, log, next);
                },
                (xml, corsHeaders, next) => parseString(xml, next),
                (json, next) => {
                    const uploadId = json.InitiateMultipartUploadResult.UploadId[0];
                    const partBody = Buffer.from('I am a part\n', 'utf8');
                    const partHash = crypto.createHash('md5').update(partBody).digest('hex');
                    const partRequest = new DummyRequest(
                        {
                            bucketName,
                            namespace,
                            objectKey,
                            headers: { host: `${bucketName}.s3.amazonaws.com` },
                            url: `/${objectKey}?partNumber=1&uploadId=${uploadId}`,
                            query: { partNumber: '1', uploadId },
                            partHash,
                            actionImplicitDenies: false,
                        },
                        partBody,
                    );
                    objectPutPart(authInfo, partRequest, undefined, log, err => next(err, uploadId, partHash));
                },
            ],
            cb,
        );
    }

    function makeCompleteRequest(uploadId, partHash, extraHeaders) {
        const completeBody =
            '<CompleteMultipartUpload>' +
            '<Part>' +
            '<PartNumber>1</PartNumber>' +
            `<ETag>"${partHash}"</ETag>` +
            '</Part>' +
            '</CompleteMultipartUpload>';
        return {
            bucketName,
            namespace,
            objectKey,
            parsedHost: 's3.amazonaws.com',
            url: `/${objectKey}?uploadId=${uploadId}`,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                ...extraHeaders,
            },
            query: { uploadId },
            post: completeBody,
            actionImplicitDenies: false,
        };
    }

    beforeEach(() => cleanup());

    it('should accept CompleteMPU when no x-amz-checksum-type header is sent', done => {
        const initiateHeaders = {
            'x-amz-checksum-algorithm': 'CRC32',
            'x-amz-checksum-type': 'FULL_OBJECT',
        };
        setupMpu(initiateHeaders, (err, uploadId, partHash) => {
            assert.ifError(err);
            const req = makeCompleteRequest(uploadId, partHash, {});
            completeMultipartUpload(authInfo, req, log, completeErr => {
                assert.ifError(completeErr);
                done();
            });
        });
    });

    it('should accept CompleteMPU when x-amz-checksum-type matches the MPU type', done => {
        const initiateHeaders = {
            'x-amz-checksum-algorithm': 'CRC32',
            'x-amz-checksum-type': 'FULL_OBJECT',
        };
        setupMpu(initiateHeaders, (err, uploadId, partHash) => {
            assert.ifError(err);
            const req = makeCompleteRequest(uploadId, partHash, {
                'x-amz-checksum-type': 'FULL_OBJECT',
            });
            completeMultipartUpload(authInfo, req, log, completeErr => {
                assert.ifError(completeErr);
                done();
            });
        });
    });

    it('should reject CompleteMPU with InvalidRequest when x-amz-checksum-type does not match the MPU type', done => {
        const initiateHeaders = {
            'x-amz-checksum-algorithm': 'CRC32',
            'x-amz-checksum-type': 'FULL_OBJECT',
        };
        setupMpu(initiateHeaders, (err, uploadId, partHash) => {
            assert.ifError(err);
            const req = makeCompleteRequest(uploadId, partHash, {
                'x-amz-checksum-type': 'COMPOSITE',
            });
            completeMultipartUpload(authInfo, req, log, completeErr => {
                assert(completeErr);
                assert.strictEqual(completeErr.is.InvalidRequest, true);
                // AWS-style mode-mismatch wording.
                assert.strictEqual(
                    completeErr.description,
                    'The upload was created using the FULL_OBJECT checksum ' +
                        'mode. The complete request must use the same checksum ' +
                        'mode.',
                );
                done();
            });
        });
    });

    it('should reject CompleteMPU with InvalidRequest when x-amz-checksum-type value is bogus', done => {
        const initiateHeaders = {
            'x-amz-checksum-algorithm': 'CRC32',
            'x-amz-checksum-type': 'FULL_OBJECT',
        };
        setupMpu(initiateHeaders, (err, uploadId, partHash) => {
            assert.ifError(err);
            const req = makeCompleteRequest(uploadId, partHash, {
                'x-amz-checksum-type': 'BOGUS',
            });
            completeMultipartUpload(authInfo, req, log, completeErr => {
                assert(completeErr);
                assert.strictEqual(completeErr.is.InvalidRequest, true);
                assert.strictEqual(completeErr.description, 'Value for x-amz-checksum-type header is invalid.');
                done();
            });
        });
    });

    it('should compare x-amz-checksum-type case-insensitively', done => {
        const initiateHeaders = {
            'x-amz-checksum-algorithm': 'CRC32',
            'x-amz-checksum-type': 'FULL_OBJECT',
        };
        setupMpu(initiateHeaders, (err, uploadId, partHash) => {
            assert.ifError(err);
            const req = makeCompleteRequest(uploadId, partHash, {
                'x-amz-checksum-type': 'full_object',
            });
            completeMultipartUpload(authInfo, req, log, completeErr => {
                assert.ifError(completeErr);
                done();
            });
        });
    });
});

describe('CompleteMultipartUpload body-checksum bypass', () => {
    const log = new DummyRequestLogger();

    it(
        'validateMethodChecksumNoChunking returns null for completeMultipartUpload ' +
            'even when x-amz-checksum-sha256 does not match the body digest',
        async () => {
            const body = Buffer.from(
                '<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>' +
                    '<ETag>"abc"</ETag></Part></CompleteMultipartUpload>',
            );
            // A syntactically valid SHA256 base64 digest that is NOT the digest of `body`
            // (it's the digest of the empty string). On CompleteMPU this header carries
            // the expected final-object checksum, not a body checksum, so pre-validation
            // must skip it.
            const finalObjectChecksum = '47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=';
            const request = {
                apiMethod: 'completeMultipartUpload',
                headers: { 'x-amz-checksum-sha256': finalObjectChecksum },
            };
            const err = await validateMethodChecksumNoChunking(request, body, log);
            assert.strictEqual(err, null);
        },
    );

    it(
        'validateMethodChecksumNoChunking still rejects body mismatch for methods ' +
            'that remain in checksumedMethods (sanity check)',
        async () => {
            const body = Buffer.from('{"Objects":[]}');
            const finalObjectChecksum = '47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=';
            const request = {
                apiMethod: 'multiObjectDelete',
                headers: { 'x-amz-checksum-sha256': finalObjectChecksum },
            };
            const err = await validateMethodChecksumNoChunking(request, body, log);
            assert(err, 'expected an error for body checksum mismatch');
            assert.strictEqual(err.is.BadDigest, true);
        },
    );
});
