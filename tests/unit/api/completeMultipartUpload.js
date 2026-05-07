const assert = require('assert');
const crypto = require('crypto');
const async = require('async');
const { parseString } = require('xml2js');

const { bucketPut } = require('../../../lib/api/bucketPut');
const initiateMultipartUpload = require('../../../lib/api/initiateMultipartUpload');
const objectPutPart = require('../../../lib/api/objectPutPart');
const completeMultipartUpload = require('../../../lib/api/completeMultipartUpload');
const metadata = require('../../../lib/metadata/wrapper');
const { validatePerPartChecksums, computeFinalChecksum } = completeMultipartUpload;
const {
    validateMethodChecksumNoChunking,
    algorithms,
} = require('../../../lib/api/apiUtils/integrity/validateChecksums');
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
        'should skip body-checksum validation for completeMultipartUpload ' +
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
        'should still reject body mismatch for methods that remain in checksumedMethods ' + '(sanity check)',
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

describe('computeFinalChecksum', () => {
    const log = new DummyRequestLogger();
    const uploadId = UPLOAD_ID;

    function partListFromStored(stored) {
        return stored.map(s => ({
            key: s.key,
            ETag: `"${s.value.ETag}"`,
            size: s.value.Size,
            locations: s.value.partLocations,
        }));
    }

    it('should return null when MPU has no checksumAlgorithm', async () => {
        const stored = [makeStoredPart(1, { algorithm: 'sha256', value: SAMPLE_DIGESTS.sha256[0] })];
        const got = await computeFinalChecksum(stored, partListFromStored(stored), {}, SPLITTER, uploadId, log);
        assert.strictEqual(got, null);
    });

    it('should return null when MPU has no checksumType', async () => {
        const stored = [makeStoredPart(1, { algorithm: 'sha256', value: SAMPLE_DIGESTS.sha256[0] })];
        const got = await computeFinalChecksum(
            stored,
            partListFromStored(stored),
            { checksumAlgorithm: 'sha256' },
            SPLITTER,
            uploadId,
            log,
        );
        assert.strictEqual(got, null);
    });

    it('should return COMPOSITE checksum with -N suffix for SHA256 MPU', async () => {
        const [d1, d2, d3] = [SAMPLE_DIGESTS.sha256[0], SAMPLE_DIGESTS.sha256[1], SAMPLE_DIGESTS.sha256[0]];
        const stored = [
            makeStoredPart(1, { algorithm: 'sha256', value: d1 }),
            makeStoredPart(2, { algorithm: 'sha256', value: d2 }),
            makeStoredPart(3, { algorithm: 'sha256', value: d3 }),
        ];
        const got = await computeFinalChecksum(
            stored,
            partListFromStored(stored),
            { checksumAlgorithm: 'sha256', checksumType: 'COMPOSITE' },
            SPLITTER,
            uploadId,
            log,
        );
        assert(got);
        assert.strictEqual(got.algorithm, 'sha256');
        assert.strictEqual(got.type, 'COMPOSITE');
        assert(got.value.endsWith('-3'), `expected -N suffix, got ${got.value}`);
        // computeCompositeMPUChecksum's deterministic output for these
        // exact placeholder digests:
        const expected = crypto
            .createHash('sha256')
            .update(Buffer.concat([d1, d2, d3].map(x => Buffer.from(x, 'base64'))))
            .digest('base64');
        assert.strictEqual(got.value, `${expected}-3`);
    });

    ['sha1', 'crc32', 'crc32c'].forEach(algo => {
        it(`should compute COMPOSITE checksum for ${algo.toUpperCase()}`, async () => {
            const [d1, d2] = SAMPLE_DIGESTS[algo];
            const stored = [
                makeStoredPart(1, { algorithm: algo, value: d1 }),
                makeStoredPart(2, { algorithm: algo, value: d2 }),
            ];
            const got = await computeFinalChecksum(
                stored,
                partListFromStored(stored),
                { checksumAlgorithm: algo, checksumType: 'COMPOSITE' },
                SPLITTER,
                uploadId,
                log,
            );
            assert(got);
            assert.strictEqual(got.algorithm, algo);
            assert.strictEqual(got.type, 'COMPOSITE');
            assert(got.value.endsWith('-2'));
        });
    });

    it('should return FULL_OBJECT checksum without -N suffix for CRC64NVME', async () => {
        // Real CRCs over real bytes so we can verify against the equivalent
        // direct CRC of the concatenation.
        const a = crypto.randomBytes(1024);
        const b = crypto.randomBytes(2048);
        const dA = await algorithms.crc64nvme.digest(a);
        const dB = await algorithms.crc64nvme.digest(b);
        const stored = [
            {
                key: `${UPLOAD_ID}${SPLITTER}1`,
                value: {
                    ETag: 'e',
                    Size: a.length,
                    ChecksumAlgorithm: 'crc64nvme',
                    ChecksumValue: dA,
                    partLocations: [],
                },
            },
            {
                key: `${UPLOAD_ID}${SPLITTER}2`,
                value: {
                    ETag: 'e',
                    Size: b.length,
                    ChecksumAlgorithm: 'crc64nvme',
                    ChecksumValue: dB,
                    partLocations: [],
                },
            },
        ];
        const got = await computeFinalChecksum(
            stored,
            partListFromStored(stored),
            { checksumAlgorithm: 'crc64nvme', checksumType: 'FULL_OBJECT' },
            SPLITTER,
            uploadId,
            log,
        );
        assert(got);
        assert.strictEqual(got.algorithm, 'crc64nvme');
        assert.strictEqual(got.type, 'FULL_OBJECT');
        assert(!got.value.includes('-'), `FULL_OBJECT should have no -N suffix, got ${got.value}`);
        const expected = await algorithms.crc64nvme.digest(Buffer.concat([a, b]));
        assert.strictEqual(got.value, expected);
    });

    it('should return null and log when a part is missing ChecksumValue', async () => {
        const stored = [
            makeStoredPart(1, { algorithm: 'sha256', value: SAMPLE_DIGESTS.sha256[0] }),
            makeStoredPart(2, null),
            makeStoredPart(3, { algorithm: 'sha256', value: SAMPLE_DIGESTS.sha256[1] }),
        ];
        const got = await computeFinalChecksum(
            stored,
            partListFromStored(stored),
            { checksumAlgorithm: 'sha256', checksumType: 'COMPOSITE' },
            SPLITTER,
            uploadId,
            log,
        );
        assert.strictEqual(got, null);
    });

    it('should return null when checksumType is unknown', async () => {
        const stored = [makeStoredPart(1, { algorithm: 'sha256', value: SAMPLE_DIGESTS.sha256[0] })];
        const got = await computeFinalChecksum(
            stored,
            partListFromStored(stored),
            { checksumAlgorithm: 'sha256', checksumType: 'WEIRD' },
            SPLITTER,
            uploadId,
            log,
        );
        assert.strictEqual(got, null);
    });

    it(
        'should return null when underlying compute reports an error ' + '(crc64nvme COMPOSITE is not allowed)',
        async () => {
            const stored = [makeStoredPart(1, { algorithm: 'crc64nvme', value: SAMPLE_DIGESTS.crc64nvme[0] })];
            const got = await computeFinalChecksum(
                stored,
                partListFromStored(stored),
                { checksumAlgorithm: 'crc64nvme', checksumType: 'COMPOSITE' },
                SPLITTER,
                uploadId,
                log,
            );
            assert.strictEqual(got, null);
        },
    );

    it('should compute over filteredPartList (subset), not all storedParts', async () => {
        const [d1, d2, d3] = [SAMPLE_DIGESTS.sha256[0], SAMPLE_DIGESTS.sha256[1], SAMPLE_DIGESTS.sha256[0]];
        const stored = [
            makeStoredPart(1, { algorithm: 'sha256', value: d1 }),
            makeStoredPart(2, { algorithm: 'sha256', value: d2 }),
            makeStoredPart(3, { algorithm: 'sha256', value: d3 }),
        ];
        // User completes only parts 1 and 3, dropping 2 (orphan).
        const filtered = [stored[0], stored[2]].map(s => ({
            key: s.key,
            ETag: `"${s.value.ETag}"`,
            size: s.value.Size,
            locations: s.value.partLocations,
        }));
        const got = await computeFinalChecksum(
            stored,
            filtered,
            { checksumAlgorithm: 'sha256', checksumType: 'COMPOSITE' },
            SPLITTER,
            uploadId,
            log,
        );
        assert(got);
        assert(got.value.endsWith('-2'), `should reflect 2 completed parts, got ${got.value}`);
        const expected = crypto
            .createHash('sha256')
            .update(Buffer.concat([d1, d3].map(x => Buffer.from(x, 'base64'))))
            .digest('base64');
        assert.strictEqual(got.value, `${expected}-2`);
    });
});

describe('CompleteMultipartUpload final-object checksum storage', () => {
    const log = new DummyRequestLogger();
    const authInfo = makeAuthInfo('accessKey1');
    const namespace = 'default';
    const bucketName = 'bucketname-final-checksum';
    const objectKey = 'testObject';
    const partBody = Buffer.from('I am a part\n', 'utf8');
    const partHash = crypto.createHash('md5').update(partBody).digest('hex');

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

    // (algorithm, type) pairs valid for an MPU per AWS rules.
    // shouldStore reflects Part 3's gating: only FULL_OBJECT is persisted.
    const STORAGE_MATRIX = [
        { algorithm: 'crc32', type: 'FULL_OBJECT', shouldStore: true },
        { algorithm: 'crc32c', type: 'FULL_OBJECT', shouldStore: true },
        { algorithm: 'crc64nvme', type: 'FULL_OBJECT', shouldStore: true },
        { algorithm: 'crc32', type: 'COMPOSITE', shouldStore: false },
        { algorithm: 'crc32c', type: 'COMPOSITE', shouldStore: false },
        { algorithm: 'sha1', type: 'COMPOSITE', shouldStore: false },
        { algorithm: 'sha256', type: 'COMPOSITE', shouldStore: false },
    ];

    function bucketPutP() {
        return new Promise((resolve, reject) =>
            bucketPut(authInfo, bucketPutRequest, log, err => (err ? reject(err) : resolve())),
        );
    }

    function initiateMpuP(headers) {
        return new Promise((resolve, reject) => {
            initiateMultipartUpload(
                authInfo,
                {
                    bucketName,
                    namespace,
                    objectKey,
                    headers: { host: `${bucketName}.s3.amazonaws.com`, ...headers },
                    url: `/${objectKey}?uploads`,
                    actionImplicitDenies: false,
                },
                log,
                (err, xml) => {
                    if (err) {
                        return reject(err);
                    }
                    return parseString(xml, (parseErr, json) =>
                        parseErr ? reject(parseErr) : resolve(json.InitiateMultipartUploadResult.UploadId[0]),
                    );
                },
            );
        });
    }

    function uploadPartP(uploadId, headers = {}) {
        return new Promise((resolve, reject) => {
            const partRequest = new DummyRequest(
                {
                    bucketName,
                    namespace,
                    objectKey,
                    headers: { host: `${bucketName}.s3.amazonaws.com`, ...headers },
                    url: `/${objectKey}?partNumber=1&uploadId=${uploadId}`,
                    query: { partNumber: '1', uploadId },
                    partHash,
                    actionImplicitDenies: false,
                },
                partBody,
            );
            objectPutPart(authInfo, partRequest, undefined, log, err => (err ? reject(err) : resolve()));
        });
    }

    function completeMpuP(uploadId, partChecksumXml = '') {
        const completeBody =
            '<CompleteMultipartUpload>' +
            '<Part>' +
            '<PartNumber>1</PartNumber>' +
            `<ETag>"${partHash}"</ETag>${partChecksumXml}` +
            '</Part>' +
            '</CompleteMultipartUpload>';
        return new Promise((resolve, reject) => {
            completeMultipartUpload(
                authInfo,
                {
                    bucketName,
                    namespace,
                    objectKey,
                    parsedHost: 's3.amazonaws.com',
                    url: `/${objectKey}?uploadId=${uploadId}`,
                    headers: { host: `${bucketName}.s3.amazonaws.com` },
                    query: { uploadId },
                    post: completeBody,
                    actionImplicitDenies: false,
                },
                log,
                err => (err ? reject(err) : resolve()),
            );
        });
    }

    function fetchObjectMDP() {
        return new Promise((resolve, reject) =>
            metadata.getObjectMD(bucketName, objectKey, {}, log, (err, md) => (err ? reject(err) : resolve(md))),
        );
    }

    beforeEach(() => cleanup());

    STORAGE_MATRIX.forEach(({ algorithm, type, shouldStore }) => {
        const upper = algorithm.toUpperCase();
        const verb = shouldStore ? 'should persist' : 'should not persist';
        const tag = TAG_BY_ALGO[algorithm];

        it(`${verb} ${type} ${upper} checksum on the ObjectMD`, async () => {
            await bucketPutP();
            const uploadId = await initiateMpuP({
                'x-amz-checksum-algorithm': upper,
                'x-amz-checksum-type': type,
            });
            // Pre-compute the part's checksum so we can supply it on
            // UploadPart and (for COMPOSITE non-default) in the Complete body.
            const partChecksum = await algorithms[algorithm].digest(partBody);
            const uploadHeaders = type === 'COMPOSITE' ? { [`x-amz-checksum-${algorithm}`]: partChecksum } : {};
            await uploadPartP(uploadId, uploadHeaders);
            const partChecksumXml = type === 'COMPOSITE' ? `<${tag}>${partChecksum}</${tag}>` : '';
            await completeMpuP(uploadId, partChecksumXml);
            const md = await fetchObjectMDP();
            if (shouldStore) {
                assert(md.checksum, `expected ${type} ${upper} checksum on ObjectMD`);
                assert.strictEqual(md.checksum.checksumAlgorithm, algorithm);
                assert.strictEqual(md.checksum.checksumType, type);
                assert(typeof md.checksum.checksumValue === 'string');
                assert(md.checksum.checksumValue.length > 0);
            } else {
                assert.strictEqual(md.checksum, undefined, `${type} ${upper} should not persist on ObjectMD`);
            }
        });
    });

    it('should persist FULL_OBJECT CRC64NVME checksum for default MPU (no checksum headers)', async () => {
        // No x-amz-checksum-algorithm / x-amz-checksum-type headers — AWS
        // defaults to crc64nvme/FULL_OBJECT and still persists the result.
        await bucketPutP();
        const uploadId = await initiateMpuP({});
        await uploadPartP(uploadId);
        await completeMpuP(uploadId);
        const md = await fetchObjectMDP();
        assert(md.checksum, 'default MPU should still persist a checksum');
        assert.strictEqual(md.checksum.checksumAlgorithm, 'crc64nvme');
        assert.strictEqual(md.checksum.checksumType, 'FULL_OBJECT');
    });

    it('should not leak checksumAlgorithm/Type/IsDefault into ObjectMD top-level fields', async () => {
        // keysNotNeeded keeps these MPU-overview-only keys out of metaHeaders,
        // which prevents them from sticking around on the final ObjectMD.
        await bucketPutP();
        const uploadId = await initiateMpuP({
            'x-amz-checksum-algorithm': 'CRC32',
            'x-amz-checksum-type': 'FULL_OBJECT',
        });
        await uploadPartP(uploadId);
        await completeMpuP(uploadId);
        const md = await fetchObjectMDP();
        assert.strictEqual(md.checksumAlgorithm, undefined);
        assert.strictEqual(md.checksumType, undefined);
        assert.strictEqual(md.checksumIsDefault, undefined);
    });
});
