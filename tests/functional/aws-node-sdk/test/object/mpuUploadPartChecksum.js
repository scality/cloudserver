const assert = require('assert');
const {
    CreateBucketCommand,
    CreateMultipartUploadCommand,
    AbortMultipartUploadCommand,
    UploadPartCommand,
    DeleteBucketCommand,
    ListPartsCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { algorithms } = require('../../../../../lib/api/apiUtils/integrity/validateChecksums');

const bucket = `mpu-part-checksum-test-${Date.now()}`;
const key = 'test-part-checksum-key';
const partBody = Buffer.from('I am a part body for checksum testing', 'utf8');

const allAlgos = ['CRC32', 'CRC32C', 'SHA1', 'SHA256'];

// Maps algo name to the UploadPartCommand checksum field name
const checksumField = {
    CRC32: 'ChecksumCRC32',
    CRC32C: 'ChecksumCRC32C',
    SHA1: 'ChecksumSHA1',
    SHA256: 'ChecksumSHA256',
};

// Pre-compute correct digests for partBody
const correctDigest = {};
// A valid-length but incorrect digest for each algo
const wrongDigest = {};

before(async () => {
    for (const algo of allAlgos) {
        correctDigest[algo] = await algorithms[algo.toLowerCase()].digest(partBody);
    }
    // Generate wrong digests: flip the first character
    for (const algo of allAlgos) {
        const correct = correctDigest[algo];
        const flipped = correct[0] === 'A' ? `B${correct.slice(1)}` : `A${correct.slice(1)}`;
        wrongDigest[algo] = flipped;
    }
});

async function assertPartChecksumStored(s3, uploadId, partNumber,
    checksumHeader, expectedChecksum) {
    const listRes = await s3.send(new ListPartsCommand({
        Bucket: bucket,
        Key: key,
        UploadId: uploadId,
    }));
    const found = listRes.Parts.find(part => part.PartNumber === partNumber);
    assert(found, `Expected part ${partNumber} in ListParts response`);
    assert.strictEqual(found[checksumHeader], expectedChecksum);
}

describe('UploadPart checksum validation', () =>
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        before(async () => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));
        });

        after(async () => {
            await bucketUtil.empty(bucket);
            await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
        });

        // For each non-default MPU algo, test that:
        // - matching algo with correct digest succeeds
        // - matching algo with wrong digest fails with BadDigest
        // - every other algo is rejected with InvalidRequest
        // - no checksum header is accepted
        allAlgos.forEach(mpuAlgo => {
            describe(`MPU created with ${mpuAlgo}`, () => {
                let uploadId;

                before(async () => {
                    const res = await s3.send(new CreateMultipartUploadCommand({
                        Bucket: bucket, Key: key,
                        ChecksumAlgorithm: mpuAlgo,
                    }));
                    uploadId = res.UploadId;
                });

                after(async () => {
                    await s3.send(new AbortMultipartUploadCommand({
                        Bucket: bucket, Key: key, UploadId: uploadId,
                    }));
                });

                it(`should accept ${mpuAlgo} with correct digest`, async () => {
                    const partNumber = 1;
                    const res = await s3.send(new UploadPartCommand({
                        Bucket: bucket, Key: key, UploadId: uploadId,
                        PartNumber: partNumber, Body: partBody,
                        [checksumField[mpuAlgo]]: correctDigest[mpuAlgo],
                    }));
                    assert.strictEqual(res[checksumField[mpuAlgo]], correctDigest[mpuAlgo]);
                    await assertPartChecksumStored(s3, uploadId, partNumber,
                        checksumField[mpuAlgo], correctDigest[mpuAlgo]);
                });

                it(`should reject ${mpuAlgo} with wrong digest (BadDigest)`, async () => {
                    await assert.rejects(
                        s3.send(new UploadPartCommand({
                            Bucket: bucket, Key: key, UploadId: uploadId,
                            PartNumber: 2, Body: partBody,
                            [checksumField[mpuAlgo]]: wrongDigest[mpuAlgo],
                        })),
                        { name: 'BadDigest' },
                    );
                });

                // Note: AWS SDK v3 always sends a default crc32 checksum,
                // so "no checksum header" cannot be tested via the SDK for
                // non-default MPUs (it would be rejected as a mismatch).

                allAlgos.filter(a => a !== mpuAlgo).forEach((otherAlgo, idx) => {
                    it(`should reject ${otherAlgo} when MPU is ${mpuAlgo} (InvalidRequest)`, async () => {
                        await assert.rejects(
                            s3.send(new UploadPartCommand({
                                Bucket: bucket, Key: key, UploadId: uploadId,
                                PartNumber: 3 + idx, Body: partBody,
                                [checksumField[otherAlgo]]: correctDigest[otherAlgo],
                            })),
                            err => {
                                assert.strictEqual(err.name, 'InvalidRequest',
                                    `expected InvalidRequest, got ${err.name}: ${err.message}`);
                                // AWS names the expected (MPU) and actual (sent) algorithms.
                                assert.match(err.message, new RegExp(
                                    `expected checksum Type: ${mpuAlgo.toLowerCase()}, ` +
                                    `actual checksum Type: ${otherAlgo.toLowerCase()}`));
                                return true;
                            },
                        );
                    });
                });
            });
        });

        // Default MPU (no ChecksumAlgorithm) should accept any algo
        describe('MPU created with no checksum (default)', () => {
            let uploadId;

            before(async () => {
                const res = await s3.send(new CreateMultipartUploadCommand({
                    Bucket: bucket, Key: key,
                }));
                uploadId = res.UploadId;
            });

            after(async () => {
                await s3.send(new AbortMultipartUploadCommand({
                    Bucket: bucket, Key: key, UploadId: uploadId,
                }));
            });

            allAlgos.forEach((algo, idx) => {
                it(`should accept ${algo} with correct digest`, async () => {
                    const res = await s3.send(new UploadPartCommand({
                        Bucket: bucket, Key: key, UploadId: uploadId,
                        PartNumber: 2 * idx + 1, Body: partBody,
                        [checksumField[algo]]: correctDigest[algo],
                    }));
                    assert.strictEqual(res[checksumField[algo]], correctDigest[algo]);
                });

                it(`should reject ${algo} with wrong digest (BadDigest)`, async () => {
                    await assert.rejects(
                        s3.send(new UploadPartCommand({
                            Bucket: bucket, Key: key, UploadId: uploadId,
                            PartNumber: 2 * idx + 2, Body: partBody,
                            [checksumField[algo]]: wrongDigest[algo],
                        })),
                        { name: 'BadDigest' },
                    );
                });
            });

            it('should return no per-part checksum when none is sent', async () => {
                // WHEN_REQUIRED so the SDK does not auto-attach a crc32: the
                // part is genuinely uploaded with no checksum.
                const noCksumS3 = new BucketUtility('default', {
                    ...sigCfg,
                    requestChecksumCalculation: 'WHEN_REQUIRED',
                    responseChecksumValidation: 'WHEN_REQUIRED',
                }).s3;
                const res = await noCksumS3.send(new UploadPartCommand({
                    Bucket: bucket, Key: key, UploadId: uploadId,
                    PartNumber: 2 * allAlgos.length + 1, Body: partBody,
                }));
                assert(res.ETag);
                const present = ['ChecksumCRC32', 'ChecksumCRC32C', 'ChecksumCRC64NVME',
                    'ChecksumSHA1', 'ChecksumSHA256'].filter(f => res[f] !== undefined);
                assert.deepStrictEqual(present, [],
                    `default MPU UploadPart should return no checksum, got: ${present.join(', ')}`);
            });
        });

        describe('per-part checksum requirement by checksum type', () => {
            // WHEN_REQUIRED so the SDK does not auto-attach a checksum, letting
            // us upload a genuinely checksum-less part.
            let noCksumS3;
            const openUploads = [];

            before(() => {
                noCksumS3 = new BucketUtility('default', {
                    ...sigCfg,
                    requestChecksumCalculation: 'WHEN_REQUIRED',
                    responseChecksumValidation: 'WHEN_REQUIRED',
                }).s3;
            });

            after(async () => {
                await Promise.all(openUploads.map(uploadId =>
                    noCksumS3.send(new AbortMultipartUploadCommand({
                        Bucket: bucket, Key: key, UploadId: uploadId,
                    })).catch(() => undefined)));
            });

            async function createMpu(algo, type) {
                const res = await noCksumS3.send(new CreateMultipartUploadCommand({
                    Bucket: bucket, Key: key, ChecksumAlgorithm: algo, ChecksumType: type,
                }));
                openUploads.push(res.UploadId);
                return res.UploadId;
            }

            ['CRC32', 'CRC32C', 'SHA1', 'SHA256'].forEach(algo => {
                it(`should reject UploadPart with no checksum on a ${algo}/COMPOSITE MPU`, async () => {
                    const uploadId = await createMpu(algo, 'COMPOSITE');
                    await assert.rejects(
                        noCksumS3.send(new UploadPartCommand({
                            Bucket: bucket, Key: key, UploadId: uploadId,
                            PartNumber: 1, Body: partBody,
                        })),
                        err => {
                            assert.strictEqual(err.name, 'InvalidRequest',
                                `expected InvalidRequest, got ${err.name}: ${err.message}`);
                            assert.match(err.message,
                                new RegExp(`expected checksum Type: ${algo.toLowerCase()}`));
                            return true;
                        },
                    );
                });
            });

            ['CRC32', 'CRC32C', 'CRC64NVME'].forEach(algo => {
                it(`should accept UploadPart with no checksum on a ${algo}/FULL_OBJECT MPU`, async () => {
                    const uploadId = await createMpu(algo, 'FULL_OBJECT');
                    const res = await noCksumS3.send(new UploadPartCommand({
                        Bucket: bucket, Key: key, UploadId: uploadId,
                        PartNumber: 1, Body: partBody,
                    }));
                    assert(res.ETag);
                    assert(res[`Checksum${algo}`],
                        `expected Checksum${algo} echoed, got: ${JSON.stringify(res)}`);
                });
            });
        });
    })
);
