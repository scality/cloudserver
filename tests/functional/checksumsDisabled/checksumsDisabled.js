const assert = require('assert');
const crypto = require('crypto');
const {
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutObjectCommand,
    GetObjectCommand,
    HeadObjectCommand,
    CopyObjectCommand,
    GetObjectAttributesCommand,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    UploadPartCopyCommand,
    CompleteMultipartUploadCommand,
    AbortMultipartUploadCommand,
    ListPartsCommand,
    PutBucketTaggingCommand,
    DeleteObjectCommand,
} = require('@aws-sdk/client-s3');

const getConfig = require('../aws-node-sdk/test/support/config');
const BucketUtility = require('../aws-node-sdk/lib/utility/bucket-util');

/*
 * These tests require CloudServer to be running with checksums DISABLED:
 *
 *     S3_INTEGRITY_CHECKS_ENABLED=false
 *
 * or `integrityChecks: { enabled: false }` in config.json. They assert the
 * opposite of the normal checksum suites, so they are deliberately kept out of
 * tests/functional/aws-node-sdk/test/ — `yarn ft_awssdk` runs that tree against
 * a server with checksums on, where every assertion here would fail.
 *
 * Run with: yarn ft_checksums_disabled
 */

const bucket = `checksums-disabled-${Date.now()}`;
const body = Buffer.from('I am the body of an object', 'utf8');
const bodyMd5 = crypto.createHash('md5').update(body).digest('base64');
// A syntactically valid CRC32 that does not match `body`.
const wrongCrc32 = 'AAAAAA==';
// 5MB, the minimum size for a non-final MPU part.
const partBody = Buffer.alloc(5 * 1024 * 1024, 'a');

// Every checksum field the SDK may surface on a response.
const CHECKSUM_FIELDS = [
    'ChecksumCRC32',
    'ChecksumCRC32C',
    'ChecksumCRC64NVME',
    'ChecksumSHA1',
    'ChecksumSHA256',
    'ChecksumType',
];

function assertNoChecksum(res, context) {
    CHECKSUM_FIELDS.forEach(field => {
        assert.strictEqual(res[field], undefined, `${context}: expected no ${field}, got ${res[field]}`);
    });
}

describe('with checksums disabled', () => {
    let s3;
    let bucketUtil;

    before(async () => {
        bucketUtil = new BucketUtility('default', {});
        s3 = new S3Client({ ...getConfig('default', {}), maxAttempts: 0 });
        await s3.send(new CreateBucketCommand({ Bucket: bucket }));

        // Fail fast and loudly rather than emitting a wall of confusing
        // assertion errors if the server was started with checksums enabled.
        const probe = await s3.send(new PutObjectCommand({ Bucket: bucket, Key: 'probe', Body: body }));
        await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: 'probe' }));
        if (CHECKSUM_FIELDS.some(f => probe[f] !== undefined)) {
            throw new Error(
                'This suite requires CloudServer running with checksums disabled ' +
                    '(S3_INTEGRITY_CHECKS_ENABLED=false); the server returned a checksum.',
            );
        }
    });

    after(async () => {
        await bucketUtil.empty(bucket);
        await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
    });

    describe('PutObject', () => {
        it('should not return a checksum when none is requested', async () => {
            const res = await s3.send(new PutObjectCommand({ Bucket: bucket, Key: 'plain', Body: body }));
            assertNoChecksum(res, 'PutObject');
        });

        it('should accept a checksum that does not match the body', async () => {
            // Enabled, this is BadDigest.
            const res = await s3.send(
                new PutObjectCommand({
                    Bucket: bucket,
                    Key: 'wrong-checksum',
                    Body: body,
                    ChecksumCRC32: wrongCrc32,
                }),
            );
            assertNoChecksum(res, 'PutObject with a wrong checksum');
        });

        it('should not return a checksum on GET, HEAD or GetObjectAttributes', async () => {
            await s3.send(new PutObjectCommand({ Bucket: bucket, Key: 'readback', Body: body }));

            const get = await s3.send(
                new GetObjectCommand({ Bucket: bucket, Key: 'readback', ChecksumMode: 'ENABLED' }),
            );
            assertNoChecksum(get, 'GetObject');
            await get.Body.transformToByteArray();

            const head = await s3.send(
                new HeadObjectCommand({ Bucket: bucket, Key: 'readback', ChecksumMode: 'ENABLED' }),
            );
            assertNoChecksum(head, 'HeadObject');

            const attrs = await s3.send(
                new GetObjectAttributesCommand({
                    Bucket: bucket,
                    Key: 'readback',
                    ObjectAttributes: ['Checksum', 'ETag'],
                }),
            );
            assert.strictEqual(attrs.Checksum, undefined, 'GetObjectAttributes should report no Checksum');
            assert(attrs.ETag, 'GetObjectAttributes should still report an ETag');
        });

        it('should still enforce Content-MD5', async () => {
            const wrongMd5 = crypto.createHash('md5').update('not the body').digest('base64');
            await assert.rejects(
                s3.send(
                    new PutObjectCommand({
                        Bucket: bucket,
                        Key: 'bad-md5',
                        Body: body,
                        ContentMD5: wrongMd5,
                    }),
                ),
                err => err.name === 'BadDigest' || err.Code === 'BadDigest',
            );
        });

        it('should accept a correct Content-MD5', async () => {
            const res = await s3.send(
                new PutObjectCommand({ Bucket: bucket, Key: 'good-md5', Body: body, ContentMD5: bodyMd5 }),
            );
            assertNoChecksum(res, 'PutObject with a valid Content-MD5');
        });

        it('should store no checksum for a zero-byte object', async () => {
            await s3.send(new PutObjectCommand({ Bucket: bucket, Key: 'empty', Body: Buffer.alloc(0) }));
            const head = await s3.send(
                new HeadObjectCommand({ Bucket: bucket, Key: 'empty', ChecksumMode: 'ENABLED' }),
            );
            assertNoChecksum(head, 'HeadObject on a zero-byte object');
        });
    });

    describe('CopyObject', () => {
        it('should not carry a checksum to the destination', async () => {
            await s3.send(new PutObjectCommand({ Bucket: bucket, Key: 'copy-src', Body: body }));
            const res = await s3.send(
                new CopyObjectCommand({
                    Bucket: bucket,
                    Key: 'copy-dst',
                    CopySource: `/${bucket}/copy-src`,
                }),
            );
            assertNoChecksum(res.CopyObjectResult || {}, 'CopyObject');

            const head = await s3.send(
                new HeadObjectCommand({ Bucket: bucket, Key: 'copy-dst', ChecksumMode: 'ENABLED' }),
            );
            assertNoChecksum(head, 'HeadObject on the copy');
        });

        it('should ignore a requested checksum algorithm', async () => {
            const res = await s3.send(
                new CopyObjectCommand({
                    Bucket: bucket,
                    Key: 'copy-dst-sha256',
                    CopySource: `/${bucket}/copy-src`,
                    ChecksumAlgorithm: 'SHA256',
                }),
            );
            assertNoChecksum(res.CopyObjectResult || {}, 'CopyObject with ChecksumAlgorithm');
        });
    });

    describe('multipart upload', () => {
        async function runMpu(key, createParams, partParams) {
            const create = await s3.send(
                new CreateMultipartUploadCommand({ Bucket: bucket, Key: key, ...createParams }),
            );
            const uploadId = create.UploadId;
            try {
                const part = await s3.send(
                    new UploadPartCommand({
                        Bucket: bucket,
                        Key: key,
                        UploadId: uploadId,
                        PartNumber: 1,
                        Body: partBody,
                        ...partParams,
                    }),
                );
                const complete = await s3.send(
                    new CompleteMultipartUploadCommand({
                        Bucket: bucket,
                        Key: key,
                        UploadId: uploadId,
                        MultipartUpload: { Parts: [{ ETag: part.ETag, PartNumber: 1 }] },
                    }),
                );
                return { create, part, complete, uploadId };
            } catch (err) {
                await s3
                    .send(new AbortMultipartUploadCommand({ Bucket: bucket, Key: key, UploadId: uploadId }))
                    .catch(() => {});
                throw err;
            }
        }

        it('should not echo a checksum algorithm from CreateMultipartUpload', async () => {
            const { create, part, complete } = await runMpu('mpu-explicit', {
                ChecksumAlgorithm: 'CRC32',
            });
            assert.strictEqual(create.ChecksumAlgorithm, undefined);
            assert.strictEqual(create.ChecksumType, undefined);
            assertNoChecksum(part, 'UploadPart');
            assertNoChecksum(complete, 'CompleteMultipartUpload');
        });

        it('should complete an MPU created with an explicit algorithm and no per-part checksums', async () => {
            // Enabled, a CRC32 MPU is COMPOSITE and UploadPart would reject a
            // part carrying no x-amz-checksum-crc32.
            const { complete } = await runMpu('mpu-no-part-checksums', { ChecksumAlgorithm: 'CRC32' });
            assert(complete.ETag, 'CompleteMultipartUpload should succeed');
            assertNoChecksum(complete, 'CompleteMultipartUpload');
        });

        it('should accept a per-part checksum that does not match the part', async () => {
            const { complete } = await runMpu(
                'mpu-wrong-part-checksum',
                { ChecksumAlgorithm: 'CRC32' },
                { ChecksumCRC32: wrongCrc32 },
            );
            assert(complete.ETag);
        });

        it('should report no checksum in ListParts', async () => {
            const create = await s3.send(
                new CreateMultipartUploadCommand({
                    Bucket: bucket,
                    Key: 'mpu-listparts',
                    ChecksumAlgorithm: 'CRC32',
                }),
            );
            const part = await s3.send(
                new UploadPartCommand({
                    Bucket: bucket,
                    Key: 'mpu-listparts',
                    UploadId: create.UploadId,
                    PartNumber: 1,
                    Body: partBody,
                }),
            );
            assert(part.ETag);

            const list = await s3.send(
                new ListPartsCommand({ Bucket: bucket, Key: 'mpu-listparts', UploadId: create.UploadId }),
            );
            assert.strictEqual(list.ChecksumAlgorithm, undefined);
            (list.Parts || []).forEach(p => assertNoChecksum(p, 'ListParts part'));

            await s3.send(
                new AbortMultipartUploadCommand({
                    Bucket: bucket,
                    Key: 'mpu-listparts',
                    UploadId: create.UploadId,
                }),
            );
        });

        it('should not store a checksum on UploadPartCopy', async () => {
            await s3.send(new PutObjectCommand({ Bucket: bucket, Key: 'copypart-src', Body: partBody }));
            const create = await s3.send(
                new CreateMultipartUploadCommand({
                    Bucket: bucket,
                    Key: 'mpu-copypart',
                    ChecksumAlgorithm: 'CRC32',
                }),
            );
            const copied = await s3.send(
                new UploadPartCopyCommand({
                    Bucket: bucket,
                    Key: 'mpu-copypart',
                    UploadId: create.UploadId,
                    PartNumber: 1,
                    CopySource: `/${bucket}/copypart-src`,
                }),
            );
            assertNoChecksum(copied.CopyPartResult || {}, 'UploadPartCopy');

            const complete = await s3.send(
                new CompleteMultipartUploadCommand({
                    Bucket: bucket,
                    Key: 'mpu-copypart',
                    UploadId: create.UploadId,
                    MultipartUpload: {
                        Parts: [{ ETag: copied.CopyPartResult.ETag, PartNumber: 1 }],
                    },
                }),
            );
            assertNoChecksum(complete, 'CompleteMultipartUpload after UploadPartCopy');
        });

        it('should not store a checksum on a ranged UploadPartCopy', async () => {
            // A copy-source range always forces a recompute when enabled.
            const create = await s3.send(
                new CreateMultipartUploadCommand({
                    Bucket: bucket,
                    Key: 'mpu-copypart-range',
                    ChecksumAlgorithm: 'CRC32',
                }),
            );
            const copied = await s3.send(
                new UploadPartCopyCommand({
                    Bucket: bucket,
                    Key: 'mpu-copypart-range',
                    UploadId: create.UploadId,
                    PartNumber: 1,
                    CopySource: `/${bucket}/copypart-src`,
                    CopySourceRange: `bytes=0-${partBody.length - 1}`,
                }),
            );
            assertNoChecksum(copied.CopyPartResult || {}, 'ranged UploadPartCopy');

            await s3.send(
                new AbortMultipartUploadCommand({
                    Bucket: bucket,
                    Key: 'mpu-copypart-range',
                    UploadId: create.UploadId,
                }),
            );
        });
    });

    describe('buffered-body endpoints', () => {
        it('should accept a wrong x-amz-checksum on PutBucketTagging', async () => {
            // Enabled, this is BadDigest.
            await s3.send(
                new PutBucketTaggingCommand({
                    Bucket: bucket,
                    Tagging: { TagSet: [{ Key: 'k', Value: 'v' }] },
                    ChecksumCRC32: wrongCrc32,
                }),
            );
        });
    });
});
