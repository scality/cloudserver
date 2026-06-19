const assert = require('assert');
const {
    CreateBucketCommand,
    PutObjectCommand,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    UploadPartCopyCommand,
    CompleteMultipartUploadCommand,
    ListPartsCommand,
    AbortMultipartUploadCommand,
    DeleteBucketCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { algorithms } = require('../../../../../lib/api/apiUtils/integrity/validateChecksums');

const bucket = `copypart-checksum-${Date.now()}`;
const sourceKey = 'copypart-checksum-source';
const sourceBody = Buffer.from('UploadPartCopy checksum source content', 'utf8');
const bigSourceKey = 'copypart-checksum-big-source';
const bigBody = Buffer.alloc(5 * 1024 * 1024, 0x61);

// algo -> the SDK CopyPartResult / CompletedPart field name
const field = algo => `Checksum${algo}`;
const allFields = ['CRC32', 'CRC32C', 'CRC64NVME', 'SHA1', 'SHA256'].map(field);
const digest = (algo, body) => algorithms[algo.toLowerCase()].digest(body);

describe('UploadPartCopy checksums', () =>
    withV4(sigCfg => {
        let s3;
        let bucketUtil;
        const openUploads = [];

        before(async () => {
            // WHEN_REQUIRED so the SDK does not auto-attach checksums on
            // CreateMPU/CompleteMPU and muddy the assertions. UploadPartCopy
            // itself never sends a body checksum.
            bucketUtil = new BucketUtility('default', {
                ...sigCfg,
                requestChecksumCalculation: 'WHEN_REQUIRED',
                responseChecksumValidation: 'WHEN_REQUIRED',
            });
            s3 = bucketUtil.s3;
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));
            // Source stored without a checksum, so the copy always recomputes.
            await s3.send(new PutObjectCommand({ Bucket: bucket, Key: sourceKey, Body: sourceBody }));
            await s3.send(new PutObjectCommand({ Bucket: bucket, Key: bigSourceKey, Body: bigBody }));
        });

        after(async () => {
            await Promise.all(openUploads.map(u =>
                s3.send(new AbortMultipartUploadCommand({
                    Bucket: bucket, Key: u.key, UploadId: u.uploadId,
                })).catch(() => undefined)));
            await bucketUtil.empty(bucket);
            await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
        });

        async function createMpu(key, opts = {}) {
            const res = await s3.send(new CreateMultipartUploadCommand({
                Bucket: bucket, Key: key, ...opts,
            }));
            openUploads.push({ key, uploadId: res.UploadId });
            return res.UploadId;
        }

        function copyPart(key, uploadId, extra = {}) {
            return s3.send(new UploadPartCopyCommand({
                Bucket: bucket, Key: key, UploadId: uploadId,
                PartNumber: 1, CopySource: `${bucket}/${sourceKey}`, ...extra,
            }));
        }

        ['CRC32', 'CRC32C', 'CRC64NVME', 'SHA1', 'SHA256'].forEach(algo => {
            it(`should return the recomputed ${algo} checksum in CopyPartResult`, async () => {
                const key = `cpr-${algo}`;
                const uploadId = await createMpu(key, { ChecksumAlgorithm: algo });
                const res = await copyPart(key, uploadId);
                assert.strictEqual(res.CopyPartResult[field(algo)], await digest(algo, sourceBody));
            });
        });

        it('should not return any checksum in CopyPartResult for a default MPU', async () => {
            const key = 'cpr-default';
            const uploadId = await createMpu(key);
            const res = await copyPart(key, uploadId);
            assert(res.CopyPartResult.ETag);
            allFields.forEach(f => assert.strictEqual(res.CopyPartResult[f], undefined,
                `default MPU CopyPartResult should not include ${f}`));
        });

        it('should recompute in the MPU algorithm when the source has a different one', async () => {
            // Source stored with CRC32; destination MPU is SHA256 -> recompute.
            const srcCrc32 = 'copypart-checksum-source-crc32';
            await s3.send(new PutObjectCommand({
                Bucket: bucket, Key: srcCrc32, Body: sourceBody, ChecksumAlgorithm: 'CRC32',
            }));
            const key = 'cpr-mismatch';
            const uploadId = await createMpu(key, { ChecksumAlgorithm: 'SHA256' });
            const res = await copyPart(key, uploadId, { CopySource: `${bucket}/${srcCrc32}` });
            assert.strictEqual(res.CopyPartResult.ChecksumSHA256, await digest('SHA256', sourceBody));
        });

        it('should checksum only the copied byte range', async () => {
            const key = 'cpr-range';
            const uploadId = await createMpu(key, { ChecksumAlgorithm: 'CRC32' });
            const res = await copyPart(key, uploadId, { CopySourceRange: 'bytes=0-3' });
            assert.strictEqual(res.CopyPartResult.ChecksumCRC32,
                await digest('CRC32', sourceBody.subarray(0, 4)));
        });

        it('should checksum a 0-byte copied part', async () => {
            const emptyKey = 'copypart-checksum-empty';
            await s3.send(new PutObjectCommand({ Bucket: bucket, Key: emptyKey, Body: '' }));
            const key = 'cpr-empty';
            const uploadId = await createMpu(key, { ChecksumAlgorithm: 'CRC32' });
            const res = await copyPart(key, uploadId, { CopySource: `${bucket}/${emptyKey}` });
            assert.strictEqual(res.CopyPartResult.ChecksumCRC32, await digest('CRC32', Buffer.alloc(0)));
        });

        [
            { algo: 'CRC32', type: 'COMPOSITE' },
            { algo: 'CRC32', type: 'FULL_OBJECT' },
            { algo: 'CRC32C', type: 'FULL_OBJECT' },
            { algo: 'SHA1', type: 'COMPOSITE' },
            { algo: 'SHA256', type: 'COMPOSITE' },
            { algo: 'CRC64NVME', type: 'FULL_OBJECT' },
        ].forEach(({ algo, type }) => {
            it(`should complete an MPU with a copied part (${algo}/${type})`, async () => {
                const key = `cmp-${algo}-${type}`;
                const uploadId = await createMpu(key, { ChecksumAlgorithm: algo, ChecksumType: type });
                const copy = await copyPart(key, uploadId);
                const partChecksum = copy.CopyPartResult[field(algo)];
                assert(partChecksum, `expected ${field(algo)} in CopyPartResult`);
                const complete = await s3.send(new CompleteMultipartUploadCommand({
                    Bucket: bucket, Key: key, UploadId: uploadId,
                    MultipartUpload: {
                        Parts: [{
                            PartNumber: 1,
                            ETag: copy.CopyPartResult.ETag,
                            [field(algo)]: partChecksum,
                        }],
                    },
                }));
                assert.strictEqual(complete.ChecksumType, type);
                assert(complete[field(algo)], `expected final ${field(algo)} on CompleteMPU response`);
                if (type === 'COMPOSITE') {
                    assert(complete[field(algo)].endsWith('-1'),
                        `expected -1 suffix for 1-part COMPOSITE, got ${complete[field(algo)]}`);
                }
            });
        });

        it('should complete a default MPU with a copied part', async () => {
            const key = 'cmp-default';
            const uploadId = await createMpu(key);
            const copy = await copyPart(key, uploadId);
            const complete = await s3.send(new CompleteMultipartUploadCommand({
                Bucket: bucket, Key: key, UploadId: uploadId,
                MultipartUpload: { Parts: [{ PartNumber: 1, ETag: copy.CopyPartResult.ETag }] },
            }));
            assert(complete.ChecksumCRC64NVME, 'expected default-MPU final ChecksumCRC64NVME');
            assert.strictEqual(complete.ChecksumType, 'FULL_OBJECT');
        });

        it('should surface the copied part checksum in ListParts for an explicit MPU', async () => {
            const key = 'lp-explicit';
            const uploadId = await createMpu(key, { ChecksumAlgorithm: 'CRC32' });
            await copyPart(key, uploadId);
            const list = await s3.send(new ListPartsCommand({ Bucket: bucket, Key: key, UploadId: uploadId }));
            assert.strictEqual(list.Parts[0].ChecksumCRC32, await digest('CRC32', sourceBody));
        });

        it('should not surface a checksum in ListParts for a default MPU', async () => {
            const key = 'lp-default';
            const uploadId = await createMpu(key);
            await copyPart(key, uploadId);
            const list = await s3.send(new ListPartsCommand({ Bucket: bucket, Key: key, UploadId: uploadId }));
            allFields.forEach(f => assert.strictEqual(list.Parts[0][f], undefined,
                `default MPU ListParts should not include ${f}`));
        });

        it('should complete a multi-part COMPOSITE MPU of copied parts with the -N suffix', async () => {
            const key = 'cmp-multi-composite';
            const uploadId = await createMpu(key, { ChecksumAlgorithm: 'CRC32', ChecksumType: 'COMPOSITE' });
            const p1 = await copyPart(key, uploadId, { PartNumber: 1, CopySource: `${bucket}/${bigSourceKey}` });
            const p2 = await copyPart(key, uploadId, { PartNumber: 2 });
            const complete = await s3.send(new CompleteMultipartUploadCommand({
                Bucket: bucket, Key: key, UploadId: uploadId,
                MultipartUpload: { Parts: [
                    { PartNumber: 1, ETag: p1.CopyPartResult.ETag, ChecksumCRC32: p1.CopyPartResult.ChecksumCRC32 },
                    { PartNumber: 2, ETag: p2.CopyPartResult.ETag, ChecksumCRC32: p2.CopyPartResult.ChecksumCRC32 },
                ] },
            }));
            assert.strictEqual(complete.ChecksumType, 'COMPOSITE');
            assert(complete.ChecksumCRC32.endsWith('-2'),
                `expected -2 suffix for a 2-part COMPOSITE, got ${complete.ChecksumCRC32}`);
        });

        it('should complete a multi-part FULL_OBJECT MPU of copied parts with the linear digest', async () => {
            const key = 'cmp-multi-full';
            const uploadId = await createMpu(key, { ChecksumAlgorithm: 'CRC32', ChecksumType: 'FULL_OBJECT' });
            const p1 = await copyPart(key, uploadId, { PartNumber: 1, CopySource: `${bucket}/${bigSourceKey}` });
            const p2 = await copyPart(key, uploadId, { PartNumber: 2 });
            const complete = await s3.send(new CompleteMultipartUploadCommand({
                Bucket: bucket, Key: key, UploadId: uploadId,
                MultipartUpload: { Parts: [
                    { PartNumber: 1, ETag: p1.CopyPartResult.ETag, ChecksumCRC32: p1.CopyPartResult.ChecksumCRC32 },
                    { PartNumber: 2, ETag: p2.CopyPartResult.ETag, ChecksumCRC32: p2.CopyPartResult.ChecksumCRC32 },
                ] },
            }));
            assert.strictEqual(complete.ChecksumType, 'FULL_OBJECT');
            assert.strictEqual(complete.ChecksumCRC32,
                await digest('CRC32', Buffer.concat([bigBody, sourceBody])));
        });

        it('should reuse a matching source checksum without recomputing', async () => {
            const srcKey = 'copypart-checksum-reuse-src';
            await s3.send(new PutObjectCommand({
                Bucket: bucket, Key: srcKey, Body: sourceBody, ChecksumAlgorithm: 'CRC32',
            }));
            const key = 'cpr-reuse';
            const uploadId = await createMpu(key, { ChecksumAlgorithm: 'CRC32' });
            const res = await copyPart(key, uploadId, { CopySource: `${bucket}/${srcKey}` });
            assert.strictEqual(res.CopyPartResult.ChecksumCRC32, await digest('CRC32', sourceBody));
        });

        it('should checksum a copied part whose source is a multi-part object', async () => {
            const srcKey = 'copypart-checksum-mpu-source';
            const srcUploadId = await createMpu(srcKey);
            const a = bigBody; // part 1 must be >= 5 MiB to complete the source MPU
            const b = Buffer.from('multipart-source-part-B', 'utf8');
            const up1 = await s3.send(new UploadPartCommand({
                Bucket: bucket, Key: srcKey, UploadId: srcUploadId, PartNumber: 1, Body: a }));
            const up2 = await s3.send(new UploadPartCommand({
                Bucket: bucket, Key: srcKey, UploadId: srcUploadId, PartNumber: 2, Body: b }));
            await s3.send(new CompleteMultipartUploadCommand({
                Bucket: bucket, Key: srcKey, UploadId: srcUploadId,
                MultipartUpload: { Parts: [{ PartNumber: 1, ETag: up1.ETag }, { PartNumber: 2, ETag: up2.ETag }] },
            }));
            const key = 'cpr-mp-source';
            const uploadId = await createMpu(key, { ChecksumAlgorithm: 'CRC32' });
            const res = await copyPart(key, uploadId, { CopySource: `${bucket}/${srcKey}` });
            assert.strictEqual(res.CopyPartResult.ChecksumCRC32, await digest('CRC32', Buffer.concat([a, b])));
        });

        it('should complete an MPU mixing an uploaded part and a copied part', async () => {
            const key = 'cmp-mixed';
            const uploadId = await createMpu(key, { ChecksumAlgorithm: 'CRC32', ChecksumType: 'FULL_OBJECT' });
            const partBody = bigBody; // uploaded part 1 must be >= 5 MiB
            const up1 = await s3.send(new UploadPartCommand({
                Bucket: bucket, Key: key, UploadId: uploadId, PartNumber: 1, Body: partBody, ChecksumAlgorithm: 'CRC32',
            }));
            const cp2 = await copyPart(key, uploadId, { PartNumber: 2 });
            const complete = await s3.send(new CompleteMultipartUploadCommand({
                Bucket: bucket, Key: key, UploadId: uploadId,
                MultipartUpload: { Parts: [
                    { PartNumber: 1, ETag: up1.ETag, ChecksumCRC32: up1.ChecksumCRC32 },
                    { PartNumber: 2, ETag: cp2.CopyPartResult.ETag, ChecksumCRC32: cp2.CopyPartResult.ChecksumCRC32 },
                ] },
            }));
            assert.strictEqual(complete.ChecksumType, 'FULL_OBJECT');
            assert.strictEqual(complete.ChecksumCRC32,
                await digest('CRC32', Buffer.concat([partBody, sourceBody])));
        });

        it('should reject CompleteMPU on a COMPOSITE MPU when the copied part checksum is omitted', async () => {
            const key = 'cmp-omit-cksum';
            const uploadId = await createMpu(key, { ChecksumAlgorithm: 'CRC32', ChecksumType: 'COMPOSITE' });
            const copy = await copyPart(key, uploadId);
            await assert.rejects(
                s3.send(new CompleteMultipartUploadCommand({
                    Bucket: bucket, Key: key, UploadId: uploadId,
                    MultipartUpload: { Parts: [{ PartNumber: 1, ETag: copy.CopyPartResult.ETag }] },
                })),
                err => {
                    assert.strictEqual(err.name, 'InvalidRequest');
                    return true;
                });
        });

        it('should reject CompleteMPU when the copied part checksum is wrong', async () => {
            const key = 'cmp-wrong-cksum';
            const uploadId = await createMpu(key, { ChecksumAlgorithm: 'CRC32', ChecksumType: 'COMPOSITE' });
            const copy = await copyPart(key, uploadId);
            await assert.rejects(
                s3.send(new CompleteMultipartUploadCommand({
                    Bucket: bucket, Key: key, UploadId: uploadId,
                    MultipartUpload: { Parts: [
                        { PartNumber: 1, ETag: copy.CopyPartResult.ETag, ChecksumCRC32: 'AAAAAA==' }] },
                })),
                err => {
                    assert.strictEqual(err.name, 'InvalidPart');
                    return true;
                });
        });
    })
);
