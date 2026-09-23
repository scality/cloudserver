const assert = require('assert');
const {
    AbortMultipartUploadCommand,
    CreateBucketCommand,
    CreateMultipartUploadCommand,
    DeleteBucketCommand,
    ListPartsCommand,
    UploadPartCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { algorithms } = require('../../../../../lib/api/apiUtils/integrity/validateChecksums');

const bucket = `list-parts-checksum-test-${Date.now()}`;
const defaultKey = 'default-no-checksum';
const checksumBodies = [Buffer.from('first checksummed part', 'utf8'), Buffer.from('second checksummed part', 'utf8')];
const defaultBodies = [
    Buffer.from('first default checksum part', 'utf8'),
    Buffer.from('second default checksum part', 'utf8'),
];
const checksumFieldByAlgorithm = {
    CRC32: 'ChecksumCRC32',
    CRC32C: 'ChecksumCRC32C',
    CRC64NVME: 'ChecksumCRC64NVME',
    SHA1: 'ChecksumSHA1',
    SHA256: 'ChecksumSHA256',
};
const checksumTypesByAlgorithm = {
    CRC32: ['COMPOSITE', 'FULL_OBJECT'],
    CRC32C: ['COMPOSITE', 'FULL_OBJECT'],
    CRC64NVME: ['FULL_OBJECT'],
    SHA1: ['COMPOSITE'],
    SHA256: ['COMPOSITE'],
};

function assertNoListPartsChecksum(partList) {
    assert.strictEqual(partList.ChecksumAlgorithm, undefined);
    assert.strictEqual(partList.ChecksumType, undefined);
    partList.Parts.forEach(part => {
        assert.strictEqual(part.ChecksumCRC32, undefined);
        assert.strictEqual(part.ChecksumCRC32C, undefined);
        assert.strictEqual(part.ChecksumCRC64NVME, undefined);
        assert.strictEqual(part.ChecksumSHA1, undefined);
        assert.strictEqual(part.ChecksumSHA256, undefined);
    });
}

describe('ListParts checksum fields', () =>
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        const openMPUs = [];

        async function abortUpload(key, uploadId) {
            await s3.send(
                new AbortMultipartUploadCommand({
                    Bucket: bucket,
                    Key: key,
                    UploadId: uploadId,
                }),
            );
            const index = openMPUs.findIndex(upload => upload.key === key && upload.uploadId === uploadId);
            if (index !== -1) {
                openMPUs.splice(index, 1);
            }
        }

        before(async () => {
            bucketUtil = new BucketUtility('default', {
                ...sigCfg,
                requestChecksumCalculation: 'WHEN_REQUIRED',
            });
            s3 = bucketUtil.s3;
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));
        });

        after(async () => {
            await Promise.all(
                openMPUs.map(upload =>
                    s3
                        .send(
                            new AbortMultipartUploadCommand({
                                Bucket: bucket,
                                Key: upload.key,
                                UploadId: upload.uploadId,
                            }),
                        )
                        .catch(() => undefined),
                ),
            );
            await bucketUtil.empty(bucket);
            await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
        });

        for (const [checksumAlgorithm, checksumTypes] of Object.entries(checksumTypesByAlgorithm)) {
            for (const checksumType of checksumTypes) {
                it(
                    `should include ${checksumAlgorithm}/${checksumType} root ` + 'and part checksum fields',
                    async () => {
                        const key = `explicit-${checksumAlgorithm}-${checksumType}`;
                        const checksumField = checksumFieldByAlgorithm[checksumAlgorithm];
                        const internalAlgorithm = checksumAlgorithm.toLowerCase();
                        const { UploadId } = await s3.send(
                            new CreateMultipartUploadCommand({
                                Bucket: bucket,
                                Key: key,
                                ChecksumAlgorithm: checksumAlgorithm,
                                ChecksumType: checksumType,
                            }),
                        );
                        openMPUs.push({ key, uploadId: UploadId });

                        const partChecksums = await Promise.all(
                            checksumBodies.map(body => algorithms[internalAlgorithm].digest(body)),
                        );

                        await Promise.all(
                            checksumBodies.map((body, index) =>
                                s3.send(
                                    new UploadPartCommand({
                                        Bucket: bucket,
                                        Key: key,
                                        UploadId,
                                        PartNumber: index + 1,
                                        Body: body,
                                        [checksumField]: partChecksums[index],
                                    }),
                                ),
                            ),
                        );

                        const partList = await s3.send(
                            new ListPartsCommand({
                                Bucket: bucket,
                                Key: key,
                                UploadId,
                            }),
                        );

                        assert.strictEqual(partList.ChecksumAlgorithm, checksumAlgorithm);
                        assert.strictEqual(partList.ChecksumType, checksumType);
                        assert.strictEqual(partList.Parts.length, checksumBodies.length);
                        partList.Parts.forEach((part, index) => {
                            assert.strictEqual(part.PartNumber, index + 1);
                            assert.strictEqual(part[checksumField], partChecksums[index]);
                        });

                        await abortUpload(key, UploadId);
                    },
                );
            }
        }

        it('should omit default checksum fields when no checksum headers are sent', async () => {
            const { UploadId } = await s3.send(
                new CreateMultipartUploadCommand({
                    Bucket: bucket,
                    Key: defaultKey,
                }),
            );
            openMPUs.push({ key: defaultKey, uploadId: UploadId });

            await Promise.all(
                defaultBodies.map((body, index) =>
                    s3.send(
                        new UploadPartCommand({
                            Bucket: bucket,
                            Key: defaultKey,
                            UploadId,
                            PartNumber: index + 1,
                            Body: body,
                        }),
                    ),
                ),
            );

            const partList = await s3.send(
                new ListPartsCommand({
                    Bucket: bucket,
                    Key: defaultKey,
                    UploadId,
                }),
            );

            assert.strictEqual(partList.Parts.length, defaultBodies.length);
            assertNoListPartsChecksum(partList);

            await abortUpload(defaultKey, UploadId);
        });
    }));
