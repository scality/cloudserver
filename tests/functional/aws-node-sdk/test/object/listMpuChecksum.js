const assert = require('assert');
const {
    CreateBucketCommand,
    CreateMultipartUploadCommand,
    AbortMultipartUploadCommand,
    ListMultipartUploadsCommand,
    DeleteBucketCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = `list-mpu-checksum-test-${Date.now()}`;
const key = 'test-checksum-key';

describe('ListMultipartUploads checksum fields', () =>
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

        describe('MPU created without checksum algorithm', () => {
            let uploadId;

            before(async () => {
                const res = await s3.send(new CreateMultipartUploadCommand({ Bucket: bucket, Key: key }));
                uploadId = res.UploadId;
            });

            after(async () => {
                await s3.send(new AbortMultipartUploadCommand({ Bucket: bucket, Key: key, UploadId: uploadId }));
            });

            it('should not include ChecksumAlgorithm or ChecksumType in listed uploads', async () => {
                const { Uploads } = await s3.send(new ListMultipartUploadsCommand({ Bucket: bucket }));
                assert.strictEqual(Uploads.length, 1);
                assert.strictEqual(Uploads[0].UploadId, uploadId);
                assert.strictEqual(Uploads[0].ChecksumAlgorithm, undefined);
                assert.strictEqual(Uploads[0].ChecksumType, undefined);
            });
        });

        describe('MPU created with checksum algorithm', () => {
            const cases = [
                { algo: 'CRC32', expectedType: 'COMPOSITE' },
                { algo: 'CRC32C', expectedType: 'COMPOSITE' },
                { algo: 'CRC64NVME', expectedType: 'FULL_OBJECT' },
                { algo: 'SHA1', expectedType: 'COMPOSITE' },
                { algo: 'SHA256', expectedType: 'COMPOSITE' },
            ];

            cases.forEach(({ algo, expectedType }) => {
                describe(`${algo}`, () => {
                    let uploadId;

                    before(async () => {
                        const res = await s3.send(
                            new CreateMultipartUploadCommand({
                                Bucket: bucket,
                                Key: key,
                                ChecksumAlgorithm: algo,
                            }),
                        );
                        uploadId = res.UploadId;
                    });

                    after(async () => {
                        await s3.send(
                            new AbortMultipartUploadCommand({
                                Bucket: bucket,
                                Key: key,
                                UploadId: uploadId,
                            }),
                        );
                    });

                    it(`should return ChecksumAlgorithm ${algo} and ChecksumType ${expectedType}`, async () => {
                        const { Uploads } = await s3.send(new ListMultipartUploadsCommand({ Bucket: bucket }));
                        assert.strictEqual(Uploads.length, 1);
                        assert.strictEqual(Uploads[0].UploadId, uploadId);
                        assert.strictEqual(Uploads[0].ChecksumAlgorithm, algo);
                        assert.strictEqual(Uploads[0].ChecksumType, expectedType);
                    });
                });
            });
        });

        describe('MPU created with explicit algorithm and type', () => {
            let uploadId;

            before(async () => {
                const res = await s3.send(
                    new CreateMultipartUploadCommand({
                        Bucket: bucket,
                        Key: key,
                        ChecksumAlgorithm: 'CRC32',
                        ChecksumType: 'FULL_OBJECT',
                    }),
                );
                uploadId = res.UploadId;
            });

            after(async () => {
                await s3.send(new AbortMultipartUploadCommand({ Bucket: bucket, Key: key, UploadId: uploadId }));
            });

            it('should return the explicit ChecksumAlgorithm and ChecksumType', async () => {
                const { Uploads } = await s3.send(new ListMultipartUploadsCommand({ Bucket: bucket }));
                assert.strictEqual(Uploads.length, 1);
                assert.strictEqual(Uploads[0].UploadId, uploadId);
                assert.strictEqual(Uploads[0].ChecksumAlgorithm, 'CRC32');
                assert.strictEqual(Uploads[0].ChecksumType, 'FULL_OBJECT');
            });
        });

        describe('multiple MPUs with mixed checksum settings', () => {
            const uploads = [];

            before(async () => {
                const noChecksum = await s3.send(
                    new CreateMultipartUploadCommand({
                        Bucket: bucket,
                        Key: `${key}-no-checksum`,
                    }),
                );
                uploads.push({ key: `${key}-no-checksum`, uploadId: noChecksum.UploadId });

                const withChecksum = await s3.send(
                    new CreateMultipartUploadCommand({
                        Bucket: bucket,
                        Key: `${key}-with-checksum`,
                        ChecksumAlgorithm: 'SHA256',
                    }),
                );
                uploads.push({ key: `${key}-with-checksum`, uploadId: withChecksum.UploadId });
            });

            after(async () => {
                await Promise.all(
                    uploads.map(u =>
                        s3.send(new AbortMultipartUploadCommand({ Bucket: bucket, Key: u.key, UploadId: u.uploadId })),
                    ),
                );
            });

            it('should return checksum fields only for uploads that have them', async () => {
                const { Uploads } = await s3.send(new ListMultipartUploadsCommand({ Bucket: bucket }));
                assert.strictEqual(Uploads.length, 2);

                const noChecksumUpload = Uploads.find(u => u.UploadId === uploads[0].uploadId);
                assert.strictEqual(noChecksumUpload.ChecksumAlgorithm, undefined);
                assert.strictEqual(noChecksumUpload.ChecksumType, undefined);

                const withChecksumUpload = Uploads.find(u => u.UploadId === uploads[1].uploadId);
                assert.strictEqual(withChecksumUpload.ChecksumAlgorithm, 'SHA256');
                assert.strictEqual(withChecksumUpload.ChecksumType, 'COMPOSITE');
            });
        });
    }));
