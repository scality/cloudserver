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

const bucket = `mpu-checksum-test-${Date.now()}`;
const key = 'test-checksum-key';

describe('CreateMultipartUpload checksum headers', () =>
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

        describe('no checksum headers', () => {
            let res;

            before(async () => {
                res = await s3.send(new CreateMultipartUploadCommand({ Bucket: bucket, Key: key }));
            });

            after(async () => {
                await s3.send(
                    new AbortMultipartUploadCommand({
                        Bucket: bucket,
                        Key: key,
                        UploadId: res.UploadId,
                    }),
                );
            });

            it('should not return ChecksumAlgorithm and ChecksumType', () => {
                assert.strictEqual(res.ChecksumAlgorithm, undefined);
                assert.strictEqual(res.ChecksumType, undefined);
            });

            it('should not include ChecksumAlgorithm or ChecksumType in ListMultipartUploads', async () => {
                const { Uploads } = await s3.send(new ListMultipartUploadsCommand({ Bucket: bucket }));
                const upload = Uploads.find(u => u.UploadId === res.UploadId);
                assert(upload, 'upload not found in listing');
                assert.strictEqual(upload.ChecksumAlgorithm, undefined);
                assert.strictEqual(upload.ChecksumType, undefined);
            });
        });

        describe('valid algorithm only', () => {
            const cases = [
                { algo: 'CRC32', expectedType: 'COMPOSITE' },
                { algo: 'CRC32C', expectedType: 'COMPOSITE' },
                { algo: 'CRC64NVME', expectedType: 'FULL_OBJECT' },
                { algo: 'SHA1', expectedType: 'COMPOSITE' },
                { algo: 'SHA256', expectedType: 'COMPOSITE' },
            ];

            cases.forEach(({ algo, expectedType }) => {
                describe(`${algo}`, () => {
                    let res;

                    before(async () => {
                        res = await s3.send(
                            new CreateMultipartUploadCommand({
                                Bucket: bucket,
                                Key: key,
                                ChecksumAlgorithm: algo,
                            }),
                        );
                    });

                    after(async () => {
                        await s3.send(
                            new AbortMultipartUploadCommand({
                                Bucket: bucket,
                                Key: key,
                                UploadId: res.UploadId,
                            }),
                        );
                    });

                    it(`should return ChecksumAlgorithm ${algo} and default ChecksumType to ${expectedType}`, () => {
                        assert.strictEqual(res.ChecksumAlgorithm, algo);
                        assert.strictEqual(res.ChecksumType, expectedType);
                    });

                    it(`should include ${algo}/${expectedType} in ListMultipartUploads`, async () => {
                        const { Uploads } = await s3.send(new ListMultipartUploadsCommand({ Bucket: bucket }));
                        const upload = Uploads.find(u => u.UploadId === res.UploadId);
                        assert(upload, 'upload not found in listing');
                        assert.strictEqual(upload.ChecksumAlgorithm, algo);
                        assert.strictEqual(upload.ChecksumType, expectedType);
                    });
                });
            });
        });

        describe('valid algorithm + type', () => {
            const validCombos = [
                ['CRC32', 'FULL_OBJECT'],
                ['CRC32', 'COMPOSITE'],
                ['CRC32C', 'FULL_OBJECT'],
                ['CRC32C', 'COMPOSITE'],
                ['CRC64NVME', 'FULL_OBJECT'],
                ['SHA1', 'COMPOSITE'],
                ['SHA256', 'COMPOSITE'],
            ];

            validCombos.forEach(([algo, type]) => {
                describe(`${algo} + ${type}`, () => {
                    let res;

                    before(async () => {
                        res = await s3.send(
                            new CreateMultipartUploadCommand({
                                Bucket: bucket,
                                Key: key,
                                ChecksumAlgorithm: algo,
                                ChecksumType: type,
                            }),
                        );
                    });

                    after(async () => {
                        await s3.send(
                            new AbortMultipartUploadCommand({
                                Bucket: bucket,
                                Key: key,
                                UploadId: res.UploadId,
                            }),
                        );
                    });

                    it(`should return ChecksumAlgorithm ${algo} and ChecksumType ${type}`, () => {
                        assert.strictEqual(res.ChecksumAlgorithm, algo);
                        assert.strictEqual(res.ChecksumType, type);
                    });

                    it(`should include ${algo}/${type} in ListMultipartUploads`, async () => {
                        const { Uploads } = await s3.send(new ListMultipartUploadsCommand({ Bucket: bucket }));
                        const upload = Uploads.find(u => u.UploadId === res.UploadId);
                        assert(upload, 'upload not found in listing');
                        assert.strictEqual(upload.ChecksumAlgorithm, algo);
                        assert.strictEqual(upload.ChecksumType, type);
                    });
                });
            });
        });

        describe('error cases', () => {
            it('should reject FULL_OBJECT with SHA256', async () => {
                try {
                    await s3.send(
                        new CreateMultipartUploadCommand({
                            Bucket: bucket,
                            Key: key,
                            ChecksumAlgorithm: 'SHA256',
                            ChecksumType: 'FULL_OBJECT',
                        }),
                    );
                    assert.fail('Expected error');
                } catch (err) {
                    assert.strictEqual(err.name, 'InvalidRequest');
                }
            });

            it('should reject FULL_OBJECT with SHA1', async () => {
                try {
                    await s3.send(
                        new CreateMultipartUploadCommand({
                            Bucket: bucket,
                            Key: key,
                            ChecksumAlgorithm: 'SHA1',
                            ChecksumType: 'FULL_OBJECT',
                        }),
                    );
                    assert.fail('Expected error');
                } catch (err) {
                    assert.strictEqual(err.name, 'InvalidRequest');
                }
            });

            it('should reject COMPOSITE with CRC64NVME', async () => {
                try {
                    await s3.send(
                        new CreateMultipartUploadCommand({
                            Bucket: bucket,
                            Key: key,
                            ChecksumAlgorithm: 'CRC64NVME',
                            ChecksumType: 'COMPOSITE',
                        }),
                    );
                    assert.fail('Expected error');
                } catch (err) {
                    assert.strictEqual(err.name, 'InvalidRequest');
                }
            });
        });
    }));
