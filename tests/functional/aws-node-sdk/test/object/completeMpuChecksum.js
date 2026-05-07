'use strict';

const assert = require('assert');
const {
    CreateBucketCommand,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    CompleteMultipartUploadCommand,
    HeadObjectCommand,
    DeleteBucketCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { algorithms } = require('../../../../../lib/api/apiUtils/integrity/validateChecksums');

const bucket = `mpu-complete-checksum-${Date.now()}`;
const partBody = Buffer.from('I am a part body for complete-MPU testing', 'utf8');

// All AWS-valid (algorithm, type) pairs for an MPU. CompleteMPU should
// surface the resulting final-object checksum and ChecksumType in the
// response for every combination here, plus the implicit default.
const COMBOS = [
    { algo: 'CRC32', type: 'FULL_OBJECT' },
    { algo: 'CRC32', type: 'COMPOSITE' },
    { algo: 'CRC32C', type: 'FULL_OBJECT' },
    { algo: 'CRC32C', type: 'COMPOSITE' },
    { algo: 'CRC64NVME', type: 'FULL_OBJECT' },
    { algo: 'SHA1', type: 'COMPOSITE' },
    { algo: 'SHA256', type: 'COMPOSITE' },
];

const tagField = algo => `Checksum${algo}`;

describe('CompleteMultipartUpload final-object checksum', () =>
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

        COMBOS.forEach(({ algo, type }) => {
            const field = tagField(algo);
            it(`should return ${algo}/${type} on CompleteMPU response`, async () => {
                const key = `complete-${algo.toLowerCase()}-${type.toLowerCase()}-${Date.now()}`;
                const partChecksum = await algorithms[algo.toLowerCase()].digest(partBody);

                const create = await s3.send(
                    new CreateMultipartUploadCommand({
                        Bucket: bucket,
                        Key: key,
                        ChecksumAlgorithm: algo,
                        ChecksumType: type,
                    }),
                );

                const uploadPart = await s3.send(
                    new UploadPartCommand({
                        Bucket: bucket,
                        Key: key,
                        UploadId: create.UploadId,
                        PartNumber: 1,
                        Body: partBody,
                        [field]: partChecksum,
                    }),
                );

                const complete = await s3.send(
                    new CompleteMultipartUploadCommand({
                        Bucket: bucket,
                        Key: key,
                        UploadId: create.UploadId,
                        MultipartUpload: {
                            Parts: [
                                {
                                    PartNumber: 1,
                                    ETag: uploadPart.ETag,
                                    [field]: partChecksum,
                                },
                            ],
                        },
                    }),
                );

                assert(complete[field], `expected ${field} in CompleteMPU response, got: ${JSON.stringify(complete)}`);
                assert.strictEqual(complete.ChecksumType, type);
                if (type === 'COMPOSITE') {
                    assert(
                        complete[field].endsWith('-1'),
                        `expected -1 suffix for 1-part COMPOSITE, got ${complete[field]}`,
                    );
                } else {
                    assert(
                        !complete[field].includes('-'),
                        `FULL_OBJECT value should have no suffix, got ${complete[field]}`,
                    );
                }

                // HeadObject with ChecksumMode=ENABLED must surface the same
                // value that CompleteMPU returned for FULL_OBJECT MPUs.
                // COMPOSITE storage is deferred, so HeadObject leaves the field absent — matching
                // cloudserver's current intentional skip.
                const head = await s3.send(
                    new HeadObjectCommand({
                        Bucket: bucket,
                        Key: key,
                        ChecksumMode: 'ENABLED',
                    }),
                );
                if (type === 'FULL_OBJECT') {
                    assert.strictEqual(
                        head[field],
                        complete[field],
                        `HeadObject ${field} should match CompleteMPU response`,
                    );
                    assert.strictEqual(head.ChecksumType, type);
                } else {
                    assert.strictEqual(
                        head[field],
                        undefined,
                        `COMPOSITE storage is deferred; HeadObject should not surface ${field}`,
                    );
                    assert.strictEqual(head.ChecksumType, undefined);
                }
            });
        });

        it('should return CRC64NVME/FULL_OBJECT on CompleteMPU response when CreateMPU sent no checksum headers', async () => {
            const key = `complete-default-${Date.now()}`;

            const create = await s3.send(
                new CreateMultipartUploadCommand({
                    Bucket: bucket,
                    Key: key,
                }),
            );

            const uploadPart = await s3.send(
                new UploadPartCommand({
                    Bucket: bucket,
                    Key: key,
                    UploadId: create.UploadId,
                    PartNumber: 1,
                    Body: partBody,
                }),
            );

            const complete = await s3.send(
                new CompleteMultipartUploadCommand({
                    Bucket: bucket,
                    Key: key,
                    UploadId: create.UploadId,
                    MultipartUpload: {
                        Parts: [{ PartNumber: 1, ETag: uploadPart.ETag }],
                    },
                }),
            );

            assert(
                complete.ChecksumCRC64NVME,
                `expected ChecksumCRC64NVME for default MPU, got: ${JSON.stringify(complete)}`,
            );
            assert.strictEqual(complete.ChecksumType, 'FULL_OBJECT');

            // Default MPU is FULL_OBJECT — checksum is persisted, so
            // HeadObject must return the same value.
            const head = await s3.send(
                new HeadObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    ChecksumMode: 'ENABLED',
                }),
            );
            assert.strictEqual(head.ChecksumCRC64NVME, complete.ChecksumCRC64NVME);
            assert.strictEqual(head.ChecksumType, 'FULL_OBJECT');
        });
    }));
