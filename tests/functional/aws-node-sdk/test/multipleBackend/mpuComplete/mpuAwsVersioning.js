const assert = require('assert');
const {
    CreateBucketCommand,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    CompleteMultipartUploadCommand,
    PutBucketVersioningCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const {
    removeAllVersions,
    versioningEnabled,
    versioningSuspended,
} = require('../../../lib/utility/versioning-util.js');

const bucketName = 'testmpuversioning';
const objectName = 'key';

describe('aws backend mpu with versioning', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        async function mpuSetup(enableVersioning) {
            await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
            
            if (enableVersioning) {
                await s3.send(new PutBucketVersioningCommand({
                    Bucket: bucketName,
                    VersioningConfiguration: versioningEnabled,
                }));
            }

            const createResult = await s3.send(new CreateMultipartUploadCommand({
                Bucket: bucketName,
                Key: objectName,
            }));
            const uploadId = createResult.UploadId;

            const uploadResult = await s3.send(new UploadPartCommand({
                Bucket: bucketName,
                Key: objectName,
                PartNumber: 1,
                UploadId: uploadId,
                Body: 'hello',
            }));
            const eTag = uploadResult.ETag;

            return { uploadId, eTag };
        }

        async function completeAndAssertMpu(uploadId, eTag, expectedVersionId) {
            const completeResult = await s3.send(new CompleteMultipartUploadCommand({
                Bucket: bucketName,
                Key: objectName,
                UploadId: uploadId,
                MultipartUpload: {
                    Parts: [{ ETag: eTag, PartNumber: 1 }],
                },
            }));
            // eslint-disable-next-line no-console
            console.log('completeResult.VersionId', completeResult);
            if (expectedVersionId) {
                assert.notEqual(completeResult.VersionId, undefined);
            } else {
                assert.strictEqual(completeResult.VersionId, undefined);
            }

            return completeResult;
        }

        afterEach(async () => {
            await removeAllVersions({ Bucket: bucketName });
            await bucketUtil.deleteOne(bucketName);
        });

        it('should complete mpu with no versioning configured', async () => {
            const { uploadId, eTag } = await mpuSetup(false);
            await completeAndAssertMpu(uploadId, eTag, undefined);
        });

        it('should complete mpu with versioning enabled', async () => {
            const { uploadId, eTag } = await mpuSetup(true);
            await completeAndAssertMpu(uploadId, eTag, 'string');
        });

        it('should complete mpu with versioning suspended', async () => {
            const { uploadId, eTag } = await mpuSetup(true);
            
            // Suspend versioning
            await s3.send(new PutBucketVersioningCommand({
                Bucket: bucketName,
                VersioningConfiguration: versioningSuspended,
            }));

            await completeAndAssertMpu(uploadId, eTag, undefined);
        });

        it('should complete mpu with null version id when suspended after being enabled', async () => {
            const { uploadId, eTag } = await mpuSetup(true);
            
            // Suspend versioning after enabling
            await s3.send(new PutBucketVersioningCommand({
                Bucket: bucketName,
                VersioningConfiguration: versioningSuspended,
            }));

            await completeAndAssertMpu(uploadId, eTag, undefined);
        });
    });
});
