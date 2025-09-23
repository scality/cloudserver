const assert = require('assert');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketVersioningCommand,
    PutObjectCommand,
    DeleteObjectCommand,
    PutObjectTaggingCommand,
    DeleteObjectTaggingCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { checkOneVersion } = require('../../lib/utility/versioning-util');

const bucketName = 'testtaggingbucket';
const objectName = 'testtaggingobject';

const invalidId = 'invalidIdWithMoreThan40BytesAndThatIsNotLongEnoughYet';

const {
    removeAllVersions,
    versioningEnabled,
} = require('../../lib/utility/versioning-util');

function _checkError(err, code, statusCode) {
    assert(err, 'Expected error but found none');
    assert.strictEqual(err.Code, code);
    assert.strictEqual(err.$metadata?.httpStatusCode, statusCode);
}


describe('Delete object tagging with versioning', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        
        beforeEach(async () => {
            await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
        });
        
        afterEach(async () => {
            await removeAllVersions({ Bucket: bucketName });
            await s3.send(new DeleteBucketCommand({ Bucket: bucketName }));
        });

        it('should be able to delete tag set with versioning', async () => {
            await s3.send(new PutBucketVersioningCommand({
                Bucket: bucketName,
                VersioningConfiguration: versioningEnabled
            }));

            const putObjectResult = await s3.send(new PutObjectCommand({
                Bucket: bucketName,
                Key: objectName
            }));
            const versionId = putObjectResult.VersionId;

            await s3.send(new PutObjectTaggingCommand({
                Bucket: bucketName,
                Key: objectName,
                VersionId: versionId,
                Tagging: {
                    TagSet: [{
                        Key: 'key1',
                        Value: 'value1',
                    }]
                },
            }));

            const deleteResult = await s3.send(new DeleteObjectTaggingCommand({
                Bucket: bucketName,
                Key: objectName,
                VersionId: versionId,
            }));

            assert.strictEqual(deleteResult.VersionId, versionId);
        });

        it('should not create version deleting object tags on a ' +
        ' version-enabled bucket where no version id is specified ', async () => {
            await s3.send(new PutBucketVersioningCommand({
                Bucket: bucketName,
                VersioningConfiguration: versioningEnabled
            }));

            const putObjectResult = await s3.send(new PutObjectCommand({
                Bucket: bucketName,
                Key: objectName
            }));
            const versionId = putObjectResult.VersionId;

            await s3.send(new PutObjectTaggingCommand({
                Bucket: bucketName,
                Key: objectName,
                VersionId: versionId,
                Tagging: {
                    TagSet: [{
                        Key: 'key1',
                        Value: 'value1',
                    }]
                },
            }));

            await s3.send(new DeleteObjectTaggingCommand({
                Bucket: bucketName,
                Key: objectName,
            }));

            await checkOneVersion(s3, bucketName, versionId);
        });

        it('should be able to delete tag set with a version of id "null"',
        async () => {
            await s3.send(new PutObjectCommand({
                Bucket: bucketName,
                Key: objectName
            }));

            await s3.send(new PutBucketVersioningCommand({
                Bucket: bucketName,
                VersioningConfiguration: versioningEnabled
            }));

            const deleteResult = await s3.send(new DeleteObjectTaggingCommand({
                Bucket: bucketName,
                Key: objectName,
                VersionId: 'null',
            }));

            assert.strictEqual(deleteResult.VersionId, 'null');
        });

        it('should return InvalidArgument deleting tag set with a non ' +
        'existing version id', async () => {
            await s3.send(new PutObjectCommand({
                Bucket: bucketName,
                Key: objectName
            }));

            await s3.send(new PutBucketVersioningCommand({
                Bucket: bucketName,
                VersioningConfiguration: versioningEnabled
            }));

            try {
                await s3.send(new DeleteObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: invalidId,
                }));
                assert.fail('Expected InvalidArgument error');
            } catch (err) {
                _checkError(err, 'InvalidArgument', 400);
            }
        });

        it('should return 405 MethodNotAllowed deleting tag set without ' +
         'version id if version specified is a delete marker', async () => {
            await s3.send(new PutBucketVersioningCommand({
                Bucket: bucketName,
                VersioningConfiguration: versioningEnabled
            }));

            await s3.send(new PutObjectCommand({
                Bucket: bucketName,
                Key: objectName
            }));

            await s3.send(new DeleteObjectCommand({
                Bucket: bucketName,
                Key: objectName
            }));

            try {
                await s3.send(new DeleteObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: objectName,
                }));
                assert.fail('Expected MethodNotAllowed error');
            } catch (err) {
                _checkError(err, 'MethodNotAllowed', 405);
            }
        });

        it('should return 405 MethodNotAllowed deleting tag set with ' +
         'version id if version specified is a delete marker', async () => {
            await s3.send(new PutBucketVersioningCommand({
                Bucket: bucketName,
                VersioningConfiguration: versioningEnabled
            }));

            await s3.send(new PutObjectCommand({
                Bucket: bucketName,
                Key: objectName
            }));

            const deleteResult = await s3.send(new DeleteObjectCommand({
                Bucket: bucketName,
                Key: objectName
            }));
            const versionId = deleteResult.VersionId;

            try {
                await s3.send(new DeleteObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: versionId,
                }));
                assert.fail('Expected MethodNotAllowed error');
            } catch (err) {
                _checkError(err, 'MethodNotAllowed', 405);
            }
        });
    });
});
