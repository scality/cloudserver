const assert = require('assert');

const {
    DeleteBucketCommand,
    PutBucketVersioningCommand,
    PutObjectCommand,
    CopyObjectCommand,
    GetObjectCommand,
    HeadObjectCommand,
    GetObjectTaggingCommand,
    GetObjectAclCommand,
    PutObjectAclCommand,
    DeleteObjectCommand,
} = require('@aws-sdk/client-s3');

const { promisify } = require('util');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { removeAllVersions } = require('../../lib/utility/versioning-util');
const customS3Request = require('../../lib/utility/customS3Request');

const removeAllVersionsPromise = promisify(removeAllVersions);


const { taggingTests } = require('../../lib/utility/tagging');
const constants = require('../../../../../constants');

const sourceBucketName = 'supersourcebucket81020165';
const sourceObjName = 'supersourceobject';
const destBucketName = 'destinationbucket81020165';
const destObjName = 'copycatobject';

const originalMetadata = {
    oldmetadata: 'same old',
    overwriteme: 'wipe me out with replace',
};
const originalCacheControl = 'max-age=1337';
const originalContentDisposition = 'attachment; filename="1337.txt";';
const originalContentEncoding = 'base64,aws-chunked';
const originalExpires = new Date(12345678);

const originalTagKey = 'key1';
const originalTagValue = 'value1';
const originalTagging = `${originalTagKey}=${originalTagValue}`;

const newMetadata = {
    newmetadata: 'new kid in town',
    overwriteme: 'wiped',
};
const newCacheControl = 'max-age=86400';
const newContentDisposition = 'attachment; filename="fname.ext";';
const newContentEncoding = 'gzip,aws-chunked';
const newExpires = new Date();

const newTagKey = 'key2';
const newTagValue = 'value2';
const newTagging = `${newTagKey}=${newTagValue}`;

const content = 'I am the best content ever';
const secondContent = 'I am the second best content ever';

const otherAccountBucketUtility = new BucketUtility('lisa', {});
const otherAccountS3 = otherAccountBucketUtility.s3;

function checkNoError(err) {
    assert.equal(err, null,
        `Expected success, got error ${JSON.stringify(err)}`);
}

function checkError(err, code) {
    assert.notEqual(err, null, 'Expected failure but got success');
    assert.strictEqual(err.Code, code);
}

function dateFromNow(diff) {
    const d = new Date();
    d.setHours(d.getHours() + diff);
    return d;
}

function dateConvert(d) {
    return new Date(d);
}

describe('Object Version Copy', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        let etag;
        let etagTrim;
        let lastModified;
        let versionId;
        let copySource;
        let copySourceVersionId;

        async function emptyAndDeleteBucket(bucketName) {
            await removeAllVersionsPromise({ Bucket: bucketName });
            await s3.send(new DeleteBucketCommand({ Bucket: bucketName }));
        }

        beforeEach(async () => {
            await bucketUtil.createOne(sourceBucketName);
            await bucketUtil.createOne(destBucketName);
            await s3.send(new PutBucketVersioningCommand({
                Bucket: sourceBucketName,
                VersioningConfiguration: { Status: 'Enabled' },
            }));
            const putRes = await s3.send(new PutObjectCommand({
                Bucket: sourceBucketName,
                Key: sourceObjName,
                Body: content,
                Metadata: originalMetadata,
                CacheControl: originalCacheControl,
                ContentDisposition: originalContentDisposition,
                ContentEncoding: originalContentEncoding,
                Expires: originalExpires,
                Tagging: originalTagging,
            }));
            etag = putRes.ETag;
            versionId = putRes.VersionId;
            copySource = `${sourceBucketName}/${sourceObjName}?versionId=${versionId}`;
            etagTrim = etag.substring(1, etag.length - 1);
            copySourceVersionId = putRes.VersionId;
            const headRes = await s3.send(new HeadObjectCommand({
                Bucket: sourceBucketName,
                Key: sourceObjName,
            }));
            lastModified = headRes.LastModified;
            await s3.send(new PutObjectCommand({
                Bucket: sourceBucketName,
                Key: sourceObjName,
                Body: secondContent,
            }));
        });

        afterEach(async () => {
            await Promise.all([
                emptyAndDeleteBucket(sourceBucketName),
                emptyAndDeleteBucket(destBucketName),
            ]);
        });

        async function requestCopy(fields) {
            return s3.send(new CopyObjectCommand({
                Bucket: destBucketName,
                Key: destObjName,
                CopySource: copySource,
                ...fields,
            }));
        }

        async function successCopyCheck(error, response, copyVersionMetadata, destBucketName, destObjName) {
            checkNoError(error);
            assert.strictEqual(response.CopySourceVersionId, copySourceVersionId);
            assert.notStrictEqual(response.CopySourceVersionId, response.VersionId);
            const destinationVersionId = response.VersionId;
            assert.strictEqual(response.CopyObjectResult.ETag, etag);
            const copyLastModified = new Date(response.CopyObjectResult.LastModified).toGMTString();
            const res = await s3.send(new GetObjectCommand({ Bucket: destBucketName,
                Key: destObjName }));
            assert.strictEqual(res.VersionId, destinationVersionId);
            const responseBody = await res.Body.transformToString();
            assert.strictEqual(responseBody, content);
            assert.deepStrictEqual(res.Metadata, copyVersionMetadata);
            assert.strictEqual(res.LastModified.toGMTString(), copyLastModified);
        }

        async function checkSuccessTagging(key, value) {
            const data = await s3.send(new GetObjectTaggingCommand({ Bucket: destBucketName, Key: destObjName }));
            assert.strictEqual(data.TagSet[0].Key, key);
            assert.strictEqual(data.TagSet[0].Value, value);
        }

        it('should copy an object from a source bucket to a different '+
            'destination bucket and copy the tag set if no tagging directive '+
            'header provided', async () => {
            await s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                CopySource: copySource }));
            await checkSuccessTagging(originalTagKey, originalTagValue);
        });

        it('should copy an object from a source bucket to a different ' +
            'destination bucket and copy the tag set if COPY tagging ' +
            'directive header provided', async () => {
            await s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                CopySource: copySource,
                TaggingDirective: 'COPY' }));
            await checkSuccessTagging(originalTagKey, originalTagValue);
        });

        it('should copy an object from a source to the same destination '+
            'updating tag if REPLACE tagging directive header provided', async () => {
            await s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                CopySource: copySource,
                TaggingDirective: 'REPLACE', Tagging: newTagging }));
            await checkSuccessTagging(newTagKey, newTagValue);
        });

        describe('Copy object with versioning updating tag set', () => {
            taggingTests.forEach(taggingTest => {
                it(taggingTest.it, async () => {
                    const key = encodeURIComponent(taggingTest.tag.key);
                    const value = encodeURIComponent(taggingTest.tag.value);
                    const tagging = `${key}=${value}`;
                    const params = { Bucket: destBucketName, Key: destObjName, CopySource: copySource,
                        TaggingDirective: 'REPLACE',
                        Tagging: tagging };
                    try {
                        await s3.send(new CopyObjectCommand(params));
                        await checkSuccessTagging(taggingTest.tag.key, taggingTest.tag.value);
                    } catch (err) {
                        if (taggingTest.error) {
                            checkError(err, taggingTest.error);
                            return;
                        }
                        checkNoError(err);
                    }
                });
            });
        });

        it('should return InvalidArgument for a request with versionId query', async () => {
            const params = { Bucket: destBucketName, Key: destObjName, CopySource: copySource };
            const query = { versionId: 'testVersionId' };
            try {
                await customS3Request(CopyObjectCommand, params, { query });
                assert.fail('Expected error but did not find one');
            } catch (err) {
                assert.strictEqual(err.name, 'InvalidArgument');
                assert.strictEqual(err.$metadata.httpStatusCode, 400);
            }
        });

        it('should return InvalidArgument for a request with empty string '+
            'versionId query', async () => {
            const params = { Bucket: destBucketName, Key: destObjName, CopySource: copySource };
            const query = { versionId: '' };
            try {
                await customS3Request(CopyObjectCommand, params, { query });
                assert.fail('Expected error but did not find one');
            } catch (err) {
                assert.strictEqual(err.name, 'InvalidArgument');
                assert.strictEqual(err.$metadata.httpStatusCode, 400);
            }
        });

        it('should copy a version from a source bucket to a different' +
            'destination bucket and copy the metadata if no metadata directive' +
            'header provided', async () => {
            const res = await s3.send(new CopyObjectCommand({ Bucket: destBucketName,
                Key: destObjName,
                CopySource: copySource }));
            await successCopyCheck(null, res, originalMetadata, destBucketName, destObjName);
        });

        it('should also copy additional headers (CacheControl, ' +
            'ContentDisposition, ContentEncoding, Expires) when copying an ' +
            'object from a source bucket to a different destination bucket', async () => {
            await s3.send(new CopyObjectCommand({ Bucket: destBucketName,
                Key: destObjName,
                CopySource: copySource }));
            const res = await s3.send(new GetObjectCommand({ Bucket: destBucketName, Key: destObjName }));
            assert.strictEqual(res.CacheControl, originalCacheControl);
            assert.strictEqual(res.ContentDisposition, originalContentDisposition);
            assert.strictEqual(res.ContentEncoding, 'base64,');
            assert.strictEqual(res.Expires.toGMTString(), originalExpires.toGMTString());
        });

        it('should copy an object from a source bucket to a different '+
            'key in the same bucket', async () => {
            const res = await s3.send(new CopyObjectCommand({ Bucket: sourceBucketName,
                Key: destObjName,
                CopySource: copySource }));
            await successCopyCheck(null, res, originalMetadata,
                sourceBucketName, destObjName);
        });

        it('should copy an object from a source to the same destination ' +
            '(update metadata)', async () => {
            const res = await s3.send(new CopyObjectCommand({ Bucket: sourceBucketName, Key: sourceObjName,
                CopySource: copySource,
                MetadataDirective: 'REPLACE',
                Metadata: newMetadata }));
            await successCopyCheck(null, res, newMetadata, sourceBucketName, sourceObjName);
        });

        it('should copy an object and replace the metadata if replace ' +
            'included as metadata directive header', async () => {
            const res = await s3.send(new CopyObjectCommand({ Bucket: destBucketName,
                Key: destObjName,
                CopySource: copySource,
                MetadataDirective: 'REPLACE',
                Metadata: newMetadata }));
            await successCopyCheck(null, res, newMetadata, destBucketName, destObjName);
        });

        it('should copy an object and replace ContentType if replace ' +
            'included as a metadata directive header, and new ContentType is ' +
            'provided', async () => {
            await s3.send(new CopyObjectCommand({ Bucket: destBucketName,
                Key: destObjName,
                CopySource: copySource,
                MetadataDirective: 'REPLACE',
                ContentType: 'image' }));
            const res = await s3.send(new GetObjectCommand({ Bucket: destBucketName, Key: destObjName }));
            assert.strictEqual(res.ContentType, 'image');
        });

        it('should copy an object and keep ContentType if replace ' +
            'included as a metadata directive header, but no new ContentType ' +
            'is provided', async () => {
            await s3.send(new CopyObjectCommand({ Bucket: destBucketName,
                Key: destObjName,
                CopySource: copySource,
                MetadataDirective: 'REPLACE' }));
            const res = await s3.send(new GetObjectCommand({ Bucket: destBucketName,
                Key: destObjName }));
            assert.strictEqual(res.ContentType, 'application/octet-stream');
        });

        it('should also replace additional headers if replace ' +
            'included as metadata directive header and new headers are ' +
            'specified', async () => {
            await s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                CopySource: copySource,
                MetadataDirective: 'REPLACE',
                CacheControl: newCacheControl,
                ContentDisposition: newContentDisposition,
                ContentEncoding: newContentEncoding,
                Expires: newExpires }));
            const res = await s3.send(new GetObjectCommand({ Bucket: destBucketName, Key: destObjName }));
            assert.strictEqual(res.CacheControl, newCacheControl);
            assert.strictEqual(res.ContentDisposition, newContentDisposition);
            assert.strictEqual(res.ContentEncoding, 'gzip,');
            assert.strictEqual(res.Expires.toGMTString(), newExpires.toGMTString());
        });

        it('should copy an object and the metadata if copy ' +
            'included as metadata directive header (and ignore any new ' +
            'metadata sent with copy request)', async () => {
            await s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                CopySource: copySource,
                MetadataDirective: 'COPY',
                Metadata: newMetadata }));
            const res = await s3.send(new GetObjectCommand({ Bucket: destBucketName, Key: destObjName }));
            assert.deepStrictEqual(res.Metadata, originalMetadata);
        });

        it('should copy an object and its additional headers if copy ' +
            'included as metadata directive header (and ignore any new ' +
            'headers sent with copy request)', async () => {
            await s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                CopySource: copySource,
                MetadataDirective: 'COPY',
                Metadata: newMetadata,
                CacheControl: newCacheControl,
                ContentDisposition: newContentDisposition,
                ContentEncoding: newContentEncoding,
                Expires: newExpires }));
            const res = await s3.send(new GetObjectCommand({ Bucket: destBucketName, Key: destObjName }));
            assert.strictEqual(res.CacheControl, originalCacheControl);
            assert.strictEqual(res.ContentDisposition, originalContentDisposition);
            assert.strictEqual(res.ContentEncoding, 'base64,');
            assert.strictEqual(res.Expires.toGMTString(), originalExpires.toGMTString());
        });

        it('should copy a 0 byte object to different destination', async () => {
            const emptyFileETag = '"d41d8cd98f00b204e9800998ecf8427e"';
            const putRes = await s3.send(new PutObjectCommand({ Bucket: sourceBucketName, Key: sourceObjName,
                Body: '',
                Metadata: originalMetadata }));
            copySource = `${sourceBucketName}/${sourceObjName}?versionId=${putRes.VersionId}`;
            const copyRes = await s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                CopySource: copySource }));
            assert.strictEqual(copyRes.CopyObjectResult.ETag, emptyFileETag);
            const getRes = await s3.send(new GetObjectCommand({ Bucket: destBucketName,
                Key: destObjName }));
            assert.deepStrictEqual(getRes.Metadata, originalMetadata);
            assert.strictEqual(getRes.ETag, emptyFileETag);
        });

        if (constants.validStorageClasses.includes('REDUCED_REDUNDANCY')) {
            it('should copy a 0 byte object to same destination', async () => {
                const emptyFileETag = '"d41d8cd98f00b204e9800998ecf8427e"';
                const putRes = await s3.send(new PutObjectCommand({ Bucket: sourceBucketName, Key: sourceObjName,
                    Body: '' }));
                copySource = `${sourceBucketName}/${sourceObjName}?versionId=${putRes.VersionId}`;
                const copyRes = await s3.send(new CopyObjectCommand({ Bucket: sourceBucketName, Key: sourceObjName,
                    CopySource: copySource,
                    StorageClass: 'REDUCED_REDUNDANCY' }));
                assert.notEqual(copyRes.VersionId, putRes.VersionId);
                assert.strictEqual(copyRes.ETag, emptyFileETag);
                const getRes = await s3.send(new GetObjectCommand({ Bucket: sourceBucketName,
                    Key: sourceObjName }));
                assert.deepStrictEqual(getRes.Metadata, {});
                assert.strictEqual(getRes.StorageClass,
                    'REDUCED_REDUNDANCY');
                assert.strictEqual(getRes.ETag, emptyFileETag);
            });

            it('should copy an object to a different destination and change ' +
                'the storage class if storage class header provided', async () => {
                await s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                    CopySource: copySource,
                    StorageClass: 'REDUCED_REDUNDANCY' }));
                const res = await s3.send(new GetObjectCommand({ Bucket: destBucketName, Key: destObjName }));
                assert.strictEqual(res.StorageClass, 'REDUCED_REDUNDANCY');
            });

            it('should copy an object to the same destination and change the ' +
                'storage class if the storage class header provided', async () => {
                await s3.send(new CopyObjectCommand({ Bucket: sourceBucketName, Key: sourceObjName,
                    CopySource: copySource,
                    StorageClass: 'REDUCED_REDUNDANCY' }));
                const res = await s3.send(new GetObjectCommand({ Bucket: sourceBucketName, Key: sourceObjName }));
                assert.strictEqual(res.StorageClass, 'REDUCED_REDUNDANCY');
            });
        }

        it('should copy an object to a new bucket and overwrite an already ' +
            'existing object in the destination bucket', async () => {
            await s3.send(new PutObjectCommand({ Bucket: destBucketName, Key: destObjName,
                Body: 'overwrite me', Metadata: originalMetadata }));
            const copyRes = await s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                CopySource: copySource,
                MetadataDirective: 'REPLACE',
                Metadata: newMetadata }));
            assert.strictEqual(copyRes.CopyObjectResult.ETag, etag);
            const getRes = await s3.send(new GetObjectCommand({ Bucket: destBucketName, Key: destObjName }));
            assert.deepStrictEqual(getRes.Metadata, newMetadata);
            assert.strictEqual(getRes.ETag, etag);
            const body = await getRes.Body.transformToString();
            assert.strictEqual(body, content);
        });

        it.skip('should copy an object and change the server side encryption' +
            'option if server side encryption header provided', async () => {
            await s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                CopySource: copySource,
                ServerSideEncryption: 'AES256' }));
            const res = await s3.send(new GetObjectCommand({ Bucket: destBucketName,
                Key: destObjName }));
            assert.strictEqual(res.ServerSideEncryption, 'AES256');
        });

        it('should return Not Implemented error for obj. encryption using '+
            'customer-provided encryption keys', async () => {
            const params = { Bucket: destBucketName, Key: 'key',
                CopySource: copySource,
                SSECustomerAlgorithm: 'AES256' };
            try {
                await s3.send(new CopyObjectCommand(params));
                assert.fail('Expected NotImplemented error');
            } catch (err) {
                assert.strictEqual(err.name, 'NotImplemented');
            }
        });

        it('should copy an object and set the acl on the new object', async () => {
            await s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                CopySource: copySource,
                ACL: 'authenticated-read' }));
            const res = await s3.send(new GetObjectAclCommand({ Bucket: destBucketName,
                Key: destObjName }));
            assert.strictEqual(res.Grants.length, 2);
            assert.strictEqual(res.Grants[0].Permission, 'FULL_CONTROL');
            assert.strictEqual(res.Grants[1].Permission, 'READ');
            assert.strictEqual(res.Grants[1].Grantee.URI,
                'http://acs.amazonaws.com/groups/global/AuthenticatedUsers');
        });

        it('should copy an object and default the acl on the new object ' +
            'to private even if the copied object had a ' +
            'different acl', async () => {
            await s3.send(new PutObjectAclCommand({ Bucket: sourceBucketName, Key: sourceObjName,
                ACL: 'authenticated-read',
                VersionId: versionId }));
            await s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                CopySource: copySource }));
            const res = await s3.send(new GetObjectAclCommand({ Bucket: destBucketName,
                Key: destObjName }));
            assert.strictEqual(res.Grants.length, 1);
            assert.strictEqual(res.Grants[0].Permission, 'FULL_CONTROL');
        });

        it('should copy a version to same object name to restore '+
            'version of object', async () => {
            const res = await s3.send(new CopyObjectCommand({ Bucket: sourceBucketName, Key: sourceObjName,
                CopySource: copySource }));
            await successCopyCheck(null, res, originalMetadata, sourceBucketName, sourceObjName);
        });

        it('should return an error if attempt to copy from nonexistent bucket', async () => {
            try {
                await s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                    CopySource: `nobucket453234/${sourceObjName}` }));
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'NoSuchBucket');
            }
        });

        it('should return an error if use invalid redirect location', async () => {
            try {
                await s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                    CopySource: copySource,
                    WebsiteRedirectLocation: 'google.com' }));
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'InvalidRedirectLocation');
            }
        });

        it('should return an error if attempt to copy to nonexistent bucket', async () => {
            try {
                await s3.send(new CopyObjectCommand({ Bucket: 'nobucket453234', Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}` }));
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'NoSuchBucket');
            }
        });

        it('should return an error if attempt to copy nonexistent object', async () => {
            try {
                await s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                    CopySource: `${sourceBucketName}/nokey` }));
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'NoSuchKey');
            }
        });

        it('should return NoSuchKey if attempt to copy version with delete marker', async () => {
            const delRes = await s3.send(new DeleteObjectCommand({ Bucket: sourceBucketName,
                Key: sourceObjName }));
            assert.strictEqual(delRes.DeleteMarker, true);
            try {
                await s3.send(new CopyObjectCommand({ Bucket: destBucketName,
                    Key: destObjName, CopySource: `${sourceBucketName}/${sourceObjName}` }));
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'NoSuchKey');
            }
        });

        it('should return InvalidRequest if attempt to copy specific version that is a delete marker', async () => {
            const delRes = await s3.send(new DeleteObjectCommand({ Bucket: sourceBucketName,
                Key: sourceObjName }));
            assert.strictEqual(delRes.DeleteMarker, true);
            const deleteMarkerId = delRes.VersionId;
            try {
                await s3.send(new CopyObjectCommand({ Bucket: destBucketName,
                    Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}` +
                    `?versionId=${deleteMarkerId}` }));
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'InvalidRequest');
            }
        });

        it('should return an error if send invalid metadata directive header', async () => {
            try {
                await s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                    CopySource: copySource,
                    MetadataDirective: 'copyHalf' }));
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'InvalidArgument');
            }
        });

        describe('copying by another account', () => {
            const otherAccountBucket = 'otheraccountbucket42342342342';
            const otherAccountKey = 'key';
            beforeEach(async () => {
                await otherAccountBucketUtility.createOne(otherAccountBucket);
            });

            afterEach(async () => {
                await otherAccountBucketUtility.empty(otherAccountBucket);
                await otherAccountBucketUtility.deleteOne(otherAccountBucket);
            });

            it('should not allow an account without read permission on the ' +
                'source object to copy the object', async () => {
                try {
                    await otherAccountS3.send(new CopyObjectCommand({ Bucket: otherAccountBucket,
                        Key: otherAccountKey,
                        CopySource: copySource }));
                    assert.fail('Expected error');
                } catch (err) {
                    checkError(err, 'AccessDenied');
                }
            });

            it('should not allow an account without write permission on the ' +
                'destination bucket to copy the object', async () => {
                await otherAccountS3.send(new PutObjectCommand({ Bucket: otherAccountBucket,
                    Key: otherAccountKey,
                    Body: '' }));
                try {
                    await otherAccountS3.send(new CopyObjectCommand({ Bucket: destBucketName,
                        Key: destObjName,
                        CopySource: `${otherAccountBucket}/${otherAccountKey}` }));
                    assert.fail('Expected error');
                } catch (err) {
                    checkError(err, 'AccessDenied');
                }
            });

            it('should allow an account with read permission on the ' +
                'source object and write permission on the destination ' +
                'bucket to copy the object', async () => {
                await s3.send(new PutObjectAclCommand({ Bucket: sourceBucketName,
                    Key: sourceObjName,
                    ACL: 'public-read',
                    VersionId: versionId }));
                await otherAccountS3.send(new CopyObjectCommand({ Bucket: otherAccountBucket,
                    Key: otherAccountKey,
                    CopySource: copySource }));
            });
        });

        it('If-Match: returns no error when ETag match, with double quotes ' +
            'around ETag', async () => {
            await requestCopy({ CopySourceIfMatch: etag });
        });

        it('If-Match: returns no error when one of ETags match, with double ' +
            'quotes around ETag', async () => {
            await requestCopy({ CopySourceIfMatch: `non-matching,${etag}` });
        });

        it('If-Match: returns no error when ETag match, without double ' +
            'quotes around ETag', async () => {
            await requestCopy({ CopySourceIfMatch: etagTrim });
        });

        it('If-Match: returns no error when one of ETags match, without ' +
            'double quotes around ETag', async () => {
            await requestCopy({ CopySourceIfMatch: `non-matching,${etagTrim}` });
        });

        it('If-Match: returns no error when ETag match with *', async () => {
            await requestCopy({ CopySourceIfMatch: '*' });
        });

        it('If-Match: returns PreconditionFailed when ETag does not match', async () => {
            try {
                await requestCopy({ CopySourceIfMatch: 'non-matching ETag' });
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'PreconditionFailed');
            }
        });

        it('If-None-Match: returns no error when ETag does not match', async () => {
            await requestCopy({ CopySourceIfNoneMatch: 'non-matching' });
        });

        it('If-None-Match: returns no error when all ETags do not match', async () => {
            await requestCopy({ CopySourceIfNoneMatch: 'non-matching,non-matching-either' });
        });

        it('If-None-Match: returns NotModified when ETag match, with double ' +
            'quotes around ETag', async () => {
            try {
                await requestCopy({ CopySourceIfNoneMatch: etag });
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'PreconditionFailed');
            }
        });

        it('If-None-Match: returns NotModified when one of ETags match, with ' +
            'double quotes around ETag', async () => {
            try {
                await requestCopy({ CopySourceIfNoneMatch: `non-matching,${etag}` });
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'PreconditionFailed');
            }
        });

        it('If-None-Match: returns NotModified when ETag match, without ' +
            'double quotes around ETag', async () => {
            try {
                await requestCopy({ CopySourceIfNoneMatch: etagTrim });
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'PreconditionFailed');
            }
        });

        it('If-None-Match: returns NotModified when one of ETags match, ' +
            'without double quotes around ETag', async () => {
            try {
                await requestCopy({ CopySourceIfNoneMatch: `non-matching,${etagTrim}` });
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'PreconditionFailed');
            }
        });

        it('If-Modified-Since: returns no error if Last modified date is ' +
            'greater', async () => {
            await requestCopy({ CopySourceIfModifiedSince: dateFromNow(-1) });
        });
        // Skipping this test, because real AWS does not provide error as
        // expected
        it.skip('If-Modified-Since: returns NotModified if Last modified ' +
            'date is lesser', async () => {
            try {
                await requestCopy({ CopySourceIfModifiedSince: dateFromNow(1) });
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'PreconditionFailed');
            }
        });

        it('If-Modified-Since: returns NotModified if Last modified '+
            'date is equal', async () => {
            try {
                await requestCopy({ CopySourceIfModifiedSince: dateConvert(lastModified) });
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'PreconditionFailed');
            }
        });

        it('If-Unmodified-Since: returns no error when lastModified date is ' +
            'greater', async () => {
            await requestCopy({ CopySourceIfUnmodifiedSince: dateFromNow(1) });
        });

        it('If-Unmodified-Since: returns no error when lastModified ' +
            'date is equal', async () => {
            await requestCopy({ CopySourceIfUnmodifiedSince: dateConvert(lastModified) });
        });

        it('If-Unmodified-Since: returns PreconditionFailed when ' +
            'lastModified date is lesser', async () => {
            try {
                await requestCopy({ CopySourceIfUnmodifiedSince: dateFromNow(-1) });
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'PreconditionFailed');
            }
        });

        it('If-Match & If-Unmodified-Since: returns no error when match Etag ' +
            'and lastModified is greater', async () => {
            await requestCopy({ CopySourceIfMatch: etagTrim, CopySourceIfUnmodifiedSince: dateFromNow(-1) });
        });

        it('If-Match match & If-Unmodified-Since match', async () => {
            await requestCopy({ CopySourceIfMatch: etagTrim, CopySourceIfUnmodifiedSince: dateFromNow(1) });
        });

        it('If-Match not match & If-Unmodified-Since not match', async () => {
            try {
                await requestCopy({ CopySourceIfMatch: 'non-matching', CopySourceIfUnmodifiedSince: dateFromNow(-1) });
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'PreconditionFailed');
            }
        });

        it('If-Match not match & If-Unmodified-Since match', async () => {
            try {
                await requestCopy({
                    CopySourceIfMatch: 'non-matching',
                    CopySourceIfUnmodifiedSince: dateFromNow(1) });
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'PreconditionFailed');
            }
        });

        it.skip('If-Match match & If-Modified-Since not match', async () => {
            await requestCopy({ CopySourceIfMatch: etagTrim, CopySourceIfModifiedSince: dateFromNow(1) });
        });

        it('If-Match match & If-Modified-Since match', async () => {
            await requestCopy({
                CopySourceIfMatch: etagTrim,
                CopySourceIfModifiedSince: dateFromNow(-1) });
        });

        it('If-Match not match & If-Modified-Since not match', async () => {
            try {
                await requestCopy({
                    CopySourceIfMatch: 'non-matching',
                    CopySourceIfModifiedSince: dateFromNow(1) });
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'PreconditionFailed');
            }
        });

        it('If-Match not match & If-Modified-Since match', async () => {
            try {
                await requestCopy({
                    CopySourceIfMatch: 'non-matching',
                    CopySourceIfModifiedSince: dateFromNow(-1) });
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'PreconditionFailed');
            }
        });

        it('If-None-Match & If-Modified-Since: returns NotModified when Etag ' +
            'does not match and lastModified is greater', async () => {
            try {
                await requestCopy({
                    CopySourceIfNoneMatch: etagTrim,
                    CopySourceIfModifiedSince: dateFromNow(-1) });
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'PreconditionFailed');
            }
        });

        it('If-None-Match not match & If-Modified-Since not match', async () => {
            try {
                await requestCopy({
                    CopySourceIfNoneMatch: etagTrim,
                    CopySourceIfModifiedSince: dateFromNow(1) });
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'PreconditionFailed');
            }
        });

        it('If-None-Match match & If-Modified-Since match', async () => {
            await requestCopy({
                CopySourceIfNoneMatch: 'non-matching',
                CopySourceIfModifiedSince: dateFromNow(-1) });
        });

        it.skip('If-None-Match match & If-Modified-Since not match', async () => {
            try {
                await requestCopy({
                    CopySourceIfNoneMatch: 'non-matching',
                    CopySourceIfModifiedSince: dateFromNow(1) });
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'PreconditionFailed');
            }
        });

        it('If-None-Match match & If-Unmodified-Since match', async () => {
            await requestCopy({
                CopySourceIfNoneMatch: 'non-matching',
                CopySourceIfUnmodifiedSince: dateFromNow(1) });
        });

        it('If-None-Match match & If-Unmodified-Since not match', async () => {
            try {
                await requestCopy({
                    CopySourceIfNoneMatch: 'non-matching',
                    CopySourceIfUnmodifiedSince: dateFromNow(-1) });
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'PreconditionFailed');
            }
        });

        it('If-None-Match not match & If-Unmodified-Since match', async () => {
            try {
                await requestCopy({
                    CopySourceIfNoneMatch: etagTrim,
                    CopySourceIfUnmodifiedSince: dateFromNow(1) });
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'PreconditionFailed');
            }
        });

        it('If-None-Match not match & If-Unmodified-Since not match', async () => {
            try {
                await requestCopy({
                    CopySourceIfNoneMatch: etagTrim,
                    CopySourceIfUnmodifiedSince: dateFromNow(-1) });
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'PreconditionFailed');
            }
        });
    });
});
