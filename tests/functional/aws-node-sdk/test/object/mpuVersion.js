const assert = require('assert');
const { isDeepStrictEqual, promisify } = require('util');

const {
    CreateBucketCommand,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    CompleteMultipartUploadCommand,
    PutObjectCommand,
    PutBucketVersioningCommand,
    DeleteObjectCommand,
    ListObjectsCommand,
    HeadObjectCommand,
    GetObjectCommand,
    PutObjectAclCommand,
    PutObjectTaggingCommand,
    PutObjectLegalHoldCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const metadata = require('../../../../../lib/metadata/wrapper');
const { DummyRequestLogger } = require('../../../../unit/helpers');
const checkError = require('../../lib/utility/checkError');
const { getMetadata, fakeMetadataArchive, isNullKeyMetadataV1 } = require('../utils/init');
const { hasColdStorage } = require('../../lib/utility/test-utils');

const {
    LOCATION_NAME_DMF,
} = require('../../../../constants');

const log = new DummyRequestLogger();

const bucketName = 'bucket1putversion33';
const objectName = 'object1putversion';
const bucketNameMD = 'restore-metadata-copy-bucket-mpu';
const mdListingParams = { listingType: 'DelimiterVersions', maxKeys: 1000 };
const archive = {
    archiveInfo: {},
    restoreRequestedAt: new Date(0).toString(),
    restoreRequestedDays: 5,
};

const fakeMetadataArchivePromise = promisify(fakeMetadataArchive);

const getMetadataPromise = promisify(getMetadata);

const metadataListObjectPromise = promisify(metadata.listObject.bind(metadata));

const metadataPutObjectMDPromise = promisify(metadata.putObjectMD.bind(metadata));

async function putMPUVersion(s3, bucketName, objectName, vId) {
    const params = { Bucket: bucketName, Key: objectName };
    const command = new CreateMultipartUploadCommand(params);
    if (vId !== undefined) {
        command.middlewareStack.add(
            next => args => {
                // eslint-disable-next-line no-param-reassign
                args.request.headers['x-scal-s3-version-id'] = vId;
                return next(args);
            },
            { step: 'build' }
        );
    }
    const resCreation = await s3.send(command);
    
    const uploadId = resCreation.UploadId;
    const uploadParams = {
        Body: 'okok',
        Bucket: bucketName,
        Key: objectName,
        PartNumber: 1,
        UploadId: uploadId,
    };
    const uploadCommand = new UploadPartCommand(uploadParams);
    if (vId !== undefined) {
        uploadCommand.middlewareStack.add(
            next => args => {
                // eslint-disable-next-line no-param-reassign
                args.request.headers['x-scal-s3-version-id'] = vId;
                return next(args);
            },
            { step: 'build' }
        );
    }
    const uploadRes = await s3.send(uploadCommand);
    
    const completeParams = {
        Bucket: bucketName,
        Key: objectName,
        MultipartUpload: {
            Parts: [
                {
                    ETag: uploadRes.ETag,
                    PartNumber: 1
                },
            ]
        },
        UploadId: uploadId,
    };
    const completeCommand = new CompleteMultipartUploadCommand(completeParams);
    if (vId !== undefined) {
        completeCommand.middlewareStack.add(
            next => args => {
                // eslint-disable-next-line no-param-reassign
                args.request.headers['x-scal-s3-version-id'] = vId;
                return next(args);
            },
            { step: 'build' }
        );
    }
    return await s3.send(completeCommand);
}

async function putMPU(s3, bucketName, objectName) {
    return putMPUVersion(s3, bucketName, objectName, undefined);
}

function checkVersionsAndUpdate(versionsBefore, versionsAfter, indexes) {
    indexes.forEach(i => {
        assert.notStrictEqual(versionsAfter[i].value.Size, versionsBefore[i].value.Size);
        assert.strictEqual(versionsAfter[i].value.ETag, versionsBefore[i].value.ETag);
        /* eslint-disable no-param-reassign */
        versionsBefore[i].value.Size = versionsAfter[i].value.Size;
        // Also update uploadId if it exists and is different since now aws sdk returns it as well
        if (versionsAfter[i].value.uploadId && versionsBefore[i].value.uploadId &&
            versionsAfter[i].value.uploadId !== versionsBefore[i].value.uploadId) {
            versionsBefore[i].value.uploadId = versionsAfter[i].value.uploadId;
        }
        /* eslint-enable no-param-reassign */
    });
}

function checkObjMdAndUpdate(objMDBefore, objMDAfter, props) {
    props.forEach(p => {
        assert.notStrictEqual(objMDAfter[p], objMDBefore[p]);
        // eslint-disable-next-line no-param-reassign
        objMDBefore[p] = objMDAfter[p];
    });
    if (objMDBefore['content-type'] && !objMDAfter['content-type']) {
        // eslint-disable-next-line no-param-reassign
        delete objMDBefore['content-type'];
    }
}

function clearUploadIdFromVersions(versions) {
    if (!versions || versions.length === 0) {
        return versions;
    }

    for (const version of versions) {
        if (version.value) {
            version.value.uploadId = undefined;
        }
    }

    return versions;
}

// TODO: CLDSRV-721 RING 10 Support ObjectRestore (cold storage) with MD v1
// The cold storage test suite is skipped as bad versionId while faking archive breaks after each bucket cleanup
const describeSkipNullMdV1 = isNullKeyMetadataV1 || !hasColdStorage ? describe.skip : describe;

describe('MPU with x-scal-s3-version-id header', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        beforeEach(async () => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            await new Promise((resolve, reject) => {
                metadata.setup(err => err ? reject(err) : resolve());
            });
            await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
            await s3.send(new CreateBucketCommand({ 
                Bucket: bucketNameMD, 
                ObjectLockEnabledForBucket: true 
            }));
        });

        afterEach(async () => {
            await bucketUtil.emptyMany([bucketName, bucketNameMD]);
            await bucketUtil.deleteMany([bucketName, bucketNameMD]);
        });

        describe('error handling validation (without cold storage location)', () => {
            it('should fail if version is invalid', async () => {
                const vParams = {
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    }
                };
                const params = { Bucket: bucketName, Key: objectName };

                try {
                    await s3.send(new PutBucketVersioningCommand(vParams));
                    await s3.send(new PutObjectCommand(params));
                    
                    try {
                        await putMPUVersion(s3, bucketName, objectName, 'aJLWKz4Ko9IjBBgXKj5KQT.G9UHv0g7P');
                        throw new Error('Expected InvalidArgument error');
                    } catch (err) {
                        checkError(err, 'InvalidArgument', 400);
                    }
                } catch (err) {
                    if (err.message === 'Expected InvalidArgument error') {
                        throw err;
                    }
                    throw new Error(`Expected success got error ${JSON.stringify(err)}`);
                }
            });

            it('should fail if key does not exist', async () => {
                try {
                    await putMPUVersion(s3, bucketName, objectName, '');
                    throw new Error('Expected NoSuchKey error');
                } catch (err) {
                    checkError(err, 'NoSuchKey', 404);
                }
            });

            it('should fail if version does not exist', async () => {
                const vParams = {
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    }
                };
                const params = { Bucket: bucketName, Key: objectName };

                try {
                    await s3.send(new PutBucketVersioningCommand(vParams));
                    await s3.send(new PutObjectCommand(params));
                    
                    try {
                        await putMPUVersion(s3, bucketName, objectName, 
                            '393833343735313131383832343239393939393952473030312020313031');
                        throw new Error('Expected NoSuchVersion error');
                    } catch (err) {
                        checkError(err, 'NoSuchVersion', 404);
                    }
                } catch (err) {
                    if (err.message === 'Expected NoSuchVersion error') {
                        throw err;
                    }
                    throw new Error(`Expected success got error ${JSON.stringify(err)}`);
                }
            });

            it('should fail if archiving is not in progress', async () => {
                const params = { Bucket: bucketName, Key: objectName };

                try {
                    await s3.send(new PutObjectCommand(params));
                    
                    try {
                        await putMPUVersion(s3, bucketName, objectName, '');
                        throw new Error('Expected InvalidObjectState error');
                    } catch (err) {
                        checkError(err, 'InvalidObjectState', 403);
                    }
                } catch (err) {
                    if (err.message === 'Expected InvalidObjectState error') {
                        throw err;
                    }
                    throw new Error(`Expected success got error ${JSON.stringify(err)}`);
                }
            });

            it('should fail if trying to overwrite a delete marker', async () => {
                const params = { Bucket: bucketName, Key: objectName };
                const vParams = {
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    }
                };
                let vId;

                try {
                    await s3.send(new PutBucketVersioningCommand(vParams));
                    await s3.send(new PutObjectCommand(params));

                    const deleteRes = await s3.send(new DeleteObjectCommand(params));
                    vId = deleteRes.VersionId;
                    
                    putMPUVersion(s3, bucketName, objectName, vId).then(() => {
                        throw new Error('Expected MethodNotAllowed error');
                    }).catch(err => {
                        checkError(err, 'MethodNotAllowed', 405);
                    });
                } catch (err) {
                    if (err.message === 'Expected MethodNotAllowed error') {
                        throw err;
                    }
                    throw new Error(`Expected success got error ${JSON.stringify(err)}`);
                }
            });
        });

        describeSkipNullMdV1('with cold storage location', () => {
            it('should overwrite an MPU object', async () => {
                let objMDBefore;
                let objMDAfter;
                let versionsBefore;

                try {
                    await putMPU(s3, bucketName, objectName);
                    
                    await fakeMetadataArchivePromise(bucketName, objectName, undefined, archive);
                    
                    objMDBefore = await getMetadataPromise(bucketName, objectName, undefined);
                    
                    const versionRes1 = await metadataListObjectPromise(bucketName, mdListingParams, log);
                    versionsBefore = versionRes1.Versions;

                    await putMPUVersion(s3, bucketName, objectName, '');
                    
                    objMDAfter = await getMetadataPromise(bucketName, objectName, undefined);
                    
                    const versionRes2 = await metadataListObjectPromise(bucketName, mdListingParams, log);
                    const versionsAfter = versionRes2.Versions;
                    
                    clearUploadIdFromVersions(versionsBefore);
                    clearUploadIdFromVersions(versionsAfter);
                    
                    assert.deepStrictEqual(versionsAfter, versionsBefore);
                    checkObjMdAndUpdate(objMDBefore, objMDAfter,
                        ['location', 'uploadId', 'microVersionId', 'x-amz-restore',
                        'archive', 'dataStoreName', 'originOp']);

                    assert.deepStrictEqual(objMDAfter, objMDBefore);
                } catch (err) {
                    throw new Error(`Expected success got error ${JSON.stringify(err)}`);
                }
            });

            it('should overwrite an object', async () => {
                    const params = { Bucket: bucketName, Key: objectName };

                    await s3.send(new PutObjectCommand(params));

                    await fakeMetadataArchivePromise(bucketName, objectName, undefined, archive);
                    
                    const objMDBefore = await getMetadataPromise(bucketName, objectName, undefined);
                    
                    const versionRes1 = await metadataListObjectPromise(bucketName, mdListingParams, log);
                    const versionsBefore = clearUploadIdFromVersions(versionRes1.Versions);

                    await putMPUVersion(s3, bucketName, objectName, '');

                    const objMDAfter = await getMetadataPromise(bucketName, objectName, undefined);

                    const versionRes2 = await metadataListObjectPromise(bucketName, mdListingParams, log);
                    const versionsAfter = clearUploadIdFromVersions(versionRes2.Versions);

                    checkVersionsAndUpdate(versionsBefore, versionsAfter, [0]);

                    assert.deepStrictEqual(versionsAfter, versionsBefore);

                    checkObjMdAndUpdate(objMDBefore, objMDAfter,
                        ['location', 'content-length', 'originOp', 'uploadId', 'microVersionId',
                        'x-amz-restore', 'archive', 'dataStoreName']);
                
                    assert.deepStrictEqual(objMDAfter, objMDBefore);
            });

            it('should overwrite a version', async () => {
                const vParams = {
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    }
                };
                const params = { Bucket: bucketName, Key: objectName };

                await s3.send(new PutBucketVersioningCommand(vParams));
                
                const putRes = await s3.send(new PutObjectCommand(params));
                const vId = putRes.VersionId;

                await fakeMetadataArchivePromise(bucketName, objectName, vId, archive);
                
                const versionRes1 = await metadataListObjectPromise(bucketName, mdListingParams, log);
                const versionsBefore = clearUploadIdFromVersions(versionRes1.Versions);

                const objMDBefore = await getMetadataPromise(bucketName, objectName, vId);

                await putMPUVersion(s3, bucketName, objectName, vId);

                const objMDAfter = await getMetadataPromise(bucketName, objectName, vId);
                
                const versionRes2 = await metadataListObjectPromise(bucketName, mdListingParams, log);
                const versionsAfter = clearUploadIdFromVersions(versionRes2.Versions);

                checkVersionsAndUpdate(versionsBefore, versionsAfter, [0]);
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                checkObjMdAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'originOp', 'uploadId', 'microVersionId',
                    'x-amz-restore', 'archive', 'dataStoreName']);
                assert.deepStrictEqual(objMDAfter, objMDBefore);
            });

            it('should overwrite the current version if empty version id header', async () => {
                const vParams = {
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    }
                };
                const params = { Bucket: bucketName, Key: objectName };

                await s3.send(new PutBucketVersioningCommand(vParams));
                
                const putRes = await s3.send(new PutObjectCommand(params));
                const vId = putRes.VersionId;

                await fakeMetadataArchivePromise(bucketName, objectName, vId, archive);
                
                const versionRes1 = await metadataListObjectPromise(bucketName, mdListingParams, log);
                const versionsBefore = clearUploadIdFromVersions(versionRes1.Versions);

                const objMDBefore = await getMetadataPromise(bucketName, objectName, vId);

                await putMPUVersion(s3, bucketName, objectName, '');

                const objMDAfter = await getMetadataPromise(bucketName, objectName, vId);
                
                const versionRes2 = await metadataListObjectPromise(bucketName, mdListingParams, log);
                const versionsAfter = clearUploadIdFromVersions(versionRes2.Versions);

                checkVersionsAndUpdate(versionsBefore, versionsAfter, [0]);
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                checkObjMdAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'originOp', 'uploadId', 'microVersionId',
                    'x-amz-restore', 'archive', 'dataStoreName']);
                assert.deepStrictEqual(objMDAfter, objMDBefore);
            });

            it('should overwrite a non-current null version', async () => {
                const vParams = {
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    }
                };
                const params = { Bucket: bucketName, Key: objectName };
                
                await s3.send(new PutObjectCommand(params));
                await s3.send(new PutBucketVersioningCommand(vParams));
                await s3.send(new PutObjectCommand(params));
                
                await fakeMetadataArchivePromise(bucketName, objectName, 'null', archive);
                const objMDBefore = await getMetadataPromise(bucketName, objectName, 'null');

                const versionRes1 = await metadataListObjectPromise(bucketName, mdListingParams, log);
                const versionsBefore = clearUploadIdFromVersions(versionRes1.Versions);

                await putMPUVersion(s3, bucketName, objectName, 'null');

                const objMDAfter = await getMetadataPromise(bucketName, objectName, 'null');

                const versionRes2 = await metadataListObjectPromise(bucketName, mdListingParams, log);
                const versionsAfter = clearUploadIdFromVersions(versionRes2.Versions);

                checkVersionsAndUpdate(versionsBefore, versionsAfter, [1]);
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                checkObjMdAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'originOp', 'uploadId', 'microVersionId',
                    'x-amz-restore', 'archive', 'dataStoreName']);
                assert.deepStrictEqual(objMDAfter, objMDBefore);
            });

            it('should overwrite the lastest version and keep nullVersionId', async () => {
                const vParams = {
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    }
                };
                const params = { Bucket: bucketName, Key: objectName };
                
                await s3.send(new PutObjectCommand(params));
                await s3.send(new PutBucketVersioningCommand(vParams));
                
                const putRes = await s3.send(new PutObjectCommand(params));
                const vId = putRes.VersionId;

                await fakeMetadataArchivePromise(bucketName, objectName, vId, archive);

                const objMDBefore = await getMetadataPromise(bucketName, objectName, vId);

                const versionRes1 = await metadataListObjectPromise(bucketName, mdListingParams, log);
                const versionsBefore = clearUploadIdFromVersions(versionRes1.Versions);

                await putMPUVersion(s3, bucketName, objectName, vId);

                const objMDAfter = await getMetadataPromise(bucketName, objectName, vId);

                const versionRes2 = await metadataListObjectPromise(bucketName, mdListingParams, log);
                const versionsAfter = clearUploadIdFromVersions(versionRes2.Versions);

                checkVersionsAndUpdate(versionsBefore, versionsAfter, [0]);
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                checkObjMdAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'originOp', 'uploadId', 'microVersionId',
                    'x-amz-restore', 'archive', 'dataStoreName']);
                assert.deepStrictEqual(objMDAfter, objMDBefore);
            });

            it('should overwrite a current null version', async () => {
                const vParams = {
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    }
                };
                const sParams = {
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Suspended',
                    }
                };
                const params = { Bucket: bucketName, Key: objectName };
                
                await s3.send(new PutBucketVersioningCommand(vParams));
                await s3.send(new PutObjectCommand(params));
                await s3.send(new PutBucketVersioningCommand(sParams));
                await s3.send(new PutObjectCommand(params));
                
                await fakeMetadataArchivePromise(bucketName, objectName, undefined, archive);

                const objMDBefore = await getMetadataPromise(bucketName, objectName, undefined);

                const versionRes1 = await metadataListObjectPromise(bucketName, mdListingParams, log);
                const versionsBefore = clearUploadIdFromVersions(versionRes1.Versions);

                await putMPUVersion(s3, bucketName, objectName, '');

                const objMDAfter = await getMetadataPromise(bucketName, objectName, undefined);

                const versionRes2 = await metadataListObjectPromise(bucketName, mdListingParams, log);
                const versionsAfter = clearUploadIdFromVersions(versionRes2.Versions);

                checkVersionsAndUpdate(versionsBefore, versionsAfter, [0]);
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                checkObjMdAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'originOp', 'uploadId', 'microVersionId',
                    'x-amz-restore', 'archive', 'dataStoreName']);
                assert.deepStrictEqual(objMDAfter, objMDBefore);
            });

            it('should overwrite a non-current version', async () => {
                const vParams = {
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    }
                };
                const params = { Bucket: bucketName, Key: objectName };
            
                await s3.send(new PutBucketVersioningCommand(vParams));
                await s3.send(new PutObjectCommand(params));
                
                const putRes = await s3.send(new PutObjectCommand(params));
                const vId = putRes.VersionId;

                await s3.send(new PutObjectCommand(params));
                
                await fakeMetadataArchivePromise(bucketName, objectName, vId, archive);

                const objMDBefore = await getMetadataPromise(bucketName, objectName, vId);
                
                const versionRes1 = await metadataListObjectPromise(bucketName, mdListingParams, log);
                const versionsBefore = clearUploadIdFromVersions(versionRes1.Versions);

                await putMPUVersion(s3, bucketName, objectName, vId);

                const objMDAfter = await getMetadataPromise(bucketName, objectName, vId);

                const versionRes2 = await metadataListObjectPromise(bucketName, mdListingParams, log);
                const versionsAfter = clearUploadIdFromVersions(versionRes2.Versions);

                checkVersionsAndUpdate(versionsBefore, versionsAfter, [1]);
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                checkObjMdAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'originOp', 'uploadId', 'microVersionId',
                    'x-amz-restore', 'archive', 'dataStoreName']);
                assert.deepStrictEqual(objMDAfter, objMDBefore);
            });

            it('should overwrite the current version', async () => {
                const vParams = {
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    }
                };
                const params = { Bucket: bucketName, Key: objectName };

                await s3.send(new PutBucketVersioningCommand(vParams));
                await s3.send(new PutObjectCommand(params));
                
                const putRes = await s3.send(new PutObjectCommand(params));
                const vId = putRes.VersionId;

                await fakeMetadataArchivePromise(bucketName, objectName, vId, archive);
                
                const versionRes1 = await metadataListObjectPromise(bucketName, mdListingParams, log);
                const versionsBefore = clearUploadIdFromVersions(versionRes1.Versions);

                const objMDBefore = await getMetadataPromise(bucketName, objectName, vId);

                await putMPUVersion(s3, bucketName, objectName, vId);

                const objMDAfter = await getMetadataPromise(bucketName, objectName, vId);
                
                const versionRes2 = await metadataListObjectPromise(bucketName, mdListingParams, log);
                const versionsAfter = clearUploadIdFromVersions(versionRes2.Versions);

                checkVersionsAndUpdate(versionsBefore, versionsAfter, [0]);
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                checkObjMdAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'originOp', 'uploadId', 'microVersionId',
                    'x-amz-restore', 'archive', 'dataStoreName']);
                assert.deepStrictEqual(objMDAfter, objMDBefore);
            });

            it('should overwrite the current version after bucket version suspended', async () => {
                const vParams = {
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    }
                };
                const sParams = {
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Suspended',
                    }
                };
                const params = { Bucket: bucketName, Key: objectName };

                await s3.send(new PutBucketVersioningCommand(vParams));
                await s3.send(new PutObjectCommand(params));
                
                const putRes = await s3.send(new PutObjectCommand(params));
                const vId = putRes.VersionId;

                await fakeMetadataArchivePromise(bucketName, objectName, vId, archive);
                
                const versionRes1 = await metadataListObjectPromise(bucketName, mdListingParams, log);
                const versionsBefore = clearUploadIdFromVersions(versionRes1.Versions);

                const objMDBefore = await getMetadataPromise(bucketName, objectName, vId);
                
                await s3.send(new PutBucketVersioningCommand(sParams));
                
                await putMPUVersion(s3, bucketName, objectName, vId);

                const objMDAfter = await getMetadataPromise(bucketName, objectName, vId);

                const versionRes2 = await metadataListObjectPromise(bucketName, mdListingParams, log);
                const versionsAfter = clearUploadIdFromVersions(versionRes2.Versions);

                checkVersionsAndUpdate(versionsBefore, versionsAfter, [0]);
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                checkObjMdAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'originOp', 'uploadId', 'microVersionId',
                    'x-amz-restore', 'archive', 'dataStoreName']);
                assert.deepStrictEqual(objMDAfter, objMDBefore);
            });

            it('should overwrite the current null version after bucket version enabled', async () => {
                const vParams = {
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    }
                };
                const params = { Bucket: bucketName, Key: objectName };

                await s3.send(new PutObjectCommand(params));
                
                await fakeMetadataArchivePromise(bucketName, objectName, undefined, archive);
                
                const versionRes1 = await metadataListObjectPromise(bucketName, mdListingParams, log);
                const versionsBefore = clearUploadIdFromVersions(versionRes1.Versions);
                
                const objMDBefore = await getMetadataPromise(bucketName, objectName, undefined);
                
                await s3.send(new PutBucketVersioningCommand(vParams));
                
                await putMPUVersion(s3, bucketName, objectName, 'null');

                const objMDAfter = await getMetadataPromise(bucketName, objectName, undefined);

                const versionRes2 = await metadataListObjectPromise(bucketName, mdListingParams, log);
                const versionsAfter = clearUploadIdFromVersions(versionRes2.Versions);

                checkVersionsAndUpdate(versionsBefore, versionsAfter, [0]);
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                checkObjMdAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'originOp', 'uploadId', 'microVersionId',
                    'x-amz-restore', 'archive', 'dataStoreName']);

                assert(isDeepStrictEqual(objMDAfter, objMDBefore), 'Objects should be deeply equal');
            });

            it('should fail if restore is already completed', async () => {
                const params = { Bucket: bucketName, Key: objectName };
                const archiveCompleted = {
                    archiveInfo: {},
                    restoreRequestedAt: new Date(0),
                    restoreRequestedDays: 5,
                    restoreCompletedAt: new Date(10),
                    restoreWillExpireAt: new Date(10 + (5 * 24 * 60 * 60 * 1000)),
                };
                await s3.send(new PutObjectCommand(params));
                
                await fakeMetadataArchivePromise(bucketName, objectName, undefined, archiveCompleted);
                
                try {
                    await putMPUVersion(s3, bucketName, objectName, '');
                    throw new Error('Expected InvalidObjectState error');
                } catch (err) {
                    checkError(err, 'InvalidObjectState', 403);
                }
            });

            [
                'non versioned',
                'versioned',
                'suspended'
            ].forEach(versioning => {
                it(`should update restore metadata while keeping storage class (${versioning})`, async () => {
                    const params = { Bucket: bucketName, Key: objectName };

                    if (versioning === 'versioned') {
                        await s3.send(new PutBucketVersioningCommand({
                            Bucket: bucketName,
                            VersioningConfiguration: { Status: 'Enabled' }
                        }));
                    } else if (versioning === 'suspended') {
                        await s3.send(new PutBucketVersioningCommand({
                            Bucket: bucketName,
                            VersioningConfiguration: { Status: 'Suspended' }
                        }));
                    }
                    
                    await s3.send(new PutObjectCommand(params));
                    
                    await fakeMetadataArchivePromise(bucketName, objectName, undefined, archive);

                    const objMDBefore = await getMetadataPromise(bucketName, objectName, undefined);

                    await metadataListObjectPromise(bucketName, mdListingParams, log);
                    
                    await putMPUVersion(s3, bucketName, objectName, '');

                    const objMDAfter = await getMetadataPromise(bucketName, objectName, undefined);
                    
                    const listRes = await s3.send(new ListObjectsCommand({ Bucket: bucketName }));
                    assert.strictEqual(listRes.Contents.length, 1);
                    assert.strictEqual(listRes.Contents[0].StorageClass, LOCATION_NAME_DMF);
                    
                    const headRes = await s3.send(new HeadObjectCommand(params));
                    assert.strictEqual(headRes.StorageClass, LOCATION_NAME_DMF);
                    
                    const getRes = await s3.send(new GetObjectCommand(params));
                    assert.strictEqual(getRes.StorageClass, LOCATION_NAME_DMF);

                    // Make sure object data location is set back to its bucket data location.
                    assert.deepStrictEqual(objMDAfter.dataStoreName, 'us-east-1');

                    assert.deepStrictEqual(objMDAfter.archive.archiveInfo, objMDBefore.archive.archiveInfo);
                    assert.deepStrictEqual(objMDAfter.archive.restoreRequestedAt,
                        objMDBefore.archive.restoreRequestedAt);
                    assert.deepStrictEqual(objMDAfter.archive.restoreRequestedDays,
                        objMDBefore.archive.restoreRequestedDays);
                    assert.deepStrictEqual(objMDAfter['x-amz-restore']['ongoing-request'], false);

                    assert(objMDAfter.archive.restoreCompletedAt);
                    assert(objMDAfter.archive.restoreWillExpireAt);
                    assert(objMDAfter['x-amz-restore']['expiry-date']);
                });
            });


            it('should "copy" all but non data-related metadata (data encryption, data size...)', async () => {
                const params = {
                    Bucket: bucketNameMD,
                    Key: objectName
                };
                const putParams = {
                    ...params,
                    Metadata: {
                        'custom-user-md': 'custom-md',
                    },
                    WebsiteRedirectLocation: 'http://custom-redirect'
                };
                const aclParams = {
                    ...params,
                    // email of user Bart defined in authdata.json
                    GrantFullControl: 'emailaddress=sampleaccount1@sampling.com',
                };
                const tagParams = {
                    ...params,
                    Tagging: {
                        TagSet: [{
                        Key: 'tag1',
                        Value: 'value1'
                        }, {
                            Key: 'tag2',
                            Value: 'value2'
                        }]
                    }
                };
                const legalHoldParams = {
                    ...params,
                    LegalHold: {
                        Status: 'ON'
                    },
                };
                const acl = {
                    'Canned': '',
                    'FULL_CONTROL': [
                        // canonicalID of user Bart
                        '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
                    ],
                    'WRITE_ACP': [],
                    'READ': [],
                    'READ_ACP': [],
                };
                const tags = { tag1: 'value1', tag2: 'value2' };
                const replicationInfo = {
                    'status': 'COMPLETED',
                    'backends': [
                            {
                                    'site': 'azure-normal',
                                    'status': 'COMPLETED',
                                    'dataStoreVersionId': '',
                            },
                    ],
                    'content': [
                            'DATA',
                            'METADATA',
                    ],
                    'destination': 'arn:aws:s3:::versioned',
                    'storageClass': 'azure-normal',
                    'role': 'arn:aws:iam::root:role/s3-replication-role',
                    'storageType': 'azure',
                    'dataStoreVersionId': '',
                    'isNFS': null,
                };
                await s3.send(new PutObjectCommand(putParams));
                await s3.send(new PutObjectAclCommand(aclParams));
                await s3.send(new PutObjectTaggingCommand(tagParams));
                await s3.send(new PutObjectLegalHoldCommand(legalHoldParams));

                const objMD = await getMetadataPromise(bucketNameMD, objectName, undefined);

                objMD.dataStoreName = LOCATION_NAME_DMF;
                objMD.archive = archive;
                objMD.replicationInfo = replicationInfo;
                // data related
                objMD['content-length'] = 99;
                objMD['content-type'] = 'testtype';
                objMD['content-md5'] = 'testmd5';
                objMD['content-encoding'] = 'testencoding';
                objMD['x-amz-server-side-encryption'] = 'aws:kms';

                
                await metadataPutObjectMDPromise(bucketNameMD, objectName, objMD, undefined, log);

                await putMPUVersion(s3, bucketNameMD, objectName, '');

                const finalObjMD = await getMetadataPromise(bucketNameMD, objectName, undefined);
                assert.deepStrictEqual(finalObjMD.acl, acl);
                assert.deepStrictEqual(finalObjMD.tags, tags);
                assert.deepStrictEqual(finalObjMD.replicationInfo, replicationInfo);
                assert.deepStrictEqual(finalObjMD.legalHold, true);
                assert.strictEqual(finalObjMD['x-amz-meta-custom-user-md'], 'custom-md');
                assert.strictEqual(finalObjMD['x-amz-website-redirect-location'], 'http://custom-redirect');
                // make sure data related metadatas ar not the same before and after
                assert.notStrictEqual(finalObjMD['x-amz-server-side-encryption'], 'aws:kms');
                assert.notStrictEqual(finalObjMD['content-length'], 99);
                assert.notStrictEqual(finalObjMD['content-encoding'], 'testencoding');
                assert.notStrictEqual(finalObjMD['content-type'], 'testtype');
                // make sure we keep the same etag and add the new restored
                // data's etag inside x-amz-restore
                assert.strictEqual(finalObjMD['content-md5'], 'testmd5');
                assert.strictEqual(typeof finalObjMD['x-amz-restore']['content-md5'], 'string');
                
                // removing legal hold to be able to clean the bucket after the test
                legalHoldParams.LegalHold.Status = 'OFF';
                await s3.send(new PutObjectLegalHoldCommand(legalHoldParams));
            });
        });
    });
});
