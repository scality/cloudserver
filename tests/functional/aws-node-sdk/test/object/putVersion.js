const assert = require('assert');
const async = require('async');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const metadata = require('../../../../../lib/metadata/wrapper');
const { DummyRequestLogger } = require('../../../../unit/helpers');
const checkError = require('../../lib/utility/checkError');
const { getMetadata, fakeMetadataArchive, isNullKeyMetadataV1 } = require('../utils/init');
const { hasColdStorage } = require('../../lib/utility/test-utils');
const { CreateBucketCommand, 
    PutObjectCommand, 
    HeadObjectCommand, 
    GetObjectCommand,
    PutObjectAclCommand,
    PutObjectTaggingCommand,
    PutObjectLegalHoldCommand,
    ListObjectsCommand,
    DeleteObjectCommand,
    PutBucketVersioningCommand } = require('@aws-sdk/client-s3');

const {
    LOCATION_NAME_DMF,
} = require('../../../../constants');
const log = new DummyRequestLogger();

const bucketName = 'bucket1putversion32';
const objectName = 'object1putversion';
const bucketNameMD = 'restore-metadata-copy-bucket';
const mdListingParams = { listingType: 'DelimiterVersions', maxKeys: 1000 };
const archive = {
    archiveInfo: {},
    restoreRequestedAt: new Date(0).toString(),
    restoreRequestedDays: 5,
};

function putObjectVersion(s3, params, vid, cb) {
    const paramsWithBody = { ...params, Body: '123' };
    const command = new PutObjectCommand(paramsWithBody);
    command.middlewareStack.add(
        next => async args => {
            // eslint-disable-next-line no-param-reassign
            args.request.headers['x-scal-s3-version-id'] = vid;
            return next(args);
        },
        {
            step: 'build',
            name: 'addVersionIdHeader', // Add a name to identify the middleware
        }
    );

    const promise = s3.send(command);
    return cb ? promise.then(res => cb(null, res), cb) : promise;
}

function clearRestoreStatus(versions) {
    if (!versions || versions.length === 0) {
        return versions;
    }

    for (const version of versions) {
        if (version.value) {
            version.value.restoreStatus = undefined;
        }
    }

    return versions;
}


function checkVersionsAndUpdate(versionsBefore, versionsAfter, indexes) {
    indexes.forEach(i => {
        assert.notStrictEqual(versionsAfter[i].value.Size, versionsBefore[i].value.Size);
        assert.strictEqual(versionsAfter[i].value.ETag, versionsBefore[i].value.ETag);
        /* eslint-disable no-param-reassign */
        versionsBefore[i].value.Size = versionsAfter[i].value.Size;
        /* eslint-enable no-param-reassign */
    });
}

function checkObjMdAndUpdate(objMDBefore, objMDAfter, props) {
    props.forEach(p => {
        assert.notStrictEqual(objMDAfter[p], objMDBefore[p]);
        // eslint-disable-next-line no-param-reassign
        objMDBefore[p] = objMDAfter[p];
    });
}

// TODO: CLDSRV-721 RING 10 Support ObjectRestore (cold storage) with MD v1
// The cold storage test suite is skipped as bad versionId while faking archive breaks after each bucket cleanup
const describeSkipNullMdV1 = isNullKeyMetadataV1 || !hasColdStorage ? describe.skip : describe;

describe('PUT object with x-scal-s3-version-id header', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        beforeEach(done => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            async.series([
                next => metadata.setup(next),
                next => s3.send(new CreateBucketCommand({ Bucket: bucketName })).then(() => {
                    next();
                }),
                next => s3.send(new CreateBucketCommand({ Bucket: bucketNameMD, 
                    ObjectLockEnabledForBucket: true })).then(() => {
                    next();
                }),
            ], done);
        });

        afterEach(async () => {
            await bucketUtil.emptyMany([bucketName, bucketNameMD]);
            await bucketUtil.deleteMany([bucketName, bucketNameMD]);
        });

        describe('error handling validation (without cold storage location)', () => {
            it('should fail if version id is invalid', done => {
                const vParams = {
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    }
                };
                const params = { Bucket: bucketName, Key: objectName };
                async.series([
                    next => s3.send(new PutBucketVersioningCommand(vParams)).then(() => next()),
                    next => s3.send(new PutObjectCommand(params)).then(() => next()),
                    next => putObjectVersion(s3, params, 'aJLWKz4Ko9IjBBgXKj5KQT.G9UHv0g7P', err => {
                        assert.strictEqual(err.name, 'InvalidArgument');
                        assert.strictEqual(err.$metadata.httpStatusCode, 400);
                        return next();
                    }),
                ], err => {
                    assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);
                    return done();
                });
            });

            it('should fail if key does not exist', done => {
                const params = { Bucket: bucketName, Key: objectName };

                async.series([
                    next => putObjectVersion(s3, params, '', err => {
                        checkError(err, 'NoSuchKey', 404);
                        return next();
                    }),
                ], err => {
                    assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);
                    return done();
                });
            });

            it('should fail if version does not exist', done => {
                const vParams = {
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    }
                };
                const params = { Bucket: bucketName, Key: objectName };

                async.series([
                    next => s3.send(new PutBucketVersioningCommand(vParams)).then(() => next()),
                    next => s3.send(new PutObjectCommand(params)).then(() => next()),
                    next => putObjectVersion(s3, params,
                    '393833343735313131383832343239393939393952473030312020313031', err => {
                        checkError(err, 'NoSuchVersion', 404);
                        return next();
                    }),
                ], err => {
                    assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);
                    return done();
                });
            });

            it('should fail if archiving is not in progress', done => {
                const params = { Bucket: bucketName, Key: objectName };

                async.series([
                    next => s3.send(new PutObjectCommand(params)).then(() => next()),
                    next => putObjectVersion(s3, params, '', err => {
                        checkError(err, 'InvalidObjectState', 403);
                        return next();
                    }),
                ], err => {
                    assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);
                    return done();
                });
            });

            it('should fail if trying to overwrite a delete marker', done => {
                const params = { Bucket: bucketName, Key: objectName };
                const vParams = {
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    }
                };
                let vId;

                async.series([
                    next => s3.send(new PutBucketVersioningCommand(vParams)).then(() => next()),
                    next => s3.send(new PutObjectCommand(params)).then(() => next()),
                    next => s3.send(new DeleteObjectCommand(params)).then(res => {
                        vId = res.VersionId;
                        return next();
                    }),
                    next => putObjectVersion(s3, params, vId, err => {
                        checkError(err, 'MethodNotAllowed', 405);
                        return next();
                    }),
                ], err => {
                    assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);
                    return done();
                });
            });
        });

        describeSkipNullMdV1('with cold storage location', () => {
            it('should overwrite an object', done => {
            const params = { Bucket: bucketName, Key: objectName };
                let objMDBefore;
                let objMDAfter;
                let versionsBefore;
                let versionsAfter;
                async.series([
                    next => s3.send(new PutObjectCommand(params)).then(() => {
                        next();
                    }),
                    next => fakeMetadataArchive(bucketName, objectName, undefined, archive, next),
                    next => getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                        objMDBefore = objMD;
                        return next(err);
                    }),
                    next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                        versionsBefore = clearRestoreStatus(res.Versions);
                        return next(err);
                    }),
                    next => putObjectVersion(s3, params, '', next),
                    next => getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                        objMDAfter = objMD;
                        return next(err);
                    }),
                    next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                        versionsAfter = clearRestoreStatus(res.Versions);
                        return next(err);
                    }),
                ], err => {
                    assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);

                    checkVersionsAndUpdate(versionsBefore, versionsAfter, [0]);
                    assert.deepStrictEqual(versionsAfter, versionsBefore);

                    checkObjMdAndUpdate(objMDBefore, objMDAfter, ['location', 'content-length',
                    'microVersionId', 'x-amz-restore', 'archive', 'dataStoreName', 'originOp']);
                    assert.deepStrictEqual(objMDAfter, objMDBefore);
                    return done();
                });
            });

            it('should overwrite a version', done => {
                const vParams = {
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    }
                };
                const params = { Bucket: bucketName, Key: objectName };
                let objMDBefore;
                let objMDAfter;
                let versionsBefore;
                let versionsAfter;
                let vId;

                async.series([
                    next => s3.send(new PutBucketVersioningCommand(vParams)).then(() => next()),
                    next => s3.send(new PutObjectCommand(params)).then(res => {
                        vId = res.VersionId;
                        return next();
                    }),
                    next => fakeMetadataArchive(bucketName, objectName, vId, archive, next),
                    next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                        versionsBefore = clearRestoreStatus(res.Versions);
                        return next(err);
                    }),
                    next => getMetadata(bucketName, objectName, vId, (err, objMD) => {
                        objMDBefore = objMD;
                        return next(err);
                    }),
                    next => putObjectVersion(s3, params, vId, next),
                    next => getMetadata(bucketName, objectName, vId, (err, objMD) => {
                        objMDAfter = objMD;
                        return next(err);
                    }),
                    next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                        versionsAfter = clearRestoreStatus(res.Versions);
                        return next(err);
                    }),
                ], err => {
                    assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);

                    checkVersionsAndUpdate(versionsBefore, versionsAfter, [0]);
                    assert.deepStrictEqual(versionsAfter, versionsBefore);

                    checkObjMdAndUpdate(objMDBefore, objMDAfter, ['location', 'content-length', 'originOp',
                    'microVersionId', 'x-amz-restore', 'archive', 'dataStoreName']);
                    assert.deepStrictEqual(objMDAfter, objMDBefore);
                    return done();
                });
            });

            it('should overwrite the current version if empty version id header', done => {
                const vParams = {
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    }
                };
                const params = { Bucket: bucketName, Key: objectName };
                let objMDBefore;
                let objMDAfter;
                let versionsBefore;
                let versionsAfter;
                let vId;

                async.series([
                    next => s3.send(new PutBucketVersioningCommand(vParams)).then(() => next()),
                    next => s3.send(new PutObjectCommand(params)).then(res => {
                        vId = res.VersionId;
                        return next();
                    }),
                    next => fakeMetadataArchive(bucketName, objectName, vId, archive, next),
                    next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                        versionsBefore = clearRestoreStatus(res.Versions);
                        return next(err);
                    }),
                    next => getMetadata(bucketName, objectName, vId, (err, objMD) => {
                        objMDBefore = objMD;
                        return next(err);
                    }),
                    next => putObjectVersion(s3, params, '', next),
                    next => getMetadata(bucketName, objectName, vId, (err, objMD) => {
                        objMDAfter = objMD;
                        return next(err);
                    }),
                    next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                        versionsAfter = clearRestoreStatus(res.Versions);
                        return next(err);
                    }),
                ], err => {
                    assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);

                    checkVersionsAndUpdate(versionsBefore, versionsAfter, [0]);
                    assert.deepStrictEqual(versionsAfter, versionsBefore);

                    checkObjMdAndUpdate(objMDBefore, objMDAfter, ['location', 'content-length', 'originOp',
                    'microVersionId', 'x-amz-restore', 'archive', 'dataStoreName']);
                    assert.deepStrictEqual(objMDAfter, objMDBefore);
                    return done();
                });
            });

            it('should overwrite a non-current null version', done => {
                const vParams = {
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    }
                };
                const params = { Bucket: bucketName, Key: objectName };
                let versionsBefore;
                let versionsAfter;
                let objMDBefore;
                let objMDAfter;

                async.series([
                    next => s3.send(new PutObjectCommand(params)).then(() => next()),
                    next => s3.send(new PutBucketVersioningCommand(vParams)).then(() => next()),
                    next => s3.send(new PutObjectCommand(params)).then(() => next()),
                    next => fakeMetadataArchive(bucketName, objectName, 'null', archive, next),
                    next => getMetadata(bucketName, objectName, 'null', (err, objMD) => {
                        objMDBefore = objMD;
                        return next(err);
                    }),
                    next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                        versionsBefore = clearRestoreStatus(res.Versions);
                        next(err);
                    }),
                    next => putObjectVersion(s3, params, 'null', next),
                    next => getMetadata(bucketName, objectName, 'null', (err, objMD) => {
                        objMDAfter = objMD;
                        return next(err);
                    }),
                    next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                        versionsAfter = clearRestoreStatus(res.Versions);
                        return next(err);
                    }),
                ], err => {
                    assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);

                    checkVersionsAndUpdate(versionsBefore, versionsAfter, [1]);
                    assert.deepStrictEqual(versionsAfter, versionsBefore);

                    checkObjMdAndUpdate(objMDBefore, objMDAfter, ['location', 'content-length', 'originOp',
                    'microVersionId', 'x-amz-restore', 'archive', 'dataStoreName']);
                    assert.deepStrictEqual(objMDAfter, objMDBefore);
                    return done();
                });
            });

            it('should overwrite the lastest version and keep nullVersionId', done => {
                const vParams = {
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    }
                };
                const params = { Bucket: bucketName, Key: objectName };
                let versionsBefore;
                let versionsAfter;
                let objMDBefore;
                let objMDAfter;
                let vId;

                async.series([
                    next => s3.send(new PutObjectCommand(params)).then(() => next()),
                    next => s3.send(new PutBucketVersioningCommand(vParams)).then(() => next()),
                    next => s3.send(new PutObjectCommand(params)).then(res => {
                        vId = res.VersionId;
                        return next();
                    }),
                    next => fakeMetadataArchive(bucketName, objectName, vId, archive, next),
                    next => getMetadata(bucketName, objectName, vId, (err, objMD) => {
                        objMDBefore = objMD;
                        return next(err);
                    }),
                    next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                        versionsBefore = clearRestoreStatus(res.Versions);
                        return next(err);
                    }),
                    next => putObjectVersion(s3, params, vId, next),
                    next => getMetadata(bucketName, objectName, vId, (err, objMD) => {
                        objMDAfter = objMD;
                        return next(err);
                    }),
                    next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                        versionsAfter = clearRestoreStatus(res.Versions);
                        return next(err);
                    }),
                ], err => {
                    assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);

                    checkVersionsAndUpdate(versionsBefore, versionsAfter, [0]);
                    assert.deepStrictEqual(versionsAfter, versionsBefore);

                    checkObjMdAndUpdate(objMDBefore, objMDAfter, ['location', 'content-length', 'originOp',
                    'microVersionId', 'x-amz-restore', 'archive', 'dataStoreName']);
                    assert.deepStrictEqual(objMDAfter, objMDBefore);
                    return done();
                });
            });

            it('should overwrite a current null version', done => {
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
                let objMDBefore;
                let objMDAfter;
                let versionsBefore;
                let versionsAfter;

                async.series([
                    next => s3.send(new PutBucketVersioningCommand(vParams)).then(() => next()),
                    next => s3.send(new PutObjectCommand(params)).then(() => next()),
                    next => s3.send(new PutBucketVersioningCommand(sParams)).then(() => next()),
                    next => s3.send(new PutObjectCommand(params)).then(() => next()),
                    next => fakeMetadataArchive(bucketName, objectName, undefined, archive, next),
                    next => getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                        objMDBefore = objMD;
                        return next(err);
                    }),
                    next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                        versionsBefore = clearRestoreStatus(res.Versions);
                        return next(err);
                    }),
                    next => putObjectVersion(s3, params, '', next),
                    next => getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                        objMDAfter = objMD;
                        return next(err);
                    }),
                    next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                        versionsAfter = clearRestoreStatus(res.Versions);
                        return next(err);
                    }),
                ], err => {
                    assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);

                    checkVersionsAndUpdate(versionsBefore, versionsAfter, [0]);
                    assert.deepStrictEqual(versionsAfter, versionsBefore);

                    checkObjMdAndUpdate(objMDBefore, objMDAfter, ['location', 'content-length', 'originOp',
                    'microVersionId', 'x-amz-restore', 'archive', 'dataStoreName']);
                    assert.deepStrictEqual(objMDAfter, objMDBefore);
                    return done();
                });
            });

            it('should overwrite a non-current version', done => {
                const vParams = {
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    }
                };
                const params = { Bucket: bucketName, Key: objectName };
                let objMDBefore;
                let objMDAfter;
                let versionsBefore;
                let versionsAfter;
                let vId;

                async.series([
                    next => s3.send(new PutBucketVersioningCommand(vParams)).then(() => next()),
                    next => s3.send(new PutObjectCommand(params)).then(() => next()),
                    next => s3.send(new PutObjectCommand(params)).then(res => {
                        vId = res.VersionId;
                        return next();
                    }),
                    next => fakeMetadataArchive(bucketName, objectName, vId, archive, next),
                    next => s3.send(new PutObjectCommand(params)).then(() => next()),
                    next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                        versionsBefore = clearRestoreStatus(res.Versions);
                        return next(err);
                    }),
                    next => getMetadata(bucketName, objectName, vId, (err, objMD) => {
                        objMDBefore = objMD;
                        return next(err);
                    }),
                    next => putObjectVersion(s3, params, vId, next),
                    next => getMetadata(bucketName, objectName, vId, (err, objMD) => {
                        objMDAfter = objMD;
                        return next(err);
                    }),
                    next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                        versionsAfter = clearRestoreStatus(res.Versions);
                        return next(err);
                    }),
                ], err => {
                    assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);

                    checkVersionsAndUpdate(versionsBefore, versionsAfter, [1]);
                    assert.deepStrictEqual(versionsAfter, versionsBefore);

                    checkObjMdAndUpdate(objMDBefore, objMDAfter, ['location', 'content-length', 'originOp',
                    'microVersionId', 'x-amz-restore', 'archive', 'dataStoreName']);
                    assert.deepStrictEqual(objMDAfter, objMDBefore);
                    return done();
                });
            });

            it('should overwrite the current version', done => {
                const vParams = {
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    }
                };
                const params = { Bucket: bucketName, Key: objectName };
                let objMDBefore;
                let objMDAfter;
                let versionsBefore;
                let versionsAfter;
                let vId;

                async.series([
                    next => s3.send(new PutBucketVersioningCommand(vParams)).then(() => next()),
                    next => s3.send(new PutObjectCommand(params)).then(() => next()),
                    next => s3.send(new PutObjectCommand(params)).then(res => {
                        vId = res.VersionId;
                        return next();
                    }),
                    next => fakeMetadataArchive(bucketName, objectName, vId, archive, next),
                    next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                        versionsBefore = clearRestoreStatus(res.Versions);
                        return next(err);
                    }),
                    next => getMetadata(bucketName, objectName, vId, (err, objMD) => {
                        objMDBefore = objMD;
                        return next(err);
                    }),
                    next => putObjectVersion(s3, params, vId, next),
                    next => getMetadata(bucketName, objectName, vId, (err, objMD) => {
                        objMDAfter = objMD;
                        return next(err);
                    }),
                    next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                        versionsAfter = clearRestoreStatus(res.Versions);
                        return next(err);
                    }),
                ], err => {
                    assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);

                    checkVersionsAndUpdate(versionsBefore, versionsAfter, [0]);
                    assert.deepStrictEqual(versionsAfter, versionsBefore);

                    checkObjMdAndUpdate(objMDBefore, objMDAfter, ['location', 'content-length', 'originOp',
                    'microVersionId', 'x-amz-restore', 'archive', 'dataStoreName']);
                    assert.deepStrictEqual(objMDAfter, objMDBefore);
                    return done();
                });
            });

            it('should overwrite the current version after bucket version suspended', done => {
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
                let objMDBefore;
                let objMDAfter;
                let versionsBefore;
                let versionsAfter;
                let vId;

                async.series([
                    next => s3.send(new PutBucketVersioningCommand(vParams)).then(() => next()),
                    next => s3.send(new PutObjectCommand(params)).then(() => next()),
                    next => s3.send(new PutObjectCommand(params)).then(res => {
                        vId = res.VersionId;
                        return next();
                    }),
                    next => fakeMetadataArchive(bucketName, objectName, vId, archive, next),
                    next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                        versionsBefore = clearRestoreStatus(res.Versions);
                        return next(err);
                    }),
                    next => getMetadata(bucketName, objectName, vId, (err, objMD) => {
                        objMDBefore = objMD;
                        return next(err);
                    }),
                    next => s3.send(new PutBucketVersioningCommand(sParams)).then(() => next()).catch(next),
                    next => putObjectVersion(s3, params, vId, next),
                    next => getMetadata(bucketName, objectName, vId, (err, objMD) => {
                        objMDAfter = objMD;
                        return next(err);
                    }),
                    next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                        versionsAfter = clearRestoreStatus(res.Versions);
                        return next(err);
                    }),
                ], err => {
                    assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);

                    checkVersionsAndUpdate(versionsBefore, versionsAfter, [0]);
                    assert.deepStrictEqual(versionsAfter, versionsBefore);

                    checkObjMdAndUpdate(objMDBefore, objMDAfter, ['location', 'content-length', 'originOp',
                    'microVersionId', 'x-amz-restore', 'archive', 'dataStoreName']);
                    assert.deepStrictEqual(objMDAfter, objMDBefore);
                    return done();
                });
            });

            it('should overwrite the current null version after bucket version enabled', done => {
                const vParams = {
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    }
                };
                const params = { Bucket: bucketName, Key: objectName };
                let objMDBefore;
                let objMDAfter;
                let versionsBefore;
                let versionsAfter;

                async.series([
                    next => s3.send(new PutObjectCommand(params)).then(() => next()),
                    next => fakeMetadataArchive(bucketName, objectName, undefined, archive, next),
                    next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                        versionsBefore = clearRestoreStatus(res.Versions);
                        return next(err);
                    }),
                    next => getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                        objMDBefore = objMD;
                        return next(err);
                    }),
                    next => s3.send(new PutBucketVersioningCommand(vParams)).then(() => next()),
                    next => putObjectVersion(s3, params, 'null', next),
                    next => getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                        objMDAfter = objMD;
                        return next(err);
                    }),
                    next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                        versionsAfter = clearRestoreStatus(res.Versions);
                        return next(err);
                    }),
                ], err => {
                    assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);

                    checkVersionsAndUpdate(versionsBefore, versionsAfter, [0]);
                    assert.deepStrictEqual(versionsAfter, versionsBefore);

                    checkObjMdAndUpdate(objMDBefore, objMDAfter, ['location', 'content-length', 'originOp',
                    'microVersionId', 'x-amz-restore', 'archive', 'dataStoreName']);
                    assert.deepStrictEqual(objMDAfter, objMDBefore);
                    return done();
                });
            });

            it('should fail if restore is already completed', done => {
                const params = { Bucket: bucketName, Key: objectName };
                const archiveCompleted = {
                    archiveInfo: {},
                    restoreRequestedAt: new Date(0),
                    restoreRequestedDays: 5,
                    restoreCompletedAt: new Date(10),
                    restoreWillExpireAt: new Date(10 + (5 * 24 * 60 * 60 * 1000)),
                };

                async.series([
                    next => s3.send(new PutObjectCommand(params)).then(() => next()),
                    next => fakeMetadataArchive(bucketName, objectName, undefined, archiveCompleted, next),
                    next => putObjectVersion(s3, params, '', err => {
                        checkError(err, 'InvalidObjectState', 403);
                        return next();
                    }),
                ], err => {
                    assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);
                    return done();
                });
            });

            [
                'non versioned',
                'versioned',
                'suspended'
            ].forEach(versioning => {
                it(`should update restore metadata while keeping storage class (${versioning})`, done => {
                    const params = { Bucket: bucketName, Key: objectName };
                    let objMDBefore;
                    let objMDAfter;
        

                    async.series([
                        next => {
                            if (versioning === 'versioned') {
                                return s3.send(new PutBucketVersioningCommand({
                                    Bucket: bucketName,
                                    VersioningConfiguration: { Status: 'Enabled' }
                                })).then(() => next());
                            } else if (versioning === 'suspended') {
                                return s3.send(new PutBucketVersioningCommand({
                                    Bucket: bucketName,
                                    VersioningConfiguration: { Status: 'Suspended' }
                                })).then(() => next());
                            }
                            return next();
                        },
                        next => s3.send(new PutObjectCommand(params)).then(() => next()),
                        next => fakeMetadataArchive(bucketName, objectName, undefined, archive, next),
                        next => getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                            objMDBefore = objMD;
                            return next(err);
                        }),
                        next => metadata.listObject(bucketName, mdListingParams, log, err => next(err)),
                        next => putObjectVersion(s3, params, '', next),
                        next => getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                            objMDAfter = objMD;
                            return next(err);
                        }),
                        next => s3.send(new ListObjectsCommand({ Bucket: bucketName })).then(res => {
                            assert.strictEqual(res.Contents.length, 1);
                            assert.strictEqual(res.Contents[0].StorageClass, LOCATION_NAME_DMF);
                            return next();
                        }).catch(err => {
                            assert.ifError(err);
                            return next(err);
                        }),
                        next => s3.send(new HeadObjectCommand(params)).then(res => {
                            assert.strictEqual(res.StorageClass, LOCATION_NAME_DMF);
                            return next();
                        }).catch(err => {
                            assert.ifError(err);
                            return next(err);
                        }),
                        next => s3.send(new GetObjectCommand(params)).then(res => {
                            assert.strictEqual(res.StorageClass, LOCATION_NAME_DMF);
                            return next();
                        }).catch(err => {
                            assert.ifError(err);
                            return next(err);
                        }),
                    ], err => {
                        assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);

                        // storage class must stay as the cold location
                        assert.deepStrictEqual(objMDAfter['x-amz-storage-class'], LOCATION_NAME_DMF);

                        /// Make sure object data location is set back to its bucket data location.
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
                        return done();
                    });
                });
            });

            it('should "copy" all but non data-related metadata (data encryption, data size...)', done => {
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
                async.series([
                    next => s3.send(new PutObjectCommand(putParams)).then(() => next()),
                    next => s3.send(new PutObjectAclCommand(aclParams)).then(() => next()),
                    next => s3.send(new PutObjectTaggingCommand(tagParams)).then(() => next()),
                    next => s3.send(new PutObjectLegalHoldCommand(legalHoldParams)).then(() => next()),
                    next => getMetadata(bucketNameMD, objectName, undefined, (err, objMD) => {
                        if (err) {
                            return next(err);
                        }
                        /* eslint-disable no-param-reassign */
                        objMD.dataStoreName = LOCATION_NAME_DMF;
                        objMD.archive = archive;
                        objMD.replicationInfo = replicationInfo;
                        // data related
                        objMD['content-length'] = 99;
                        objMD['content-type'] = 'testtype';
                        objMD['content-md5'] = 'testmd5';
                        objMD['content-encoding'] = 'testencoding';
                        objMD['x-amz-server-side-encryption'] = 'aws:kms';
                        /* eslint-enable no-param-reassign */
                        return metadata.putObjectMD(bucketNameMD, objectName, objMD, undefined, log, next);
                    }),
                    next => putObjectVersion(s3, params, '', next),
                    next => getMetadata(bucketNameMD, objectName, undefined, (err, objMD) => {
                        if (err) {
                            return next(err);
                        }
                        assert.deepStrictEqual(objMD.acl, acl);
                        assert.deepStrictEqual(objMD.tags, tags);
                        assert.deepStrictEqual(objMD.replicationInfo, replicationInfo);
                        assert.deepStrictEqual(objMD.legalHold, true);
                        assert.strictEqual(objMD['x-amz-meta-custom-user-md'], 'custom-md');
                        assert.strictEqual(objMD['x-amz-website-redirect-location'], 'http://custom-redirect');
                        // make sure data related metadatas ar not the same before and after
                        assert.notStrictEqual(objMD['x-amz-server-side-encryption'], 'aws:kms');
                        assert.notStrictEqual(objMD['content-length'], 99);
                        assert.notStrictEqual(objMD['content-encoding'], 'testencoding');
                        assert.notStrictEqual(objMD['content-type'], 'testtype');
                        // make sure we keep the same etag and add the new restored
                        // data's etag inside x-amz-restore
                        assert.strictEqual(objMD['content-md5'], 'testmd5');
                        assert.strictEqual(typeof objMD['x-amz-restore']['content-md5'], 'string');
                        return next();
                    }),
                    // removing legal hold to be able to clean the bucket after the test
                    next => {
                        legalHoldParams.LegalHold.Status = 'OFF';
                        return s3.send(new PutObjectLegalHoldCommand(legalHoldParams)).then(() => next());
                    },
                ], done);
            });

            it('should set restore originOp and drop restore-attempt metadata', done => {
                const params = { Bucket: bucketName, Key: objectName };

                async.series([
                    next => s3.send(new PutObjectCommand({
                        ...params,
                        Metadata: {
                            'custom-md': 'preserved-value',
                        },
                    })).then(() => next()).catch(next),
                    next => fakeMetadataArchive(bucketName, objectName, undefined, archive, next),
                    next => getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                        if (err) {
                            return next(err);
                        }
                        /* eslint-disable no-param-reassign */
                        objMD['x-amz-meta-scal-s3-restore-attempt'] = '3';
                        /* eslint-enable no-param-reassign */
                        return metadata.putObjectMD(bucketName, objectName, objMD, undefined, log, next);
                    }),
                    next => putObjectVersion(s3, params, '', next),
                    next => getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                        if (err) {
                            return next(err);
                        }
                        assert.strictEqual(objMD.originOp, 's3:ObjectRestore:Completed');
                        assert.strictEqual(objMD['x-amz-meta-custom-md'], 'preserved-value');
                        assert.strictEqual(objMD['x-amz-meta-scal-s3-restore-attempt'], undefined);
                        return next();
                    }),
                ], done);
            });

            it('should keep x-amz-meta-scal-version-id when restoring on ingestion bucket', async () => {
                const ingestionBucketName = `ingestion-restore-${Date.now()}`;
                const params = { Bucket: ingestionBucketName, Key: objectName };
                let putVersionId;
                try {
                    await s3.send(new CreateBucketCommand({
                        Bucket: ingestionBucketName,
                        CreateBucketConfiguration: {
                            LocationConstraint: 'us-east-2:ingest',
                        },
                    }));

                    const putRes = await s3.send(new PutObjectCommand(params));
                    putVersionId = putRes.VersionId;

                    await fakeMetadataArchive(ingestionBucketName, objectName, putVersionId, archive);

                    await putObjectVersion(s3, params, putVersionId);

                    const restoredObjMD = await getMetadata(
                        ingestionBucketName, objectName, putVersionId);

                    assert.strictEqual(restoredObjMD['x-amz-meta-scal-version-id'], putVersionId);
                } finally {
                    await bucketUtil.emptyMany([ingestionBucketName]).catch(() => {});
                    await bucketUtil.deleteMany([ingestionBucketName]).catch(() => {});
                }
            });
        });
    });
});
