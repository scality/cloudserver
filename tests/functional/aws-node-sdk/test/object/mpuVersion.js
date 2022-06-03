const assert = require('assert');
const async = require('async');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const metadata = require('../../../../../lib/metadata/wrapper');
const { DummyRequestLogger } = require('../../../../unit/helpers');
const checkError = require('../../lib/utility/checkError');
const { getMetadata, fakeMetadataRestore } = require('../utils/init');
const { addDays } = require('../../../../utilities/helpers');

const log = new DummyRequestLogger();

const bucketName = 'bucket1putversion33';
const objectName = 'object1putversion';
const mdListingParams = { listingType: 'DelimiterVersions', maxKeys: 1000 };
const archive = {
    archiveInfo: {},
    restoreRequestedAt: new Date(0).toString(),
    restoreRequestedDays: 5,
};

function putMPUVersion(s3, bucketName, objectName, vId, cb) {
    async.waterfall([
        next => {
            const params = { Bucket: bucketName, Key: objectName };
            const request = s3.createMultipartUpload(params);
            if (vId !== undefined) {
                request.on('build', () => {
                    request.httpRequest.headers['x-scal-s3-version-id'] = vId;
                });
            }
            return request.send(next);
        },
        (resCreation, next) => {
            const uploadId = resCreation.UploadId;
            const params = {
                Body: 'okok',
                Bucket: bucketName,
                Key: objectName,
                PartNumber: 1,
                UploadId: uploadId,
            };
            const request = s3.uploadPart(params);
            if (vId !== undefined) {
                request.on('build', () => {
                    request.httpRequest.headers['x-scal-s3-version-id'] = vId;
                });
            }
            return request.send((err, res) => next(err, res, uploadId));
        },
        (res, uploadId, next) => {
            const params = {
                Bucket: bucketName,
                Key: objectName,
                MultipartUpload: {
                    Parts: [
                        {
                            ETag: res.ETag,
                            PartNumber: 1
                        },
                    ]
                },
                UploadId: uploadId,
            };
            const request = s3.completeMultipartUpload(params);
            if (vId !== undefined) {
                request.on('build', () => {
                    request.httpRequest.headers['x-scal-s3-version-id'] = vId;
                });
            }
            return request.send(next);
        },
    ], err => cb(err));
}

function putMPU(s3, bucketName, objectName, cb) {
    return putMPUVersion(s3, bucketName, objectName, undefined, cb);
}

function checkVersionsAndUpdate(versionsBefore, versionsAfter, indexes) {
    indexes.forEach(i => {
        assert.notStrictEqual(versionsAfter[i].value.Size, versionsBefore[i].value.Size);
        assert.notStrictEqual(versionsAfter[i].value.ETag, versionsBefore[i].value.ETag);
        /* eslint-disable no-param-reassign */
        versionsBefore[i].value.Size = versionsAfter[i].value.Size;
        versionsBefore[i].value.ETag = versionsAfter[i].value.ETag;
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

describe('MPU with x-scal-s3-version-id header', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        beforeEach(done => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return metadata.setup(() =>
                s3.createBucket({ Bucket: bucketName }, err => {
                    if (err) {
                        assert.strictEqual(err, null, 'Creating bucket: Expected success, ' +
                            `got error ${JSON.stringify(err)}`);
                    }
                    done();
                }));
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket');
            return bucketUtil.empty(bucketName)
            .then(() => {
                process.stdout.write('Deleting bucket');
                return bucketUtil.deleteOne(bucketName);
            })
            .catch(err => {
                process.stdout.write('Error in afterEach');
                throw err;
            });
        });

        it('should overwrite an MPU object', done => {
            let objMDBefore;
            let objMDAfter;
            let versionsBefore;
            let versionsAfter;

            async.series([
                next => putMPU(s3, bucketName, objectName, next),
                next => fakeMetadataRestore(bucketName, objectName, undefined, archive, next),
                next => getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    return next(err);
                }),
                next => putMPUVersion(s3, bucketName, objectName, '', next),
                next => getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsAfter = res.Versions;
                    return next(err);
                }),
            ], err => {
                assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);

                assert.deepStrictEqual(versionsAfter, versionsBefore);

                checkObjMdAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'uploadId', 'microVersionId', 'x-amz-restore', 'archive', 'dataStoreName']);

                assert.deepStrictEqual(objMDAfter, objMDBefore);
                return done();
            });
        });

        it('should overwrite an object', done => {
            const params = { Bucket: bucketName, Key: objectName };
            let objMDBefore;
            let objMDAfter;
            let versionsBefore;
            let versionsAfter;

            async.series([
                next => s3.putObject(params, next),
                next => fakeMetadataRestore(bucketName, objectName, undefined, archive, next),
                next => getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    return next(err);
                }),
                next => putMPUVersion(s3, bucketName, objectName, '', next),
                next => getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsAfter = res.Versions;
                    return next(err);
                }),
            ], err => {
                assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);

                checkVersionsAndUpdate(versionsBefore, versionsAfter, [0]);
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                checkObjMdAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'content-md5', 'originOp', 'uploadId', 'microVersionId',
                    'x-amz-restore', 'archive', 'dataStoreName']);

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
                next => s3.putBucketVersioning(vParams, next),
                next => s3.putObject(params, (err, res) => {
                    vId = res.VersionId;
                    return next(err);
                }),
                next => fakeMetadataRestore(bucketName, objectName, vId, archive, next),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    return next(err);
                }),
                next => getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => putMPUVersion(s3, bucketName, objectName, vId, next),
                next => getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsAfter = res.Versions;
                    return next(err);
                }),
            ], err => {
                assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);

                checkVersionsAndUpdate(versionsBefore, versionsAfter, [0]);
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                checkObjMdAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'content-md5', 'originOp', 'uploadId', 'microVersionId',
                    'x-amz-restore', 'archive', 'dataStoreName']);
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
                next => s3.putBucketVersioning(vParams, next),
                next => s3.putObject(params, (err, res) => {
                    vId = res.VersionId;
                    return next(err);
                }),
                next => fakeMetadataRestore(bucketName, objectName, vId, archive, next),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    return next(err);
                }),
                next => getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => putMPUVersion(s3, bucketName, objectName, '', next),
                next => getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsAfter = res.Versions;
                    return next(err);
                }),
            ], err => {
                assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);

                checkVersionsAndUpdate(versionsBefore, versionsAfter, [0]);
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                checkObjMdAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'content-md5', 'originOp', 'uploadId', 'microVersionId',
                    'x-amz-restore', 'archive', 'dataStoreName']);
                assert.deepStrictEqual(objMDAfter, objMDBefore);
                return done();
            });
        });


        it('should fail if version is invalid', done => {
            const vParams = {
                Bucket: bucketName,
                VersioningConfiguration: {
                    Status: 'Enabled',
                }
            };
            const params = { Bucket: bucketName, Key: objectName };

            async.series([
                next => s3.putBucketVersioning(vParams, next),
                next => s3.putObject(params, next),
                next => putMPUVersion(s3, bucketName, objectName, 'aJLWKz4Ko9IjBBgXKj5KQT.G9UHv0g7P', err => {
                    checkError(err, 'InvalidArgument', 400);
                    return next();
                }),
            ], err => {
                assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);
                return done();
            });
        });

        it('should fail if key does not exist', done => {
            async.series([
                next => putMPUVersion(s3, bucketName, objectName, '', err => {
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
                next => s3.putBucketVersioning(vParams, next),
                next => s3.putObject(params, next),
                next => putMPUVersion(s3, bucketName, objectName,
                '393833343735313131383832343239393939393952473030312020313031', err => {
                    checkError(err, 'NoSuchVersion', 404);
                    return next();
                }),
            ], err => {
                assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);
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
                next => s3.putObject(params, next),
                next => s3.putBucketVersioning(vParams, next),
                next => s3.putObject(params, next),
                next => fakeMetadataRestore(bucketName, objectName, 'null', archive, next),
                next => getMetadata(bucketName, objectName, 'null', (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    return next(err);
                }),
                next => putMPUVersion(s3, bucketName, objectName, 'null', next),
                next => getMetadata(bucketName, objectName, 'null', (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsAfter = res.Versions;
                    return next(err);
                }),
            ], err => {
                assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);

                checkVersionsAndUpdate(versionsBefore, versionsAfter, [1]);
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                checkObjMdAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'content-md5', 'originOp', 'uploadId', 'microVersionId',
                    'x-amz-restore', 'archive', 'dataStoreName']);
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
                next => s3.putObject(params, next),
                next => s3.putBucketVersioning(vParams, next),
                next => s3.putObject(params, (err, res) => {
                    vId = res.VersionId;
                    return next(err);
                }),
                next => fakeMetadataRestore(bucketName, objectName, vId, archive, next),
                next => getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    next(err);
                }),
                next => putMPUVersion(s3, bucketName, objectName, vId, next),
                next => getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsAfter = res.Versions;
                    return next(err);
                }),
            ], err => {
                assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);

                checkVersionsAndUpdate(versionsBefore, versionsAfter, [0]);
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                checkObjMdAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'content-md5', 'originOp', 'uploadId', 'microVersionId',
                    'x-amz-restore', 'archive', 'dataStoreName']);
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
                next => s3.putBucketVersioning(vParams, next),
                next => s3.putObject(params, next),
                next => s3.putBucketVersioning(sParams, next),
                next => s3.putObject(params, next),
                next => fakeMetadataRestore(bucketName, objectName, undefined, archive, next),
                next => getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    next(err);
                }),
                next => putMPUVersion(s3, bucketName, objectName, '', next),
                next => getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsAfter = res.Versions;
                    return next(err);
                }),
            ], err => {
                assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);

                checkVersionsAndUpdate(versionsBefore, versionsAfter, [0]);
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                checkObjMdAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'content-md5', 'originOp', 'uploadId', 'microVersionId',
                    'x-amz-restore', 'archive', 'dataStoreName']);
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
                next => s3.putBucketVersioning(vParams, next),
                next => s3.putObject(params, next),
                next => s3.putObject(params, (err, res) => {
                    vId = res.VersionId;
                    return next(err);
                }),
                next => s3.putObject(params, next),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    return next(err);
                }),
                next => fakeMetadataRestore(bucketName, objectName, vId, archive, next),
                next => getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => putMPUVersion(s3, bucketName, objectName, vId, next),
                next => getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsAfter = res.Versions;
                    return next(err);
                }),
            ], err => {
                assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);

                checkVersionsAndUpdate(versionsBefore, versionsAfter, [1]);
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                checkObjMdAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'content-md5', 'originOp', 'uploadId', 'microVersionId',
                    'x-amz-restore', 'archive', 'dataStoreName']);
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
                next => s3.putBucketVersioning(vParams, next),
                next => s3.putObject(params, next),
                next => s3.putObject(params, (err, res) => {
                    vId = res.VersionId;
                    return next(err);
                }),
                next => fakeMetadataRestore(bucketName, objectName, vId, archive, next),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    return next(err);
                }),
                next => getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => putMPUVersion(s3, bucketName, objectName, vId, next),
                next => getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsAfter = res.Versions;
                    return next(err);
                }),
            ], err => {
                assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);

                checkVersionsAndUpdate(versionsBefore, versionsAfter, [0]);
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                checkObjMdAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'content-md5', 'originOp', 'uploadId', 'microVersionId',
                    'x-amz-restore', 'archive', 'dataStoreName']);
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
                next => s3.putBucketVersioning(vParams, next),
                next => s3.putObject(params, next),
                next => s3.putObject(params, (err, res) => {
                    vId = res.VersionId;
                    return next(err);
                }),
                next => fakeMetadataRestore(bucketName, objectName, vId, archive, next),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    return next(err);
                }),
                next => getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => s3.putBucketVersioning(sParams, next),
                next => putMPUVersion(s3, bucketName, objectName, vId, next),
                next => getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsAfter = res.Versions;
                    return next(err);
                }),
            ], err => {
                assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);

                checkVersionsAndUpdate(versionsBefore, versionsAfter, [0]);
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                checkObjMdAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'content-md5', 'originOp', 'uploadId', 'microVersionId',
                    'x-amz-restore', 'archive', 'dataStoreName']);
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
                next => s3.putObject(params, next),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    return next(err);
                }),
                next => fakeMetadataRestore(bucketName, objectName, undefined, archive, next),
                next => getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => s3.putBucketVersioning(vParams, next),
                next => putMPUVersion(s3, bucketName, objectName, 'null', next),
                next => getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsAfter = res.Versions;
                    return next(err);
                }),
            ], err => {
                assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);

                checkVersionsAndUpdate(versionsBefore, versionsAfter, [0]);
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                checkObjMdAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'content-md5', 'originOp', 'uploadId', 'microVersionId',
                    'x-amz-restore', 'archive', 'dataStoreName']);

                assert.deepStrictEqual(objMDAfter, objMDBefore);
                return done();
            });
        });

        it('should fail if archiving is not in progress', done => {
            const params = { Bucket: bucketName, Key: objectName };

            async.series([
                next => s3.putObject(params, next),
                next => putMPUVersion(s3, bucketName, objectName, '', err => {
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
                next => s3.putBucketVersioning(vParams, next),
                next => s3.putObject(params, next),
                next => s3.deleteObject(params, (err, res) => {
                    vId = res.VersionId;
                    return next(err);
                }),
                next => putMPUVersion(s3, bucketName, objectName, vId, err => {
                    checkError(err, 'MethodNotAllowed', 405);
                    return next();
                }),
            ], err => {
                assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);
                return done();
            });
        });

        it('should fail if restore is already completed', done => {
            const params = { Bucket: bucketName, Key: objectName };
            const now = Date.now();
            const archiveCompleted = {
                archiveInfo: {},
                restoreRequestedAt: new Date(0),
                restoreRequestedDays: 5,
                restoreCompletedAt: now,
                restoreWillExpireAt: addDays(now, 5),
            };

            async.series([
                next => s3.putObject(params, next),
                next => fakeMetadataRestore(bucketName, objectName, undefined, archiveCompleted, next),
                next => putMPUVersion(s3, bucketName, objectName, '', err => {
                    checkError(err, 'InvalidObjectState', 403);
                    return next();
                }),
            ], err => {
                assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);
                return done();
            });
        });

        it('should pass if restore expired but has not been cleaned up yet', done => {
            const params = { Bucket: bucketName, Key: objectName };
            const archiveCompleted = {
                archiveInfo: {},
                restoreRequestedAt: new Date(0),
                restoreRequestedDays: 5,
                restoreCompletedAt: new Date(10),
                restoreWillExpireAt: addDays(new Date(10), 5),
            };

            async.series([
                next => s3.putObject(params, next),
                next => fakeMetadataRestore(bucketName, objectName, undefined, archiveCompleted, next),
                next => putMPUVersion(s3, bucketName, objectName, '', next),
            ], err => {
                assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);
                return done();
            });
        });

        it('should update restore metadata', done => {
            const params = { Bucket: bucketName, Key: objectName };
            let objMDBefore;
            let objMDAfter;

            async.series([
                next => s3.putObject(params, next),
                next => fakeMetadataRestore(bucketName, objectName, undefined, archive, next),
                next => getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, next),
                next => putMPUVersion(s3, bucketName, objectName, '', next),
                next => getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, next),
            ], err => {
                assert.strictEqual(err, null, `Expected success got error ${JSON.stringify(err)}`);

                // Make sure object data location is set back to its bucket data location.
                assert.deepStrictEqual(objMDAfter.dataStoreName, 'us-east-1');

                assert.deepStrictEqual(objMDAfter.archive.archiveInfo, objMDBefore.archive.archiveInfo);
                assert.deepStrictEqual(objMDAfter.archive.restoreRequestedAt, objMDBefore.archive.restoreRequestedAt);
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
});
