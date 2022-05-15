const assert = require('assert');
const async = require('async');
const { versioning } = require('arsenal');

const { config } = require('../../../../../lib/Config');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const metadata = require('../../../../../lib/metadata/wrapper');
const { DummyRequestLogger } = require('../../../../unit/helpers');
const checkError = require('../../lib/utility/checkError');

const versionIdUtils = versioning.VersionID;

const log = new DummyRequestLogger();

const nonVersionedObjId =
    versionIdUtils.getInfVid(config.replicationGroupId);
const bucketName = 'bucket1putversion30';
const objectName = 'object1putversion';
const mdListingParams = { listingType: 'DelimiterVersions', maxKeys: 1000 };

function _getMetadata(bucketName, objectName, versionId, cb) {
    let decodedVersionId;
    if (versionId) {
        if (versionId === 'null') {
            decodedVersionId = nonVersionedObjId;
        } else {
            decodedVersionId = versionIdUtils.decode(versionId);
        }
        if (decodedVersionId instanceof Error) {
            return cb(new Error('Invalid version id specified'));
        }
    }
    return metadata.getObjectMD(bucketName, objectName, { versionId: decodedVersionId },
        log, (err, objMD) => {
            if (err) {
                assert.equal(err, null, 'Getting object metadata: expected success, ' +
                    `got error ${JSON.stringify(err)}`);
            }
            return cb(null, objMD);
    });
}

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
            return request.send(err => next(err));
        },
    ], err => cb(err));
}

function putMPU(s3, bucketName, objectName, cb) {
    return putMPUVersion(s3, bucketName, objectName, undefined, cb);
}

// function checkVersions(versionsBefore, versionsAfter) {
//     versionsAfter.forEach((v, i) => {
//         // assert.notEqual(v.value.Size, versionsBefore[i].value.Size);
//         assert.notEqual(v.value.ETag, versionsBefore[i].value.ETag);

//         // v.value.Size = versionsBefore[i].value.Size;
//         v.value.ETag = versionsBefore[i].value.ETag;
//     });

//     assert.deepStrictEqual(versionsAfter, versionsBefore);
// }

function checkNotEqualAndUpdate(objMDBefore, objMDAfter, props) {
    props.forEach(p => {
        assert.notEqual(objMDAfter[p], objMDBefore[p]);
        objMDAfter[p] = objMDBefore[p];
    });
}

describe('PUT object with x-scal-s3-version-id header', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        beforeEach(done => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return metadata.setup(() =>
                s3.createBucket({ Bucket: bucketName }, err => {
                    if (err) {
                        assert.equal(err, null, 'Creating bucket: Expected success, ' +
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
            const params = { Bucket: bucketName, Key: objectName };
            let objMDBefore;
            let objMDAfter;
            let versionsBefore;
            let versionsAfter;

            async.waterfall([
                next => putMPU(s3, bucketName, objectName, err => next(err)),
                next => _getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    next(err);
                }),
                next => putMPUVersion(s3, bucketName, objectName, '', next),
                next => _getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsAfter = res.Versions;
                    next(err);
                }),
            ], err => {
                assert.equal(err, null, `Expected success got error ${JSON.stringify(err)}`);

                assert.deepStrictEqual(versionsAfter, versionsBefore);

                checkNotEqualAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'uploadId']);

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

            async.waterfall([
                next => s3.putObject(params, err => next(err)),
                next => _getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    next(err);
                }),
                next => putMPUVersion(s3, bucketName, objectName, '', next),
                next => _getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsAfter = res.Versions;
                    next(err);
                }),
            ], err => {
                assert.equal(err, null, `Expected success got error ${JSON.stringify(err)}`);

                versionsAfter.forEach((v, i) => {
                    assert.notEqual(v.value.Size, versionsBefore[i].value.Size);
                    assert.notEqual(v.value.ETag, versionsBefore[i].value.ETag);

                    v.value.Size = versionsBefore[i].value.Size;
                    v.value.ETag = versionsBefore[i].value.ETag;
                });
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                assert.notEqual(objMDAfter.uploadId, objMDBefore.uploadId);
                delete(objMDAfter.uploadId);

                checkNotEqualAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'content-md5', 'originOp']);

                assert.deepStrictEqual(objMDAfter, objMDBefore);
                return done();
            });
        });

        it('should overwrite a versioned object', done => {
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

            async.waterfall([
                next => s3.putBucketVersioning(vParams, err => next(err)),
                next => s3.putObject(params, (err, res) => {
                    vId = res.VersionId;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    next(err);
                }),
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => putMPUVersion(s3, bucketName, objectName, vId, next),
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsAfter = res.Versions;
                    next(err);
                }),
            ], err => {
                assert.equal(err, null, `Expected success got error ${JSON.stringify(err)}`);

                versionsAfter.forEach((v, i) => {
                    assert.notEqual(v.value.Size, versionsBefore[i].value.Size);
                    assert.notEqual(v.value.ETag, versionsBefore[i].value.ETag);

                    v.value.Size = versionsBefore[i].value.Size;
                    v.value.ETag = versionsBefore[i].value.ETag;
                });
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                assert.notEqual(objMDAfter.uploadId, objMDBefore.uploadId);
                delete(objMDAfter.uploadId);

                checkNotEqualAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'content-md5', 'originOp']);
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

            async.waterfall([
                next => s3.putBucketVersioning(vParams, err => next(err)),
                next => s3.putObject(params, (err, res) => {
                    vId = res.VersionId;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    next(err);
                }),
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => putMPUVersion(s3, bucketName, objectName, '', next),
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsAfter = res.Versions;
                    next(err);
                }),
            ], err => {
                assert.equal(err, null, `Expected success got error ${JSON.stringify(err)}`);

                versionsAfter.forEach((v, i) => {
                    assert.notEqual(v.value.Size, versionsBefore[i].value.Size);
                    assert.notEqual(v.value.ETag, versionsBefore[i].value.ETag);

                    v.value.Size = versionsBefore[i].value.Size;
                    v.value.ETag = versionsBefore[i].value.ETag;
                });
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                assert.notEqual(objMDAfter.uploadId, objMDBefore.uploadId);
                delete(objMDAfter.uploadId);

                checkNotEqualAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'content-md5', 'originOp']);
                assert.deepStrictEqual(objMDAfter, objMDBefore);
                return done();
            });
        });


        it('should fail if version specified is invalid', done => {
            const vParams = {
                Bucket: bucketName,
                VersioningConfiguration: {
                    Status: 'Enabled',
                }
            };
            const params = { Bucket: bucketName, Key: objectName };

            async.waterfall([
                next => s3.putBucketVersioning(vParams, err => next(err)),
                next => s3.putObject(params, err => next(err)),
                next => putMPUVersion(s3, bucketName, objectName, 'aJLWKz4Ko9IjBBgXKj5KQT.G9UHv0g7P', err => {
                    checkError(err, 'InvalidArgument', 400);
                    return next();
                }),
            ], err => {
                assert.equal(err, null, `Expected success got error ${JSON.stringify(err)}`);
                return done();
            });
        });

        it('should fail if key specified does not exist', done => {
            const params = { Bucket: bucketName, Key: objectName };

            async.waterfall([
                next => putMPUVersion(s3, bucketName, objectName, '', err => {
                    checkError(err, 'NoSuchKey', 404);
                    return next();
                }),
            ], err => {
                assert.equal(err, null, `Expected success got error ${JSON.stringify(err)}`);
                return done();
            });
        });

        it('should fail if version specified does not exist', done => {
            const vParams = {
                Bucket: bucketName,
                VersioningConfiguration: {
                    Status: 'Enabled',
                }
            };
            const params = { Bucket: bucketName, Key: objectName };

            async.waterfall([
                next => s3.putBucketVersioning(vParams, err => next(err)),
                next => s3.putObject(params, err => next(err)),
                next => putMPUVersion(s3, bucketName, objectName, '393833343735313131383832343239393939393952473030312020313031', err => {
                    checkError(err, 'NoSuchVersion', 404);
                    return next();
                }),
            ], err => {
                assert.equal(err, null, `Expected success got error ${JSON.stringify(err)}`);
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

            async.waterfall([
                next => s3.putObject(params, err => next(err)),
                next => s3.putBucketVersioning(vParams, err => next(err)),
                next => s3.putObject(params, err => next(err)),
                next => _getMetadata(bucketName, objectName, 'null', (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    next(err);
                }),
                next => putMPUVersion(s3, bucketName, objectName, 'null', next),
                next => _getMetadata(bucketName, objectName, 'null', (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsAfter = res.Versions;
                    next(err);
                }),
            ], err => {
                assert.equal(err, null, `Expected success got error ${JSON.stringify(err)}`);

                assert.notEqual(versionsAfter[1].value.Size, versionsBefore[1].value.Size);
                assert.notEqual(versionsAfter[1].value.ETag, versionsBefore[1].value.ETag);

                versionsAfter[1].value.Size = versionsBefore[1].value.Size;
                versionsAfter[1].value.ETag = versionsBefore[1].value.ETag;
                assert.deepStrictEqual(versionsAfter, versionsBefore);
                
                assert.notEqual(objMDAfter.uploadId, objMDBefore.uploadId);
                delete(objMDAfter.uploadId);

                checkNotEqualAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'content-md5', 'originOp']);
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

            async.waterfall([
                next => s3.putObject(params, err => next(err)),
                next => s3.putBucketVersioning(vParams, err => next(err)),
                next => s3.putObject(params, (err, res) => {
                    vId = res.VersionId;
                    return next(err);
                }),
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    next(err);
                }),
                next => putMPUVersion(s3, bucketName, objectName, vId, next),
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsAfter = res.Versions;
                    next(err);
                }),
            ], err => {
                assert.equal(err, null, `Expected success got error ${JSON.stringify(err)}`);

                assert.notEqual(versionsAfter[0].value.Size, versionsBefore[0].value.Size);
                assert.notEqual(versionsAfter[0].value.ETag, versionsBefore[0].value.ETag);

                versionsAfter[0].value.Size = versionsBefore[0].value.Size;
                versionsAfter[0].value.ETag = versionsBefore[0].value.ETag;
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                assert.notEqual(objMDAfter.uploadId, objMDBefore.uploadId);
                delete(objMDAfter.uploadId);

                checkNotEqualAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'content-md5', 'originOp']);
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

            async.waterfall([
                next => s3.putBucketVersioning(vParams, err => next(err)),
                next => s3.putObject(params, err => next(err)),
                next => s3.putBucketVersioning(sParams, err => next(err)),
                next => s3.putObject(params, err => next(err)),
                next => _getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    next(err);
                }),
                next => putMPUVersion(s3, bucketName, objectName, '', next),
                next => _getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsAfter = res.Versions;
                    next(err);
                }),
            ], err => {
                assert.equal(err, null, `Expected success got error ${JSON.stringify(err)}`);

                assert.notEqual(versionsAfter[0].value.Size, versionsBefore[0].value.Size);
                assert.notEqual(versionsAfter[0].value.ETag, versionsBefore[0].value.ETag);

                versionsAfter[0].value.Size = versionsBefore[0].value.Size;
                versionsAfter[0].value.ETag = versionsBefore[0].value.ETag;
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                assert.notEqual(objMDAfter.uploadId, objMDBefore.uploadId);
                delete(objMDAfter.uploadId);

                checkNotEqualAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'content-md5', 'originOp']);
                assert.deepStrictEqual(objMDAfter, objMDBefore);
                return done();
            });
        });

        it('should overwrite a non-current versioned object', done => {
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

            async.waterfall([
                next => s3.putBucketVersioning(vParams, err => next(err)),
                next => s3.putObject(params, err => next(err)),
                next => s3.putObject(params, (err, res) => {
                    vId = res.VersionId;
                    return next(err);
                }),
                next => s3.putObject(params, err => next(err)),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    next(err);
                }),
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => putMPUVersion(s3, bucketName, objectName, vId, next),
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsAfter = res.Versions;
                    next(err);
                }),
            ], err => {
                assert.equal(err, null, `Expected success got error ${JSON.stringify(err)}`);
                
                assert.notEqual(versionsAfter[1].value.Size, versionsBefore[1].value.Size);
                assert.notEqual(versionsAfter[1].value.ETag, versionsBefore[1].value.ETag);

                versionsAfter[1].value.Size = versionsBefore[1].value.Size;
                versionsAfter[1].value.ETag = versionsBefore[1].value.ETag;
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                assert.notEqual(objMDAfter.uploadId, objMDBefore.uploadId);
                delete(objMDAfter.uploadId);

                checkNotEqualAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'content-md5', 'originOp']);
                assert.deepStrictEqual(objMDAfter, objMDBefore);
                return done();
            });
        });

        it('should overwrite the current versioned object', done => {
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

            async.waterfall([
                next => s3.putBucketVersioning(vParams, err => next(err)),
                next => s3.putObject(params, err => next(err)),
                next => s3.putObject(params, (err, res) => {
                    vId = res.VersionId;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    next(err);
                }),
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => putMPUVersion(s3, bucketName, objectName, vId, next),
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsAfter = res.Versions;
                    next(err);
                }),
            ], err => {
                assert.equal(err, null, `Expected success got error ${JSON.stringify(err)}`);

                assert.notEqual(versionsAfter[0].value.Size, versionsBefore[0].value.Size);
                assert.notEqual(versionsAfter[0].value.ETag, versionsBefore[0].value.ETag);

                versionsAfter[0].value.Size = versionsBefore[0].value.Size;
                versionsAfter[0].value.ETag = versionsBefore[0].value.ETag;
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                assert.notEqual(objMDAfter.uploadId, objMDBefore.uploadId);
                delete(objMDAfter.uploadId);

                checkNotEqualAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'content-md5', 'originOp']);
                assert.deepStrictEqual(objMDAfter, objMDBefore);
                return done();
            });
        });

        it('should overwrite the current versioned object after bucket version suspended', done => {
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

            async.waterfall([
                next => s3.putBucketVersioning(vParams, err => next(err)),
                next => s3.putObject(params, err => next(err)),
                next => s3.putObject(params, (err, res) => {
                    vId = res.VersionId;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    next(err);
                }),
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => s3.putBucketVersioning(sParams, err => next(err)),
                next => putMPUVersion(s3, bucketName, objectName, vId, next),
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsAfter = res.Versions;
                    next(err);
                }),
            ], err => {
                assert.equal(err, null, `Expected success got error ${JSON.stringify(err)}`);

                assert.notEqual(versionsAfter[0].value.Size, versionsBefore[0].value.Size);
                assert.notEqual(versionsAfter[0].value.ETag, versionsBefore[0].value.ETag);

                versionsAfter[0].value.Size = versionsBefore[0].value.Size;
                versionsAfter[0].value.ETag = versionsBefore[0].value.ETag;
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                assert.notEqual(objMDAfter.uploadId, objMDBefore.uploadId);
                delete(objMDAfter.uploadId);

                checkNotEqualAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'content-md5', 'originOp']);
                assert.deepStrictEqual(objMDAfter, objMDBefore);
                return done();
            });
        });

        it('should overwrite the current null object after bucket version enabled', done => {
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

            async.waterfall([
                next => s3.putObject(params, err => next(err)),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    next(err);
                }),
                next => _getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => s3.putBucketVersioning(vParams, err => next(err)),
                next => putMPUVersion(s3, bucketName, objectName, 'null', next),
                next => _getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsAfter = res.Versions;
                    next(err);
                }),
            ], err => {
                assert.equal(err, null, `Expected success got error ${JSON.stringify(err)}`);

                assert.notEqual(versionsAfter[0].value.Size, versionsBefore[0].value.Size);
                assert.notEqual(versionsAfter[0].value.ETag, versionsBefore[0].value.ETag);

                versionsAfter[0].value.Size = versionsBefore[0].value.Size;
                versionsAfter[0].value.ETag = versionsBefore[0].value.ETag;
                assert.deepStrictEqual(versionsAfter, versionsBefore);

                assert.notEqual(objMDAfter.uploadId, objMDBefore.uploadId);
                delete(objMDAfter.uploadId);

                checkNotEqualAndUpdate(objMDBefore, objMDAfter,
                    ['location', 'content-length', 'content-md5', 'originOp']);

                assert.deepStrictEqual(objMDAfter, objMDBefore);
                return done();
            });
        });
    });
});
