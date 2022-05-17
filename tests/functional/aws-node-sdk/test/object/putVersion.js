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
const bucketName = 'bucket1putversion31';
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
                assert.strictEqual(err, null, 'Getting object metadata: expected success, ' +
                    `got error ${JSON.stringify(err)}`);
            }
            return cb(null, objMD);
    });
}

function putObjectVersion(s3, params, vid, next) {
    const paramsWithBody = { ...params, Body: '123' };
    const request = s3.putObject(paramsWithBody);
    request.on('build', () => {
        request.httpRequest.headers['x-scal-s3-version-id'] = vid;
    });
    return request.send(next);
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

        it('should overwrite an object', done => {
            const params = { Bucket: bucketName, Key: objectName };
            let objMDBefore;
            let objMDAfter;
            let versionsBefore;
            let versionsAfter;

            async.series([
                next => s3.putObject(params, next),
                next => _getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    return next(err);
                }),
                next => putObjectVersion(s3, params, '', next),
                next => _getMetadata(bucketName, objectName, undefined, (err, objMD) => {
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

                checkObjMdAndUpdate(objMDBefore, objMDAfter, ['location', 'content-length', 'content-md5',
                'microVersionId']);
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
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    return next(err);
                }),
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => putObjectVersion(s3, params, vId, next),
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
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

                checkObjMdAndUpdate(objMDBefore, objMDAfter, ['location', 'content-length',
                'content-md5', 'microVersionId']);
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
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    return next(err);
                }),
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => putObjectVersion(s3, params, '', next),
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
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

                checkObjMdAndUpdate(objMDBefore, objMDAfter, ['location', 'content-length',
                'content-md5', 'microVersionId']);
                assert.deepStrictEqual(objMDAfter, objMDBefore);
                return done();
            });
        });


        it('should fail if version id is invalid', done => {
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
                next => putObjectVersion(s3, params, 'aJLWKz4Ko9IjBBgXKj5KQT.G9UHv0g7P', err => {
                    checkError(err, 'InvalidArgument', 400);
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
                next => s3.putBucketVersioning(vParams, next),
                next => s3.putObject(params, next),
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
                next => _getMetadata(bucketName, objectName, 'null', (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    next(err);
                }),
                next => putObjectVersion(s3, params, 'null', next),
                next => _getMetadata(bucketName, objectName, 'null', (err, objMD) => {
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

                checkObjMdAndUpdate(objMDBefore, objMDAfter, ['location', 'content-length',
                'content-md5', 'microVersionId']);
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
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    return next(err);
                }),
                next => putObjectVersion(s3, params, vId, next),
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
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

                checkObjMdAndUpdate(objMDBefore, objMDAfter, ['location', 'content-length',
                'content-md5', 'microVersionId']);
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
                next => _getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    return next(err);
                }),
                next => putObjectVersion(s3, params, '', next),
                next => _getMetadata(bucketName, objectName, undefined, (err, objMD) => {
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

                checkObjMdAndUpdate(objMDBefore, objMDAfter, ['location', 'content-length',
                'content-md5', 'microVersionId']);
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
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => putObjectVersion(s3, params, vId, next),
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
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

                checkObjMdAndUpdate(objMDBefore, objMDAfter, ['location', 'content-length',
                'content-md5', 'microVersionId']);
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
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    return next(err);
                }),
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => putObjectVersion(s3, params, vId, next),
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
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

                checkObjMdAndUpdate(objMDBefore, objMDAfter, ['location', 'content-length',
                'content-md5', 'microVersionId']);
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
                next => metadata.listObject(bucketName, mdListingParams, log, (err, res) => {
                    versionsBefore = res.Versions;
                    return next(err);
                }),
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => s3.putBucketVersioning(sParams, next),
                next => putObjectVersion(s3, params, vId, next),
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
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

                checkObjMdAndUpdate(objMDBefore, objMDAfter, ['location', 'content-length',
                'content-md5', 'microVersionId']);
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
                next => _getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => s3.putBucketVersioning(vParams, next),
                next => putObjectVersion(s3, params, 'null', next),
                next => _getMetadata(bucketName, objectName, undefined, (err, objMD) => {
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

                checkObjMdAndUpdate(objMDBefore, objMDAfter, ['location', 'content-length',
                'content-md5', 'microVersionId']);
                assert.deepStrictEqual(objMDAfter, objMDBefore);
                return done();
            });
        });
    });
});
