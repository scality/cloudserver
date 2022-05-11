const assert = require('assert');
const async = require('async');
const { versioning } = require('arsenal');

const { config } = require('../../../../../lib/Config');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const metadata = require('../../../../../lib/metadata/wrapper');
const { DummyRequestLogger } = require('../../../../unit/helpers');

const versionIdUtils = versioning.VersionID;

const log = new DummyRequestLogger();

const nonVersionedObjId =
    versionIdUtils.getInfVid(config.replicationGroupId);
const bucketName = 'bucket1putversion15';
const objectName = 'object1putversion';

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
    console.log('decodedVersionId!!!', decodedVersionId);
    return metadata.getObjectMD(bucketName, objectName, { versionId: decodedVersionId },
        log, (err, objMD) => {
            if (err) {
                assert.equal(err, null, 'Getting object metadata: expected success, ' +
                    `got error ${JSON.stringify(err)}`);
            }
            return cb(null, objMD);
    });
}

function putObjectVersion(s3, params, vid, next) {
    const request = s3.putObject(params);
    request.on('build', () => {
        request.httpRequest.headers['x-scal-s3-version-id'] = vid;
    });
    return request.send(next);
}

function updateVersionsLastModified(versionsBefore, versionsAfter) {
    versionsBefore.forEach((v, i) => {
        // eslint-disable-next-line no-param-reassign
        versionsAfter[i].LastModified = v.LastModified;
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

        it('should overwrite an object', done => {
            const params = { Bucket: bucketName, Key: objectName };
            let objMDBefore;

            async.waterfall([
                next => s3.putObject(params, next),
                (res, next) => _getMetadata(bucketName, objectName, undefined, next),
                (objMD, next) => {
                    objMDBefore = objMD;
                    return putObjectVersion(s3, params, 'null', next);
                },
                (res, next) => _getMetadata(bucketName, objectName, undefined, next),
            ], (err, objMDAfter) => {
                assert.equal(err, null, `Expected success got error ${JSON.stringify(err)}`);
                // only the last-modified date should be updated.
                console.log('objMDBefore!!!', objMDBefore);
                console.log('objMDAfter!!!', objMDAfter);
                assert.notEqual(objMDAfter['last-modified'], objMDBefore['last-modified']);
                // eslint-disable-next-line no-param-reassign
                objMDAfter['last-modified'] = objMDBefore['last-modified'];
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
                next => metadata.listObject(bucketName, {}, log, (err, res) => {
                    versionsBefore = res.Contents;
                    next(err);
                }),
                next => _getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => putObjectVersion(s3, params, vId, err => next(err)),
                next => _getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, {}, log, (err, res) => {
                    versionsAfter = res.Contents;
                    next(err);
                }),
            ], err => {
                assert.equal(err, null, `Expected success got error ${JSON.stringify(err)}`);

                // only the last-modified date should be updated.
                assert.notEqual(versionsAfter[0].value.LastModified, versionsBefore[0].value.LastModified);
                assert.notEqual(versionsAfter[1].value.LastModified, versionsBefore[1].value.LastModified);
                versionsAfter[0].value.LastModified = versionsBefore[0].value.LastModified;
                versionsAfter[1].value.LastModified = versionsBefore[1].value.LastModified;
                assert.deepStrictEqual(versionsBefore, versionsAfter);

                assert.notEqual(objMDAfter['last-modified'], objMDBefore['last-modified']);
                // eslint-disable-next-line no-param-reassign
                objMDAfter['last-modified'] = objMDBefore['last-modified'];
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

            async.waterfall([
                next => s3.putObject(params, err => next(err)),
                next => s3.putBucketVersioning(vParams, err => next(err)),
                next => s3.putObject(params, err => next(err)),
                next => _getMetadata(bucketName, objectName, 'null', (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, {}, log, (err, res) => {
                    versionsBefore = res.Contents;
                    next(err);
                }),
                next => putObjectVersion(s3, params, 'null', err => next(err)),
                next => _getMetadata(bucketName, objectName, 'null', (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, {}, log, (err, res) => {
                    versionsAfter = res.Contents;
                    next(err);
                }),
            ], err => {
                assert.equal(err, null, `Expected success got error ${JSON.stringify(err)}`);

                console.log('versionsBefore!!!', versionsBefore);
                console.log('versionsAfter!!!', versionsAfter);
                // only the last-modified date should be updated.
                assert.notEqual(versionsAfter[2].value.LastModified, versionsBefore[2].value.LastModified);
                versionsAfter[2].value.LastModified = versionsBefore[2].value.LastModified;
                assert.deepStrictEqual(versionsBefore, versionsAfter);

                assert.notEqual(objMDAfter['last-modified'], objMDBefore['last-modified']);
                // eslint-disable-next-line no-param-reassign
                objMDAfter['last-modified'] = objMDBefore['last-modified'];
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
                next => metadata.listObject(bucketName, {}, log, (err, res) => {
                    versionsBefore = res.Contents;
                    next(err);
                }),
                next => putObjectVersion(s3, params, 'null', err => next(err)),
                next => _getMetadata(bucketName, objectName, undefined, (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, {}, log, (err, res) => {
                    versionsAfter = res.Contents;
                    next(err);
                }),
            ], err => {
                assert.equal(err, null, `Expected success got error ${JSON.stringify(err)}`);

                console.log('versionsBefore!!!', versionsBefore);
                console.log('versionsAfter!!!', versionsAfter);

                assert.notEqual(versionsAfter[0].value.LastModified, versionsBefore[0].value.LastModified);
                versionsAfter[0].value.LastModified = versionsBefore[0].value.LastModified;
                assert.deepStrictEqual(versionsBefore, versionsAfter);

                assert.notEqual(objMDAfter['last-modified'], objMDBefore['last-modified']);
                // eslint-disable-next-line no-param-reassign
                objMDAfter['last-modified'] = objMDBefore['last-modified'];
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
                next => s3.putObject(params, (err, res) => {
                    vId = res.VersionId;
                    return next(err);
                }),
                next => s3.putObject(params, err => next(err)),
                next => metadata.listObject(bucketName, {}, log, (err, res) => {
                    versionsBefore = res.Contents;
                    next(err);
                }),
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => putObjectVersion(s3, params, vId, err => next(err)),
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, {}, log, (err, res) => {
                    versionsAfter = res.Contents;
                    next(err);
                }),
            ], err => {
                assert.equal(err, null, `Expected success got error ${JSON.stringify(err)}`);

                assert.notEqual(versionsAfter[2].value.LastModified, versionsBefore[2].value.LastModified);
                versionsAfter[2].value.LastModified = versionsBefore[2].value.LastModified;
                assert.deepStrictEqual(versionsBefore, versionsAfter);

                assert.notEqual(objMDAfter['last-modified'], objMDBefore['last-modified']);
                // eslint-disable-next-line no-param-reassign
                objMDAfter['last-modified'] = objMDBefore['last-modified'];
                console.log('objMDBefore!!!', objMDBefore);
                console.log('objMDAfter!!!', objMDAfter);
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
                next => metadata.listObject(bucketName, {}, log, (err, res) => {
                    versionsBefore = res.Contents;
                    next(err);
                }),
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDBefore = objMD;
                    return next(err);
                }),
                next => putObjectVersion(s3, params, vId, err => next(err)),
                next => _getMetadata(bucketName, objectName, vId, (err, objMD) => {
                    objMDAfter = objMD;
                    return next(err);
                }),
                next => metadata.listObject(bucketName, {}, log, (err, res) => {
                    versionsAfter = res.Contents;
                    next(err);
                }),
            ], err => {
                assert.equal(err, null, `Expected success got error ${JSON.stringify(err)}`);

                assert.notEqual(versionsAfter[0].value.LastModified, versionsBefore[0].value.LastModified);
                assert.notEqual(versionsAfter[1].value.LastModified, versionsBefore[1].value.LastModified);
                versionsAfter[0].value.LastModified = versionsBefore[0].value.LastModified;
                versionsAfter[1].value.LastModified = versionsBefore[1].value.LastModified;
                assert.deepStrictEqual(versionsBefore, versionsAfter);

                console.log('objMDBefore!!!', objMDBefore);
                console.log('objMDAfter!!!', objMDAfter);
                assert.notEqual(objMDBefore['last-modified'], objMDAfter['last-modified']);
                // eslint-disable-next-line no-param-reassign
                objMDAfter['last-modified'] = objMDBefore['last-modified'];
                assert.deepStrictEqual(objMDAfter, objMDBefore);
                return done();
            });
        });
    });
});

