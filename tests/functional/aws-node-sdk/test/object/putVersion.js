const assert = require('assert');
const async = require('async');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const metadata = require('../../../../../lib/metadata/wrapper');
const { DummyRequestLogger } = require('../../../../unit/helpers');

const log = new DummyRequestLogger();

const bucketName = 'bucket1putversion14';
const objectName = 'object1putversion';

function _getMetadata(bucketName, objectName, versionId, cb) {
    return metadata.getObjectMD(bucketName, objectName, { versionId }, log, (err, objMD) => {
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

describe('PUT object', () => {
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

        it('HEHOOOOO', done => {
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
                // only the creation date should be updated.
                // eslint-disable-next-line no-param-reassign
                objMDAfter['last-modified'] = objMDBefore['last-modified'];
                assert.deepStrictEqual(objMDBefore, objMDAfter);
                return done();
            });
        });

        it('HEHOOOOO 2', done => {
            const vParams = {
                Bucket: bucketName,
                VersioningConfiguration: {
                    Status: 'Enabled',
                }
            };
            const params = { Bucket: bucketName, Key: objectName };
            let objMDBefore;

            async.waterfall([
                next => s3.putBucketVersioning(vParams, err => next(err)),
                next => s3.putObject(params, next),
                (res, next) => _getMetadata(bucketName, objectName, undefined,
                    (err, objMD) => next(err, objMD, res.VersionId)),
                (objMD, versionId, next) => {
                    objMDBefore = objMD;
                    return putObjectVersion(s3, params, versionId, next);
                },
                (res, next) => _getMetadata(bucketName, objectName, undefined, next),
            ], (err, objMDAfter) => {
                assert.equal(err, null, `Expected success got error ${JSON.stringify(err)}`);
                // only the creation date should be updated.
                // eslint-disable-next-line no-param-reassign
                objMDAfter['last-modified'] = objMDBefore['last-modified'];
                assert.deepStrictEqual(objMDBefore, objMDAfter);
                return done();
            });
        });
    });
});

