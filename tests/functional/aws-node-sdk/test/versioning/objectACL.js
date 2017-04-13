import assert from 'assert';
import async from 'async';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

import {
    removeAllVersions,
    versioningEnabled,
    versioningSuspended,
} from '../../lib/utility/versioning-util.js';

const counter = 100;
let bucket;
const key = '/';
const invalidId = 'invalidId';
// formats differ for AWS and S3, use respective sample ids to obtain
// correct error response in tests
const nonExistingId = process.env.AWS_ON_AIR ?
    'MhhyTHhmZ4cxSi4Y9SMe5P7UJAz7HLJ9' :
    '3939393939393939393936493939393939393939756e6437';

function _assertNoError(err, desc) {
    assert.strictEqual(err, null, `Unexpected err ${desc}: ${err}`);
}


describe('versioned put and get object acl ::', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        // need a wrapper because sdk apparently does not include version id in
        // exposed data object for put/get acl methods
        function _wrapDataObject(method, params, callback) {
            let request = undefined;
            async.waterfall([
                next => {
                    request = s3[method](params, next);
                },
                (data, next) => {
                    const responseHeaders = request.response
                    .httpResponse.headers;
                    const dataObj = Object.assign({
                        VersionId: responseHeaders['x-amz-version-id'],
                    }, data);
                    return next(null, dataObj);
                },
            ], callback);
        }

        function _getObjectAcl(params, callback) {
            _wrapDataObject('getObjectAcl', params, callback);
        }

        function _putObjectAcl(params, callback) {
            _wrapDataObject('putObjectAcl', params, callback);
        }

        function _putAndGetAcl(cannedAcl, versionId, expected, cb) {
            const params = {
                Bucket: bucket,
                Key: key,
                ACL: cannedAcl,
            };
            if (versionId) {
                params.VersionId = versionId;
            }
            _putObjectAcl(params, (err, data) => {
                if (expected.error) {
                    assert.strictEqual(expected.error.code, err.code);
                    assert.strictEqual(expected.error.statusCode,
                        err.statusCode);
                } else {
                    _assertNoError(err, 'putting object acl with version id:' +
                        `${versionId}`);
                    assert.strictEqual(data.VersionId, expected.versionId,
                        `expected version id '${expected.versionId}' in ` +
                        `putacl res headers, got '${data.VersionId}' instead`);
                }
                delete params.ACL;
                _getObjectAcl(params, (err, data) => {
                    if (expected.error) {
                        assert.strictEqual(expected.error.code, err.code);
                        assert.strictEqual(expected.error.statusCode,
                            err.statusCode);
                    } else {
                        _assertNoError(err,
                            `getting object acl with version id: ${versionId}`);
                        assert.strictEqual(data.VersionId, expected.versionId,
                            `expected version id '${expected.versionId}' in ` +
                            `getacl res headers, got '${data.VersionId}'`);
                        assert.strictEqual(data.Grants.length, 2);
                    }
                    cb();
                });
            });
        }

        function _testBehaviorVersioningEnabledOrSuspended(versionIds) {
            it('should return 405 MethodNotAllowed putting acl without ' +
            'version id if latest version is a delete marker', done => {
                const aclParams = {
                    Bucket: bucket,
                    Key: key,
                    ACL: 'public-read-write',
                };
                s3.deleteObject({ Bucket: bucket, Key: key }, (err, data) => {
                    assert.strictEqual(err, null,
                        `Unexpected err deleting object: ${err}`);
                    assert.strictEqual(data.DeleteMarker, 'true');
                    assert(data.VersionId);
                    _putObjectAcl(aclParams, err => {
                        assert(err);
                        assert.strictEqual(err.code, 'MethodNotAllowed');
                        assert.strictEqual(err.statusCode, 405);
                        done();
                    });
                });
            });

            it('should return 405 MethodNotAllowed putting acl with ' +
            'version id if version specified is a delete marker', done => {
                const aclParams = {
                    Bucket: bucket,
                    Key: key,
                    ACL: 'public-read-write',
                };
                s3.deleteObject({ Bucket: bucket, Key: key }, (err, data) => {
                    assert.strictEqual(err, null,
                        `Unexpected err deleting object: ${err}`);
                    assert.strictEqual(data.DeleteMarker, 'true');
                    assert(data.VersionId);
                    aclParams.VersionId = data.VersionId;
                    _putObjectAcl(aclParams, err => {
                        assert(err);
                        assert.strictEqual(err.code, 'MethodNotAllowed');
                        assert.strictEqual(err.statusCode, 405);
                        done();
                    });
                });
            });

            it('should return 404 NoSuchKey getting acl without ' +
            'version id if latest version is a delete marker', done => {
                const aclParams = {
                    Bucket: bucket,
                    Key: key,
                };
                s3.deleteObject({ Bucket: bucket, Key: key }, (err, data) => {
                    assert.strictEqual(err, null,
                        `Unexpected err deleting object: ${err}`);
                    assert.strictEqual(data.DeleteMarker, 'true');
                    assert(data.VersionId);
                    _getObjectAcl(aclParams, err => {
                        assert(err);
                        assert.strictEqual(err.code, 'NoSuchKey');
                        assert.strictEqual(err.statusCode, 404);
                        done();
                    });
                });
            });

            it('should return 405 MethodNotAllowed getting acl with ' +
            'version id if version specified is a delete marker', done => {
                const latestVersion = versionIds[versionIds.length - 1];
                const aclParams = {
                    Bucket: bucket,
                    Key: key,
                    VersionId: latestVersion,
                };
                s3.deleteObject({ Bucket: bucket, Key: key }, (err, data) => {
                    assert.strictEqual(err, null,
                        `Unexpected err deleting object: ${err}`);
                    assert.strictEqual(data.DeleteMarker, 'true');
                    assert(data.VersionId);
                    aclParams.VersionId = data.VersionId;
                    _getObjectAcl(aclParams, err => {
                        assert(err);
                        assert.strictEqual(err.code, 'MethodNotAllowed');
                        assert.strictEqual(err.statusCode, 405);
                        done();
                    });
                });
            });

            it('non-version specific put and get ACL should target latest ' +
            'version AND return version ID in response headers', done => {
                const latestVersion = versionIds[versionIds.length - 1];
                const expectedRes = { versionId: latestVersion };
                _putAndGetAcl('public-read', undefined, expectedRes, done);
            });

            it('version specific put and get ACL should return version ID ' +
            'in response headers', done => {
                const firstVersion = versionIds[0];
                const expectedRes = { versionId: firstVersion };
                _putAndGetAcl('public-read', firstVersion, expectedRes, done);
            });

            it('version specific put and get ACL (version id = "null") ' +
            'should return version ID ("null") in response headers', done => {
                const expectedRes = { versionId: 'null' };
                _putAndGetAcl('public-read', 'null', expectedRes, done);
            });
        }

        beforeEach(done => {
            bucket = `versioning-bucket-acl-${Date.now()}`;
            s3.createBucket({ Bucket: bucket }, done);
        });

        afterEach(done => {
            removeAllVersions({ Bucket: bucket }, err => {
                if (err) {
                    return done(err);
                }
                return s3.deleteBucket({ Bucket: bucket }, done);
            });
        });

        describe('in bucket w/o versioning cfg :: ', () => {
            beforeEach(done => {
                s3.putObject({ Bucket: bucket, Key: key }, done);
            });

            it('should not return version id for non-version specific ' +
            'put and get ACL', done => {
                const expectedRes = { versionId: undefined };
                _putAndGetAcl('public-read', undefined, expectedRes, done);
            });

            it('should not return version id for version specific ' +
            'put and get ACL (version id = "null")', done => {
                const expectedRes = { versionId: undefined };
                _putAndGetAcl('public-read', 'null', expectedRes, done);
            });

            it('should return NoSuchVersion if attempting to put or get acl ' +
            'for non-existing version', done => {
                const error = { code: 'NoSuchVersion', statusCode: 404 };
                _putAndGetAcl('private', nonExistingId, { error }, done);
            });

            it('should return InvalidArgument if attempting to put/get acl ' +
            'for invalid hex string', done => {
                const error = { code: 'InvalidArgument', statusCode: 400 };
                _putAndGetAcl('private', invalidId, { error }, done);
            });
        });

        describe('on a version-enabled bucket with non-versioned object :: ',
        () => {
            const versionIds = [];

            beforeEach(done => {
                const params = { Bucket: bucket, Key: key };
                async.waterfall([
                    callback => s3.putObject(params, err => callback(err)),
                    callback => s3.putBucketVersioning({
                        Bucket: bucket,
                        VersioningConfiguration: versioningEnabled,
                    }, err => callback(err)),
                ], done);
            });

            afterEach(done => {
                // cleanup versionIds just in case
                versionIds.length = 0;
                done();
            });

            describe('before putting new versions :: ', () => {
                it('non-version specific put and get ACL should now ' +
                'return version ID ("null") in response headers', done => {
                    const expectedRes = { versionId: 'null' };
                    _putAndGetAcl('public-read', undefined, expectedRes, done);
                });
            });

            describe('after putting new versions :: ', () => {
                beforeEach(done => {
                    const params = { Bucket: bucket, Key: key };
                    async.timesSeries(counter, (i, next) =>
                        s3.putObject(params, (err, data) => {
                            _assertNoError(err, `putting version #${i}`);
                            versionIds.push(data.VersionId);
                            next(err);
                        }), done);
                });

                _testBehaviorVersioningEnabledOrSuspended(versionIds);
            });
        });

        describe('on version-suspended bucket with non-versioned object :: ',
        () => {
            const versionIds = [];

            beforeEach(done => {
                const params = { Bucket: bucket, Key: key };
                async.waterfall([
                    callback => s3.putObject(params, err => callback(err)),
                    callback => s3.putBucketVersioning({
                        Bucket: bucket,
                        VersioningConfiguration: versioningSuspended,
                    }, err => callback(err)),
                ], done);
            });

            afterEach(done => {
                // cleanup versionIds just in case
                versionIds.length = 0;
                done();
            });

            describe('before putting new versions :: ', () => {
                it('non-version specific put and get ACL should still ' +
                'return version ID ("null") in response headers', done => {
                    const expectedRes = { versionId: 'null' };
                    _putAndGetAcl('public-read', undefined, expectedRes, done);
                });
            });

            describe('after putting new versions :: ', () => {
                beforeEach(done => {
                    const params = { Bucket: bucket, Key: key };
                    async.waterfall([
                        callback => s3.putBucketVersioning({
                            Bucket: bucket,
                            VersioningConfiguration: versioningEnabled,
                        }, err => callback(err)),
                        callback => async.timesSeries(counter, (i, next) =>
                            s3.putObject(params, (err, data) => {
                                _assertNoError(err, `putting version #${i}`);
                                versionIds.push(data.VersionId);
                                next(err);
                            }), err => callback(err)),
                        callback => s3.putBucketVersioning({
                            Bucket: bucket,
                            VersioningConfiguration: versioningSuspended,
                        }, err => callback(err)),
                    ], done);
                });

                _testBehaviorVersioningEnabledOrSuspended(versionIds);
            });
        });
    });
});
