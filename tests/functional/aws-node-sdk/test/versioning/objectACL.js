import assert from 'assert';
import async from 'async';
import { versioning } from 'arsenal';

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
const VID_INF = versioning.VersionID.VID_INF;
const nonExistingId = versioning.VersionID
    .encode(`${VID_INF.slice(VID_INF.length - 1)}7`);

function _assertNoError(err, desc) {
    assert.strictEqual(err, null, `Unexpected err ${desc}: ${err}`);
}

const testing = process.env.VERSIONING === 'no' ? describe.skip : describe;

testing('put and get object acl with versioning', () => {
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

        function _putAndGetAcl(cannedAcl, versionId, putResVerId,
            getResVerId, cb) {
            const params = {
                Bucket: bucket,
                Key: key,
                ACL: cannedAcl,
            };
            if (versionId) {
                params.VersionId = versionId;
            }
            _putObjectAcl(params, (err, data) => {
                _assertNoError(err, 'putting object acl with version id:' +
                    `${versionId}`);
                assert.strictEqual(data.VersionId, putResVerId,
                    `expected version id '${putResVerId}' in ` +
                    `putacl res headers, got '${data.VersionId}' instead`);
                delete params.ACL;
                _getObjectAcl(params, (err, data) => {
                    _assertNoError(err,
                        `getting object acl with version id: ${versionId}`);
                    assert.strictEqual(data.VersionId, getResVerId,
                        `expected version id '${getResVerId}' in ` +
                        `getacl res headers, got '${data.VersionId}' instead`);
                    assert.strictEqual(data.Grants.length, 2);
                    cb();
                });
            });
        }

        function _testBehaviorVersioningEnabledOrSuspended(versionIds) {
            it('non-version specific put and get ACL should target latest ' +
            'version AND return version ID in response headers', done => {
                const latestVersion = versionIds[versionIds.length - 1];
                _putAndGetAcl('public-read', undefined, latestVersion,
                    latestVersion, done);
            });

            it('version specific put and get ACL should return version ID ' +
            'in response headers', done => {
                const firstVersion = versionIds[0];
                _putAndGetAcl('public-read', firstVersion, firstVersion,
                    firstVersion, done);
            });

            it('version specific put and get ACL (version id = "null") ' +
            'should return version ID ("null") in response headers', done => {
                _putAndGetAcl('public-read', 'null', 'null', 'null', done);
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

        describe('in a bucket without versioning configuration', () => {
            beforeEach(done => {
                s3.putObject({ Bucket: bucket, Key: key }, done);
            });

            it('should not return version id for non-version specific ' +
            'put and get ACL', done => {
                _putAndGetAcl('public-read', undefined, undefined,
                undefined, done);
            });

            it('should not return version id for version specific ' +
            'put and get ACL (version id = "null")', done => {
                _putAndGetAcl('public-read', 'null', undefined,
                undefined, done);
            });

            it('should return NoSuchVersion if attempting to put acl for ' +
            'non-existing version', done => {
                const params = {
                    Bucket: bucket,
                    Key: key,
                    VersionId: nonExistingId,
                    ACL: 'private',
                };
                s3.putObjectAcl(params, err => {
                    assert(err, 'Expected err but did not find one');
                    assert.strictEqual(err.code, 'NoSuchVersion');
                    assert.strictEqual(err.statusCode, 404);
                    done();
                });
            });

            it('should return InvalidArgument if attempting to put acl for ' +
            'invalid hex string', done => {
                const params = { Bucket: bucket, Key: key, VersionId: invalidId,
                    ACL: 'private' };
                s3.putObjectAcl(params, err => {
                    assert(err, 'Expected err but did not find one');
                    assert.strictEqual(err.code, 'InvalidArgument');
                    assert.strictEqual(err.statusCode, 400);
                    done();
                });
            });

            it('should return NoSuchVersion if attempting to get acl for ' +
            'non-existing version', done => {
                const params = { Bucket: bucket, Key: key,
                    VersionId: nonExistingId };
                s3.getObjectAcl(params, err => {
                    assert(err, 'Expected err but did not find one');
                    assert.strictEqual(err.code, 'NoSuchVersion');
                    assert.strictEqual(err.statusCode, 404);
                    done();
                });
            });

            it('should return InvalidArgument if attempting to get acl for ' +
            'invalid id', done => {
                const params = {
                    Bucket: bucket,
                    Key: key,
                    VersionId: invalidId,
                };
                s3.getObjectAcl(params, err => {
                    assert(err, 'Expected err but did not find one');
                    assert.strictEqual(err.code, 'InvalidArgument');
                    assert.strictEqual(err.statusCode, 400);
                    done();
                });
            });
        });

        describe('on a version-enabled bucket with non-versioned object',
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

            describe('before putting new versions', () => {
                it('non-version specific put and get ACL should now ' +
                'return version ID ("null") in response headers', done => {
                    _putAndGetAcl('public-read', undefined, 'null',
                    'null', done);
                });
            });

            describe('after putting new versions', () => {
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

        describe('on version-suspended bucket with non-versioned object',
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

            describe('before putting new versions', () => {
                it('non-version specific put and get ACL should still ' +
                'return version ID ("null") in response headers', done => {
                    _putAndGetAcl('public-read', undefined, 'null',
                    'null', done);
                });
            });

            describe('after putting new versions', () => {
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
