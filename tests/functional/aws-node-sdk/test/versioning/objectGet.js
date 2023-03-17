const assert = require('assert');
const async = require('async');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const {
    removeAllVersions,
    versioningEnabled,
    versioningSuspended,
} = require('../../lib/utility/versioning-util.js');

const key = 'objectKey';
// formats differ for AWS and S3, use respective sample ids to obtain
// correct error response in tests
const nonExistingId = process.env.AWS_ON_AIR ?
    'MhhyTHhmZ4cxSi4Y9SMe5P7UJAz7HLJ9' :
    '3939393939393939393936493939393939393939756e6437';

function _assertNoError(err, desc) {
    assert.ifError(err, `Unexpected err ${desc}: ${err}`);
}

function _assertError(err, statusCode, code) {
    assert.notEqual(err, null,
        'Expected failure but got success');
    assert.strictEqual(err.code, code);
    assert.strictEqual(err.statusCode, statusCode);
}


describe('get behavior on versioning-enabled bucket', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        let bucket;

        beforeEach(done => {
            bucket = `versioning-bucket-${Date.now()}`;
            s3.createBucket({ Bucket: bucket }, err => {
                _assertNoError(err, 'createBucket');
                return s3.putBucketVersioning({
                    Bucket: bucket,
                    VersioningConfiguration: versioningEnabled,
                }, done);
            });
        });

        afterEach(done => {
            removeAllVersions({ Bucket: bucket }, err => {
                _assertNoError(err, 'removeAllVersions');
                return s3.deleteBucket({ Bucket: bucket }, done);
            });
        });

        describe('behavior when only version put is a regular version', () => {
            beforeEach(function beforeEachF(done) {
                s3.putObject({ Bucket: bucket, Key: key }, (err, data) => {
                    _assertNoError(err, 'putObject');
                    this.currentTest.versionId = data.VersionId;
                    done();
                });
            });

            it('should be able to get the object version', function itF(done) {
                s3.getObject({
                    Bucket: bucket,
                    Key: key,
                    VersionId: this.test.versionId,
                }, (err, data) => {
                    assert.ifError(err);
                    assert.strictEqual(data.ContentLength, 0);
                    done();
                });
            });

            it('it should return NoSuchVersion if try to get a non-existing object version', done => {
                s3.getObject({
                    Bucket: bucket,
                    Key: key,
                    VersionId: nonExistingId,
                },
                err => {
                    _assertError(err, 404, 'NoSuchVersion');
                    done();
                });
            });

            it('it should return NoSuchVersion if try to get a non-existing null version', done => {
                s3.getObject({
                    Bucket: bucket,
                    Key: key,
                    VersionId: 'null',
                },
                err => {
                    _assertError(err, 404, 'NoSuchVersion');
                    done();
                });
            });

            it('it should return NoSuchVersion if try to get a deleted noncurrent null version', done => {
                async.series([
                    next => s3.putBucketVersioning({
                        Bucket: bucket,
                        VersioningConfiguration: versioningSuspended,
                    }, next),
                    next => s3.putObject({ Bucket: bucket, Key: key }, next),
                    next => s3.putBucketVersioning({
                        Bucket: bucket,
                        VersioningConfiguration: versioningEnabled,
                    }, next),
                    next => s3.putObject({ Bucket: bucket, Key: key }, next),
                    next => s3.deleteObject({ Bucket: bucket, Key: key, VersionId: 'null' }, next),
                    next => s3.getObject({
                        Bucket: bucket,
                        Key: key,
                        VersionId: 'null',
                    }, err => {
                        _assertError(err, 404, 'NoSuchVersion');
                        next();
                    }),
                ], done);
            });
        });

        describe('behavior when only version put is a delete marker', () => {
            beforeEach(function beforeEachF(done) {
                s3.deleteObject({ Bucket: bucket, Key: key },
                  (err, data) => {
                      _assertNoError(err, 'deleteObject');
                      this.currentTest.deleteVersionId = data.VersionId;
                      done(err);
                  });
            });

            it('should not be able to get a delete marker', function itF(done) {
                s3.getObject({
                    Bucket: bucket,
                    Key: key,
                    VersionId: this.test.deleteVersionId,
                }, function test1(err) {
                    _assertError(err, 405, 'MethodNotAllowed');
                    const headers = this.httpResponse.headers;
                    assert.strictEqual(headers['x-amz-delete-marker'], 'true');
                    done();
                });
            });

            it('it should return NoSuchKey if try to get object whose ' +
            'latest version is a delete marker', done => {
                s3.getObject({
                    Bucket: bucket,
                    Key: key,
                }, function test2(err) {
                    _assertError(err, 404, 'NoSuchKey');
                    const headers = this.httpResponse.headers;
                    assert.strictEqual(headers['x-amz-delete-marker'], 'true');
                    done();
                });
            });
        });

        describe('behavior when put version with content then put delete ' +
        'marker', () => {
            beforeEach(function beforeEachF(done) {
                s3.putObject({ Bucket: bucket, Key: key }, (err, data) => {
                    _assertNoError(err, 'putObject');
                    this.currentTest.versionId = data.VersionId;
                    s3.deleteObject({ Bucket: bucket, Key: key },
                      (err, data) => {
                          _assertNoError(err, 'deleteObject');
                          this.currentTest.deleteVersionId = data.VersionId;
                          done(err);
                      });
                });
            });

            it('should not be able to get a delete marker', function itF(done) {
                s3.getObject({
                    Bucket: bucket,
                    Key: key,
                    VersionId: this.test.deleteVersionId,
                }, function test3(err) {
                    _assertError(err, 405, 'MethodNotAllowed');
                    const headers = this.httpResponse.headers;
                    assert.strictEqual(headers['x-amz-delete-marker'], 'true');
                    done();
                });
            });

            it('should be able to get a version that was put prior to the ' +
            'delete marker', function itF(done) {
                s3.getObject({
                    Bucket: bucket,
                    Key: key,
                    VersionId: this.test.versionId },
                (err, data) => {
                    _assertNoError(err, 'getObject');
                    assert.strictEqual(data.VersionId, this.test.versionId);
                    done();
                });
            });

            it('should return NoSuchKey if get object without version and ' +
            'latest version is a delete marker',
            done => {
                s3.getObject({
                    Bucket: bucket,
                    Key: key,
                }, function test4(err) {
                    _assertError(err, 404, 'NoSuchKey');
                    const headers = this.httpResponse.headers;
                    assert.strictEqual(headers['x-amz-delete-marker'], 'true');
                    done();
                });
            });
        });

        describe('x-amz-tagging-count with versioning', () => {
            let params;
            let paramsTagging;
            beforeEach(function beforeEach(done) {
                params = {
                    Bucket: bucket,
                    Key: key,
                };
                paramsTagging = {
                    Bucket: bucket,
                    Key: key,
                    Tagging: {
                        TagSet: [
                            {
                                Key: 'key1',
                                Value: 'value',
                            },
                        ],
                    },
                };
                s3.putObject(params, (err, data) => {
                    if (err) {
                        return done(err);
                    }
                    this.currentTest.versionId = data.VersionId;
                    return done();
                });
            });

            it('should not return "x-amz-tagging-count" if no tag ' +
            'associated with the object',
            function itF(done) {
                params.VersionId = this.test.VersionId;
                s3.getObject(params, (err, data) => {
                    if (err) {
                        return done(err);
                    }
                    assert.strictEqual(data.TagCount, undefined);
                    return done();
                });
            });

            describe('tag associated with the object ', () => {
                beforeEach(done => s3.putObjectTagging(paramsTagging, done));

                it('should return "x-amz-tagging-count" header that provides ' +
                'the count of number of tags associated with the object',
                function itF(done) {
                    params.VersionId = this.test.VersionId;
                    s3.getObject(params, (err, data) => {
                        if (err) {
                            return done(err);
                        }
                        assert.equal(data.TagCount, 1);
                        return done();
                    });
                });
            });
        });
    });
});
