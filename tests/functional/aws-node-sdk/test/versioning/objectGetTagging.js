const assert = require('assert');
const async = require('async');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const {
    removeAllVersions,
    versioningEnabled,
} = require('../../lib/utility/versioning-util');

const bucketName = 'testtaggingbucket';
const objectName = 'testtaggingobject';

function _checkError(err, code, statusCode) {
    expect(err).toBeTruthy();
    expect(err.code).toBe(code);
    expect(err.statusCode).toBe(statusCode);
}


describe('Get object tagging with versioning', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        beforeEach(done => s3.createBucket({ Bucket: bucketName }, done));
        afterEach(done => {
            removeAllVersions({ Bucket: bucketName }, err => {
                if (err) {
                    return done(err);
                }
                return s3.deleteBucket({ Bucket: bucketName }, done);
            });
        });

        test('should be able to get tag with versioning', done => {
            const taggingConfig = { TagSet: [
                {
                    Key: 'key1',
                    Value: 'value1',
                }] };
            async.waterfall([
                next => s3.putBucketVersioning({ Bucket: bucketName,
                    VersioningConfiguration: versioningEnabled },
                  err => next(err)),
                next => s3.putObject({ Bucket: bucketName, Key: objectName },
                  (err, data) => next(err, data.VersionId)),
                (versionId, next) => s3.putObjectTagging({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: versionId,
                    Tagging: taggingConfig,
                }, err => next(err, versionId)),
                (versionId, next) => s3.getObjectTagging({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: versionId,
                }, (err, data) => next(err, data, versionId)),
            ], (err, data, versionId) => {
                assert.ifError(err, `Found unexpected err ${err}`);
                expect(data.VersionId).toBe(versionId);
                assert.deepStrictEqual(data.TagSet, taggingConfig.TagSet);
                done();
            });
        });

        test('should be able to get tag with a version of id "null"', done => {
            async.waterfall([
                next => s3.putObject({ Bucket: bucketName, Key: objectName },
                err => next(err)),
                next => s3.putBucketVersioning({ Bucket: bucketName,
                    VersioningConfiguration: versioningEnabled },
                  err => next(err)),
                next => s3.getObjectTagging({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: 'null',
                }, (err, data) => next(err, data)),
            ], (err, data) => {
                assert.ifError(err, `Found unexpected err ${err}`);
                expect(data.VersionId).toBe('null');
                done();
            });
        });

        test('should return InvalidArgument getting tag with a non existing ' +
        'version id', done => {
            async.waterfall([
                next => s3.putObject({ Bucket: bucketName, Key: objectName },
                err => next(err)),
                next => s3.putBucketVersioning({ Bucket: bucketName,
                    VersioningConfiguration: versioningEnabled },
                  err => next(err)),
                next => s3.getObjectTagging({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: 'notexisting',
                }, (err, data) => next(err, data)),
            ], err => {
                _checkError(err, 'InvalidArgument', 400);
                done();
            });
        });

        test('should return 404 NoSuchKey getting tag without ' +
         'version id if version specified is a delete marker', done => {
            async.waterfall([
                next => s3.putBucketVersioning({ Bucket: bucketName,
                    VersioningConfiguration: versioningEnabled },
                  err => next(err)),
                next => s3.putObject({ Bucket: bucketName, Key: objectName },
                  err => next(err)),
                next => s3.deleteObject({ Bucket: bucketName, Key: objectName },
                  err => next(err)),
                next => s3.getObjectTagging({
                    Bucket: bucketName,
                    Key: objectName,
                }, (err, data) => next(err, data)),
            ], err => {
                _checkError(err, 'NoSuchKey', 404);
                done();
            });
        });

        test('should return 405 MethodNotAllowed getting tag with ' +
         'version id if version specified is a delete marker', done => {
            async.waterfall([
                next => s3.putBucketVersioning({ Bucket: bucketName,
                    VersioningConfiguration: versioningEnabled },
                  err => next(err)),
                next => s3.putObject({ Bucket: bucketName, Key: objectName },
                  err => next(err)),
                next => s3.deleteObject({ Bucket: bucketName, Key: objectName },
                  (err, data) => next(err, data.VersionId)),
                (versionId, next) => s3.getObjectTagging({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: versionId,
                }, (err, data) => next(err, data)),
            ], err => {
                _checkError(err, 'MethodNotAllowed', 405);
                done();
            });
        });
    });
});
