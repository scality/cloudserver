import assert from 'assert';
const async = require('async');

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

const bucketName = 'testtaggingbucket';
const objectName = 'testtaggingobject';

import {
    removeAllVersions,
    versioningEnabled,
} from '../../lib/utility/versioning-util';

function _checkError(err, code, statusCode) {
    assert(err, 'Expected error but found none');
    assert.strictEqual(err.code, code);
    assert.strictEqual(err.statusCode, statusCode);
}


describe('Put object tagging with versioning', () => {
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

        it('should be able to put tag with versioning', done => {
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
                    Tagging: { TagSet: [
                        {
                            Key: 'key1',
                            Value: 'value1',
                        }] },
                }, (err, data) => next(err, data, versionId)),
            ], (err, data, versionId) => {
                assert.ifError(err, `Found unexpected err ${err}`);
                assert.strictEqual(data.VersionId, versionId);
                done();
            });
        });

        it('should be able to put tag with a version of id "null"', done => {
            async.waterfall([
                next => s3.putObject({ Bucket: bucketName, Key: objectName },
                err => next(err)),
                next => s3.putBucketVersioning({ Bucket: bucketName,
                  VersioningConfiguration: versioningEnabled },
                  err => next(err)),
                next => s3.putObjectTagging({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: 'null',
                    Tagging: { TagSet: [
                        {
                            Key: 'key1',
                            Value: 'value1',
                        }] },
                }, (err, data) => next(err, data)),
            ], (err, data) => {
                assert.ifError(err, `Found unexpected err ${err}`);
                assert.strictEqual(data.VersionId, 'null');
                done();
            });
        });

        it('should return InvalidArgument putting tag with a non existing ' +
        'version id', done => {
            async.waterfall([
                next => s3.putObject({ Bucket: bucketName, Key: objectName },
                err => next(err)),
                next => s3.putBucketVersioning({ Bucket: bucketName,
                  VersioningConfiguration: versioningEnabled },
                  err => next(err)),
                next => s3.putObjectTagging({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: 'notexisting',
                    Tagging: { TagSet: [
                        {
                            Key: 'key1',
                            Value: 'value1',
                        }] },
                }, (err, data) => next(err, data)),
            ], err => {
                _checkError(err, 'InvalidArgument', 400);
                done();
            });
        });

        it('should return 405 MethodNotAllowed putting tag without ' +
         'version id if version specified is a delete marker', done => {
            async.waterfall([
                next => s3.putBucketVersioning({ Bucket: bucketName,
                  VersioningConfiguration: versioningEnabled },
                  err => next(err)),
                next => s3.putObject({ Bucket: bucketName, Key: objectName },
                  err => next(err)),
                next => s3.deleteObject({ Bucket: bucketName, Key: objectName },
                  err => next(err)),
                next => s3.putObjectTagging({
                    Bucket: bucketName,
                    Key: objectName,
                    Tagging: { TagSet: [
                        {
                            Key: 'key1',
                            Value: 'value1',
                        }] },
                }, (err, data) => next(err, data)),
            ], err => {
                _checkError(err, 'MethodNotAllowed', 405);
                done();
            });
        });

        it('should return 405 MethodNotAllowed putting tag with ' +
         'version id if version specified is a delete marker', done => {
            async.waterfall([
                next => s3.putBucketVersioning({ Bucket: bucketName,
                  VersioningConfiguration: versioningEnabled },
                  err => next(err)),
                next => s3.putObject({ Bucket: bucketName, Key: objectName },
                  err => next(err)),
                next => s3.deleteObject({ Bucket: bucketName, Key: objectName },
                  (err, data) => next(err, data.VersionId)),
                (versionId, next) => s3.putObjectTagging({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: versionId,
                    Tagging: { TagSet: [
                        {
                            Key: 'key1',
                            Value: 'value1',
                        }] },
                }, (err, data) => next(err, data)),
            ], err => {
                _checkError(err, 'MethodNotAllowed', 405);
                done();
            });
        });
    });
});
