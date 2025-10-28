const assert = require('assert');
const async = require('async');
const {promisify} = require('util');

const {
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketVersioningCommand,
    PutObjectCommand,
    PutObjectTaggingCommand,
    DeleteObjectCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { checkOneVersion } = require('../../lib/utility/versioning-util');

const {
    removeAllVersions,
    versioningEnabled,
} = require('../../lib/utility/versioning-util');

const removeAllVersionsPromise= promisify(removeAllVersions);

const bucketName = 'testtaggingbucket';
const objectName = 'testtaggingobject';

const invalidId = 'invalidIdWithMoreThan40BytesAndThatIsNotLongEnoughYet';

function _checkError(err, code, statusCode) {
    assert(err, 'Expected error but found none');
    assert.strictEqual(err.name, code);
    assert.strictEqual(err.$metadata?.httpStatusCode, statusCode);
}

describe('Put object tagging with versioning', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        beforeEach(async () => {
            await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
        });

        afterEach(async () => {
            await removeAllVersionsPromise({ Bucket: bucketName });
            await bucketUtil.empty(bucketName);
            await s3.send(new DeleteBucketCommand({ Bucket: bucketName }));
        });

        it('should be able to put tag with versioning', done => {
            async.waterfall([
                next => s3.send(new PutBucketVersioningCommand({
                    Bucket: bucketName,
                    VersioningConfiguration: versioningEnabled,
                }))
                    .then(() => next())
                    .catch(next),
                
                next => s3.send(new PutObjectCommand({ 
                    Bucket: bucketName, 
                    Key: objectName 
                }))
                    .then(data => next(null, data.VersionId))
                    .catch(next),
                
                (versionId, next) => s3.send(new PutObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: versionId,
                    Tagging: { TagSet: [
                        {
                            Key: 'key1',
                            Value: 'value1',
                        }] },
                }))
                    .then(data => next(null, data, versionId))
                    .catch(next),
            ], (err, data, versionId) => {
                assert.ifError(err, `Found unexpected err ${err}`);
                assert.strictEqual(data.VersionId, versionId);
                done();
            });
        });

        it('should not create version putting object tags on a ' +
        ' version-enabled bucket where no version id is specified ', done => {
            async.waterfall([
                next => s3.send(new PutBucketVersioningCommand({
                    Bucket: bucketName,
                    VersioningConfiguration: versioningEnabled,
                }))
                    .then(() => next())
                    .catch(next),
                
                next => s3.send(new PutObjectCommand({ 
                    Bucket: bucketName, 
                    Key: objectName 
                }))
                    .then(data => next(null, data.VersionId))
                    .catch(next),
                
                (versionId, next) => s3.send(new PutObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    Tagging: { TagSet: [
                        {
                            Key: 'key1',
                            Value: 'value1',
                        }] },
                }))
                    .then(() => next(null, versionId))
                    .catch(next),
                
                (versionId, next) =>
                    checkOneVersion(s3, bucketName, versionId)
                        .then(() => next())
                        .catch(next),
            ], done);
        });

        it('should be able to put tag with a version of id "null"', done => {
            async.waterfall([
                next => s3.send(new PutObjectCommand({ 
                    Bucket: bucketName, 
                    Key: objectName 
                }))
                    .then(() => next())
                    .catch(next),
                
                next => s3.send(new PutBucketVersioningCommand({
                    Bucket: bucketName,
                    VersioningConfiguration: versioningEnabled,
                }))
                    .then(() => next())
                    .catch(next),
                
                next => s3.send(new PutObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: 'null',
                    Tagging: { TagSet: [
                        {
                            Key: 'key1',
                            Value: 'value1',
                        }] },
                }))
                    .then(data => next(null, data))
                    .catch(next),
            ], (err, data) => {
                assert.ifError(err, `Found unexpected err ${err}`);
                assert.strictEqual(data.VersionId, 'null');
                done();
            });
        });

        it('should return InvalidArgument putting tag with a non existing ' +
        'version id', done => {
            async.waterfall([
                next => s3.send(new PutObjectCommand({ 
                    Bucket: bucketName, 
                    Key: objectName 
                }))
                    .then(() => next())
                    .catch(next),
                
                next => s3.send(new PutBucketVersioningCommand({
                    Bucket: bucketName,
                    VersioningConfiguration: versioningEnabled,
                }))
                    .then(() => next())
                    .catch(next),
                
                next => s3.send(new PutObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: invalidId,
                    Tagging: { TagSet: [
                        {
                            Key: 'key1',
                            Value: 'value1',
                        }] },
                }))
                    .then(data => next(null, data))
                    .catch(next),
            ], err => {
                _checkError(err, 'InvalidArgument', 400);
                done();
            });
        });

        it('should return 405 MethodNotAllowed putting tag without ' +
         'version id if version specified is a delete marker', done => {
            async.waterfall([
                next => s3.send(new PutBucketVersioningCommand({
                    Bucket: bucketName,
                    VersioningConfiguration: versioningEnabled,
                }))
                    .then(() => next())
                    .catch(next),
                
                next => s3.send(new PutObjectCommand({ 
                    Bucket: bucketName, 
                    Key: objectName 
                }))
                    .then(() => next())
                    .catch(next),
                
                next => s3.send(new DeleteObjectCommand({ 
                    Bucket: bucketName, 
                    Key: objectName 
                }))
                    .then(() => next())
                    .catch(next),
                
                next => s3.send(new PutObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    Tagging: { TagSet: [
                        {
                            Key: 'key1',
                            Value: 'value1',
                        }] },
                }))
                    .then(data => next(null, data))
                    .catch(next),
            ], err => {
                _checkError(err, 'MethodNotAllowed', 405);
                done();
            });
        });

        it('should return 405 MethodNotAllowed putting tag with ' +
         'version id if version specified is a delete marker', done => {
            async.waterfall([
                next => s3.send(new PutBucketVersioningCommand({
                    Bucket: bucketName,
                    VersioningConfiguration: versioningEnabled,
                }))
                    .then(() => next())
                    .catch(next),
                
                next => s3.send(new PutObjectCommand({ 
                    Bucket: bucketName, 
                    Key: objectName 
                }))
                    .then(() => next())
                    .catch(next),
                
                next => s3.send(new DeleteObjectCommand({ 
                    Bucket: bucketName, 
                    Key: objectName 
                }))
                    .then(data => next(null, data.VersionId))
                    .catch(next),
                
                (versionId, next) => s3.send(new PutObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: versionId,
                    Tagging: { TagSet: [
                        {
                            Key: 'key1',
                            Value: 'value1',
                        }] },
                }))
                    .then(data => next(null, data))
                    .catch(next),
            ], err => {
                _checkError(err, 'MethodNotAllowed', 405);
                done();
            });
        });
    });
});
