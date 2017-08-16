const assert = require('assert');
const async = require('async');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucketName = 'testdeletetaggingbucket';
const objectName = 'testtaggingobject';
const objectNameAcl = 'testtaggingobjectacl';

const taggingConfig = { TagSet: [
    {
        Key: 'key1',
        Value: 'value1',
    },
    {
        Key: 'key2',
        Value: 'value2',
    },
] };

function _checkError(err, code, statusCode) {
    assert(err, 'Expected error but found none');
    assert.strictEqual(err.code, code);
    assert.strictEqual(err.statusCode, statusCode);
}

describe('DELETE object taggings', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const otherAccountBucketUtility = new BucketUtility('lisa', {});
        const otherAccountS3 = otherAccountBucketUtility.s3;

        beforeEach(done => s3.createBucket({ Bucket: bucketName }, err => {
            if (err) {
                return done(err);
            }
            return s3.putObject({ Bucket: bucketName, Key: objectName }, done);
        }));

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

        it('should delete tag set', done => {
            s3.putObjectTagging({
                Bucket: bucketName,
                Key: objectName,
                Tagging: taggingConfig,
            }, err => {
                assert.ifError(err, `putObjectTagging error: ${err}`);
                s3.deleteObjectTagging({ Bucket: bucketName, Key: objectName },
                (err, data) => {
                    assert.ifError(err, `Found unexpected err ${err}`);
                    assert.strictEqual(Object.keys(data).length, 0);
                    done();
                });
            });
        });

        it('should delete a non-existing tag set', done => {
            s3.deleteObjectTagging({ Bucket: bucketName, Key: objectName },
            (err, data) => {
                assert.ifError(err, `Found unexpected err ${err}`);
                assert.strictEqual(Object.keys(data).length, 0);
                done();
            });
        });

        it('should return NoSuchKey deleting tag set to a non-existing object',
        done => {
            s3.deleteObjectTagging({
                Bucket: bucketName,
                Key: 'nonexisting',
            }, err => {
                _checkError(err, 'NoSuchKey', 404);
                done();
            });
        });
        it('should return 403 AccessDenied deleting tag set with another ' +
        'account', done => {
            otherAccountS3.deleteObjectTagging({ Bucket: bucketName, Key:
              objectName }, err => {
                _checkError(err, 'AccessDenied', 403);
                done();
            });
        });

        it('should return 403 AccessDenied deleting tag set with a different ' +
        'account to an object with ACL "public-read-write"',
        done => {
            s3.putObjectAcl({ Bucket: bucketName, Key: objectName,
            ACL: 'public-read-write' }, err => {
                if (err) {
                    return done(err);
                }
                return otherAccountS3.deleteObjectTagging({ Bucket: bucketName,
                  Key: objectName }, err => {
                    _checkError(err, 'AccessDenied', 403);
                    done();
                });
            });
        });

        it('should return 403 AccessDenied deleting tag set to an object' +
        ' in a bucket created with a different account',
        done => {
            async.waterfall([
                next => s3.putBucketAcl({ Bucket: bucketName, ACL:
                  'public-read-write' }, err => next(err)),
                next => otherAccountS3.putObject({ Bucket: bucketName, Key:
                    objectNameAcl }, err => next(err)),
                next => otherAccountS3.deleteObjectTagging({ Bucket: bucketName,
                      Key: objectNameAcl }, err => next(err)),
            ], err => {
                _checkError(err, 'AccessDenied', 403);
                done();
            });
        });

        it('should delete tag set to an object in a bucket created with same ' +
        'account even though object put by other account', done => {
            async.waterfall([
                next => s3.putBucketAcl({ Bucket: bucketName, ACL:
                  'public-read-write' }, err => next(err)),
                next => otherAccountS3.putObject({ Bucket: bucketName, Key:
                    objectNameAcl }, err => next(err)),
                next => s3.deleteObjectTagging({ Bucket: bucketName,
                      Key: objectNameAcl }, err => next(err)),
            ], done);
        });
    });
});
