const assert = require('assert');
const async = require('async');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { taggingTests } = require('../../lib/utility/tagging');

const bucketName = 'testtaggingbucket';
const objectName = 'testtaggingobject';
const objectNameAcl = 'testtaggingobjectacl';

const taggingConfig = { TagSet: [
    {
        Key: 'key1',
        Value: 'value1',
    }] };

function generateMultipleTagConfig(number) {
    const tags = [];
    for (let i = 0; i < number; i++) {
        tags.push({ Key: `myKey${i}`, Value: `myValue${i}` });
    }
    return {
        TagSet: tags,
    };
}
function generateTaggingConfig(key, value) {
    return {
        TagSet: [
            {
                Key: key,
                Value: value,
            },
        ],
    };
}

function _checkError(err, code, statusCode) {
    assert(err, 'Expected error but found none');
    assert.strictEqual(err.code, code);
    assert.strictEqual(err.statusCode, statusCode);
}

describe('PUT object taggings', () => {
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

        taggingTests.forEach(taggingTest => {
            it(taggingTest.it, done => {
                const taggingConfig = generateTaggingConfig(taggingTest.tag.key,
                  taggingTest.tag.value);
                s3.putObjectTagging({ Bucket: bucketName, Key: objectName,
                    Tagging: taggingConfig }, (err, data) => {
                    if (taggingTest.error) {
                        _checkError(err, taggingTest.error, 400);
                    } else {
                        assert.ifError(err, `Found unexpected err ${err}`);
                        assert.strictEqual(Object.keys(data).length, 0);
                    }
                    done();
                });
            });
        });

        it('should return BadRequest if putting more that 10 tags', done => {
            const taggingConfig = generateMultipleTagConfig(11);
            s3.putObjectTagging({ Bucket: bucketName, Key: objectName,
                Tagging: taggingConfig }, err => {
                _checkError(err, 'BadRequest', 400);
                done();
            });
        });

        it('should return InvalidTag if using the same key twice', done => {
            s3.putObjectTagging({ Bucket: bucketName, Key: objectName,
                Tagging: { TagSet: [
                    {
                        Key: 'key1',
                        Value: 'value1',
                    },
                    {
                        Key: 'key1',
                        Value: 'value2',
                    },
                ] },
            }, err => {
                _checkError(err, 'InvalidTag', 400);
                done();
            });
        });

        it('should return InvalidTag if key is an empty string', done => {
            s3.putObjectTagging({ Bucket: bucketName, Key: objectName,
                Tagging: { TagSet: [
                    {
                        Key: '',
                        Value: 'value1',
                    },
                ] },
            }, err => {
                _checkError(err, 'InvalidTag', 400);
                done();
            });
        });

        it('should be able to put an empty Tag set', done => {
            s3.putObjectTagging({ Bucket: bucketName, Key: objectName,
                Tagging: { TagSet: [] },
            }, (err, data) => {
                assert.ifError(err, `Found unexpected err ${err}`);
                assert.strictEqual(Object.keys(data).length, 0);
                done();
            });
        });

        it('should return NoSuchKey put tag to a non-existing object', done => {
            s3.putObjectTagging({
                Bucket: bucketName,
                Key: 'nonexisting',
                Tagging: taggingConfig,
            }, err => {
                _checkError(err, 'NoSuchKey', 404);
                done();
            });
        });

        it('should return 403 AccessDenied putting tag with another account',
        done => {
            otherAccountS3.putObjectTagging({ Bucket: bucketName, Key:
              objectName, Tagging: taggingConfig,
            }, err => {
                _checkError(err, 'AccessDenied', 403);
                done();
            });
        });

        it('should return 403 AccessDenied putting tag with a different ' +
        'account to an object with ACL "public-read-write"',
        done => {
            s3.putObjectAcl({ Bucket: bucketName, Key: objectName,
                ACL: 'public-read-write' }, err => {
                if (err) {
                    return done(err);
                }
                return otherAccountS3.putObjectTagging({ Bucket: bucketName,
                    Key: objectName, Tagging: taggingConfig,
                }, err => {
                    _checkError(err, 'AccessDenied', 403);
                    done();
                });
            });
        });

        it('should return 403 AccessDenied putting tag to an object ' +
        'in a bucket created with a different account',
        done => {
            async.waterfall([
                next => s3.putBucketAcl({ Bucket: bucketName, ACL:
                  'public-read-write' }, err => next(err)),
                next => otherAccountS3.putObject({ Bucket: bucketName, Key:
                    objectNameAcl }, err => next(err)),
                next => otherAccountS3.putObjectTagging({ Bucket: bucketName,
                    Key: objectNameAcl, Tagging: taggingConfig,
                }, err => next(err)),
            ], err => {
                _checkError(err, 'AccessDenied', 403);
                done();
            });
        });

        it('should put tag to an object in a bucket created with same ' +
        'account', done => {
            async.waterfall([
                next => s3.putBucketAcl({ Bucket: bucketName, ACL:
                  'public-read-write' }, err => next(err)),
                next => otherAccountS3.putObject({ Bucket: bucketName, Key:
                    objectNameAcl }, err => next(err)),
                next => s3.putObjectTagging({ Bucket: bucketName,
                    Key: objectNameAcl, Tagging: taggingConfig,
                }, err => next(err)),
            ], done);
        });
    });
});
