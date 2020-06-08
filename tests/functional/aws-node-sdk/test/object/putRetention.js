const assert = require('assert');
const async = require('async');
const moment = require('moment');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucketName = 'lockenabledputbucket';
const unlockedBucket = 'locknotenabledputbucket';
const objectName = 'putobjectretentionobject';

const retentionConfig = {
    Mode: 'GOVERNANCE',
    RetainUntilDate: moment().add(1, 'Days').toISOString(),
};

function _checkError(err, code, statusCode) {
    assert(err, 'Expected error but found none');
    assert.strictEqual(err.code, code);
    assert.strictEqual(err.statusCode, statusCode);
}

describe('PUT object retention', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const otherAccountBucketUtility = new BucketUtility('lisa', {});
        const otherAccountS3 = otherAccountBucketUtility.s3;

        beforeEach(() => async.waterfall([
            next => s3.createBucket({ Bucket: bucketName }, next),
            next => s3.createBucket({ Bucket: unlockedBucket }, next),
            next => s3.putObject({ Bucket: bucketName, Key: objectName }, next),
        ], err => assert.ifError(err)
        ));

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

        it('should return AccessDenied putting retention with another account',
        done => {
            otherAccountS3.putObjectRetention({
                Bucket: bucketName,
                Key: objectName,
                ObjectRetention: retentionConfig,
            }, err => {
                _checkError(err, 'AccessDenied', 403);
                done();
            });
        });

        it('should return NoSuchKey error if key does not exist', done => {
            s3.putObjectRetention({
                Bucket: bucketName,
                Key: 'thiskeydoesnotexist',
                ObjectRetention: retentionConfig,
            }, err => {
                _checkError(err, 'NoSuchKey', 403);
                done();
            });
        });

        it('should return NoSuchVersion error if version does not exist', done => {
            s3.putObjectRetention({
                Bucket: bucketName,
                Key: objectName,
                VersionId: 'thisversioniddoesnotexist',
                ObjectRetention: retentionConfig,
            }, err => {
                _checkError(err, 'NoSuchVersion', 403);
                done();
            });
        });

        it('should return InvalidRequest error putting retention to object ' +
        'in bucket with no object lock enabled', done => {
            s3.putObjectRetention({
                Bucket: unlockedBucket,
                Key: objectName,
                ObjectRetention: retentionConfig,
            }, err => {
                _checkError(err, 'InvalidRequest', 403);
                done();
            });
        });

        it('should return MethodNotAllowed if object version is delete marker',
        done => {
            s3.deleteObject({ Bucket: bucketName, Key: objectName }, err => {
                assert.ifError(err);
                s3.putObjectRetention({
                    Bucket: bucketName,
                    Key: objectName,
                    ObjectRetention: retentionConfig,
                }, err => {
                    _checkError(err, 'MethodNotAllowed', 403);
                    done();
                });
            });
        });

        it('should put object retention', done => {
            s3.putObjectRetention({
                Bucket: bucketName,
                Key: objectName,
                ObjectRetention: retentionConfig,
            }, err => {
                assert.ifError(err);
                done();
            });
        });
    });
});
