const assert = require('assert');
const async = require('async');

const { config } = require('../../../../../lib/Config');
const { describeSkipIfNotMultiple, genUniqID } = require('./utils');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const oldLocations = config.locationConstraints;
const newLocations = {
    'us-east-1': {
        'type': 'file',
        'objectId': 'us-east-1',
        'legacyAwsBehavior': true,
        'details': {}
    },
    'newlocation': {
        'type': 'file',
        'objectId': 'newlocation',
        'legacyAwsBehavior': false,
        'details': {}
    }
};
const testLocation = 'newlocation';
const bucket = `lcupdateevent${genUniqID()}`;
const body = Buffer.from('I am a body', 'utf8');
const bodyMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
let bucketUtil;
let s3;

describeSkipIfNotMultiple('APIs after location constraints update event',
() => {
    withV4(sigCfg => {
        beforeEach(() => {
            config.setLocationConstraints(newLocations);
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            process.stdout.write('Creating bucket\n');
            return s3.createBucketAsync({ Bucket: bucket,
                CreateBucketConfiguration: {
                    LocationConstraint: testLocation,
                }
            }).catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
    });

        afterEach(() => {
            config.setLocationConstraints(oldLocations);
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(bucket)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(bucket);
            })
            .catch(err => {
                process.stdout.write(`Error in after: ${err}\n`);
                throw err;
            });
        });

        it('should put an object to updated location constraint', done => {
            const key = `somekey-${genUniqID()}`;
            s3.putObject({ Bucket: bucket, Key: key, Body: body }, err => {
                assert.ifError(err);
                done();
            });
        });

        it('should get an object from updated location constraint', done => {
            const key = `somekey-${genUniqID()}`;
            s3.putObject({ Bucket: bucket, Key: key, Body: body }, err => {
                assert.ifError(err);
                s3.getObject({ Bucket: bucket, Key: key }, (err, res) => {
                    assert.ifError(err);
                    assert.strictEqual(res.ETag, `"${bodyMD5}"`);
                    done();
                });
            });
        });

        it('should list objects from updated location constraint', done => {
            const key = `somekey-${genUniqID()}`;
            s3.putObject({ Bucket: bucket, Key: key, Body: body }, err => {
                assert.ifError(err);
                s3.listObjects({ Bucket: bucket }, (err, res) => {
                    assert.ifError(err);
                    assert.strictEqual(res.Contents[0].ETag, `"${bodyMD5}"`);
                    assert.strictEqual(res.Contents[0].Key, key);
                    done();
                });
            });
        });

        it('should delete an object from updated location constraint', done => {
            const key = `somekey-${genUniqID()}`;
            s3.putObject({ Bucket: bucket, Key: key }, err => {
                assert.ifError(err);
                s3.deleteObject({ Bucket: bucket, Key: key }, err => {
                    assert.ifError(err);
                    done();
                });
            });
        });

        it('should initiate and abort an mpu in updated location constraint',
        done => {
            const key = `somekey-${genUniqID()}`;
            s3.createMultipartUpload({ Bucket: bucket, Key: key },
            (err, res) => {
                assert.ifError(err);
                const uploadId = res.UploadId;
                s3.abortMultipartUpload(
                { Bucket: bucket, Key: key, UploadId: uploadId }, err => {
                    assert.ifError(err);
                    done();
                });
            });
        });

        it('should initiate mpu, upload part, and abort mpu in updated ' +
        'location constraint', done => {
            const key = `somekey-${genUniqID()}`;
            async.waterfall([
                next => s3.createMultipartUpload(
                    { Bucket: bucket, Key: key }, next),
                (res, next) => s3.putPart({
                    Bucket: bucket,
                    Key: key,
                    UploadId: res.UploadId,
                    PartNumber: 1,
                }, err => next(err, res.UploadId)),
                (uploadId, next) => s3.abortMultipartUpload(
                    { Bucket: bucket, Key: key, UploadId: uploadId }, next),
            ], err => {
                assert.ifError(err);
                done();
            });
        });

        it('should initiate mpu, upload part, and complete an mpu in updated ' +
        'location constraint', done => {
            const key = `somekey-${genUniqID()}`;
            async.waterfall([
                next => s3.createMultipartUpload(
                    { Bucket: bucket, Key: key }, next),
                (res, next) => s3.putPart({
                    Bucket: bucket,
                    Key: key,
                    UploadId: res.UploadId,
                    PartNumber: 1,
                }, err => next(err, res.UploadId)),
                (uploadId, next) => s3.completeMultipartUpload(
                    { Bucket: bucket, Key: key, UploadId: uploadId }, next),
            ], err => {
                assert.ifError(err);
                done();
            });
        });
    });
});
