const assert = require('assert');
const async = require('async');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { minimumAllowedPartSize } = require('../../../../../../constants');
const { removeAllVersions } = require('../../../lib/utility/versioning-util');
const {
    awsLocation,
    enableVersioning,
    suspendVersioning,
    putToAwsBackend,
    awsGetLatestVerId,
    getAndAssertResult,
    describeSkipIfNotMultiple,
} = require('../utils');

const data = ['a', 'b'].map(char => Buffer.alloc(minimumAllowedPartSize, char));
const concattedData = Buffer.concat(data);

const bucket = 'buckettestmultiplebackendmpuawsversioning';

function mpuSetup(s3, key, location, cb) {
    const partArray = [];
    async.waterfall([
        next => {
            const params = {
                Bucket: bucket,
                Key: key,
                Metadata: { 'scal-location-constraint': location },
            };
            s3.createMultipartUpload(params, (err, res) => {
                assert.strictEqual(err, null, `err creating mpu: ${err}`);
                const uploadId = res.UploadId;
                assert(uploadId);
                assert.strictEqual(res.Bucket, bucket);
                assert.strictEqual(res.Key, key);
                next(err, uploadId);
            });
        },
        (uploadId, next) => {
            const partParams = {
                Bucket: bucket,
                Key: key,
                PartNumber: 1,
                UploadId: uploadId,
                Body: data[0],
            };
            s3.uploadPart(partParams, (err, res) => {
                assert.strictEqual(err, null, `err uploading part 1: ${err}`);
                partArray.push({ ETag: res.ETag, PartNumber: 1 });
                next(err, uploadId);
            });
        },
        (uploadId, next) => {
            const partParams = {
                Bucket: bucket,
                Key: key,
                PartNumber: 2,
                UploadId: uploadId,
                Body: data[1],
            };
            s3.uploadPart(partParams, (err, res) => {
                assert.strictEqual(err, null, `err uploading part 2: ${err}`);
                partArray.push({ ETag: res.ETag, PartNumber: 2 });
                next(err, uploadId);
            });
        },
    ], (err, uploadId) => {
        process.stdout.write('Created MPU and put two parts\n');
        cb(err, uploadId, partArray);
    });
}

function completeAndAssertMpu(s3, params, cb) {
    const { bucket, key, uploadId, partArray, expectVersionId,
        expectedGetVersionId } = params;
    s3.completeMultipartUpload({
        Bucket: bucket,
        Key: key,
        UploadId: uploadId,
        MultipartUpload: { Parts: partArray },
    }, (err, data) => {
        assert.strictEqual(err, null, `Err completing MPU: ${err}`);
        if (expectVersionId) {
            assert.notEqual(data.VersionId, undefined);
        } else {
            assert.strictEqual(data.VersionId, undefined);
        }
        const expectedVersionId = expectedGetVersionId || data.VersionId;
        getAndAssertResult(s3, { bucket, key, body: concattedData,
            expectedVersionId }, cb);
    });
}

describeSkipIfNotMultiple('AWS backend complete mpu with versioning',
function testSuite() {
    this.timeout(30000);
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        beforeEach(done => s3.createBucket({
            Bucket: bucket,
            CreateBucketConfiguration: {
                LocationConstraint: awsLocation,
            },
        }, done));
        afterEach(done => {
            removeAllVersions({ Bucket: bucket }, err => {
                if (err) {
                    return done(err);
                }
                return s3.deleteBucket({ Bucket: bucket }, done);
            });
        });

        it('versioning not configured: should not return version id ' +
        'completing mpu', done => {
            const key = `somekey-${Date.now()}`;
            mpuSetup(s3, key, awsLocation, (err, uploadId, partArray) => {
                completeAndAssertMpu(s3, { bucket, key, uploadId, partArray,
                    expectVersionId: false }, done);
            });
        });

        it('versioning not configured: if complete mpu on already-existing ' +
        'object, metadata should be overwritten but data of previous version' +
        'in AWS should not be deleted', function itF(done) {
            const key = `somekey-${Date.now()}`;
            async.waterfall([
                next => putToAwsBackend(s3, bucket, key, '', err => next(err)),
                next => awsGetLatestVerId(key, '', next),
                (awsVerId, next) => {
                    this.test.awsVerId = awsVerId;
                    next();
                },
                next => mpuSetup(s3, key, awsLocation, next),
                (uploadId, partArray, next) => completeAndAssertMpu(s3,
                    { bucket, key, uploadId, partArray, expectVersionId:
                        false }, next),
                next => s3.deleteObject({ Bucket: bucket, Key: key, VersionId:
                    'null' }, next),
                (delData, next) => getAndAssertResult(s3, { bucket, key,
                    expectedError: 'NoSuchKey' }, next),
                next => awsGetLatestVerId(key, '', next),
                (awsVerId, next) => {
                    assert.strictEqual(awsVerId, this.test.awsVerId);
                    next();
                },
            ], done);
        });

        it('versioning suspended: should not return version id completing mpu',
        done => {
            const key = `somekey-${Date.now()}`;
            async.waterfall([
                next => suspendVersioning(s3, bucket, next),
                next => mpuSetup(s3, key, awsLocation, next),
                (uploadId, partArray, next) => completeAndAssertMpu(s3,
                    { bucket, key, uploadId, partArray, expectVersionId: false,
                    expectedGetVersionId: 'null' }, next),
            ], done);
        });

        it('versioning enabled: should return version id completing mpu',
        done => {
            const key = `somekey-${Date.now()}`;
            async.waterfall([
                next => enableVersioning(s3, bucket, next),
                next => mpuSetup(s3, key, awsLocation, next),
                (uploadId, partArray, next) => completeAndAssertMpu(s3,
                    { bucket, key, uploadId, partArray, expectVersionId: true },
                    next),
            ], done);
        });
    });
});
