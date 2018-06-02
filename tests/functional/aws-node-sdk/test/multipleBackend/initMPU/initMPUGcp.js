const async = require('async');
const assert = require('assert');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { describeSkipIfNotMultiple, gcpClient, gcpBucketMPU, gcpLocation,
    genUniqID } = require('../utils');
const { createMpuKey } =
    require('../../../../../../lib/data/external/GCP').GcpUtils;

const bucket = `initmpugcp${genUniqID()}`;
const keyName = `somekey-${genUniqID()}`;

let s3;
let bucketUtil;

describeSkipIfNotMultiple('Initiate MPU to GCP', () => {
    withV4(sigCfg => {
        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(bucket)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(bucket);
            })
            .catch(err => {
                process.stdout.write(`Error in afterEach: ${err}\n`);
                throw err;
            });
        });
        describe('Basic test: ', () => {
            beforeEach(done =>
              s3.createBucket({ Bucket: bucket,
                  CreateBucketConfiguration: {
                      LocationConstraint: gcpLocation,
                  },
              }, done));
            afterEach(function afterEachF(done) {
                const params = {
                    Bucket: bucket,
                    Key: keyName,
                    UploadId: this.currentTest.uploadId,
                };
                s3.abortMultipartUpload(params, done);
            });
            it('should create MPU and list in-progress multipart uploads',
            function ifF(done) {
                const params = {
                    Bucket: bucket,
                    Key: keyName,
                    Metadata: { 'scal-location-constraint': gcpLocation },
                };
                async.waterfall([
                    next => s3.createMultipartUpload(params, (err, res) => {
                        this.test.uploadId = res.UploadId;
                        assert(this.test.uploadId);
                        assert.strictEqual(res.Bucket, bucket);
                        assert.strictEqual(res.Key, keyName);
                        next(err);
                    }),
                    next => s3.listMultipartUploads(
                      { Bucket: bucket }, (err, res) => {
                          assert.strictEqual(res.NextKeyMarker, keyName);
                          assert.strictEqual(res.NextUploadIdMarker,
                            this.test.uploadId);
                          assert.strictEqual(res.Uploads[0].Key, keyName);
                          assert.strictEqual(res.Uploads[0].UploadId,
                            this.test.uploadId);
                          next(err);
                      }),
                    next => {
                        const mpuKey =
                            createMpuKey(keyName, this.test.uploadId, 'init');
                        const params = {
                            Bucket: gcpBucketMPU,
                            Key: mpuKey,
                        };
                        gcpClient.getObject(params, err => {
                            assert.ifError(err,
                                `Expected success, but got err ${err}`);
                            next();
                        });
                    },
                ], done);
            });
        });
    });
});
