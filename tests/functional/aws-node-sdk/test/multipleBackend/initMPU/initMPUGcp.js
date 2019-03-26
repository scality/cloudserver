const async = require('async');
const assert = require('assert');
const arsenal = require('arsenal');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { describeSkipIfNotMultipleOrCeph, gcpClient, gcpBucketMPU, gcpLocation,
    genUniqID } = require('../utils');
const { createMpuKey } = arsenal.storage.data.external.GcpUtils;

const bucket = `initmpugcp${genUniqID()}`;
const keyName = `somekey-${genUniqID()}`;

let s3;
let bucketUtil;

describeSkipIfNotMultipleOrCeph('Initiate MPU to GCP', () => {
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
            let testContext;

            beforeEach(() => {
                testContext = {};
            });

            beforeEach(done =>
              s3.createBucket({ Bucket: bucket,
                  CreateBucketConfiguration: {
                      LocationConstraint: gcpLocation,
                  },
              }, done));
            afterEach(done => {
                const params = {
                    Bucket: bucket,
                    Key: keyName,
                    UploadId: testContext.currentTest.uploadId,
                };
                s3.abortMultipartUpload(params, done);
            });
            test(
                'should create MPU and list in-progress multipart uploads',
                done => {
                    const params = {
                        Bucket: bucket,
                        Key: keyName,
                        Metadata: { 'scal-location-constraint': gcpLocation },
                    };
                    async.waterfall([
                        next => s3.createMultipartUpload(params, (err, res) => {
                            testContext.test.uploadId = res.UploadId;
                            expect(testContext.test.uploadId).toBeTruthy();
                            expect(res.Bucket).toBe(bucket);
                            expect(res.Key).toBe(keyName);
                            next(err);
                        }),
                        next => s3.listMultipartUploads(
                          { Bucket: bucket }, (err, res) => {
                              expect(res.NextKeyMarker).toBe(keyName);
                              expect(res.NextUploadIdMarker).toBe(testContext.test.uploadId);
                              expect(res.Uploads[0].Key).toBe(keyName);
                              expect(res.Uploads[0].UploadId).toBe(testContext.test.uploadId);
                              next(err);
                          }),
                        next => {
                            const mpuKey =
                                createMpuKey(keyName, testContext.test.uploadId, 'init');
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
                }
            );
        });
    });
});
