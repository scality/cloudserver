const async = require('async');
const assert = require('assert');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { describeSkipIfNotMultipleOrCeph, azureLocation, getAzureContainerName,
    genUniqID } = require('../utils');

const keyName = `somekey-${genUniqID()}`;

const azureContainerName = getAzureContainerName(azureLocation);
let s3;
let bucketUtil;

describeSkipIfNotMultipleOrCeph('Initiate MPU to AZURE', () => {
    withV4(sigCfg => {
        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(azureContainerName)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(azureContainerName);
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
              s3.createBucket({ Bucket: azureContainerName,
                  CreateBucketConfiguration: {
                      LocationConstraint: azureLocation,
                  },
              }, done));
            afterEach(done => {
                const params = {
                    Bucket: azureContainerName,
                    Key: keyName,
                    UploadId: testContext.currentTest.uploadId,
                };
                s3.abortMultipartUpload(params, done);
            });
            test(
                'should create MPU and list in-progress multipart uploads',
                done => {
                    const params = {
                        Bucket: azureContainerName,
                        Key: keyName,
                        Metadata: { 'scal-location-constraint': azureLocation },
                    };
                    async.waterfall([
                        next => s3.createMultipartUpload(params, (err, res) => {
                            testContext.test.uploadId = res.UploadId;
                            expect(testContext.test.uploadId).toBeTruthy();
                            expect(res.Bucket).toBe(azureContainerName);
                            expect(res.Key).toBe(keyName);
                            next(err);
                        }),
                        next => s3.listMultipartUploads(
                          { Bucket: azureContainerName }, (err, res) => {
                              expect(res.NextKeyMarker).toBe(keyName);
                              expect(res.NextUploadIdMarker).toBe(testContext.test.uploadId);
                              expect(res.Uploads[0].Key).toBe(keyName);
                              expect(res.Uploads[0].UploadId).toBe(testContext.test.uploadId);
                              next(err);
                          }),
                    ], done);
                }
            );
        });
    });
});
