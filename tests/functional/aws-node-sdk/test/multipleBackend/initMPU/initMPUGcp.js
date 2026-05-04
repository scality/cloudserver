const async = require('async');
const assert = require('assert');
const arsenal = require('arsenal');
const {
    CreateBucketCommand,
    AbortMultipartUploadCommand,
    CreateMultipartUploadCommand,
    ListMultipartUploadsCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { gcpClient, gcpBucketMPU, gcpLocation,
    genUniqID, describeSkipIfNotMultiple } = require('../utils');
const { createMpuKey } = arsenal.storage.data.external.GcpUtils;

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
                s3.send(new CreateBucketCommand({
                    Bucket: bucket,
                    CreateBucketConfiguration: {
                        LocationConstraint: gcpLocation,
                    },
                }))
                .then(() => done())
                .catch(done));
            afterEach(function afterEachF(done) {
                const params = {
                    Bucket: bucket,
                    Key: keyName,
                    UploadId: this.currentTest.uploadId,
                };
                s3.send(new AbortMultipartUploadCommand(params))
                    .then(() => done())
                    .catch(done);
            });
            it('should create MPU and list in-progress multipart uploads',
            function ifF(done) {
                const params = {
                    Bucket: bucket,
                    Key: keyName,
                    Metadata: { 'scal-location-constraint': gcpLocation },
                };
                async.waterfall([
                    next => {
                        s3.send(new CreateMultipartUploadCommand(params))
                            .then(res => {
                                this.test.uploadId = res.UploadId;
                                assert(this.test.uploadId);
                                assert.strictEqual(res.Bucket, bucket);
                                assert.strictEqual(res.Key, keyName);
                                next();
                            })
                            .catch(next);
                    },
                    next => {
                        s3.send(new ListMultipartUploadsCommand({
                            Bucket: bucket,
                        }))
                            .then(res => {
                                assert.strictEqual(res.NextKeyMarker, keyName);
                                assert.strictEqual(res.NextUploadIdMarker,
                                    this.test.uploadId);
                                assert.strictEqual(res.Uploads[0].Key, keyName);
                                assert.strictEqual(res.Uploads[0].UploadId,
                                    this.test.uploadId);
                                next();
                            })
                            .catch(next);
                    },
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
