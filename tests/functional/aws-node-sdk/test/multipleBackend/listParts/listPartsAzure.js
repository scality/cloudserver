const assert = require('assert');
const async = require('async');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { expectedETag, uniqName, getAzureClient, getAzureContainerName }
  = require('../utils');
const { config } = require('../../../../../../lib/Config');
const constants = require('../../../../../../constants');
const maxSubPartSize = constants.maxSubPartSize;

const azureLocation = 'azuretest';
const keyObject = 'abortazure';
const azureClient = getAzureClient();
const azureContainerName = getAzureContainerName();

const describeSkipIfNotMultiple = (config.backends.data !== 'multiple'
    || process.env.S3_END_TO_END) ? describe.skip : describe;

/* eslint-enable camelcase */

let bucketUtil;
let s3;

describeSkipIfNotMultiple('MultipleBackend abort MPU to AZURE', function
describeF() {
    this.timeout(250000);
    withV4(sigCfg => {
        beforeEach(function beforeEachF() {
            this.currentTest.key = uniqName(keyObject);
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });
        describe('with bucket location header', () => {
            beforeEach(function beF(done) {
                async.waterfall([
                    next => s3.createBucket({ Bucket: azureContainerName,
                    }, err => next(err)),
                    next => s3.createMultipartUpload({
                        Bucket: azureContainerName,
                        Key: this.currentTest.key,
                        Metadata: { 'scal-location-constraint': azureLocation },
                    }, (err, res) => {
                        if (err) {
                            return next(err);
                        }
                        this.currentTest.uploadId = res.UploadId;
                        return next();
                    }),
                ], done);
            });

            afterEach(done => s3.deleteBucket({ Bucket: azureContainerName },
              done));

            describe('with one part bigger that max subpart', () => {
                beforeEach(function beF(done) {
                    const body = Buffer.alloc(maxSubPartSize + 10);
                    const params = {
                        Bucket: azureContainerName,
                        Key: this.currentTest.key,
                        UploadId: this.currentTest.uploadId,
                        PartNumber: 1,
                        Body: body,
                    };
                    s3.uploadPart(params, (err, res) => {
                        assert.strictEqual(err, null, 'Expected success, ' +
                        `got error: ${err}`);
                        const eTagExpected = expectedETag(body);
                        assert.strictEqual(res.ETag, eTagExpected);
                        return done();
                    });
                });
                it.only('should list parts', function itF(done) {
                    s3.listParts({
                        Bucket: azureContainerName,
                        Key: this.test.key,
                        UploadId: this.test.uploadId,
                    }, (err, res) => {
                        assert.strictEqual(err, null, 'Expected success, ' +
                        `got error: ${err}`);
                        console.log('err!!!', err);
                        console.log('res!!!', res);
                        return done();
                    });
                });
            });
        });
    });
});
