const assert = require('assert');

const { config } = require('../../../../../../lib/Config');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');

const describeSkipIfNotMultiple = (config.backends.data !== 'multiple'
    || process.env.S3_END_TO_END) ? describe.skip : describe;

const azureLocation = 'azuretest';
let azureContainerName;
const bodyFirstPart = Buffer.alloc(10);
const bodySecondPart = Buffer.alloc(104857610);

if (config.locationConstraints[azureLocation] &&
config.locationConstraints[azureLocation].details &&
config.locationConstraints[azureLocation].details.azureContainerName) {
    azureContainerName =
      config.locationConstraints[azureLocation].details.azureContainerName;
}

let bucketUtil;
let s3;

describeSkipIfNotMultiple('List parts of MPU on Azure data backend', () => {
    withV4(sigCfg => {
        beforeEach(function beforeEachFn() {
            this.currentTest.key = `somekey-${Date.now()}`;
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketAsync({ Bucket: azureContainerName })
            .then(() => s3.createMultipartUploadAsync({
                Bucket: azureContainerName, Key: this.currentTest.key,
                Metadata: { 'scal-location-constraint': azureLocation } }))
            .then(res => {
                this.currentTest.uploadId = res.UploadId;
                return s3.uploadPartAsync({ Bucket: azureContainerName,
                    Key: this.currentTest.key, PartNumber: 1,
                    UploadId: this.currentTest.uploadId, Body: bodyFirstPart });
            }).then(res => {
                this.currentTest.firstEtag = res.ETag;
            }).then(() => s3.uploadPartAsync({ Bucket: azureContainerName,
                Key: this.currentTest.key, PartNumber: 2,
                UploadId: this.currentTest.uploadId, Body: bodySecondPart })
            ).then(res => {
                this.currentTest.secondEtag = res.ETag;
            })
            .catch(err => {
                process.stdout.write(`Error in beforeEach: ${err}\n`);
                throw err;
            });
        });

        afterEach(function afterEachFn() {
            process.stdout.write('Emptying bucket');
            return s3.abortMultipartUploadAsync({
                Bucket: azureContainerName, Key: this.currentTest.key,
                UploadId: this.currentTest.uploadId,
            })
            .then(() => bucketUtil.empty(azureContainerName))
            .then(() => {
                process.stdout.write('Deleting bucket');
                return bucketUtil.deleteOne(azureContainerName);
            })
            .catch(err => {
                process.stdout.write('Error in afterEach');
                throw err;
            });
        });

        it('should list both parts', function itFn(done) {
            s3.listParts({
                Bucket: azureContainerName,
                Key: this.test.key,
                UploadId: this.test.uploadId },
            (err, data) => {
                assert.equal(err, null, `Err listing parts: ${err}`);
                assert.strictEqual(data.Parts.length, 2);
                assert.strictEqual(data.Parts[0].PartNumber, 1);
                assert.strictEqual(data.Parts[0].Size, 10);
                assert.strictEqual(data.Parts[0].ETag, this.test.firstEtag);
                assert.strictEqual(data.Parts[1].PartNumber, 2);
                assert.strictEqual(data.Parts[1].Size, 104857610);
                assert.strictEqual(data.Parts[1].ETag, this.test.secondEtag);
                done();
            });
        });

        it('should only list the second part', function itFn(done) {
            s3.listParts({
                Bucket: azureContainerName,
                Key: this.test.key,
                PartNumberMarker: 1,
                UploadId: this.test.uploadId },
            (err, data) => {
                assert.equal(err, null, `Err listing parts: ${err}`);
                assert.strictEqual(data.Parts[0].PartNumber, 2);
                assert.strictEqual(data.Parts[0].Size, 104857610);
                assert.strictEqual(data.Parts[0].ETag, this.test.secondEtag);
                done();
            });
        });
    });
});
