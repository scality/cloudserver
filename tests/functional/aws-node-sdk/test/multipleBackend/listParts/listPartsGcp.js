const assert = require('assert');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { describeSkipIfNotMultipleOrCeph, gcpLocation, genUniqID }
    = require('../utils');

const bucket = `listpartsgcp${genUniqID()}`;
const firstPartSize = 10;
const bodyFirstPart = Buffer.alloc(firstPartSize);
const secondPartSize = 15;
const bodySecondPart = Buffer.alloc(secondPartSize);

let bucketUtil;
let s3;

describeSkipIfNotMultipleOrCeph('List parts of MPU on GCP data backend', () => {
    withV4(sigCfg => {
        beforeEach(() => {
            this.currentTest.key = `somekey-${genUniqID()}`;
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketAsync({ Bucket: bucket })
            .then(() => s3.createMultipartUploadAsync({
                Bucket: bucket, Key: this.currentTest.key,
                Metadata: { 'scal-location-constraint': gcpLocation } }))
            .then(res => {
                this.currentTest.uploadId = res.UploadId;
                return s3.uploadPartAsync({ Bucket: bucket,
                    Key: this.currentTest.key, PartNumber: 1,
                    UploadId: this.currentTest.uploadId, Body: bodyFirstPart });
            }).then(res => {
                this.currentTest.firstEtag = res.ETag;
            }).then(() => s3.uploadPartAsync({ Bucket: bucket,
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

        afterEach(() => {
            process.stdout.write('Emptying bucket');
            return s3.abortMultipartUploadAsync({
                Bucket: bucket, Key: this.currentTest.key,
                UploadId: this.currentTest.uploadId,
            })
            .then(() => bucketUtil.empty(bucket))
            .then(() => {
                process.stdout.write('Deleting bucket');
                return bucketUtil.deleteOne(bucket);
            })
            .catch(err => {
                process.stdout.write('Error in afterEach');
                throw err;
            });
        });

        test('should list both parts', done => {
            s3.listParts({
                Bucket: bucket,
                Key: this.test.key,
                UploadId: this.test.uploadId },
            (err, data) => {
                expect(err).toEqual(null);
                expect(data.Parts.length).toBe(2);
                expect(data.Parts[0].PartNumber).toBe(1);
                expect(data.Parts[0].Size).toBe(firstPartSize);
                expect(data.Parts[0].ETag).toBe(this.test.firstEtag);
                expect(data.Parts[1].PartNumber).toBe(2);
                expect(data.Parts[1].Size).toBe(secondPartSize);
                expect(data.Parts[1].ETag).toBe(this.test.secondEtag);
                done();
            });
        });

        test('should only list the second part', done => {
            s3.listParts({
                Bucket: bucket,
                Key: this.test.key,
                PartNumberMarker: 1,
                UploadId: this.test.uploadId },
            (err, data) => {
                expect(err).toEqual(null);
                expect(data.Parts[0].PartNumber).toBe(2);
                expect(data.Parts[0].Size).toBe(secondPartSize);
                expect(data.Parts[0].ETag).toBe(this.test.secondEtag);
                done();
            });
        });
    });
});
