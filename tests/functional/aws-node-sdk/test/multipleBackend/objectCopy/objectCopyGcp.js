const assert = require('assert');
const async = require('async');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const constants = require('../../../../../../constants');
const { describeSkipIfNotMultiple, memLocation, awsLocation,
    gcpClient, gcpBucket, gcpLocation, gcpLocationMismatch } =
    require('../utils');
const { createEncryptedBucketPromise } =
    require('../../../lib/utility/createEncryptedBucket');

const bucket = 'buckettestmultiplebackendobjectcopy';
const bucketGcp = 'buckettestmultiplebackendobjectcopy-gcp';
const body = Buffer.from('I am a body', 'utf8');
const bigBody = new Buffer(5 * 1024 * 1024);
const normalMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const bigMD5 = '5f363e0e58a95f06cbe9bbc662c5dfb6';
const emptyMD5 = 'd41d8cd98f00b204e9800998ecf8427e';
const locMetaHeader = constants.objectLocationConstraintHeader.substring(11);

let bucketUtil;
let s3;

function putSourceObj(key, location, objSize, bucket, cb) {
    const sourceParams = { Bucket: bucket, Key: key,
        Metadata: {
            'test-header': 'copyme',
        },
    };
    if (location) {
        sourceParams.Metadata['scal-location-constraint'] = location;
    }
    if (objSize && objSize.big) {
        sourceParams.Body = bigBody;
    } else if (!objSize) {
        sourceParams.Body = body;
    }
    s3.putObject(sourceParams, (err, result) => {
        assert.equal(err, null, `Error putting source object: ${err}`);
        if (objSize && objSize.empty) {
            assert.strictEqual(result.ETag, `"${emptyMD5}"`);
        } else if (objSize && objSize.big) {
            assert.strictEqual(result.ETag, `"${bigMD5}"`);
        } else {
            assert.strictEqual(result.ETag, `"${normalMD5}"`);
        }
        cb();
    });
}

function assertGetObjects(params, mdDirective, objSize, callback) {
    const { sourceKey, sourceBucket, sourceLoc, destKey, destBucket, destLoc,
        gcpKey } = params;
    const sourceGetParams = { Bucket: sourceBucket, Key: sourceKey };
    const destGetParams = { Bucket: destBucket, Key: destKey };
    async.series([
        cb => s3.getObject(sourceGetParams, cb),
        cb => s3.getObject(destGetParams, cb),
        cb => gcpClient.getObject({ Bucket: gcpBucket, Key: gcpKey }, cb),
    ], (err, results) => {
        assert.equal(err, null, `Error in assertGetObjects: ${err}`);
        const [sourceRes, destRes, gcpRes] = results;
        const gcpMD5 = gcpRes.ETag;
        if (objSize && objSize.empty) {
            assert.strictEqual(sourceRes.ETag, `"${emptyMD5}"`);
            assert.strictEqual(destRes.ETag, `"${emptyMD5}"`);
            assert.strictEqual(gcpMD5, `"${emptyMD5}"`);
            assert.strictEqual(gcpRes.ContentLength, '0');
        } else if (objSize && objSize.big) {
            assert.strictEqual(sourceRes.ETag, `"${bigMD5}"`);
            assert.strictEqual(destRes.ETag, `"${bigMD5}"`);
            if (process.env.ENABLE_KMS_ENCRYPTION === 'true') {
                assert.strictEqual(sourceRes.ServerSideEncryption, 'AES256');
                assert.strictEqual(destRes.ServerSideEncryption, 'AES256');
            } else {
                assert.strictEqual(gcpMD5, `"${bigMD5}"`);
            }
        } else {
            if (process.env.ENABLE_KMS_ENCRYPTION === 'true') {
                assert.strictEqual(sourceRes.ServerSideEncryption, 'AES256');
                assert.strictEqual(destRes.ServerSideEncryption, 'AES256');
            } else {
                assert.strictEqual(sourceRes.ETag, `"${normalMD5}"`);
                assert.strictEqual(destRes.ETag, `"${normalMD5}"`);
                assert.strictEqual(gcpMD5, `"${normalMD5}"`);
            }
        }
        if (mdDirective === 'COPY') {
            assert.strictEqual(sourceRes.Metadata['test-header'],
                destRes.Metadata['test-header']);
            assert.strictEqual(gcpRes.Metadata['test-header'],
                destRes.Metadata['test-header']);
        }
        assert.strictEqual(sourceRes.ContentLength, destRes.ContentLength);
        assert.strictEqual(sourceRes.Metadata[locMetaHeader], sourceLoc);
        assert.strictEqual(destRes.Metadata[locMetaHeader], destLoc);
        callback();
    });
}

describeSkipIfNotMultiple('MultipleBackend object copy: GCP',
function describeFn() {
    this.timeout(250000);
    withV4(sigCfg => {
        beforeEach(function beFn() {
            this.currentTest.key = `gcpputkey-${Date.now()}`;
            this.currentTest.copyKey = `gcpcopykey-${Date.now()}`;
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            process.stdout.write('Creating bucket\n');
            if (process.env.ENABLE_KMS_ENCRYPTION === 'true') {
                s3.createBucketAsync = createEncryptedBucketPromise;
            }
            return s3.createBucketAsync({ Bucket: bucket,
                CreateBucketConfiguration: {
                    LocationConstraint: memLocation,
                },
            })
            .then(() => s3.createBucketAsync({ Bucket: bucketGcp,
              CreateBucketConfiguration: {
                  LocationConstraint: gcpLocation,
              },
            }))
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(bucket)
            .then(() => bucketUtil.empty(bucketGcp))
            .then(() => {
                process.stdout.write(`Deleting bucket: ${bucket}\n`);
                return bucketUtil.deleteOne(bucket);
            })
            .then(() => {
                process.stdout.write(`Deleting bucket: ${bucketGcp}\n`);
                return bucketUtil.deleteOne(bucketGcp);
            })
            .catch(err => {
                process.stdout.write(`Error in afterEach: ${err}\n`);
                throw err;
            });
        });

        const copyTests = [
            {
                msg: 'should copy an object from mem to GCP',
                params: { mdDirective: 'REPLACE', objSize: null,
                    MD5: normalMD5, checkCopy: true },
                input: { sourceBucket: bucket, sourceLoc: memLocation,
                    destBucket: bucket, destLoc: gcpLocation },
                output: { sourceBucket: bucket, sourceLoc: memLocation,
                    destBucket: bucket, destLoc: gcpLocation },
            },
            {
                msg: 'should copy an object with no location constraint from ' +
                    'mem to GCP',
                params: { mdDirective: 'COPY', objSize: null,
                    MD5: normalMD5, checkCopy: true },
                input: { sourceBucket: bucket, sourceLoc: undefined,
                    destBucket: bucketGcp, destLoc: undefined },
                output: { sourceBucket: bucket, sourceLoc: undefined,
                    destBucket: bucketGcp, destLoc: undefined },
            },
            {
                msg: 'should copy an object from GCP to mem',
                params: { mdDirective: 'REPLACE', objSize: null,
                    MD5: normalMD5, checkCopy: false },
                input: { sourceBucket: bucket, sourceLoc: gcpLocation,
                    destBucket: bucket, destLoc: memLocation },
                output: { sourceBucket: bucket, sourceLoc: gcpLocation,
                    destBucket: bucket, destLoc: memLocation },
            },
            {
                msg: 'should copy an object from AWS to GCP',
                params: { mdDirective: 'REPLACE', objSize: null,
                    MD5: normalMD5, checkCopy: true },
                input: { sourceBucket: bucket, sourceLoc: awsLocation,
                    destBucket: bucket, destLoc: gcpLocation },
                output: { sourceBucket: bucket, sourceLoc: awsLocation,
                    destBucket: bucket, destLoc: gcpLocation },
            },
            {
                msg: 'should copy an object from GCP to AWS',
                params: { mdDirective: 'REPLACE', objSize: null,
                    MD5: normalMD5, checkCopy: false },
                input: { sourceBucket: bucket, sourceLoc: gcpLocation,
                    destBucket: bucket, destLoc: awsLocation },
                output: { sourceBucket: bucket, sourceLoc: gcpLocation,
                    destBucket: bucket, destLoc: awsLocation },
            },
            {
                msg: 'should copy an object from GCP to AWS',
                params: { mdDirective: 'REPLACE', objSize: null,
                    MD5: normalMD5, checkCopy: false },
                input: { sourceBucket: bucket, sourceLoc: gcpLocation,
                    destBucket: bucket, destLoc: awsLocation },
                output: { sourceBucket: bucket, sourceLoc: gcpLocation,
                    destBucket: bucket, destLoc: awsLocation },
            },
            {
                msg: 'should copy an object from GCP to mem with "REPLACE" ' +
                    'directive and no location constraint md',
                params: { mdDirective: 'REPLACE', objSize: null,
                    MD5: normalMD5, checkCopy: false },
                input: { sourceBucket: bucket, sourceLoc: gcpLocation,
                    destBucket: bucket, destLoc: undefined },
                output: { sourceBucket: bucket, sourceLoc: gcpLocation,
                    destBucket: bucket, destLoc: undefined },
            },
            {
                msg: 'should copy an object from mem to GCP with "REPLACE" ' +
                    'directive and no location constraint md',
                params: { mdDirective: 'REPLACE', objSize: null,
                    MD5: normalMD5, checkCopy: true },
                input: { sourceBucket: bucket, sourceLoc: undefined,
                    destBucket: bucketGcp, destLoc: undefined },
                output: { sourceBucket: bucket, sourceLoc: undefined,
                    destBucket: bucketGcp, destLoc: undefined },
            },
            {
                msg: 'should copy an object from GCP to GCP showing sending ' +
                    'metadata location constraint is ignored in COPY directive',
                params: { mdDirective: 'COPY', objSize: null,
                    MD5: normalMD5, checkCopy: true },
                input: { sourceBucket: bucketGcp, sourceLoc: gcpLocation,
                    destBucket: bucketGcp, destLoc: memLocation },
                output: { sourceBucket: bucketGcp, sourceLoc: gcpLocation,
                    destBucket: bucketGcp, destLoc: gcpLocation },
            },
            {
                msg: 'should copy an object with no location constraint from ' +
                    'GCP to GCP relying on the bucket location constraint',
                params: { mdDirective: 'COPY', objSize: null,
                    MD5: normalMD5, checkCopy: true },
                input: { sourceBucket: bucketGcp, sourceLoc: undefined,
                    destBucket: bucketGcp, destLoc: undefined },
                output: { sourceBucket: bucketGcp, sourceLoc: undefined,
                    destBucket: bucketGcp, destLoc: undefined },
            },
            {
                msg: 'should copy an object from GCP to meme because bucket ' +
                    'destination location is mem',
                params: { mdDirective: 'COPY', objSize: null,
                    MD5: normalMD5, checkCopy: false },
                input: { sourceBucket: bucket, sourceLoc: gcpLocation,
                    destBucket: bucket, destLoc: undefined },
                output: { sourceBucket: bucket, sourceLoc: gcpLocation,
                    destBucket: bucket, destLoc: memLocation },
            },
            {
                msg: 'should copy an object from bucketmatch=false ' +
                    'GCP location to MPU with a bucketmatch=false GCP location',
                params: { mdDirective: 'REPLACE', objSize: null,
                    MD5: normalMD5, checkCopy: true, destMismatch: true },
                input: { sourceBucket: bucket, sourceLoc: gcpLocationMismatch,
                    destBucket: bucket, destLoc: gcpLocationMismatch },
                output: { sourceBucket: bucket, sourceLoc: gcpLocationMismatch,
                    destBucket: bucket, destLoc: gcpLocationMismatch },
            },
            {
                msg: 'should copy an object from bucketmatch=false ' +
                    'GCP location to MPU with a bucketmatch=true GCP location',
                params: { mdDirective: 'REPLACE', objSize: null,
                    MD5: normalMD5, checkCopy: true, destMismatch: false },
                input: { sourceBucket: bucket, sourceLoc: gcpLocationMismatch,
                    destBucket: bucket, destLoc: gcpLocation },
                output: { sourceBucket: bucket, sourceLoc: gcpLocationMismatch,
                    destBucket: bucket, destLoc: gcpLocation },
            },
            {
                msg: 'should copy an object from bucketmatch=true ' +
                    'GCP location to MPU with a bucketmatch=false GCP location',
                params: { mdDirective: 'REPLACE', objSize: null,
                    MD5: normalMD5, checkCopy: true, destMismatch: true },
                input: { sourceBucket: bucket, sourceLoc: gcpLocation,
                    destBucket: bucket, destLoc: gcpLocationMismatch },
                output: { sourceBucket: bucket, sourceLoc: gcpLocation,
                    destBucket: bucket, destLoc: gcpLocationMismatch },
            },
            {
                msg: 'should copy a 0-byte object from mem to GCP',
                params: { mdDirective: 'REPLACE', objSize: { empty: true },
                    MD5: emptyMD5, checkCopy: true },
                input: { sourceBucket: bucket, sourceLoc: memLocation,
                    destBucket: bucket, destLoc: gcpLocation },
                output: { sourceBucket: bucket, sourceLoc: memLocation,
                    destBucket: bucket, destLoc: gcpLocation },
            },
            {
                msg: 'should copy a 0-byte object from on GCP',
                params: { mdDirective: 'REPLACE', objSize: { empty: true },
                    MD5: emptyMD5, checkCopy: true },
                input: { sourceBucket: bucket, sourceLoc: gcpLocation,
                    destBucket: bucket, destLoc: gcpLocation },
                output: { sourceBucket: bucket, sourceLoc: gcpLocation,
                    destBucket: bucket, destLoc: gcpLocation },
            },
            {
                msg: 'should copy a 5MB object from mem to GCP',
                params: { mdDirective: 'REPLACE', objSize: { big: true },
                    MD5: bigMD5, checkCopy: true },
                input: { sourceBucket: bucket, sourceLoc: memLocation,
                    destBucket: bucket, destLoc: gcpLocation },
                output: { sourceBucket: bucket, sourceLoc: memLocation,
                    destBucket: bucket, destLoc: gcpLocation },
            },
            {
                msg: 'should copy a 5MB object from on GCP',
                params: { mdDirective: 'REPLACE', objSize: { big: true },
                    MD5: bigMD5, checkCopy: true },
                input: { sourceBucket: bucket, sourceLoc: gcpLocation,
                    destBucket: bucket, destLoc: gcpLocation },
                output: { sourceBucket: bucket, sourceLoc: gcpLocation,
                    destBucket: bucket, destLoc: gcpLocation },
            },
        ];
        copyTests.forEach(test => it(test.msg, function itFn(done) {
            const { mdDirective, objSize, MD5, checkCopy,
                destMismatch } = test.params;
            const { sourceLoc, sourceBucket, destLoc, destBucket } = test.input;
            const testParams = Object.assign({}, test.output);
            testParams.sourceKey = this.test.key;
            testParams.destKey = this.test.copyKey;
            testParams.gcpKey = checkCopy ? this.test.copyKey : this.test.key;
            if (destMismatch) {
                testParams.gcpKey = `${destBucket}/${testParams.gcpKey}`;
            }
            putSourceObj(this.test.key, sourceLoc, objSize, sourceBucket,
            () => {
                const copyParams = {
                    Bucket: destBucket,
                    Key: this.test.copyKey,
                    CopySource: `/${sourceBucket}/${this.test.key}`,
                    MetadataDirective: mdDirective,
                };
                if (destLoc) {
                    copyParams.Metadata = {
                        'scal-location-constraint': destLoc };
                }
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${MD5}"`);
                    assertGetObjects(testParams, mdDirective, objSize, done);
                });
            });
        }));

        it('should return error if GCP source object has ' +
        'been deleted', function itFn(done) {
            putSourceObj(this.test.key, gcpLocation, null, bucket,
            () => {
                gcpClient.headObject({
                    Bucket: gcpBucket,
                    Key: this.test.key,
                }, (err, res) => {
                    assert.equal(err, null, 'Error retrieving object version ' +
                        `from GCP: ${err}`);
                    gcpClient.deleteObject({
                        Bucket: gcpBucket,
                        Key: this.test.key,
                        VersionId: res.VersionId },
                    err => {
                        assert.equal(err, null, 'Error deleting object from ' +
                            `GCP: ${err}`);
                        const copyParams = {
                            Bucket: bucket,
                            Key: this.test.copyKey,
                            CopySource: `/${bucket}/${this.test.key}`,
                            MetadataDirective: 'COPY',
                        };
                        s3.copyObject(copyParams, err => {
                            assert.strictEqual(err.code, 'ServiceUnavailable');
                            done();
                        });
                    });
                });
            });
        });
    });
});
