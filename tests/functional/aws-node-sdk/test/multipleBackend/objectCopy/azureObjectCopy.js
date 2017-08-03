const assert = require('assert');
const async = require('async');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const constants = require('../../../../../../constants');
const { config } = require('../../../../../../lib/Config');
const { getAzureClient, getAzureContainerName, convertMD5 } =
    require('../utils');
const { createEncryptedBucketPromise } =
    require('../../../lib/utility/createEncryptedBucket');

const azureLocation = 'azuretest';
const azureClient = getAzureClient();
const azureContainerName = getAzureContainerName();
const awsLocation = 'aws-test';

const bucket = 'buckettestmultiplebackendobjectcopy';
const body = Buffer.from('I am a body', 'utf8');
const bigBody = new Buffer(5000000);
const normalMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const bigMD5 = '';
const emptyMD5 = 'd41d8cd98f00b204e9800998ecf8427e';
const locMetaHeader = constants.objectLocationConstraintHeader.substring(11);

let bucketUtil;
let s3;
const describeSkipIfNotMultiple = (config.backends.data !== 'multiple'
    || process.env.S3_END_TO_END) ? describe.skip : describe;

function putSourceObj(key, location, objSize, cb) {
    const sourceParams = { Bucket: bucket, Key: key,
        Metadata: {
            'scal-location-constraint': location,
            'test-header': 'copyme',
        },
    };
    if (objSize && objSize.big) {
        sourceParams.Body = bigBody;
    } else {
        if (!objSize.empty) {
            sourceParams.Body = body;
        }
    }
    s3.putObject(sourceParams, (err, result) => {
        assert.equal(err, null, `Error putting source object: ${err}`);
        if (objSize.empty) {
            assert.strictEqual(result.ETag, `"${emptyMD5}"`);
        } else if (objSize.big) {
            assert.strictEqual(result.ETag, `"${bigMD5}"`);
        } else {
            assert.strictEqual(result.ETag, `"${normalMD5}"`);
        }
        cb();
    });
}

function assertGetObjects(sourceKey, sourceBucket, sourceLoc, destKey,
destBucket, destLoc, azureKey, mdDirective, objSize, callback) {
    const sourceGetParams = { Bucket: sourceBucket, Key: sourceKey };
    const destGetParams = { Bucket: destBucket, Key: destKey };

    async.series([
        cb => s3.getObject(sourceGetParams, cb),
        cb => s3.getObject(destGetParams, cb),
        cb => azureClient.getBlobProperties(azureContainerName, azureKey, cb),
    ], (err, results) => {
        assert.equal(err, null, `Error in assertGetObjects: ${err}`);

        const [sourceRes, destRes, azureRes] = results;
        const convertedMD5 = convertMD5(azureRes[0].contentSettings.contentMD5);
        if (objSize && objSize.empty) {
            assert.strictEqual(sourceRes.ETag, `"${emptyMD5}"`);
            assert.strictEqual(destRes.ETag, `"${emptyMD5}"`);
            assert.strictEqual(convertedMD5, `${emptyMD5}`);
            assert.strictEqual('0', azureRes[0].contentLength);
        } else if (objSize && objSize.big) {
            assert.strictEqual(sourceRes.ETag, `"${bigMD5}"`);
            assert.strictEqual(destRes.ETag, `"${bigMD5}"`);
            assert.strictEqual(convertedMD5, `${bigMD5}`);
        } else {
            if (process.env.ENABLE_KMS_ENCRYPTION === 'true') {
                assert.strictEqual(sourceRes.ServerSideEncryption, 'AES256');
                assert.strictEqual(destRes.ServerSideEncryption, 'AES256');
            } else {
                assert.strictEqual(sourceRes.ETag, `"${normalMD5}"`);
                assert.strictEqual(destRes.ETag, `"${normalMD5}"`);
                assert.strictEqual(convertedMD5, `${normalMD5}`);
            }
        }
        if (mdDirective === 'COPY') {
            assert.strictEqual(sourceRes.Metadata['test-header'],
                destRes.Metadata['test-header']);
        }
        assert.strictEqual(sourceRes.ContentLength, destRes.ContentLength);
        assert.strictEqual(sourceRes.Metadata[locMetaHeader], sourceLoc);
        assert.strictEqual(destRes.Metadata[locMetaHeader], destLoc);
        callback();
    });
}

describeSkipIfNotMultiple('MultipleBackend object copy', function testSuite() {
    this.timeout(60000);
    withV4(sigCfg => {
        beforeEach(function beFn() {
            this.currentTest.key = `azureputkey-${Date.now()}`;
            this.currentTest.copyKey = `azurecopyKey-${Date.now()}`;
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            process.stdout.write('Creating bucket\n');
            if (process.env.ENABLE_KMS_ENCRYPTION === 'true') {
                s3.createBucketAsync = createEncryptedBucketPromise;
            }
            return s3.createBucketAsync({ Bucket: bucket })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
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

        it('should copy an object from mem to Azure', function itFn(done) {
            putSourceObj(this.test.key, 'mem', null, () => {
                const copyParams = {
                    Bucket: bucket,
                    Key: this.test.copyKey,
                    CopySource: `/${bucket}/${this.test.key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: { 'scal-location-constraint': azureLocation },
                };
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${normalMD5}"`);
                    assertGetObjects(this.test.key, bucket, 'mem',
                        this.test.copyKey, bucket, azureLocation,
                        this.test.copyKey, 'REPLACE', null, done);
                });
            });
        });

        it('should copy an object from Azure to mem', function itFn(done) {
            putSourceObj(this.test.key, azureLocation, null, () => {
                const copyParams = {
                    Bucket: bucket,
                    Key: this.test.copyKey,
                    CopySource: `/${bucket}/${this.test.key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: { 'scal-location-constraint': 'mem' },
                };
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${normalMD5}"`);
                    assertGetObjects(this.test.key, bucket, azureLocation,
                        this.test.copyKey, bucket, 'mem', this.test.key,
                        'REPLACE', null, done);
                });
            });
        });

        it('should copy an object from AWS to Azure', function itFn(done) {
            putSourceObj(this.test.key, awsLocation, null, () => {
                const copyParams = {
                    Bucket: bucket,
                    Key: this.test.copyKey,
                    CopySource: `/${bucket}/${this.test.key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: { 'scal-location-constraint': azureLocation },
                };
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${normalMD5}"`);
                    assertGetObjects(this.test.key, bucket, awsLocation,
                        this.test.copyKey, bucket, azureLocation,
                        this.test.copyKey, 'REPLACE', null, done);
                });
            });
        });

        it('should copy an object from Azure to AWS', function itFn(done) {
            putSourceObj(this.test.key, azureLocation, null, () => {
                const copyParams = {
                    Bucket: bucket,
                    Key: this.test.copyKey,
                    CopySource: `/${bucket}/${this.test.key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: { 'scal-location-constraint': awsLocation },
                };
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${normalMD5}"`);
                    assertGetObjects(this.test.key, bucket, azureLocation,
                        this.test.copyKey, bucket, awsLocation, this.test.key,
                        'REPLACE', null, done);
                });
            });
        });

        it('should copy an object from mem to Azure and retain metadata',
        function itFn(done) {
            putSourceObj(this.test.key, 'mem', null, () => {
                const copyParams = {
                    Bucket: bucket,
                    Key: this.test.copyKey,
                    CopySource: `/${bucket}/${this.test.key}`,
                    MetadataDirective: 'COPY',
                    Metadata: { 'scal-location-constraint': azureLocation },
                };
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${normalMD5}"`);
                    assertGetObjects(this.test.key, bucket, 'mem',
                        this.test.copyKey, bucket, azureLocation,
                        this.test.copyKey, 'COPY', null, done);
                });
            });
        });

        it('should copy an object on Azure', function itFn(done) {
            putSourceObj(this.test.key, azureLocation, null, () => {
                const copyParams = {
                    Bucket: bucket,
                    Key: this.test.copyKey,
                    CopySource: `/${bucket}/${this.test.key}`,
                    MetadataDirective: 'COPY',
                };
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${normalMD5}"`);
                    assertGetObjects(this.test.key, bucket, azureLocation,
                        this.test.copyKey, bucket, azureLocation,
                        this.test.copyKey, 'COPY', null, done);
                });
            });
        });

        it('should copy a 0-byte object from mem to Azure',
        function itFn(done) {
            putSourceObj(this.test.key, 'mem', { empty: true }, () => {
                const copyParams = {
                    Bucket: bucket,
                    Key: this.test.copyKey,
                    CopySource: `/${bucket}/${this.test.key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: { 'scal-location-constraint': azureLocation },
                };
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${emptyMD5}"`);
                    assertGetObjects(this.test.key, bucket, 'mem',
                        this.test.copyKey, bucket, azureLocation,
                        this.test.copyKey, 'REPLACE', { empty: true }, done);
                });
            });
        });

        it('should copy a 0-byte object on Azure', function itFn(done) {
            putSourceObj(this.test.key, azureLocation, { empty: true }, () => {
                const copyParams = {
                    Bucket: bucket,
                    Key: this.test.copyKey,
                    CopySource: `/${bucket}/${this.test.key}`,
                    MetadataDirective: 'COPY',
                };
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${emptyMD5}"`);
                    assertGetObjects(this.test.key, bucket, azureLocation,
                        this.test.copyKey, bucket, azureLocation,
                        this.test.copyKey, 'COPY', { empty: true }, done);
                });
            });
        });

        it('should copy a 5MB object from mem to Azure', function itFn(done) {
            putSourceObj(this.test.key, 'mem', { big: true }, () => {
                const copyParams = {
                    Bucket: bucket,
                    Key: this.test.copyKey,
                    CopySource: `/${bucket}/${this.test.key}`,
                    MetadataDirective: 'COPY',
                };
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, `Err copying object: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${bigMD5}"`);
                    assertGetObjects(this.test.key, bucket, 'mem',
                        this.test.copyKey, bucket, azureLocation,
                        this.test.copyKey, 'COPY', { big: true }, done);
                });
            });
        });

        it('should copy a 5MB object on Azure', function itFn(done) {
            putSourceObj(this.test.key, azureLocation, { big: true }, () => {
                const copyParams = {
                    Bucket: bucket,
                    Key: this.test.copyKey,
                    CopySource: `/${bucket}/${this.test.key}`,
                    MetadataDirective: 'COPY',
                };
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, `Err copying object: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${bigMD5}"`);
                    assertGetObjects(this.test.key, bucket, azureLocation,
                        this.test.copyKey, bucket, azureLocation,
                        this.test.copyKey, 'COPY', { big: true }, done);
                });
            });
        });

        it('should return error if Azure source object has ' +
        'been deleted', function itFn(done) {
            putSourceObj(this.test.key, azureLocation, null, () => {
                azureClient.deleteBlob(azureContainerName, this.test.key,
                err => {
                    assert.equal(err, null, 'Error deleting object from ' +
                        `Azure: ${err}`);
                    const copyParams = {
                        Bucket: bucket,
                        Key: this.test.copyKey,
                        CopySource: `/${bucket}/${this.test.key}`,
                        MetadataDirective: 'COPY',
                    };
                    s3.copyObject(copyParams, err => {
                        assert.strictEqual(err.code, 'InternalError');
                        done();
                    });
                });
            });
        });
    });
});
