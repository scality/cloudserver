const assert = require('assert');
const async = require('async');
const AWS = require('aws-sdk');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const constants = require('../../../../../../constants');
const { config } = require('../../../../../../lib/Config');
const { getRealAwsConfig } = require('../../support/awsConfig');
const { createEncryptedBucketPromise } =
    require('../../../lib/utility/createEncryptedBucket');

const awsLocation = 'aws-test';
const awsLocation2 = 'aws-test-2';
const awsLocationMismatch = 'aws-test-mismatch';
const awsLocationEncryption = 'aws-test-encryption';
const bucket = 'buckettestmultiplebackendobjectcopy';
const bucketEncrypted = 'bucketenryptedtestmultiplebackendobjectcopy';
const body = Buffer.from('I am a body', 'utf8');
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const emptyMD5 = 'd41d8cd98f00b204e9800998ecf8427e';
const locMetaHeader = constants.objectLocationConstraintHeader.substring(11);
const { versioningEnabled } = require('../../../lib/utility/versioning-util');

let bucketUtil;
let s3;
let awsS3;
const describeSkipIfNotMultiple = (config.backends.data !== 'multiple'
    || process.env.S3_END_TO_END) ? describe.skip : describe;

function putSourceObj(location, isEmptyObj, bucket, cb) {
    const key = `somekey-${Date.now()}`;
    const sourceParams = { Bucket: bucket, Key: key,
        Metadata: {
            'scal-location-constraint': location,
            'test-header': 'copyme',
        },
    };
    if (!isEmptyObj) {
        sourceParams.Body = body;
    }
    process.stdout.write('Putting source object\n');
    console.log('sourceParams!!!', sourceParams);
    s3.putObject(sourceParams, (err, result) => {
        assert.equal(err, null, `Error putting source object: ${err}`);
        if (isEmptyObj) {
            assert.strictEqual(result.ETag, `"${emptyMD5}"`);
        } else {
            assert.strictEqual(result.ETag, `"${correctMD5}"`);
        }
        cb(key);
    });
}

function assertGetObjects(sourceKey, sourceBucket, sourceLoc, destKey,
destBucket, destLoc, awsKey, mdDirective, isEmptyObj, awsS3, awsLocation,
callback) {
    const awsBucket =
        config.locationConstraints[awsLocation].details.bucketName;
    const sourceGetParams = { Bucket: sourceBucket, Key: sourceKey };
    const destGetParams = { Bucket: destBucket, Key: destKey };
    const awsParams = { Bucket: awsBucket, Key: awsKey };
    async.series([
        cb => s3.getObject(sourceGetParams, cb),
        cb => s3.getObject(destGetParams, cb),
        cb => awsS3.getObject(awsParams, cb),
    ], (err, results) => {
        assert.equal(err, null, `Error in assertGetObjects: ${err}`);
        const [sourceRes, destRes, awsRes] = results;
        if (isEmptyObj) {
            assert.strictEqual(sourceRes.ETag, `"${emptyMD5}"`);
            assert.strictEqual(destRes.ETag, `"${emptyMD5}"`);
            assert.strictEqual(awsRes.ETag, `"${emptyMD5}"`);
        } else if (process.env.ENABLE_KMS_ENCRYPTION === 'true') {
            assert.strictEqual(sourceRes.ServerSideEncryption, 'AES256');
            assert.strictEqual(destRes.ServerSideEncryption, 'AES256');
        } else {
            assert.strictEqual(sourceRes.ETag, `"${correctMD5}"`);
            assert.strictEqual(destRes.ETag, `"${correctMD5}"`);
            assert.deepStrictEqual(sourceRes.Body, destRes.Body);
            assert.strictEqual(awsRes.ETag, `"${correctMD5}"`);
            assert.deepStrictEqual(sourceRes.Body, awsRes.Body);
        }
        if (destLoc === awsLocationEncryption) {
            assert.strictEqual(awsRes.ServerSideEncryption, 'AES256');
        } else {
            assert.strictEqual(awsRes.ServerSideEncryption, undefined);
        }
        if (mdDirective === 'COPY') {
            assert.deepStrictEqual(sourceRes.Metadata['test-header'],
                destRes.Metadata['test-header']);
        } else if (mdDirective === 'REPLACE') {
            assert.strictEqual(destRes.Metadata['test-header'],
              undefined);
        }
        if (destLoc === awsLocation) {
            assert.strictEqual(awsRes.Metadata[locMetaHeader], destLoc);
            if (mdDirective === 'COPY') {
                assert.deepStrictEqual(sourceRes.Metadata['test-header'],
                    awsRes.Metadata['test-header']);
            } else if (mdDirective === 'REPLACE') {
                assert.strictEqual(awsRes.Metadata['test-header'],
                  undefined);
            }
        }
        assert.strictEqual(sourceRes.ContentLength, destRes.ContentLength);
        assert.strictEqual(sourceRes.Metadata[locMetaHeader], sourceLoc);
        assert.strictEqual(destRes.Metadata[locMetaHeader], destLoc);
        callback();
    });
}

describeSkipIfNotMultiple('MultipleBackend object copy',
function testSuite() {
    this.timeout(250000);
    withV4(sigCfg => {
        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            const awsConfig = getRealAwsConfig(awsLocation);
            awsS3 = new AWS.S3(awsConfig);
            process.stdout.write('Creating bucket\n');
            if (process.env.ENABLE_KMS_ENCRYPTION === 'true') {
                s3.createBucketAsync = createEncryptedBucketPromise;
            }
            return s3.createBucketAsync({ Bucket: bucket })
            .then(() => s3.createBucketAsync({ Bucket: bucketEncrypted,
              CreateBucketConfiguration: {
                  LocationConstraint: awsLocationEncryption,
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
            .then(() => bucketUtil.empty(bucketEncrypted))
            .then(() => {
                process.stdout.write(`Deleting bucket ${bucket}\n`);
                return bucketUtil.deleteOne(bucket);
            })
            .then(() => {
                process.stdout.write(`Deleting bucket ${bucketEncrypted}\n`);
                return bucketUtil.deleteOne(bucketEncrypted);
            })
            .catch(err => {
                process.stdout.write(`Error in afterEach: ${err}\n`);
                throw err;
            });
        });

        it('should copy an object from mem to AWS', done => {
            putSourceObj('mem', false, bucket, key => {
                const copyKey = `copyKey-${Date.now()}`;
                const copyParams = {
                    Bucket: bucket,
                    Key: copyKey,
                    CopySource: `/${bucket}/${key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: {
                        'scal-location-constraint': awsLocation },
                };
                process.stdout.write('Copying object\n');
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${correctMD5}"`);
                    assertGetObjects(key, bucket, 'mem', copyKey, bucket,
                        awsLocation, copyKey, 'REPLACE', false, awsS3,
                        awsLocation, done);
                });
            });
        });

        it('should copy an object from mem to AWS with encryption', done => {
            putSourceObj('mem', false, bucket, key => {
                const copyKey = `copyKey-${Date.now()}`;
                const copyParams = {
                    Bucket: bucket,
                    Key: copyKey,
                    CopySource: `/${bucket}/${key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: {
                        'scal-location-constraint': awsLocationEncryption },
                };
                process.stdout.write('Copying object\n');
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${correctMD5}"`);
                    assertGetObjects(key, bucket, 'mem', copyKey, bucket,
                        awsLocationEncryption, copyKey, 'REPLACE', false,
                        awsS3, awsLocation, done);
                });
            });
        });

        it('should return NotImplemented copying an object from mem to a ' +
        'versioning enable AWS bucket', done => {
            putSourceObj('mem', false, bucket, key => {
                const copyKey = `copyKey-${Date.now()}`;
                const copyParams = {
                    Bucket: bucket,
                    Key: copyKey,
                    CopySource: `/${bucket}/${key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: {
                        'scal-location-constraint': awsLocation },
                };
                s3.putBucketVersioning({
                    Bucket: bucket,
                    VersioningConfiguration: versioningEnabled,
                }, err => {
                    assert.equal(err, null, 'putBucketVersioning: ' +
                        `Expected success, got error ${err}`);
                    process.stdout.write('Copying object\n');
                    s3.copyObject(copyParams, err => {
                        assert.strictEqual(err.code, 'NotImplemented');
                        done();
                    });
                });
            });
        });

        it('should copy an object from AWS to mem', done => {
            putSourceObj(awsLocation, false, bucket, key => {
                const copyKey = `copyKey-${Date.now()}`;
                const copyParams = {
                    Bucket: bucket,
                    Key: copyKey,
                    CopySource: `/${bucket}/${key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: {
                        'scal-location-constraint': 'mem' },
                };
                process.stdout.write('Copying object\n');
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${correctMD5}"`);
                    assertGetObjects(key, bucket, awsLocation, copyKey, bucket,
                        'mem', key, 'REPLACE', false, awsS3, awsLocation, done);
                });
            });
        });

        it('should copy an object from mem to AWS and retain metadata',
        done => {
            putSourceObj('mem', false, bucket, key => {
                const copyKey = `copyKey-${Date.now()}`;
                const copyParams = {
                    Bucket: bucket,
                    Key: copyKey,
                    CopySource: `/${bucket}/${key}`,
                    MetadataDirective: 'COPY',
                    Metadata: {
                        'scal-location-constraint': awsLocation },
                };
                process.stdout.write('Copying object\n');
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${correctMD5}"`);
                    assertGetObjects(key, bucket, 'mem', copyKey, bucket,
                        awsLocation, copyKey, 'COPY', false, awsS3,
                        awsLocation, done);
                });
            });
        });

        it('should copy an object on AWS', done => {
            putSourceObj(awsLocation, false, bucket, key => {
                const copyKey = `copyKey-${Date.now()}`;
                const copyParams = {
                    Bucket: bucket,
                    Key: copyKey,
                    CopySource: `/${bucket}/${key}`,
                    MetadataDirective: 'COPY',
                };
                process.stdout.write('Copying object\n');
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${correctMD5}"`);
                    assertGetObjects(key, bucket, awsLocation, copyKey, bucket,
                        awsLocation, copyKey, 'COPY', false, awsS3,
                        awsLocation, done);
                });
            });
        });

        it('should copy an object on AWS with encryption', done => {
            putSourceObj(awsLocation, false, bucket, key => {
                const copyKey = `copyKey-${Date.now()}`;
                const copyParams = {
                    Bucket: bucket,
                    Key: copyKey,
                    CopySource: `/${bucket}/${key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: {
                        'scal-location-constraint': awsLocationEncryption },
                };
                process.stdout.write('Copying object\n');
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${correctMD5}"`);
                    assertGetObjects(key, bucket, awsLocation, copyKey, bucket,
                        awsLocationEncryption, copyKey, 'REPLACE', false, awsS3,
                        awsLocation, done);
                });
            });
        });

        it.only('should copy an object on AWS with bucket encryption', done => {
            putSourceObj(awsLocation, false, bucketEncrypted, key => {
                const copyKey = `copyKey-${Date.now()}`;
                const copyParams = {
                    Bucket: bucketEncrypted,
                    Key: copyKey,
                    CopySource: `/${bucketEncrypted}/${key}`,
                    MetadataDirective: 'COPY',
                };
                process.stdout.write('Copying object\n');
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${correctMD5}"`);
                    assertGetObjects(key, bucketEncrypted, awsLocation, copyKey,
                        bucketEncrypted, awsLocationEncryption, copyKey, 'COPY',
                        false, awsS3, awsLocation, done);
                });
            });
        });

        it('should copy an object on AWS location with bucketMatch equals ' +
        'false to a different AWS location with bucketMatch equals true',
        done => {
            putSourceObj(awsLocationMismatch, false, bucket, key => {
                const copyKey = `copyKey-${Date.now()}`;
                const copyParams = {
                    Bucket: bucket,
                    Key: copyKey,
                    CopySource: `/${bucket}/${key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: {
                        'scal-location-constraint': awsLocation },
                };
                process.stdout.write('Copying object\n');
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${correctMD5}"`);
                    assertGetObjects(key, bucket, awsLocationMismatch, copyKey,
                        bucket, awsLocation, copyKey, 'REPLACE', false, awsS3,
                        awsLocation, done);
                });
            });
        });

        it('should copy an object on AWS to a different AWS location ' +
        'with source object READ access',
        done => {
            const awsConfig2 = getRealAwsConfig(awsLocation2);
            const awsS3Two = new AWS.S3(awsConfig2);
            const copyKey = `copyKey-${Date.now()}`;
            const awsBucket =
                config.locationConstraints[awsLocation].details.bucketName;
            async.waterfall([
                // giving access to the object on the AWS side
                next => putSourceObj(awsLocation, false, bucket, key =>
                  next(null, key)),
                (key, next) => awsS3.putObjectAcl(
                  { Bucket: awsBucket, Key: key,
                  ACL: 'public-read' }, err => next(err, key)),
                (key, next) => {
                    const copyParams = {
                        Bucket: bucket,
                        Key: copyKey,
                        CopySource: `/${bucket}/${key}`,
                        MetadataDirective: 'REPLACE',
                        Metadata: {
                            'scal-location-constraint': awsLocation2 },
                    };
                    process.stdout.write('Copying object\n');
                    s3.copyObject(copyParams, (err, result) => {
                        assert.equal(err, null, 'Expected success ' +
                        `but got error: ${err}`);
                        assert.strictEqual(result.CopyObjectResult.ETag,
                            `"${correctMD5}"`);
                        next(err, key);
                    });
                },
                (key, next) =>
                assertGetObjects(key, bucket, awsLocation, copyKey,
                  bucket, awsLocation2, copyKey, 'REPLACE', false,
                  awsS3Two, awsLocation2, next),
            ], done);
        });

        it('should return error AccessDenied copying an object on AWS to a ' +
        'different AWS account without source object READ access',
        done => {
            putSourceObj(awsLocation, false, bucket, key => {
                const copyKey = `copyKey-${Date.now()}`;
                const copyParams = {
                    Bucket: bucket,
                    Key: copyKey,
                    CopySource: `/${bucket}/${key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: {
                        'scal-location-constraint': awsLocation2 },
                };
                process.stdout.write('Copying object\n');
                s3.copyObject(copyParams, err => {
                    assert.strictEqual(err.code, 'AccessDenied');
                    done();
                });
            });
        });

        it('should copy an object on AWS with REPLACE', done => {
            putSourceObj(awsLocation, false, bucket, key => {
                const copyKey = `copyKey-${Date.now()}`;
                const copyParams = {
                    Bucket: bucket,
                    Key: copyKey,
                    CopySource: `/${bucket}/${key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: {
                        'scal-location-constraint': awsLocation },
                };
                process.stdout.write('Copying object\n');
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${correctMD5}"`);
                    assertGetObjects(key, bucket, awsLocation, copyKey, bucket,
                        awsLocation, copyKey, 'REPLACE', false, awsS3,
                        awsLocation, done);
                });
            });
        });

        it('should copy a 0-byte object from mem to AWS', done => {
            putSourceObj('mem', true, bucket, key => {
                const copyKey = `copyKey-${Date.now()}`;
                const copyParams = {
                    Bucket: bucket,
                    Key: copyKey,
                    CopySource: `/${bucket}/${key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: {
                        'scal-location-constraint': awsLocation },
                };
                process.stdout.write('Copying object\n');
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${emptyMD5}"`);
                    assertGetObjects(key, bucket, 'mem', copyKey, bucket,
                        awsLocation, copyKey, 'REPLACE', true, awsS3,
                        awsLocation, done);
                });
            });
        });

        it('should copy a 0-byte object on AWS', done => {
            putSourceObj(awsLocation, true, bucket, key => {
                const copyKey = `copyKey-${Date.now()}`;
                const copyParams = {
                    Bucket: bucket,
                    Key: copyKey,
                    CopySource: `/${bucket}/${key}`,
                    MetadataDirective: 'COPY',
                };
                process.stdout.write('Copying object\n');
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${emptyMD5}"`);
                    assertGetObjects(key, bucket, awsLocation, copyKey, bucket,
                        awsLocation, copyKey, 'COPY', true, awsS3,
                        awsLocation, done);
                });
            });
        });

        it('should return error if AWS source object has ' +
        'been deleted', done => {
            putSourceObj(awsLocation, false, bucket, key => {
                const awsBucket =
                    config.locationConstraints[awsLocation].details.bucketName;
                awsS3.deleteObject({ Bucket: awsBucket, Key: key }, err => {
                    assert.equal(err, null, 'Error deleting object from AWS: ' +
                        `${err}`);
                    const copyKey = `copyKey-${Date.now()}`;
                    const copyParams = { Bucket: bucket, Key: copyKey,
                        CopySource: `/${bucket}/${key}`,
                        MetadataDirective: 'COPY',
                    };
                    process.stdout.write('Copying object\n');
                    s3.copyObject(copyParams, err => {
                        assert.strictEqual(err.code, 'InternalError');
                        done();
                    });
                });
            });
        });
    });
});
