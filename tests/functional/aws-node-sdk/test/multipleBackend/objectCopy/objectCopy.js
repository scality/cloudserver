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
const { describeSkipIfNotMultiple, awsS3, memLocation, awsLocation,
    azureLocation, awsLocation2, awsLocationMismatch, awsLocationEncryption,
    genUniqID } = require('../utils');

const bucket = `objectcopybucket${genUniqID()}`;
const bucketAws = `objectcopyaws${genUniqID()}`;
const awsServerSideEncryptionbucket = `objectcopyawssse${genUniqID()}`;
const body = Buffer.from('I am a body', 'utf8');
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const emptyMD5 = 'd41d8cd98f00b204e9800998ecf8427e';
const locMetaHeader = constants.objectLocationConstraintHeader.substring(11);

let bucketUtil;
let s3;

function putSourceObj(location, isEmptyObj, bucket, cb) {
    const key = `somekey-${genUniqID()}`;
    const sourceParams = { Bucket: bucket, Key: key,
        Metadata: {
            'test-header': 'copyme',
        },
    };
    if (location) {
        sourceParams.Metadata['scal-location-constraint'] = location;
    }
    if (!isEmptyObj) {
        sourceParams.Body = body;
    }
    process.stdout.write('Putting source object\n');
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

describeSkipIfNotMultiple('MultipleBackend object copy: AWS',
function testSuite() {
    this.timeout(250000);
    withV4(sigCfg => {
        beforeEach(() => {
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
            .then(() => s3.createBucketAsync({
                Bucket: awsServerSideEncryptionbucket,
                CreateBucketConfiguration: {
                    LocationConstraint: awsLocationEncryption,
                },
            }))
            .then(() => s3.createBucketAsync({ Bucket: bucketAws,
              CreateBucketConfiguration: {
                  LocationConstraint: awsLocation,
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
            .then(() => bucketUtil.empty(bucketAws))
            .then(() => bucketUtil.empty(awsServerSideEncryptionbucket))
            .then(() => {
                process.stdout.write(`Deleting bucket ${bucket}\n`);
                return bucketUtil.deleteOne(bucket);
            })
            .then(() => {
                process.stdout.write('Deleting bucket ' +
                `${awsServerSideEncryptionbucket}\n`);
                return bucketUtil.deleteOne(awsServerSideEncryptionbucket);
            })
            .then(() => {
                process.stdout.write(`Deleting bucket ${bucketAws}\n`);
                return bucketUtil.deleteOne(bucketAws);
            })
            .catch(err => {
                process.stdout.write(`Error in afterEach: ${err}\n`);
                throw err;
            });
        });

        it('should copy an object from mem to AWS relying on ' +
        'destination bucket location',
        done => {
            putSourceObj(memLocation, false, bucket, key => {
                const copyKey = `copyKey-${genUniqID()}`;
                const copyParams = {
                    Bucket: bucketAws,
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
                    assertGetObjects(key, bucket, memLocation, copyKey,
                        bucketAws, awsLocation, copyKey, 'COPY', false, awsS3,
                        awsLocation, done);
                });
            });
        });

        it('should copy an object from Azure to AWS relying on ' +
        'destination bucket location',
        done => {
            putSourceObj(azureLocation, false, bucket, key => {
                const copyKey = `copyKey-${genUniqID()}`;
                const copyParams = {
                    Bucket: bucketAws,
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
                    assertGetObjects(key, bucket, azureLocation, copyKey,
                        bucketAws, awsLocation, copyKey, 'COPY', false, awsS3,
                        awsLocation, done);
                });
            });
        });

        it('should copy an object without location contraint from mem ' +
        'to AWS relying on destination bucket location',
        done => {
            putSourceObj(null, false, bucket, key => {
                const copyKey = `copyKey-${genUniqID()}`;
                const copyParams = {
                    Bucket: bucketAws,
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
                    assertGetObjects(key, bucket, undefined, copyKey,
                        bucketAws, undefined, copyKey, 'COPY', false, awsS3,
                        awsLocation, done);
                });
            });
        });

        it('should copy an object from AWS to mem relying on destination ' +
        'bucket location',
        done => {
            putSourceObj(awsLocation, false, bucketAws, key => {
                const copyKey = `copyKey-${genUniqID()}`;
                const copyParams = {
                    Bucket: bucket,
                    Key: copyKey,
                    CopySource: `/${bucketAws}/${key}`,
                    MetadataDirective: 'COPY',
                };
                process.stdout.write('Copying object\n');
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${correctMD5}"`);
                    assertGetObjects(key, bucketAws, awsLocation, copyKey,
                      bucket, memLocation, key, 'COPY', false, awsS3,
                        awsLocation, done);
                });
            });
        });

        it('should copy an object from mem to AWS', done => {
            putSourceObj(memLocation, false, bucket, key => {
                const copyKey = `copyKey-${genUniqID()}`;
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
                    assertGetObjects(key, bucket, memLocation, copyKey, bucket,
                        awsLocation, copyKey, 'REPLACE', false, awsS3,
                        awsLocation, done);
                });
            });
        });

        it('should copy an object from mem to AWS with aws server side ' +
        'encryption', done => {
            putSourceObj(memLocation, false, bucket, key => {
                const copyKey = `copyKey-${genUniqID()}`;
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
                    assertGetObjects(key, bucket, memLocation, copyKey, bucket,
                        awsLocationEncryption, copyKey, 'REPLACE', false,
                        awsS3, awsLocation, done);
                });
            });
        });

        it('should copy an object from AWS to mem with encryption with ' +
        'REPLACE directive but no location constraint', done => {
            putSourceObj(awsLocation, false, bucket, key => {
                const copyKey = `copyKey-${genUniqID()}`;
                const copyParams = {
                    Bucket: bucket,
                    Key: copyKey,
                    CopySource: `/${bucket}/${key}`,
                    MetadataDirective: 'REPLACE',
                };
                process.stdout.write('Copying object\n');
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${correctMD5}"`);
                    assertGetObjects(key, bucket, awsLocation, copyKey, bucket,
                        undefined, key, 'REPLACE', false,
                        awsS3, awsLocation, done);
                });
            });
        });

        it('should copy an object on AWS with aws server side encryption',
        done => {
            putSourceObj(awsLocation, false, bucket, key => {
                const copyKey = `copyKey-${genUniqID()}`;
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

        it('should copy an object on AWS with aws server side ' +
        'encrypted bucket', done => {
            putSourceObj(awsLocation, false, awsServerSideEncryptionbucket,
            key => {
                const copyKey = `copyKey-${genUniqID()}`;
                const copyParams = {
                    Bucket: awsServerSideEncryptionbucket,
                    Key: copyKey,
                    CopySource: `/${awsServerSideEncryptionbucket}/${key}`,
                    MetadataDirective: 'COPY',
                };
                process.stdout.write('Copying object\n');
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${correctMD5}"`);
                    assertGetObjects(key, awsServerSideEncryptionbucket,
                        awsLocation, copyKey, awsServerSideEncryptionbucket,
                        awsLocationEncryption, copyKey, 'COPY',
                        false, awsS3, awsLocation, done);
                });
            });
        });

        it('should copy an object from mem to AWS with encryption with ' +
        'REPLACE directive but no location constraint', done => {
            putSourceObj(null, false, bucket, key => {
                const copyKey = `copyKey-${genUniqID()}`;
                const copyParams = {
                    Bucket: bucketAws,
                    Key: copyKey,
                    CopySource: `/${bucket}/${key}`,
                    MetadataDirective: 'REPLACE',
                };
                process.stdout.write('Copying object\n');
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${correctMD5}"`);
                    assertGetObjects(key, bucket, undefined, copyKey,
                        bucketAws, undefined, copyKey, 'REPLACE', false,
                        awsS3, awsLocation, done);
                });
            });
        });

        it('should copy an object from AWS to mem with "COPY" ' +
        'directive and aws location metadata',
        done => {
            putSourceObj(awsLocation, false, bucket, key => {
                const copyKey = `copyKey-${genUniqID()}`;
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
                    assertGetObjects(key, bucket, awsLocation, copyKey, bucket,
                        memLocation, key, 'COPY', false, awsS3,
                        awsLocation, done);
                });
            });
        });

        it('should copy an object on AWS', done => {
            putSourceObj(awsLocation, false, bucket, key => {
                const copyKey = `copyKey-${genUniqID()}`;
                const copyParams = {
                    Bucket: bucket,
                    Key: copyKey,
                    CopySource: `/${bucket}/${key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: { 'scal-location-constraint': awsLocation },
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

        it('should copy an object on AWS location with bucketMatch equals ' +
        'false to a different AWS location with bucketMatch equals true',
        done => {
            putSourceObj(awsLocationMismatch, false, bucket, key => {
                const copyKey = `copyKey-${genUniqID()}`;
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
            const copyKey = `copyKey-${genUniqID()}`;
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
                const copyKey = `copyKey-${genUniqID()}`;
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
                const copyKey = `copyKey-${genUniqID()}`;
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
            putSourceObj(memLocation, true, bucket, key => {
                const copyKey = `copyKey-${genUniqID()}`;
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
                    assertGetObjects(key, bucket, memLocation, copyKey, bucket,
                        awsLocation, copyKey, 'REPLACE', true, awsS3,
                        awsLocation, done);
                });
            });
        });

        it('should copy a 0-byte object on AWS', done => {
            putSourceObj(awsLocation, true, bucket, key => {
                const copyKey = `copyKey-${genUniqID()}`;
                const copyParams = {
                    Bucket: bucket,
                    Key: copyKey,
                    CopySource: `/${bucket}/${key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: { 'scal-location-constraint': awsLocation },
                };
                process.stdout.write('Copying object\n');
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${emptyMD5}"`);
                    assertGetObjects(key, bucket, awsLocation, copyKey, bucket,
                        awsLocation, copyKey, 'REPLACE', true, awsS3,
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
                    const copyKey = `copyKey-${genUniqID()}`;
                    const copyParams = { Bucket: bucket, Key: copyKey,
                        CopySource: `/${bucket}/${key}`,
                        MetadataDirective: 'REPLACE',
                        Metadata: { 'scal-location-constraint': awsLocation },
                    };
                    process.stdout.write('Copying object\n');
                    s3.copyObject(copyParams, err => {
                        assert.strictEqual(err.code, 'ServiceUnavailable');
                        done();
                    });
                });
            });
        });
    });
});
