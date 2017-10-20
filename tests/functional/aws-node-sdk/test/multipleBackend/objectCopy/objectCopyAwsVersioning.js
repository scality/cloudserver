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
const {
    describeSkipIfNotMultiple,
    awsS3,
    awsBucket,
    memLocation,
    awsLocation,
    awsLocation2,
    awsLocationMismatch,
    awsLocationEncryption,
    enableVersioning,
} = require('../utils');

const bucket = 'buckettestmultiplebackendobjectcopyawsversioning';
const someBody = Buffer.from('I am a body', 'utf8');
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const emptyMD5 = 'd41d8cd98f00b204e9800998ecf8427e';
const locMetaHeader = constants.objectLocationConstraintHeader.substring(11);
const { versioningEnabled } = require('../../../lib/utility/versioning-util');

let bucketUtil;
let s3;



function assertGetObjects(sourceParams, destParams, destKey,
destBucket, destLoc, awsKey, mdDirective, isEmptyObj, awsS3, awsLocation,
callback) {
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
        } else if (destLoc === awsLocationEncryption) {
            assert.strictEqual(awsRes.ServerSideEncryption, 'AES256');
        } else {
            assert.strictEqual(sourceRes.ETag, `"${correctMD5}"`);
            assert.strictEqual(destRes.ETag, `"${correctMD5}"`);
            assert.deepStrictEqual(sourceRes.Body, destRes.Body);
            assert.strictEqual(awsRes.ETag, `"${correctMD5}"`);
            assert.deepStrictEqual(sourceRes.Body, awsRes.Body);
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

describeSkipIfNotMultiple('AWS backend object copy with versioning',
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

        /* it('should copy an object from mem to AWS', done => {
            putSourceObj(memLocation, false, key => {
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
                    assertGetObjects(key, bucket, memLocation, copyKey, bucket,
                        awsLocation, copyKey, 'REPLACE', false, awsS3,
                        awsLocation, done);
                });
            });
        });

        it('should copy an object from mem to AWS with encryption', done => {
            putSourceObj(memLocation, false, key => {
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
                    assertGetObjects(key, bucket, memLocation, copyKey, bucket,
                        awsLocationEncryption, copyKey, 'REPLACE', false,
                        awsS3, awsLocation, done);
                });
            });
        }); */

/*
copy from mem / file => aws (versioning enabled)
—> object put should return version id, should be able to get successfully

copy from aws => aws (versioning enabled)
—> object copy should return version id, should be able to get successfully
*/

/*
// create generate source & dest key?
const source = {
    location: memLocation,
    isEmpty: false,
    key: `sourceKey-${Date.now()}`,
}
const dest = {
    location: awsLocation,

}
const directive = 'REPLACE';
*/

        // steps to generalize:
        // put source object
        // put versioning
        // copy object
        // do a get to both sides & aws?

        // function that takes test params and generates params for putting
        // the source object and the copy object params
        // also generates the params needed for the get asserts
        // copy function will take the copy params and assert params <--
        // this
        //

        function _getTestMetadata(location) {
            return {
                'scal-location-constraint': location,
                'test-header': 'copyme',
            };
        }

        function putSourceObj(testParams, cb) {
            const { sourceBucket, sourceLocation, isEmptyObj } = testParams;
            const sourceKey = `somekey-${Date.now()}`;
            const sourceParams = {
                Bucket: sourceBucket,
                Key: sourceKey,
                Metadata: _getTestMetadata(sourceLocation),
            };
            if (!isEmptyObj) {
                sourceParams.Body = someBody;
            }
            s3.putObject(sourceParams, (err, result) => {
                assert.strictEqual(err, null,
                    `Error putting source object: ${err}`);
                if (isEmptyObj) {
                    assert.strictEqual(result.ETag, `"${emptyMD5}"`);
                } else {
                    assert.strictEqual(result.ETag, `"${correctMD5}"`);
                }
                Object.assign(testParams, {
                    sourceKey,
                    sourceVersionId: result.VersionId,
                });
                cb(null);
            });
        }

        it.only('should get a version id copying an object from mem to a ' +
        'versioning enabled AWS bucket', function itF(done) {
            const testParams = {
                sourceBucket: bucket,
                sourceLocation: memLocation,
//                sourceVersionId:
                destBucket: bucket,
                destLocation: awsLocation,
                destBucketVersioningState: 'Enabled',
                isEmpty: false,
                directive: 'REPLACE',
            };
            // an example:
            // put object with source location in source bucket (ver enabled)
            // return version id
            // enable versioning on bucket
            // copy object from source bucket to dest bucket
            // assert version id returned putting object in dest bucket
            // return version id
            // finally assert objects in all buckets

            async.waterfall([
                next => putSourceObj(testParams),
                next => enableVersioning(s3, testParams.sourceBucket, next),
                next =>

            ]);
            putSourceObj(memLocation, false, key => {
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
                    s3.copyObject(copyParams, (err, data) => {
                        const source = {
                            key, bucket, loc: awsLocation,
                        };
                        const dest = { key: copyKey, bucket, loc: memLocation }
                        sourceKey, sourceBucket, sourceLoc,

                        console.log('data from aws', data)
                        assert.strictEqual(err, null,
                            `Got err copying object: ${err}`);
                        assertGetObjects(key, bucket, awsLocation, copyKey,
                            bucket, memLocation, key, 'REPLACE', false, awsS3,
                            awsLocation, done);
                        assert(data.VersionId, 'Expected version id');
                        done();
                    });
                });
            });
        });

        /* it('should copy an object from AWS to mem', done => {
            putSourceObj(awsLocation, false, key => {
                const copyKey = `copyKey-${Date.now()}`;
                const copyParams = {
                    Bucket: bucket,
                    Key: copyKey,
                    CopySource: `/${bucket}/${key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: {
                        'scal-location-constraint': memLocation },
                };
                process.stdout.write('Copying object\n');
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${correctMD5}"`);
                    assertGetObjects(key, bucket, awsLocation, copyKey, bucket,
                        memLocation, key, 'REPLACE', false, awsS3,
                        awsLocation, done);
                });
            });
        });

        it('should copy an object from mem to AWS and retain metadata',
        done => {
            putSourceObj(memLocation, false, key => {
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
                    assertGetObjects(key, bucket, memLocation, copyKey, bucket,
                        awsLocation, copyKey, 'COPY', false, awsS3,
                        awsLocation, done);
                });
            });
        });

        it('should copy an object on AWS', done => {
            putSourceObj(awsLocation, false, key => {
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

        it('should copy an object on AWS location with bucketMatch equals ' +
        'false to a different AWS location with bucketMatch equals true',
        done => {
            putSourceObj(awsLocationMismatch, false, key => {
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
                next => putSourceObj(awsLocation, false, key =>
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
            putSourceObj(awsLocation, false, key => {
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
            putSourceObj(awsLocation, false, key => {
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
            putSourceObj(memLocation, true, key => {
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
                    assertGetObjects(key, bucket, memLocation, copyKey, bucket,
                        awsLocation, copyKey, 'REPLACE', true, awsS3,
                        awsLocation, done);
                });
            });
        });

        it('should copy a 0-byte object on AWS', done => {
            putSourceObj(awsLocation, true, key => {
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
            putSourceObj(awsLocation, false, key => {
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
        }); */
    });
});
