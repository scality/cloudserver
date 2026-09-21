const assert = require('assert');
const {
    S3Client,
    PutObjectCommand,
    GetObjectCommand,
    CopyObjectCommand,
    PutObjectAclCommand,
    CreateBucketCommand,
    DeleteObjectCommand,
} = require('@aws-sdk/client-s3');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const constants = require('../../../../../../constants');
const { config } = require('../../../../../../lib/Config');
const { getRealAwsConfig } = require('../../support/awsConfig');
const { createEncryptedBucketPromise } = require('../../../lib/utility/createEncryptedBucket');
const {
    describeSkipIfNotMultiple,
    awsS3,
    memLocation,
    awsLocation,
    azureLocation,
    awsLocation2,
    awsLocationMismatch,
    awsLocationEncryption,
    genUniqID,
} = require('../utils');

const bucket = `objectcopybucket${genUniqID()}`;
const bucketAws = `objectcopyaws${genUniqID()}`;
const awsServerSideEncryptionbucket = `objectcopyawssse${genUniqID()}`;
const body = Buffer.from('I am a body', 'utf8');
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const emptyMD5 = 'd41d8cd98f00b204e9800998ecf8427e';
const locMetaHeader = constants.objectLocationConstraintHeader.substring(11);

let bucketUtil;
let s3;

async function putSourceObj(location, isEmptyObj, bucket) {
    const key = `somekey-${genUniqID()}`;
    const sourceParams = {
        Bucket: bucket,
        Key: key,
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
    const result = await s3.send(new PutObjectCommand(sourceParams));
    if (isEmptyObj) {
        assert.strictEqual(result.ETag, `"${emptyMD5}"`);
    } else {
        assert.strictEqual(result.ETag, `"${correctMD5}"`);
    }
    return key;
}

async function assertGetObjects(
    sourceKey,
    sourceBucket,
    sourceLoc,
    destKey,
    destBucket,
    destLoc,
    awsKey,
    mdDirective,
    isEmptyObj,
    awsS3,
    awsLocation,
) {
    const awsBucket = config.locationConstraints[awsLocation].details.bucketName;
    const sourceGetParams = { Bucket: sourceBucket, Key: sourceKey };
    const destGetParams = { Bucket: destBucket, Key: destKey };
    const awsParams = { Bucket: awsBucket, Key: awsKey };

    const [sourceRes, destRes, awsRes] = await Promise.all([
        s3.send(new GetObjectCommand(sourceGetParams)),
        s3.send(new GetObjectCommand(destGetParams)),
        awsS3.send(new GetObjectCommand(awsParams)),
    ]);
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
        assert.deepStrictEqual(sourceRes.Metadata['test-header'], destRes.Metadata['test-header']);
    } else if (mdDirective === 'REPLACE') {
        assert.strictEqual(destRes.Metadata['test-header'], undefined);
    }
    if (destLoc === awsLocation) {
        assert.strictEqual(awsRes.Metadata[locMetaHeader], destLoc);
        if (mdDirective === 'COPY') {
            assert.deepStrictEqual(sourceRes.Metadata['test-header'], awsRes.Metadata['test-header']);
        } else if (mdDirective === 'REPLACE') {
            assert.strictEqual(awsRes.Metadata['test-header'], undefined);
        }
    }
    assert.strictEqual(sourceRes.ContentLength, destRes.ContentLength);
    assert.strictEqual(sourceRes.Metadata[locMetaHeader], sourceLoc);
    assert.strictEqual(destRes.Metadata[locMetaHeader], destLoc);
}

describeSkipIfNotMultiple('MultipleBackend object copy: AWS', function testSuite() {
    this.timeout(250000);
    withV4(sigCfg => {
        beforeEach(async () => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            process.stdout.write('Creating bucket\n');

            if (process.env.ENABLE_KMS_ENCRYPTION === 'true') {
                await createEncryptedBucketPromise({ Bucket: bucket });
                await createEncryptedBucketPromise({ Bucket: awsServerSideEncryptionbucket });
                await createEncryptedBucketPromise({ Bucket: bucketAws });
            } else {
                await s3.send(
                    new CreateBucketCommand({
                        Bucket: bucket,
                        CreateBucketConfiguration: {
                            LocationConstraint: memLocation,
                        },
                    }),
                );

                await s3.send(
                    new CreateBucketCommand({
                        Bucket: awsServerSideEncryptionbucket,
                        CreateBucketConfiguration: {
                            LocationConstraint: awsLocationEncryption,
                        },
                    }),
                );

                await s3.send(
                    new CreateBucketCommand({
                        Bucket: bucketAws,
                        CreateBucketConfiguration: {
                            LocationConstraint: awsLocation,
                        },
                    }),
                );
            }
        });

        afterEach(async () => {
            process.stdout.write('Emptying bucket\n');
            await bucketUtil.empty(bucket);
            await bucketUtil.empty(bucketAws);
            await bucketUtil.empty(awsServerSideEncryptionbucket);
            await bucketUtil.deleteOne(bucket);
            await bucketUtil.deleteOne(awsServerSideEncryptionbucket);
            await bucketUtil.deleteOne(bucketAws);
        });

        it('should copy an object from mem to AWS relying on ' + 'destination bucket location', async () => {
            const key = await putSourceObj(memLocation, false, bucket);
            const copyKey = `copyKey-${genUniqID()}`;
            const copyParams = {
                Bucket: bucketAws,
                Key: copyKey,
                CopySource: `/${bucket}/${key}`,
                MetadataDirective: 'COPY',
            };
            process.stdout.write('Copying object\n');
            const result = await s3.send(new CopyObjectCommand(copyParams));
            assert.strictEqual(result.CopyObjectResult.ETag, `"${correctMD5}"`);
            await assertGetObjects(
                key,
                bucket,
                memLocation,
                copyKey,
                bucketAws,
                awsLocation,
                copyKey,
                'COPY',
                false,
                awsS3,
                awsLocation,
            );
        });

        it('should copy an object from Azure to AWS relying on ' + 'destination bucket location', async () => {
            const key = await putSourceObj(azureLocation, false, bucket);
            const copyKey = `copyKey-${genUniqID()}`;
            const copyParams = {
                Bucket: bucketAws,
                Key: copyKey,
                CopySource: `/${bucket}/${key}`,
                MetadataDirective: 'COPY',
            };
            process.stdout.write('Copying object\n');
            const result = await s3.send(new CopyObjectCommand(copyParams));
            assert.strictEqual(result.CopyObjectResult.ETag, `"${correctMD5}"`);
            await assertGetObjects(
                key,
                bucket,
                azureLocation,
                copyKey,
                bucketAws,
                awsLocation,
                copyKey,
                'COPY',
                false,
                awsS3,
                awsLocation,
            );
        });

        it(
            'should copy an object without location contraint from mem ' +
                'to AWS relying on destination bucket location',
            async () => {
                const key = await putSourceObj(null, false, bucket);
                const copyKey = `copyKey-${genUniqID()}`;
                const copyParams = {
                    Bucket: bucketAws,
                    Key: copyKey,
                    CopySource: `/${bucket}/${key}`,
                    MetadataDirective: 'COPY',
                };
                process.stdout.write('Copying object\n');
                const result = await s3.send(new CopyObjectCommand(copyParams));
                assert.strictEqual(result.CopyObjectResult.ETag, `"${correctMD5}"`);
                await assertGetObjects(
                    key,
                    bucket,
                    undefined,
                    copyKey,
                    bucketAws,
                    undefined,
                    copyKey,
                    'COPY',
                    false,
                    awsS3,
                    awsLocation,
                );
            },
        );

        it('should copy an object from AWS to mem relying on destination ' + 'bucket location', async () => {
            const key = await putSourceObj(awsLocation, false, bucketAws);
            const copyKey = `copyKey-${genUniqID()}`;
            const copyParams = {
                Bucket: bucket,
                Key: copyKey,
                CopySource: `/${bucketAws}/${key}`,
                MetadataDirective: 'COPY',
            };
            process.stdout.write('Copying object\n');
            const result = await s3.send(new CopyObjectCommand(copyParams));
            assert.strictEqual(result.CopyObjectResult.ETag, `"${correctMD5}"`);
            await assertGetObjects(
                key,
                bucketAws,
                awsLocation,
                copyKey,
                bucket,
                memLocation,
                key,
                'COPY',
                false,
                awsS3,
                awsLocation,
            );
        });

        it('should copy an object from mem to AWS', async () => {
            const key = await putSourceObj(memLocation, false, bucket);
            const copyKey = `copyKey-${genUniqID()}`;
            const copyParams = {
                Bucket: bucket,
                Key: copyKey,
                CopySource: `/${bucket}/${key}`,
                MetadataDirective: 'REPLACE',
                Metadata: {
                    'scal-location-constraint': awsLocation,
                },
            };
            process.stdout.write('Copying object\n');
            const result = await s3.send(new CopyObjectCommand(copyParams));
            assert.strictEqual(result.CopyObjectResult.ETag, `"${correctMD5}"`);
            await assertGetObjects(
                key,
                bucket,
                memLocation,
                copyKey,
                bucket,
                awsLocation,
                copyKey,
                'REPLACE',
                false,
                awsS3,
                awsLocation,
            );
        });

        it('should copy an object from mem to AWS with aws server ' + 'side encryption', async () => {
            const key = await putSourceObj(memLocation, false, bucket);
            const copyKey = `copyKey-${genUniqID()}`;
            const copyParams = {
                Bucket: bucket,
                Key: copyKey,
                CopySource: `/${bucket}/${key}`,
                MetadataDirective: 'REPLACE',
                Metadata: {
                    'scal-location-constraint': awsLocationEncryption,
                },
            };
            process.stdout.write('Copying object\n');
            const result = await s3.send(new CopyObjectCommand(copyParams));
            assert.strictEqual(result.CopyObjectResult.ETag, `"${correctMD5}"`);
            await assertGetObjects(
                key,
                bucket,
                memLocation,
                copyKey,
                bucket,
                awsLocationEncryption,
                copyKey,
                'REPLACE',
                false,
                awsS3,
                awsLocation,
            );
        });

        it(
            'should copy an object from AWS to mem with encryption with ' +
                'REPLACE directive but no location constraint',
            async () => {
                const key = await putSourceObj(awsLocation, false, bucket);
                const copyKey = `copyKey-${genUniqID()}`;
                const copyParams = {
                    Bucket: bucket,
                    Key: copyKey,
                    CopySource: `/${bucket}/${key}`,
                    MetadataDirective: 'REPLACE',
                };
                process.stdout.write('Copying object\n');
                const result = await s3.send(new CopyObjectCommand(copyParams));
                assert.strictEqual(result.CopyObjectResult.ETag, `"${correctMD5}"`);
                await assertGetObjects(
                    key,
                    bucket,
                    awsLocation,
                    copyKey,
                    bucket,
                    undefined,
                    key,
                    'REPLACE',
                    false,
                    awsS3,
                    awsLocation,
                );
            },
        );

        it('should copy an object on AWS with aws server side ' + 'encryption', async () => {
            const key = await putSourceObj(awsLocation, false, bucket);
            const copyKey = `copyKey-${genUniqID()}`;
            const copyParams = {
                Bucket: bucket,
                Key: copyKey,
                CopySource: `/${bucket}/${key}`,
                MetadataDirective: 'REPLACE',
                Metadata: {
                    'scal-location-constraint': awsLocationEncryption,
                },
            };
            process.stdout.write('Copying object\n');
            const result = await s3.send(new CopyObjectCommand(copyParams));
            assert.strictEqual(result.CopyObjectResult.ETag, `"${correctMD5}"`);
            await assertGetObjects(
                key,
                bucket,
                awsLocation,
                copyKey,
                bucket,
                awsLocationEncryption,
                copyKey,
                'REPLACE',
                false,
                awsS3,
                awsLocation,
            );
        });

        it('should copy an object on AWS with aws server side ' + 'encrypted bucket', async () => {
            const key = await putSourceObj(awsLocation, false, awsServerSideEncryptionbucket);
            const copyKey = `copyKey-${genUniqID()}`;
            const copyParams = {
                Bucket: awsServerSideEncryptionbucket,
                Key: copyKey,
                CopySource: `/${awsServerSideEncryptionbucket}/${key}`,
                MetadataDirective: 'COPY',
            };
            process.stdout.write('Copying object\n');
            const result = await s3.send(new CopyObjectCommand(copyParams));
            assert.strictEqual(result.CopyObjectResult.ETag, `"${correctMD5}"`);
            await assertGetObjects(
                key,
                awsServerSideEncryptionbucket,
                awsLocation,
                copyKey,
                awsServerSideEncryptionbucket,
                awsLocationEncryption,
                copyKey,
                'COPY',
                false,
                awsS3,
                awsLocation,
            );
        });

        it(
            'should copy an object from mem to AWS with encryption with ' +
                'REPLACE directive but no location constraint',
            async () => {
                const key = await putSourceObj(null, false, bucket);
                const copyKey = `copyKey-${genUniqID()}`;
                const copyParams = {
                    Bucket: bucketAws,
                    Key: copyKey,
                    CopySource: `/${bucket}/${key}`,
                    MetadataDirective: 'REPLACE',
                };
                process.stdout.write('Copying object\n');
                const result = await s3.send(new CopyObjectCommand(copyParams));
                assert.strictEqual(result.CopyObjectResult.ETag, `"${correctMD5}"`);
                await assertGetObjects(
                    key,
                    bucket,
                    undefined,
                    copyKey,
                    bucketAws,
                    undefined,
                    copyKey,
                    'REPLACE',
                    false,
                    awsS3,
                    awsLocation,
                );
            },
        );

        it('should copy an object from AWS to mem with "COPY" ' + 'directive and aws location metadata', async () => {
            const key = await putSourceObj(awsLocation, false, bucket);
            const copyKey = `copyKey-${genUniqID()}`;
            const copyParams = {
                Bucket: bucket,
                Key: copyKey,
                CopySource: `/${bucket}/${key}`,
                MetadataDirective: 'COPY',
                Metadata: {
                    'scal-location-constraint': awsLocation,
                },
            };
            process.stdout.write('Copying object\n');
            const result = await s3.send(new CopyObjectCommand(copyParams));
            assert.strictEqual(result.CopyObjectResult.ETag, `"${correctMD5}"`);
            await assertGetObjects(
                key,
                bucket,
                awsLocation,
                copyKey,
                bucket,
                memLocation,
                key,
                'COPY',
                false,
                awsS3,
                awsLocation,
            );
        });

        it('should copy an object on AWS', async () => {
            const key = await putSourceObj(awsLocation, false, bucket);
            const copyKey = `copyKey-${genUniqID()}`;
            const copyParams = {
                Bucket: bucket,
                Key: copyKey,
                CopySource: `/${bucket}/${key}`,
                MetadataDirective: 'REPLACE',
                Metadata: { 'scal-location-constraint': awsLocation },
            };
            process.stdout.write('Copying object\n');
            const result = await s3.send(new CopyObjectCommand(copyParams));
            assert.strictEqual(result.CopyObjectResult.ETag, `"${correctMD5}"`);
            await assertGetObjects(
                key,
                bucket,
                awsLocation,
                copyKey,
                bucket,
                awsLocation,
                copyKey,
                'REPLACE',
                false,
                awsS3,
                awsLocation,
            );
        });

        it(
            'should copy an object on AWS location with bucketMatch equals ' +
                'false to a different AWS location with bucketMatch equals true',
            async () => {
                const key = await putSourceObj(awsLocationMismatch, false, bucket);
                const copyKey = `copyKey-${genUniqID()}`;
                const copyParams = {
                    Bucket: bucket,
                    Key: copyKey,
                    CopySource: `/${bucket}/${key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: {
                        'scal-location-constraint': awsLocation,
                    },
                };
                process.stdout.write('Copying object\n');
                const result = await s3.send(new CopyObjectCommand(copyParams));
                assert.strictEqual(result.CopyObjectResult.ETag, `"${correctMD5}"`);
                await assertGetObjects(
                    key,
                    bucket,
                    awsLocationMismatch,
                    copyKey,
                    bucket,
                    awsLocation,
                    copyKey,
                    'REPLACE',
                    false,
                    awsS3,
                    awsLocation,
                );
            },
        );

        it('should copy an object on AWS to a different AWS location ' + 'with source object READ access', async () => {
            const awsConfig2 = getRealAwsConfig(awsLocation2);
            const awsS3Two = new S3Client(awsConfig2);
            const copyKey = `copyKey-${genUniqID()}`;
            const awsBucket = config.locationConstraints[awsLocation].details.bucketName;

            // giving access to the object on the AWS side
            const key = await putSourceObj(awsLocation, false, bucket);
            await awsS3.send(
                new PutObjectAclCommand({
                    Bucket: awsBucket,
                    Key: key,
                    ACL: 'public-read',
                }),
            );

            const copyParams = {
                Bucket: bucket,
                Key: copyKey,
                CopySource: `/${bucket}/${key}`,
                MetadataDirective: 'REPLACE',
                Metadata: {
                    'scal-location-constraint': awsLocation2,
                },
            };
            process.stdout.write('Copying object\n');
            const result = await s3.send(new CopyObjectCommand(copyParams));
            assert.strictEqual(result.CopyObjectResult.ETag, `"${correctMD5}"`);

            await assertGetObjects(
                key,
                bucket,
                awsLocation,
                copyKey,
                bucket,
                awsLocation2,
                copyKey,
                'REPLACE',
                false,
                awsS3Two,
                awsLocation2,
            );
        });

        it(
            'should return error AccessDenied copying an object on ' +
                'AWS to a different AWS account without source object READ access',
            async () => {
                const key = await putSourceObj(awsLocation, false, bucket);
                const copyKey = `copyKey-${genUniqID()}`;
                const copyParams = {
                    Bucket: bucket,
                    Key: copyKey,
                    CopySource: `/${bucket}/${key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: {
                        'scal-location-constraint': awsLocation2,
                    },
                };
                process.stdout.write('Copying object\n');
                try {
                    await s3.send(new CopyObjectCommand(copyParams));
                    assert.fail('Expected AccessDenied error');
                } catch (err) {
                    assert.strictEqual(err.name, 'AccessDenied');
                }
            },
        );

        it('should copy an object on AWS with REPLACE', async () => {
            const key = await putSourceObj(awsLocation, false, bucket);
            const copyKey = `copyKey-${genUniqID()}`;
            const copyParams = {
                Bucket: bucket,
                Key: copyKey,
                CopySource: `/${bucket}/${key}`,
                MetadataDirective: 'REPLACE',
                Metadata: {
                    'scal-location-constraint': awsLocation,
                },
            };
            process.stdout.write('Copying object\n');
            const result = await s3.send(new CopyObjectCommand(copyParams));
            assert.strictEqual(result.CopyObjectResult.ETag, `"${correctMD5}"`);
            await assertGetObjects(
                key,
                bucket,
                awsLocation,
                copyKey,
                bucket,
                awsLocation,
                copyKey,
                'REPLACE',
                false,
                awsS3,
                awsLocation,
            );
        });

        it('should copy a 0-byte object from mem to AWS', async () => {
            const key = await putSourceObj(memLocation, true, bucket);
            const copyKey = `copyKey-${genUniqID()}`;
            const copyParams = {
                Bucket: bucket,
                Key: copyKey,
                CopySource: `/${bucket}/${key}`,
                MetadataDirective: 'REPLACE',
                Metadata: {
                    'scal-location-constraint': awsLocation,
                },
            };
            process.stdout.write('Copying object\n');
            const result = await s3.send(new CopyObjectCommand(copyParams));
            assert.strictEqual(result.CopyObjectResult.ETag, `"${emptyMD5}"`);
            await assertGetObjects(
                key,
                bucket,
                memLocation,
                copyKey,
                bucket,
                awsLocation,
                copyKey,
                'REPLACE',
                true,
                awsS3,
                awsLocation,
            );
        });

        it('should copy a 0-byte object on AWS', async () => {
            const key = await putSourceObj(awsLocation, true, bucket);
            const copyKey = `copyKey-${genUniqID()}`;
            const copyParams = {
                Bucket: bucket,
                Key: copyKey,
                CopySource: `/${bucket}/${key}`,
                MetadataDirective: 'REPLACE',
                Metadata: { 'scal-location-constraint': awsLocation },
            };
            process.stdout.write('Copying object\n');
            const result = await s3.send(new CopyObjectCommand(copyParams));
            assert.strictEqual(result.CopyObjectResult.ETag, `"${emptyMD5}"`);
            await assertGetObjects(
                key,
                bucket,
                awsLocation,
                copyKey,
                bucket,
                awsLocation,
                copyKey,
                'REPLACE',
                true,
                awsS3,
                awsLocation,
            );
        });

        it('should return error if AWS source object has ' + 'been deleted', async () => {
            const key = await putSourceObj(awsLocation, false, bucket);
            const awsBucket = config.locationConstraints[awsLocation].details.bucketName;

            await awsS3.send(new DeleteObjectCommand({ Bucket: awsBucket, Key: key }));

            const copyKey = `copyKey-${genUniqID()}`;
            const copyParams = {
                Bucket: bucket,
                Key: copyKey,
                CopySource: `/${bucket}/${key}`,
                MetadataDirective: 'REPLACE',
                Metadata: { 'scal-location-constraint': awsLocation },
            };
            process.stdout.write('Copying object\n');
            try {
                await s3.send(new CopyObjectCommand(copyParams));
                assert.fail('Expected ServiceUnavailable error');
            } catch (err) {
                assert.strictEqual(err.name, 'ServiceUnavailable');
            }
        });
    });
});
