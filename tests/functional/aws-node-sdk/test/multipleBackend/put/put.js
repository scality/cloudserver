const assert = require('assert');
const {
    PutObjectCommand,
    GetObjectCommand,
    HeadObjectCommand,
    PutBucketVersioningCommand,
    CreateBucketCommand,
    GetBucketLocationCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { config } = require('../../../../../../lib/Config');
const { createEncryptedBucketPromise } =
    require('../../../lib/utility/createEncryptedBucket');
const { versioningEnabled } = require('../../../lib/utility/versioning-util');

const { describeSkipIfNotMultiple, getAwsRetry, awsLocation,
    awsLocationEncryption, memLocation, fileLocation, genUniqID }
    = require('../utils');
const bucket = `putaws${genUniqID()}`;
const body = Buffer.from('I am a body', 'utf8');
const bigBody = Buffer.alloc(10485760);
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const emptyMD5 = 'd41d8cd98f00b204e9800998ecf8427e';
// AWS handles objects larger than 5MB as MPUs, so returned ETag differs
const bigS3MD5 = 'f1c9645dbc14efddc7d8a322685f26eb';
const bigAWSMD5 = 'a7d414b9133d6483d9a1c4e04e856e3b-2';

let bucketUtil;
let s3;

async function getAwsSuccess(key, awsMD5, location) {
    return new Promise((resolve, reject) => {
        getAwsRetry({ key }, 0, (err, res) => {
            if (err) {
                reject(new Error(`Expected success, got error on direct AWS call: ${err}`));
                return;
            }
            
            if (location === awsLocationEncryption) {
                // doesn't check ETag because it's different
                // with every PUT with encryption
                assert.strictEqual(res.ServerSideEncryption, 'AES256');
            }
            if (process.env.ENABLE_KMS_ENCRYPTION !== 'true') {
                assert.strictEqual(res.ETag, `"${awsMD5}"`);
            }
            assert.strictEqual(res.Metadata['scal-location-constraint'],
                location);
            resolve(res);
        });
    });
}

async function getAwsError(key, expectedError) {
    return new Promise((resolve, reject) => {
        getAwsRetry({ key }, 0, err => {
            try {
                assert.notStrictEqual(err, undefined,
                    'Expected error but did not find one');
                assert.strictEqual(err.name, expectedError);
                resolve();
            } catch (assertionError) {
                reject(assertionError);
            }
        });
    });
}

async function awsGetCheck(objectKey, s3MD5, awsMD5, location) {
    const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: objectKey }));
    assert.strictEqual(res.ETag, `"${s3MD5}"`);
    
    if (process.env.ENABLE_KMS_ENCRYPTION === 'true') {
        assert.strictEqual(res.ServerSideEncryption, 'AES256');
    }
    
    process.stdout.write('Getting object from AWS\n');
    return await getAwsSuccess(objectKey, awsMD5, location);
}


describeSkipIfNotMultiple('MultipleBackend put object', function testSuite() {
    this.timeout(250000);
    withV4(sigCfg => {
        beforeEach(async () => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            process.stdout.write('Creating bucket\n');
            
            if (process.env.ENABLE_KMS_ENCRYPTION === 'true') {
                await createEncryptedBucketPromise({ Bucket: bucket });
            } else {
                await s3.send(new CreateBucketCommand({ Bucket: bucket }));
            }
        });

        afterEach(async () => {
            process.stdout.write('Emptying bucket\n');
            await bucketUtil.empty(bucket);
            await bucketUtil.deleteOne(bucket);
        });

        // aws-sdk now (v2.363.0) returns 'UriParameterError' error
        it.skip('should return an error to put request without a valid ' +
        'bucket name',
            async () => {
                const key = `somekey-${genUniqID()}`;
                try {
                    await s3.send(new PutObjectCommand({ Bucket: '', Key: key }));
                    throw new Error('Expected failure but got success');
                } catch (err) {
                    assert.strictEqual(err.code, 'MethodNotAllowed');
                }
            });

        describeSkipIfNotMultiple('with set location from "x-amz-meta-scal-' +
            'location-constraint" header', function describe() {
            if (!process.env.S3_END_TO_END) {
                this.retries(2);
            }

            it('should return an error to put request without a valid ' +
                'location constraint', async () => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': 'fail-region' } };
                try {
                    await s3.send(new PutObjectCommand(params));
                    throw new Error('Expected failure but got success');
                } catch (err) {
                    assert.strictEqual(err.code, 'InvalidArgument');
                }
            });

            it('should put an object to mem', async () => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': memLocation },
                };
                
                await s3.send(new PutObjectCommand(params)).then(() => {
                    process.stdout.write('Putting object succeeded\n');
                }).catch(err => {
                    throw new Error(`Expected success, got error: ${err}`);
                });
                const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
                assert.strictEqual(res.ETag, `"${correctMD5}"`);
            });

            it('should put a 0-byte object to mem', async () => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key,
                    Metadata: { 'scal-location-constraint': memLocation },
                };
                
                await s3.send(new PutObjectCommand(params)).then(() => {
                    process.stdout.write('Putting object succeeded\n');
                }).catch(err => {
                    throw new Error(`Expected success, got error: ${err}`);
                });
                const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
                assert.strictEqual(res.ETag, `"${emptyMD5}"`);
            });

            it('should put only metadata to mem with mdonly header', async () => {
                const key = `mdonly-${genUniqID()}`;
                const b64 = Buffer.from(correctMD5, 'hex').toString('base64');
                const params = { Bucket: bucket, Key: key,
                    Metadata: { 'scal-location-constraint': awsLocation,
                    'mdonly': 'true',
                    'md5chksum': b64,
                    'size': body.length.toString(),
                    } };
                await s3.send(new PutObjectCommand(params)).then(() => {
                    process.stdout.write('Putting object succeeded\n');
                }).catch(err => {
                    throw new Error(`Expected success, got error: ${err}`);
                });
                const res = await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
                assert.strictEqual(res.ETag, `"${correctMD5}"`);
                await getAwsError(key, 'NoSuchKey');
            });

            it('should put actual object with body and mdonly header', async () => {
                const key = `mdonly-${genUniqID()}`;
                const b64 = Buffer.from(correctMD5, 'hex').toString('base64');
                const params = { Bucket: bucket, Key: key, Body: body,
                    Metadata: { 'scal-location-constraint': awsLocation,
                    'mdonly': 'true',
                    'md5chksum': b64,
                    'size': body.length.toString(),
                    } };
                await s3.send(new PutObjectCommand(params)).then(() => {
                    process.stdout.write('Putting object succeeded\n');
                }).catch(err => {
                    throw new Error(`Expected success, got error: ${err}`);
                });
                const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
                assert.strictEqual(res.ETag, `"${correctMD5}"`);
                await awsGetCheck(key, correctMD5, correctMD5, awsLocation);
            });

            it('should put 0-byte normally with mdonly header', async () => {
                const key = `mdonly-${genUniqID()}`;
                const b64 = Buffer.from(emptyMD5, 'hex').toString('base64');
                const params = { Bucket: bucket, Key: key,
                    Metadata: { 'scal-location-constraint': awsLocation,
                    'mdonly': 'true',
                    'md5chksum': b64,
                    'size': '0',
                    } };
                await s3.send(new PutObjectCommand(params)).then(() => {
                    process.stdout.write('Putting object succeeded\n');
                }).catch(err => {
                    throw new Error(`Expected success, got error: ${err}`);
                });
                await awsGetCheck(key, emptyMD5, emptyMD5, awsLocation);
            });

            it('should put a 0-byte object to AWS', async () => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key,
                    Metadata: { 'scal-location-constraint': awsLocation },
                };
                await s3.send(new PutObjectCommand(params)).then(() => {
                    process.stdout.write('Putting object succeeded\n');
                }).catch(err => {
                    throw new Error(`Expected success, got error: ${err}`);
                });
                await awsGetCheck(key, emptyMD5, emptyMD5, awsLocation);
            });

            it('should put an object to file', async () => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': fileLocation },
                };
                await s3.send(new PutObjectCommand(params)).then(() => {
                    process.stdout.write('Putting object succeeded\n');
                }).catch(err => {
                    throw new Error(`Expected success, got error: ${err}`);
                });
                const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
                assert.strictEqual(res.ETag, `"${correctMD5}"`);
            });

            it('should put an object to AWS', async () => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': awsLocation } };
                await s3.send(new PutObjectCommand(params)).then(() => {
                    process.stdout.write('Putting object succeeded\n');
                }).catch(err => {
                    throw new Error(`Expected success, got error: ${err}`);
                });
                await awsGetCheck(key, correctMD5, correctMD5, awsLocation);
            });

            it('should encrypt body only if bucket encrypted putting ' +
            'object to AWS',
            async () => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': awsLocation } };
                await s3.send(new PutObjectCommand(params)).then(() => {
                    process.stdout.write('Putting object succeeded\n');
                }).catch(err => {
                    throw new Error(`Expected success, got error: ${err}`);
                });
                await getAwsSuccess(key, correctMD5, awsLocation);
            });


            it('should put an object to AWS with encryption', async () => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint':
                    awsLocationEncryption } };
                await s3.send(new PutObjectCommand(params)).then(() => {
                    process.stdout.write('Putting object succeeded\n');
                }).catch(err => {
                    throw new Error(`Expected success, got error: ${err}`);
                });
                await awsGetCheck(key, correctMD5, correctMD5,
                  awsLocationEncryption);
            });

            it('should return a version id putting object to ' +
            'to AWS with versioning enabled', async () => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key, Body: body,
                    Metadata: { 'scal-location-constraint': awsLocation } };
                await s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: versioningEnabled,
                }));
                const res = await s3.send(new PutObjectCommand(params));
                assert.strictEqual(res.VersionId);
                await getAwsSuccess(key, correctMD5, awsLocation);
            });

            it('should put a large object to AWS', async () => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: bigBody,
                    Metadata: { 'scal-location-constraint': awsLocation } };
                await s3.send(new PutObjectCommand(params)).then(() => {
                    process.stdout.write('Putting object succeeded\n');
                }).catch(err => {
                    throw new Error(`Expected success, got error: ${err}`);
                });
                await awsGetCheck(key, bigS3MD5, bigAWSMD5, awsLocation);
            });

            it('should put objects with same key to AWS ' +
            'then file, and object should only be present in file', async () => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': awsLocation } };
                await s3.send(new PutObjectCommand(params)).then(() => {
                    process.stdout.write('Putting object succeeded\n');
                }).catch(err => {
                    throw new Error(`Expected success, got error: ${err}`);
                });
                params.Metadata =
                    { 'scal-location-constraint': fileLocation };
                await s3.send(new PutObjectCommand(params)).then(() => {
                    process.stdout.write('Putting object succeeded\n');
                }).catch(err => {
                    throw new Error(`Expected success, got error: ${err}`);
                });
                const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
                assert.strictEqual(
                    res.Metadata['scal-location-constraint'],
                    fileLocation);
                return await getAwsError(key, 'NoSuchKey');
            });

            it('should put objects with same key to file ' +
            'then AWS, and object should only be present on AWS', async () => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': fileLocation } };
                await s3.send(new PutObjectCommand(params)).then(() => {
                    process.stdout.write('Putting object succeeded\n');
                }).catch(err => {
                    throw new Error(`Expected success, got error: ${err}`);
                });
                params.Metadata = {
                    'scal-location-constraint': awsLocation };
                await s3.send(new PutObjectCommand(params)).then(() => {
                    process.stdout.write('Putting object succeeded\n');
                }).catch(err => {
                    throw new Error(`Expected success, got error: ${err}`);
                });
                await awsGetCheck(key, correctMD5, correctMD5,
                    awsLocation);
            });

            it('should put two objects to AWS with same ' +
            'key, and newest object should be returned', async () => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': awsLocation,
                        'unique-header': 'first object' } };
                await s3.send(new PutObjectCommand(params)).then(() => {
                    process.stdout.write('Putting object succeeded\n');
                }).catch(err => {
                    throw new Error(`Expected success, got error: ${err}`);
                });
                params.Metadata = { 'scal-location-constraint': awsLocation,
                        'unique-header': 'second object' };
                await s3.send(new PutObjectCommand(params)).then(() => {
                    process.stdout.write('Putting object succeeded\n');
                }).catch(err => {
                    throw new Error(`Expected success, got error: ${err}`);
                });
                await awsGetCheck(key, correctMD5, correctMD5,
                    awsLocation, result => {
                        assert.strictEqual(result.Metadata
                            ['unique-header'], 'second object');
                    });
            });
        });
    });
});

describeSkipIfNotMultiple('MultipleBackend put object based on bucket location',
() => {
    withV4(sigCfg => {
        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });

        afterEach(async () => {
            process.stdout.write('Emptying bucket\n');
            await bucketUtil.empty(bucket)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(bucket);
            })
            .catch(err => {
                process.stdout.write(`Error in afterEach: ${err}\n`);
                throw err;
            });
        });

        it('should put an object to mem with no location header',
        async () => {
            process.stdout.write('Creating bucket\n');
            await s3.send(new CreateBucketCommand({ Bucket: bucket,
                CreateBucketConfiguration: {
                    LocationConstraint: memLocation,
                },
            }));
            process.stdout.write('Putting object\n');
            const key = `somekey-${genUniqID()}`;
            const params = { Bucket: bucket, Key: key, Body: body };
            await s3.send(new PutObjectCommand(params)).then(() => {
                process.stdout.write('Putting object succeeded\n');
            }).catch(err => {
                throw new Error(`Expected success, got error: ${err}`);
            });
            const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
            assert.strictEqual(res.ETag, `"${correctMD5}"`);
        });

        it('should put an object to file with no location header', async () => {
            process.stdout.write('Creating bucket\n');
            await s3.send(new CreateBucketCommand({ Bucket: bucket,
                CreateBucketConfiguration: {
                    LocationConstraint: fileLocation,
                },
            }));
            process.stdout.write('Putting object\n');
            const key = `somekey-${genUniqID()}`;
            const params = { Bucket: bucket, Key: key, Body: body };
            await s3.send(new PutObjectCommand(params)).then(() => {
                process.stdout.write('Putting object succeeded\n');
            }).catch(err => {
                throw new Error(`Expected success, got error: ${err}`);
            });
            const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
            assert.strictEqual(res.ETag, `"${correctMD5}"`);
        });

        it('should put an object to AWS with no location header', async () => {
            process.stdout.write('Creating bucket\n');
            await s3.send(new CreateBucketCommand({ Bucket: bucket,
                CreateBucketConfiguration: {
                    LocationConstraint: awsLocation,
                },
            }));
            process.stdout.write('Putting object\n');
            const key = `somekey-${genUniqID()}`;
            const params = { Bucket: bucket, Key: key, Body: body };
            await s3.send(new PutObjectCommand(params)).then(() => {
                process.stdout.write('Putting object succeeded\n');
            }).catch(err => {
                throw new Error(`Expected success, got error: ${err}`);
            });
            await awsGetCheck(key, correctMD5, correctMD5, undefined);
        });
    });
});

describe('MultipleBackend put based on request endpoint', () => {
    withV4(sigCfg => {
        before(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });
        after(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(bucket)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(bucket);
            })
            .catch(err => {
                process.stdout.write(`Error in after: ${err}\n`);
                throw err;
            });
        });

        it('should create bucket in corresponding backend', async () => {
            process.stdout.write('Creating bucket');
            
            // Create bucket using AWS SDK v3
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));
            
            const key = `somekey-${genUniqID()}`;
            
            await s3.send(new PutObjectCommand({ 
                Bucket: bucket, 
                Key: key, 
                Body: body 
            }));            
            const locationData = await s3.send(new GetBucketLocationCommand({ Bucket: bucket }));            
            const objectData = await s3.send(new GetObjectCommand({ 
                Bucket: bucket, 
                Key: key 
            }));
            const host = s3.config.endpoint.hostname;
            let endpoint = config.restEndpoints[host];
            // s3 returns '' for us-east-1
            if (endpoint === 'us-east-1') {
                endpoint = '';
            }
            
            assert.strictEqual(locationData.LocationConstraint, endpoint);
            assert.strictEqual(objectData.ETag, `"${correctMD5}"`);
        });
    });
});
