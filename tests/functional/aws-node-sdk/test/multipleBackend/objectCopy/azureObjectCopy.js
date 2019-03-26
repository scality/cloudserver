const assert = require('assert');
const async = require('async');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const constants = require('../../../../../../constants');
const {
    describeSkipIfNotMultipleOrCeph,
    getAzureClient,
    getAzureContainerName,
    convertMD5,
    memLocation,
    awsLocation,
    azureLocation,
    azureLocation2,
    azureLocationMismatch,
    genUniqID,
} = require('../utils');
const { createEncryptedBucketPromise } =
    require('../../../lib/utility/createEncryptedBucket');

const azureClient = getAzureClient();
const azureContainerName = getAzureContainerName(azureLocation);

const bucket = `objectcopybucket${genUniqID()}`;
const bucketAzure = `objectcopyazure${genUniqID()}`;
const body = Buffer.from('I am a body', 'utf8');
const bigBody = new Buffer(5 * 1024 * 1024);
const normalMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const bigMD5 = '5f363e0e58a95f06cbe9bbc662c5dfb6';
const emptyMD5 = 'd41d8cd98f00b204e9800998ecf8427e';
const locMetaHeader = constants.objectLocationConstraintHeader.substring(11);

const azureTimeout = 40000;

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
        expect(err).toEqual(null);
        if (objSize && objSize.empty) {
            expect(result.ETag).toBe(`"${emptyMD5}"`);
        } else if (objSize && objSize.big) {
            expect(result.ETag).toBe(`"${bigMD5}"`);
        } else {
            expect(result.ETag).toBe(`"${normalMD5}"`);
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
        expect(err).toEqual(null);
        const [sourceRes, destRes, azureRes] = results;
        const convertedMD5 = convertMD5(azureRes[0].contentSettings.contentMD5);
        if (objSize && objSize.empty) {
            expect(sourceRes.ETag).toBe(`"${emptyMD5}"`);
            expect(destRes.ETag).toBe(`"${emptyMD5}"`);
            expect(convertedMD5).toBe(`${emptyMD5}`);
            expect('0').toBe(azureRes[0].contentLength);
        } else if (objSize && objSize.big) {
            expect(sourceRes.ETag).toBe(`"${bigMD5}"`);
            expect(destRes.ETag).toBe(`"${bigMD5}"`);
            if (process.env.ENABLE_KMS_ENCRYPTION === 'true') {
                expect(sourceRes.ServerSideEncryption).toBe('AES256');
                expect(destRes.ServerSideEncryption).toBe('AES256');
            } else {
                expect(convertedMD5).toBe(`${bigMD5}`);
            }
        } else {
            if (process.env.ENABLE_KMS_ENCRYPTION === 'true') {
                expect(sourceRes.ServerSideEncryption).toBe('AES256');
                expect(destRes.ServerSideEncryption).toBe('AES256');
            } else {
                expect(sourceRes.ETag).toBe(`"${normalMD5}"`);
                expect(destRes.ETag).toBe(`"${normalMD5}"`);
                expect(convertedMD5).toBe(`${normalMD5}`);
            }
        }
        if (mdDirective === 'COPY') {
            expect(sourceRes.Metadata['test-header']).toBe(destRes.Metadata['test-header']);
            expect(azureRes[0].metadata.test_header).toBe(destRes.Metadata['test-header']);
        }
        expect(sourceRes.ContentLength).toBe(destRes.ContentLength);
        expect(sourceRes.Metadata[locMetaHeader]).toBe(sourceLoc);
        expect(destRes.Metadata[locMetaHeader]).toBe(destLoc);
        callback();
    });
}

describeSkipIfNotMultipleOrCeph('MultipleBackend object copy: Azure',
function testSuite() {
    this.timeout(250000);
    withV4(sigCfg => {
        beforeEach(() => {
            this.currentTest.key = `azureputkey-${genUniqID()}`;
            this.currentTest.copyKey = `azurecopyKey-${genUniqID()}`;
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
            .then(() => s3.createBucketAsync({ Bucket: bucketAzure,
              CreateBucketConfiguration: {
                  LocationConstraint: azureLocation,
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
            .then(() => bucketUtil.empty(bucketAzure))
            .then(() => {
                process.stdout.write(`Deleting bucket: ${bucket}\n`);
                return bucketUtil.deleteOne(bucket);
            })
            .then(() => {
                process.stdout.write(`Deleting bucket: ${bucketAzure}\n`);
                return bucketUtil.deleteOne(bucketAzure);
            })
            .catch(err => {
                process.stdout.write(`Error in afterEach: ${err}\n`);
                throw err;
            });
        });

        test('should copy an object from mem to Azure', done => {
            putSourceObj(this.test.key, memLocation, null, bucket, () => {
                const copyParams = {
                    Bucket: bucket,
                    Key: this.test.copyKey,
                    CopySource: `/${bucket}/${this.test.key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: { 'scal-location-constraint': azureLocation },
                };
                s3.copyObject(copyParams, (err, result) => {
                    expect(err).toEqual(null);
                    expect(result.CopyObjectResult.ETag).toBe(`"${normalMD5}"`);
                    assertGetObjects(this.test.key, bucket, memLocation,
                        this.test.copyKey, bucket, azureLocation,
                        this.test.copyKey, 'REPLACE', null, done);
                });
            });
        });

        test(
            'should copy an object with no location contraint from mem to Azure',
            done => {
                putSourceObj(this.test.key, null, null, bucket, () => {
                    const copyParams = {
                        Bucket: bucketAzure,
                        Key: this.test.copyKey,
                        CopySource: `/${bucket}/${this.test.key}`,
                        MetadataDirective: 'COPY',
                    };
                    s3.copyObject(copyParams, (err, result) => {
                        expect(err).toEqual(null);
                        expect(result.CopyObjectResult.ETag).toBe(`"${normalMD5}"`);
                        assertGetObjects(this.test.key, bucket, undefined,
                            this.test.copyKey, bucketAzure, undefined,
                            this.test.copyKey, 'COPY', null, done);
                    });
                });
            }
        );

        test('should copy an object from Azure to mem', done => {
            putSourceObj(this.test.key, azureLocation, null, bucket, () => {
                const copyParams = {
                    Bucket: bucket,
                    Key: this.test.copyKey,
                    CopySource: `/${bucket}/${this.test.key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: { 'scal-location-constraint': memLocation },
                };
                s3.copyObject(copyParams, (err, result) => {
                    expect(err).toEqual(null);
                    expect(result.CopyObjectResult.ETag).toBe(`"${normalMD5}"`);
                    assertGetObjects(this.test.key, bucket, azureLocation,
                        this.test.copyKey, bucket, memLocation, this.test.key,
                        'REPLACE', null, done);
                });
            });
        });

        test('should copy an object from AWS to Azure', done => {
            putSourceObj(this.test.key, awsLocation, null, bucket, () => {
                const copyParams = {
                    Bucket: bucket,
                    Key: this.test.copyKey,
                    CopySource: `/${bucket}/${this.test.key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: { 'scal-location-constraint': azureLocation },
                };
                s3.copyObject(copyParams, (err, result) => {
                    expect(err).toEqual(null);
                    expect(result.CopyObjectResult.ETag).toBe(`"${normalMD5}"`);
                    assertGetObjects(this.test.key, bucket, awsLocation,
                        this.test.copyKey, bucket, azureLocation,
                        this.test.copyKey, 'REPLACE', null, done);
                });
            });
        });

        test('should copy an object from Azure to AWS', done => {
            putSourceObj(this.test.key, azureLocation, null, bucket, () => {
                const copyParams = {
                    Bucket: bucket,
                    Key: this.test.copyKey,
                    CopySource: `/${bucket}/${this.test.key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: { 'scal-location-constraint': awsLocation },
                };
                s3.copyObject(copyParams, (err, result) => {
                    expect(err).toEqual(null);
                    expect(result.CopyObjectResult.ETag).toBe(`"${normalMD5}"`);
                    assertGetObjects(this.test.key, bucket, azureLocation,
                        this.test.copyKey, bucket, awsLocation, this.test.key,
                        'REPLACE', null, done);
                });
            });
        });

        test('should copy an object from Azure to mem with "REPLACE" directive ' +
        'and no location constraint md', done => {
            putSourceObj(this.test.key, azureLocation, null, bucket, () => {
                const copyParams = {
                    Bucket: bucket,
                    Key: this.test.copyKey,
                    CopySource: `/${bucket}/${this.test.key}`,
                    MetadataDirective: 'REPLACE',
                };
                s3.copyObject(copyParams, (err, result) => {
                    expect(err).toEqual(null);
                    expect(result.CopyObjectResult.ETag).toBe(`"${normalMD5}"`);
                    assertGetObjects(this.test.key, bucket, azureLocation,
                        this.test.copyKey, bucket, undefined, this.test.key,
                        'REPLACE', null, done);
                });
            });
        });

        test('should copy an object from mem to Azure with "REPLACE" directive ' +
        'and no location constraint md', done => {
            putSourceObj(this.test.key, null, null, bucket, () => {
                const copyParams = {
                    Bucket: bucketAzure,
                    Key: this.test.copyKey,
                    CopySource: `/${bucket}/${this.test.key}`,
                    MetadataDirective: 'REPLACE',
                };
                s3.copyObject(copyParams, (err, result) => {
                    expect(err).toEqual(null);
                    expect(result.CopyObjectResult.ETag).toBe(`"${normalMD5}"`);
                    assertGetObjects(this.test.key, bucket, undefined,
                        this.test.copyKey, bucketAzure, undefined,
                        this.test.copyKey, 'REPLACE', null, done);
                });
            });
        });

        test('should copy an object from Azure to Azure showing sending ' +
        'metadata location constraint this doesn\'t matter with COPY directive', done => {
            putSourceObj(this.test.key, azureLocation, null, bucketAzure,
            () => {
                const copyParams = {
                    Bucket: bucketAzure,
                    Key: this.test.copyKey,
                    CopySource: `/${bucketAzure}/${this.test.key}`,
                    MetadataDirective: 'COPY',
                    Metadata: { 'scal-location-constraint': memLocation },
                };
                s3.copyObject(copyParams, (err, result) => {
                    expect(err).toEqual(null);
                    expect(result.CopyObjectResult.ETag).toBe(`"${normalMD5}"`);
                    assertGetObjects(this.test.key, bucketAzure, azureLocation,
                        this.test.copyKey, bucketAzure, azureLocation,
                        this.test.copyKey, 'COPY', null, done);
                });
            });
        });

        test('should copy an object with no location constraint from Azure to ' +
        'Azure relying on the bucket location constraint', done => {
            putSourceObj(this.test.key, null, null, bucketAzure,
            () => {
                const copyParams = {
                    Bucket: bucketAzure,
                    Key: this.test.copyKey,
                    CopySource: `/${bucketAzure}/${this.test.key}`,
                    MetadataDirective: 'COPY',
                };
                s3.copyObject(copyParams, (err, result) => {
                    expect(err).toEqual(null);
                    expect(result.CopyObjectResult.ETag).toBe(`"${normalMD5}"`);
                    assertGetObjects(this.test.key, bucketAzure, undefined,
                        this.test.copyKey, bucketAzure, undefined,
                        this.test.copyKey, 'COPY', null, done);
                });
            });
        });

        test('should copy an object from Azure to mem because bucket ' +
        'destination location is mem', done => {
            putSourceObj(this.test.key, azureLocation, null, bucket, () => {
                const copyParams = {
                    Bucket: bucket,
                    Key: this.test.copyKey,
                    CopySource: `/${bucket}/${this.test.key}`,
                    MetadataDirective: 'COPY',
                };
                s3.copyObject(copyParams, (err, result) => {
                    expect(err).toEqual(null);
                    expect(result.CopyObjectResult.ETag).toBe(`"${normalMD5}"`);
                    assertGetObjects(this.test.key, bucket, azureLocation,
                        this.test.copyKey, bucket, memLocation,
                        this.test.key, 'COPY', null, done);
                });
            });
        });

        test('should copy an object on Azure to a different Azure ' +
        'account without source object READ access', done => {
            putSourceObj(this.test.key, azureLocation2, null, bucket, () => {
                const copyParams = {
                    Bucket: bucket,
                    Key: this.test.copyKey,
                    CopySource: `/${bucket}/${this.test.key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: { 'scal-location-constraint': azureLocation },
                };
                s3.copyObject(copyParams, (err, result) => {
                    expect(err).toEqual(null);
                    expect(result.CopyObjectResult.ETag).toBe(`"${normalMD5}"`);
                    assertGetObjects(this.test.key, bucket, azureLocation2,
                        this.test.copyKey, bucket, azureLocation,
                        this.test.copyKey, 'REPLACE', null, done);
                });
            });
        });

        test('should copy a 5MB object on Azure to a different Azure ' +
        'account without source object READ access', done => {
            putSourceObj(this.test.key, azureLocation2, { big: true }, bucket,
            () => {
                const copyParams = {
                    Bucket: bucket,
                    Key: this.test.copyKey,
                    CopySource: `/${bucket}/${this.test.key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: { 'scal-location-constraint': azureLocation },
                };
                s3.copyObject(copyParams, (err, result) => {
                    expect(err).toEqual(null);
                    expect(result.CopyObjectResult.ETag).toBe(`"${bigMD5}"`);
                    assertGetObjects(this.test.key, bucket, azureLocation2,
                        this.test.copyKey, bucket, azureLocation,
                        this.test.copyKey, 'REPLACE', { big: true }, done);
                });
            });
        });

        test('should copy an object from bucketmatch=false ' +
        'Azure location to MPU with a bucketmatch=false Azure location', done => {
            putSourceObj(this.test.key, azureLocationMismatch, null, bucket,
            () => {
                const copyParams = {
                    Bucket: bucket,
                    Key: this.test.copyKey,
                    CopySource: `/${bucket}/${this.test.key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: { 'scal-location-constraint':
                    azureLocationMismatch },
                };
                s3.copyObject(copyParams, (err, result) => {
                    expect(err).toEqual(null);
                    expect(result.CopyObjectResult.ETag).toBe(`"${normalMD5}"`);
                    assertGetObjects(this.test.key, bucket,
                        azureLocationMismatch,
                        this.test.copyKey, bucket, azureLocationMismatch,
                        `${bucket}/${this.test.copyKey}`, 'REPLACE', null,
                        done);
                });
            });
        });

        test('should copy an object from bucketmatch=false ' +
        'Azure location to MPU with a bucketmatch=true Azure location', done => {
            putSourceObj(this.test.key, azureLocationMismatch, null, bucket,
            () => {
                const copyParams = {
                    Bucket: bucket,
                    Key: this.test.copyKey,
                    CopySource: `/${bucket}/${this.test.key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: { 'scal-location-constraint': azureLocation },
                };
                s3.copyObject(copyParams, (err, result) => {
                    expect(err).toEqual(null);
                    expect(result.CopyObjectResult.ETag).toBe(`"${normalMD5}"`);
                    assertGetObjects(this.test.key, bucket,
                        azureLocationMismatch,
                        this.test.copyKey, bucket, azureLocation,
                        this.test.copyKey, 'REPLACE', null, done);
                });
            });
        });

        test('should copy an object from bucketmatch=true ' +
        'Azure location to MPU with a bucketmatch=false Azure location', done => {
            putSourceObj(this.test.key, azureLocation, null, bucket, () => {
                const copyParams = {
                    Bucket: bucket,
                    Key: this.test.copyKey,
                    CopySource: `/${bucket}/${this.test.key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: { 'scal-location-constraint':
                    azureLocationMismatch },
                };
                s3.copyObject(copyParams, (err, result) => {
                    expect(err).toEqual(null);
                    expect(result.CopyObjectResult.ETag).toBe(`"${normalMD5}"`);
                    assertGetObjects(this.test.key, bucket,
                        azureLocation,
                        this.test.copyKey, bucket, azureLocationMismatch,
                        `${bucket}/${this.test.copyKey}`,
                        'REPLACE', null, done);
                });
            });
        });

        test('should copy a 0-byte object from mem to Azure', done => {
            putSourceObj(this.test.key, memLocation, { empty: true }, bucket,
            () => {
                const copyParams = {
                    Bucket: bucket,
                    Key: this.test.copyKey,
                    CopySource: `/${bucket}/${this.test.key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: { 'scal-location-constraint': azureLocation },
                };
                s3.copyObject(copyParams, (err, result) => {
                    expect(err).toEqual(null);
                    expect(result.CopyObjectResult.ETag).toBe(`"${emptyMD5}"`);
                    assertGetObjects(this.test.key, bucket, memLocation,
                        this.test.copyKey, bucket, azureLocation,
                        this.test.copyKey, 'REPLACE', { empty: true }, done);
                });
            });
        });

        test('should copy a 0-byte object on Azure', done => {
            putSourceObj(this.test.key, azureLocation, { empty: true }, bucket,
            () => {
                const copyParams = {
                    Bucket: bucket,
                    Key: this.test.copyKey,
                    CopySource: `/${bucket}/${this.test.key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: { 'scal-location-constraint': azureLocation },
                };
                s3.copyObject(copyParams, (err, result) => {
                    expect(err).toEqual(null);
                    expect(result.CopyObjectResult.ETag).toBe(`"${emptyMD5}"`);
                    assertGetObjects(this.test.key, bucket, azureLocation,
                        this.test.copyKey, bucket, azureLocation,
                        this.test.copyKey, 'REPLACE', { empty: true }, done);
                });
            });
        });

        test('should copy a 5MB object from mem to Azure', done => {
            putSourceObj(this.test.key, memLocation, { big: true }, bucket,
            () => {
                const copyParams = {
                    Bucket: bucket,
                    Key: this.test.copyKey,
                    CopySource: `/${bucket}/${this.test.key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: { 'scal-location-constraint': azureLocation },
                };
                s3.copyObject(copyParams, (err, result) => {
                    expect(err).toEqual(null);
                    expect(result.CopyObjectResult.ETag).toBe(`"${bigMD5}"`);
                    setTimeout(() => {
                        assertGetObjects(this.test.key, bucket, memLocation,
                            this.test.copyKey, bucket, azureLocation,
                            this.test.copyKey, 'REPLACE', { big: true }, done);
                    }, azureTimeout);
                });
            });
        });

        test('should copy a 5MB object on Azure', done => {
            putSourceObj(this.test.key, azureLocation, { big: true }, bucket,
            () => {
                const copyParams = {
                    Bucket: bucket,
                    Key: this.test.copyKey,
                    CopySource: `/${bucket}/${this.test.key}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: { 'scal-location-constraint': azureLocation },
                };
                s3.copyObject(copyParams, (err, result) => {
                    expect(err).toEqual(null);
                    expect(result.CopyObjectResult.ETag).toBe(`"${bigMD5}"`);
                    setTimeout(() => {
                        assertGetObjects(this.test.key, bucket, azureLocation,
                            this.test.copyKey, bucket, azureLocation,
                            this.test.copyKey, 'REPLACE', { big: true }, done);
                    }, azureTimeout);
                });
            });
        });

        test('should return error if Azure source object has ' +
        'been deleted', done => {
            putSourceObj(this.test.key, azureLocation, null, bucket,
            () => {
                azureClient.deleteBlob(azureContainerName, this.test.key,
                err => {
                    expect(err).toEqual(null);
                    const copyParams = {
                        Bucket: bucket,
                        Key: this.test.copyKey,
                        CopySource: `/${bucket}/${this.test.key}`,
                        MetadataDirective: 'COPY',
                    };
                    s3.copyObject(copyParams, err => {
                        expect(err.code).toBe('ServiceUnavailable');
                        done();
                    });
                });
            });
        });
    });
});
