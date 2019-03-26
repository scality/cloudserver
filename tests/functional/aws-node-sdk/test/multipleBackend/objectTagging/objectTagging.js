const assert = require('assert');
const async = require('async');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { describeSkipIfNotMultiple, awsS3, awsBucket, getAwsRetry,
    getAzureClient, getAzureContainerName, convertMD5, memLocation,
    fileLocation, awsLocation, azureLocation, genUniqID,
    isCEPH } = require('../utils');

const azureClient = getAzureClient();
const azureContainerName = getAzureContainerName(azureLocation);
const bucket = `taggingbucket${genUniqID()}`;
const body = Buffer.from('I am a body', 'utf8');
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const emptyMD5 = 'd41d8cd98f00b204e9800998ecf8427e';
const mpuMD5 = 'e4c2438a8f503658547a77959890dcab-1';

const cloudTimeout = 10000;

let bucketUtil;
let s3;

const putParams = { Bucket: bucket, Body: body };

const testBackends = [memLocation, fileLocation, awsLocation];
if (!isCEPH) {
    testBackends.push(azureLocation);
}

const tagString = 'key1=value1&key2=value2';
const putTags = {
    TagSet: [
        {
            Key: 'key1',
            Value: 'value1',
        },
        {
            Key: 'key2',
            Value: 'value2',
        },
    ],
};
const tagObj = { key1: 'value1', key2: 'value2' };

function getAndAssertObjectTags(tagParams, callback) {
    return s3.getObjectTagging(tagParams, (err, res) => {
        expect(res.TagSet.length).toBe(2);
        expect(res.TagSet[0].Key).toBe(putTags.TagSet[0].Key);
        expect(res.TagSet[0].Value).toBe(putTags.TagSet[0].Value);
        expect(res.TagSet[1].Key).toBe(putTags.TagSet[1].Key);
        expect(res.TagSet[1].Value).toBe(putTags.TagSet[1].Value);
        return callback();
    });
}


function awsGet(key, tagCheck, isEmpty, isMpu, callback) {
    process.stdout.write('Getting object from AWS\n');
    getAwsRetry({ key }, 0, (err, res) => {
        expect(err).toEqual(null);
        if (isEmpty) {
            expect(res.ETag).toBe(`"${emptyMD5}"`);
        } else if (isMpu) {
            expect(res.ETag).toBe(`"${mpuMD5}"`);
        } else {
            expect(res.ETag).toBe(`"${correctMD5}"`);
        }
        if (tagCheck) {
            expect(res.TagCount).toBe('2');
        } else {
            expect(res.TagCount).toBe(undefined);
        }
        return callback();
    });
}

function azureGet(key, tagCheck, isEmpty, callback) {
    process.stdout.write('Getting object from Azure\n');
    azureClient.getBlobProperties(azureContainerName, key,
    (err, res) => {
        expect(err).toEqual(null);
        const resMD5 = convertMD5(res.contentSettings.contentMD5);
        if (isEmpty) {
            expect(resMD5).toBe(`${emptyMD5}`);
        } else {
            expect(resMD5).toBe(`${correctMD5}`);
        }
        if (tagCheck) {
            expect(res.metadata.tags).toBe(JSON.stringify(tagObj));
        } else {
            expect(res.metadata.tags).toBe(undefined);
        }
        return callback();
    });
}

function getObject(key, backend, tagCheck, isEmpty, isMpu, callback) {
    function get(cb) {
        process.stdout.write('Getting object\n');
        s3.getObject({ Bucket: bucket, Key: key }, (err, res) => {
            expect(err).toEqual(null);
            if (isEmpty) {
                expect(res.ETag).toBe(`"${emptyMD5}"`);
            } else if (isMpu) {
                expect(res.ETag).toBe(`"${mpuMD5}"`);
            } else {
                expect(res.ETag).toBe(`"${correctMD5}"`);
            }
            expect(res.Metadata['scal-location-constraint']).toBe(backend);
            if (tagCheck) {
                expect(res.TagCount).toBe('2');
            } else {
                expect(res.TagCount).toBe(undefined);
            }
            return cb();
        });
    }
    if (backend === 'awsbackend') {
        get(() => awsGet(key, tagCheck, isEmpty, isMpu, callback));
    } else if (backend === 'azurebackend') {
        setTimeout(() => {
            get(() => azureGet(key, tagCheck, isEmpty, callback));
        }, cloudTimeout);
    } else {
        get(callback);
    }
}

function mpuWaterfall(params, cb) {
    async.waterfall([
        next => s3.createMultipartUpload(params, (err, data) => {
            expect(err).toEqual(null);
            next(null, data.UploadId);
        }),
        (uploadId, next) => {
            const partParams = { Bucket: bucket, Key: params.Key, PartNumber: 1,
                UploadId: uploadId, Body: body };
            s3.uploadPart(partParams, (err, result) => {
                expect(err).toEqual(null);
                next(null, uploadId, result.ETag);
            });
        },
        (uploadId, eTag, next) => {
            const compParams = { Bucket: bucket, Key: params.Key,
                MultipartUpload: {
                    Parts: [{ ETag: eTag, PartNumber: 1 }],
                },
                UploadId: uploadId };
            s3.completeMultipartUpload(compParams, err => {
                expect(err).toEqual(null);
                next();
            });
        },
    ], err => {
        expect(err).toEqual(null);
        cb();
    });
}

describeSkipIfNotMultiple('Object tagging with multiple backends',
function testSuite() {
    if (!process.env.S3_END_TO_END) {
        this.retries(2);
    }
    this.timeout(80000);
    withV4(sigCfg => {
        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
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

        describe('putObject with tags and putObjectTagging', () => {
            testBackends.forEach(backend => {
                const itSkipIfAzureOrCeph = backend === 'azurebackend' ||
                                            isCEPH ? it.skip : it;
                test(`should put an object with tags to ${backend} backend`, done => {
                    const key = `somekey-${genUniqID()}`;
                    const params = Object.assign({ Key: key, Tagging: tagString,
                        Metadata: { 'scal-location-constraint': backend } },
                         putParams);
                    process.stdout.write('Putting object\n');
                    s3.putObject(params, err => {
                        expect(err).toEqual(null);
                        getObject(key, backend, true, false, false, done);
                    });
                });

                test(`should put a 0 byte object with tags to ${backend} backend`, done => {
                    const key = `somekey-${genUniqID()}`;
                    const params = {
                        Bucket: bucket,
                        Key: key,
                        Tagging: tagString,
                        Metadata: { 'scal-location-constraint': backend },
                    };
                    process.stdout.write('Putting object\n');
                    s3.putObject(params, err => {
                        expect(err).toEqual(null);
                        getObject(key, backend, true, true, false, done);
                    });
                });

                test(`should put tags to preexisting object in ${backend} ` +
                'backend', done => {
                    const key = `somekey-${genUniqID()}`;
                    const params = Object.assign({ Key: key, Metadata:
                        { 'scal-location-constraint': backend } }, putParams);
                    process.stdout.write('Putting object\n');
                    s3.putObject(params, err => {
                        expect(err).toEqual(null);
                        const putTagParams = { Bucket: bucket, Key: key,
                            Tagging: putTags };
                        process.stdout.write('Putting object tags\n');
                        s3.putObjectTagging(putTagParams, err => {
                            expect(err).toEqual(null);
                            getObject(key, backend, true, false, false, done);
                        });
                    });
                });

                test('should put tags to preexisting 0 byte object in ' +
                `${backend} backend`, done => {
                    const key = `somekey-${genUniqID()}`;
                    const params = {
                        Bucket: bucket,
                        Key: key,
                        Metadata: { 'scal-location-constraint': backend },
                    };
                    process.stdout.write('Putting object\n');
                    s3.putObject(params, err => {
                        expect(err).toEqual(null);
                        const putTagParams = { Bucket: bucket, Key: key,
                            Tagging: putTags };
                        process.stdout.write('Putting object tags\n');
                        s3.putObjectTagging(putTagParams, err => {
                            expect(err).toEqual(null);
                            getObject(key, backend, true, true, false, done);
                        });
                    });
                });

                itSkipIfAzureOrCeph('should put tags to completed MPU ' +
                `object in ${backend}`, done => {
                    const key = `somekey-${genUniqID()}`;
                    const params = {
                        Bucket: bucket,
                        Key: key,
                        Metadata: { 'scal-location-constraint': backend },
                    };
                    mpuWaterfall(params, () => {
                        const putTagParams = { Bucket: bucket, Key: key,
                            Tagging: putTags };
                        process.stdout.write('Putting object\n');
                        s3.putObjectTagging(putTagParams, err => {
                            expect(err).toEqual(null);
                            getObject(key, backend, true, false, true, done);
                        });
                    });
                });
            });

            test('should not return error putting tags to correct object ' +
            'version in AWS, even if a delete marker was created directly ' +
            'on AWS before tags are put', done => {
                const key = `somekey-${genUniqID()}`;
                const params = Object.assign({ Key: key, Metadata:
                    { 'scal-location-constraint': awsLocation } }, putParams);
                process.stdout.write('Putting object\n');
                s3.putObject(params, err => {
                    expect(err).toEqual(null);
                    process.stdout.write('Deleting object from AWS\n');
                    awsS3.deleteObject({ Bucket: awsBucket, Key: key }, err => {
                        expect(err).toEqual(null);
                        const putTagParams = { Bucket: bucket, Key: key,
                            Tagging: putTags };
                        process.stdout.write('Putting object tags\n');
                        s3.putObjectTagging(putTagParams, err => {
                            expect(err).toBe(null);
                            done();
                        });
                    });
                });
            });
        });

        describe('getObjectTagging', () => {
            testBackends.forEach(backend => {
                test(`should get tags from object on ${backend} backend`, done => {
                    const key = `somekey-${genUniqID()}`;
                    const params = Object.assign({ Key: key, Tagging: tagString,
                        Metadata: { 'scal-location-constraint': backend } },
                        putParams);
                    process.stdout.write('Putting object\n');
                    s3.putObject(params, err => {
                        expect(err).toEqual(null);
                        const tagParams = { Bucket: bucket, Key: key };
                        getAndAssertObjectTags(tagParams, done);
                    });
                });
            });

            test('should not return error on getting tags from object that has ' +
            'had a delete marker put directly on AWS', done => {
                const key = `somekey-${genUniqID()}`;
                const params = Object.assign({ Key: key, Tagging: tagString,
                    Metadata: { 'scal-location-constraint': awsLocation } },
                    putParams);
                process.stdout.write('Putting object\n');
                s3.putObject(params, err => {
                    expect(err).toEqual(null);
                    process.stdout.write('Deleting object from AWS\n');
                    awsS3.deleteObject({ Bucket: awsBucket, Key: key }, err => {
                        expect(err).toEqual(null);
                        const tagParams = { Bucket: bucket, Key: key };
                        getAndAssertObjectTags(tagParams, done);
                    });
                });
            });
        });

        describe('deleteObjectTagging', () => {
            testBackends.forEach(backend => {
                test(`should delete tags from object on ${backend} backend`, done => {
                    const key = `somekey-${genUniqID()}`;
                    const params = Object.assign({ Key: key, Tagging: tagString,
                        Metadata: { 'scal-location-constraint': backend } },
                        putParams);
                    process.stdout.write('Putting object\n');
                    s3.putObject(params, err => {
                        expect(err).toEqual(null);
                        const tagParams = { Bucket: bucket, Key: key };
                        s3.deleteObjectTagging(tagParams, err => {
                            expect(err).toEqual(null);
                            getObject(key, backend, false, false, false, done);
                        });
                    });
                });
            });

            test('should not return error on deleting tags from object that ' +
            'has had delete markers put directly on AWS', done => {
                const key = `somekey-${genUniqID()}`;
                const params = Object.assign({ Key: key, Tagging: tagString,
                    Metadata: { 'scal-location-constraint': awsLocation } },
                    putParams);
                process.stdout.write('Putting object\n');
                s3.putObject(params, err => {
                    expect(err).toEqual(null);
                    process.stdout.write('Deleting object from AWS\n');
                    awsS3.deleteObject({ Bucket: awsBucket, Key: key }, err => {
                        expect(err).toEqual(null);
                        const tagParams = { Bucket: bucket, Key: key };
                        s3.deleteObjectTagging(tagParams, err => {
                            expect(err).toBe(null);
                            done();
                        });
                    });
                });
            });
        });
    });
});
