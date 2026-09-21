const assert = require('assert');
const async = require('async');
const {
    CreateBucketCommand,
    PutObjectCommand,
    GetObjectCommand,
    PutObjectTaggingCommand,
    DeleteObjectTaggingCommand,
    GetObjectTaggingCommand,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    CompleteMultipartUploadCommand,
} = require('@aws-sdk/client-s3');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const {
    describeSkipIfNotMultiple,
    awsS3,
    awsBucket,
    getAwsRetry,
    getAzureClient,
    getAzureContainerName,
    convertMD5,
    memLocation,
    fileLocation,
    awsLocation,
    azureLocation,
    genUniqID,
} = require('../utils');

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
testBackends.push(azureLocation);

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
    return s3
        .send(new GetObjectTaggingCommand(tagParams))
        .then(res => {
            assert.strictEqual(res.TagSet.length, 2);
            assert.strictEqual(res.TagSet[0].Key, putTags.TagSet[0].Key);
            assert.strictEqual(res.TagSet[0].Value, putTags.TagSet[0].Value);
            assert.strictEqual(res.TagSet[1].Key, putTags.TagSet[1].Key);
            assert.strictEqual(res.TagSet[1].Value, putTags.TagSet[1].Value);
            callback();
        })
        .catch(callback);
}

function awsGet(key, tagCheck, isEmpty, isMpu, callback) {
    process.stdout.write('Getting object from AWS\n');
    getAwsRetry({ key }, 0, (err, res) => {
        assert.equal(err, null);
        if (isEmpty) {
            assert.strictEqual(res.ETag, `"${emptyMD5}"`);
        } else if (isMpu) {
            assert.strictEqual(res.ETag, `"${mpuMD5}"`);
        } else {
            assert.strictEqual(res.ETag, `"${correctMD5}"`);
        }
        if (tagCheck) {
            assert.strictEqual(res.TagCount, 2);
        } else {
            assert.strictEqual(res.TagCount, undefined);
        }
        return callback();
    });
}

function azureGet(key, tagCheck, isEmpty, callback) {
    process.stdout.write('Getting object from Azure\n');
    azureClient
        .getContainerClient(azureContainerName)
        .getProperties(key)
        .then(
            res => {
                const resMD5 = convertMD5(res.contentSettings.contentMD5);
                if (isEmpty) {
                    assert.strictEqual(resMD5, `${emptyMD5}`);
                } else {
                    assert.strictEqual(resMD5, `${correctMD5}`);
                }
                if (tagCheck) {
                    assert.strictEqual(res.metadata.tags, JSON.stringify(tagObj));
                } else {
                    assert.strictEqual(res.metadata.tags, undefined);
                }
                return callback();
            },
            err => {
                assert.equal(err, null);
                return callback();
            },
        );
}

function getObject(key, backend, tagCheck, isEmpty, isMpu, callback) {
    function get(cb) {
        process.stdout.write('Getting object\n');
        s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }))
            .then(res => {
                if (isEmpty) {
                    assert.strictEqual(res.ETag, `"${emptyMD5}"`);
                } else if (isMpu) {
                    assert.strictEqual(res.ETag, `"${mpuMD5}"`);
                } else {
                    assert.strictEqual(res.ETag, `"${correctMD5}"`);
                }
                assert.strictEqual(res.Metadata['scal-location-constraint'], backend);
                if (tagCheck) {
                    assert.strictEqual(res.TagCount, 2);
                } else {
                    assert.strictEqual(res.TagCount, undefined);
                }
                cb();
            })
            .catch(cb);
    }
    if (backend === 'awsbackend') {
        get(err => {
            if (err) {
                callback(err);
                return;
            }
            awsGet(key, tagCheck, isEmpty, isMpu, callback);
        });
    } else if (backend === 'azurebackend') {
        setTimeout(() => {
            get(err => {
                if (err) {
                    callback(err);
                    return;
                }
                azureGet(key, tagCheck, isEmpty, callback);
            });
        }, cloudTimeout);
    } else {
        get(callback);
    }
}

function mpuWaterfall(params, cb) {
    async.waterfall(
        [
            next => {
                s3.send(new CreateMultipartUploadCommand(params))
                    .then(data => next(null, data.UploadId))
                    .catch(next);
            },
            (uploadId, next) => {
                const partParams = { Bucket: bucket, Key: params.Key, PartNumber: 1, UploadId: uploadId, Body: body };
                s3.send(new UploadPartCommand(partParams))
                    .then(result => next(null, uploadId, result.ETag))
                    .catch(next);
            },
            (uploadId, eTag, next) => {
                const compParams = {
                    Bucket: bucket,
                    Key: params.Key,
                    MultipartUpload: {
                        Parts: [{ ETag: eTag, PartNumber: 1 }],
                    },
                    UploadId: uploadId,
                };
                s3.send(new CompleteMultipartUploadCommand(compParams))
                    .then(() => next())
                    .catch(next);
            },
        ],
        err => {
            assert.equal(err, null);
            cb(err);
        },
    );
}

describeSkipIfNotMultiple('Object tagging with multiple backends', function testSuite() {
    if (!process.env.S3_END_TO_END) {
        this.retries(2);
    }
    this.timeout(80000);
    withV4(sigCfg => {
        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.send(new CreateBucketCommand({ Bucket: bucket })).catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil
                .empty(bucket)
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
                const itSkipIfAzure = backend === 'azurebackend' ? it.skip : it;
                it(`should put an object with tags to ${backend} backend`, done => {
                    const key = `somekey-${genUniqID()}`;
                    const params = Object.assign(
                        { Key: key, Tagging: tagString, Metadata: { 'scal-location-constraint': backend } },
                        putParams,
                    );
                    process.stdout.write('Putting object\n');
                    s3.send(new PutObjectCommand(params))
                        .then(() => {
                            getObject(key, backend, true, false, false, done);
                        })
                        .catch(done);
                });

                it(`should put a 0 byte object with tags to ${backend} backend`, done => {
                    const key = `somekey-${genUniqID()}`;
                    const params = {
                        Bucket: bucket,
                        Key: key,
                        Tagging: tagString,
                        Metadata: { 'scal-location-constraint': backend },
                    };
                    process.stdout.write('Putting object\n');
                    s3.send(new PutObjectCommand(params))
                        .then(() => {
                            getObject(key, backend, true, true, false, done);
                        })
                        .catch(done);
                });

                it(`should put tags to preexisting object in ${backend} ` + 'backend', done => {
                    const key = `somekey-${genUniqID()}`;
                    const params = Object.assign(
                        { Key: key, Metadata: { 'scal-location-constraint': backend } },
                        putParams,
                    );
                    process.stdout.write('Putting object\n');
                    s3.send(new PutObjectCommand(params))
                        .then(() => {
                            const putTagParams = { Bucket: bucket, Key: key, Tagging: putTags };
                            process.stdout.write('Putting object tags\n');
                            return s3.send(new PutObjectTaggingCommand(putTagParams));
                        })
                        .then(() => {
                            getObject(key, backend, true, false, false, done);
                        })
                        .catch(done);
                });

                it('should put tags to preexisting 0 byte object in ' + `${backend} backend`, done => {
                    const key = `somekey-${genUniqID()}`;
                    const params = {
                        Bucket: bucket,
                        Key: key,
                        Metadata: { 'scal-location-constraint': backend },
                    };
                    process.stdout.write('Putting object\n');
                    s3.send(new PutObjectCommand(params))
                        .then(() => {
                            const putTagParams = { Bucket: bucket, Key: key, Tagging: putTags };
                            process.stdout.write('Putting object tags\n');
                            return s3.send(new PutObjectTaggingCommand(putTagParams));
                        })
                        .then(() => {
                            getObject(key, backend, true, true, false, done);
                        })
                        .catch(done);
                });

                itSkipIfAzure('should put tags to completed MPU ' + `object in ${backend}`, done => {
                    const key = `somekey-${genUniqID()}`;
                    const params = {
                        Bucket: bucket,
                        Key: key,
                        Metadata: { 'scal-location-constraint': backend },
                    };
                    mpuWaterfall(params, err => {
                        if (err) {
                            done(err);
                            return;
                        }
                        const putTagParams = { Bucket: bucket, Key: key, Tagging: putTags };
                        process.stdout.write('Putting object\n');
                        s3.send(new PutObjectTaggingCommand(putTagParams))
                            .then(() => {
                                getObject(key, backend, true, false, true, done);
                            })
                            .catch(done);
                    });
                });
            });

            it(
                'should not return error putting tags to correct object ' +
                    'version in AWS, even if a delete marker was created directly ' +
                    'on AWS before tags are put',
                done => {
                    const key = `somekey-${genUniqID()}`;
                    const params = Object.assign(
                        { Key: key, Metadata: { 'scal-location-constraint': awsLocation } },
                        putParams,
                    );
                    process.stdout.write('Putting object\n');
                    s3.send(new PutObjectCommand(params))
                        .then(
                            () =>
                                new Promise((resolve, reject) => {
                                    process.stdout.write('Deleting object from AWS\n');
                                    awsS3.deleteObject({ Bucket: awsBucket, Key: key }, err => {
                                        if (err) {
                                            reject(err);
                                            return;
                                        }
                                        resolve();
                                    });
                                }),
                        )
                        .then(() => {
                            const putTagParams = { Bucket: bucket, Key: key, Tagging: putTags };
                            process.stdout.write('Putting object tags\n');
                            return s3.send(new PutObjectTaggingCommand(putTagParams));
                        })
                        .then(() => done())
                        .catch(done);
                },
            );
        });

        describe('getObjectTagging', () => {
            testBackends.forEach(backend => {
                it(`should get tags from object on ${backend} backend`, done => {
                    const key = `somekey-${genUniqID()}`;
                    const params = Object.assign(
                        { Key: key, Tagging: tagString, Metadata: { 'scal-location-constraint': backend } },
                        putParams,
                    );
                    process.stdout.write('Putting object\n');
                    s3.send(new PutObjectCommand(params))
                        .then(() => {
                            const tagParams = { Bucket: bucket, Key: key };
                            getAndAssertObjectTags(tagParams, done);
                        })
                        .catch(done);
                });
            });

            it(
                'should not return error on getting tags from object that has ' +
                    'had a delete marker put directly on AWS',
                done => {
                    const key = `somekey-${genUniqID()}`;
                    const params = Object.assign(
                        { Key: key, Tagging: tagString, Metadata: { 'scal-location-constraint': awsLocation } },
                        putParams,
                    );
                    process.stdout.write('Putting object\n');
                    s3.send(new PutObjectCommand(params))
                        .then(
                            () =>
                                new Promise((resolve, reject) => {
                                    process.stdout.write('Deleting object from AWS\n');
                                    awsS3.deleteObject({ Bucket: awsBucket, Key: key }, err => {
                                        if (err) {
                                            reject(err);
                                            return;
                                        }
                                        resolve();
                                    });
                                }),
                        )
                        .then(() => {
                            const tagParams = { Bucket: bucket, Key: key };
                            getAndAssertObjectTags(tagParams, done);
                        })
                        .catch(done);
                },
            );
        });

        describe('deleteObjectTagging', () => {
            testBackends.forEach(backend => {
                it(`should delete tags from object on ${backend} backend`, done => {
                    const key = `somekey-${genUniqID()}`;
                    const params = Object.assign(
                        { Key: key, Tagging: tagString, Metadata: { 'scal-location-constraint': backend } },
                        putParams,
                    );
                    process.stdout.write('Putting object\n');
                    s3.send(new PutObjectCommand(params))
                        .then(() => {
                            const tagParams = { Bucket: bucket, Key: key };
                            return s3.send(new DeleteObjectTaggingCommand(tagParams));
                        })
                        .then(() => {
                            getObject(key, backend, false, false, false, done);
                        })
                        .catch(done);
                });
            });

            it(
                'should not return error on deleting tags from object that ' +
                    'has had delete markers put directly on AWS',
                done => {
                    const key = `somekey-${genUniqID()}`;
                    const params = Object.assign(
                        { Key: key, Tagging: tagString, Metadata: { 'scal-location-constraint': awsLocation } },
                        putParams,
                    );
                    process.stdout.write('Putting object\n');
                    s3.send(new PutObjectCommand(params))
                        .then(
                            () =>
                                new Promise((resolve, reject) => {
                                    process.stdout.write('Deleting object from AWS\n');
                                    awsS3.deleteObject({ Bucket: awsBucket, Key: key }, err => {
                                        if (err) {
                                            reject(err);
                                            return;
                                        }
                                        resolve();
                                    });
                                }),
                        )
                        .then(() => {
                            const tagParams = { Bucket: bucket, Key: key };
                            return s3.send(new DeleteObjectTaggingCommand(tagParams));
                        })
                        .then(() => done())
                        .catch(done);
                },
            );
        });
    });
});
