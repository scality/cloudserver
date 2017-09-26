const assert = require('assert');
const async = require('async');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { describeSkipIfNotMultiple, awsS3, getAzureClient, getAzureContainerName,
    convertMD5, memLocation, fileLocation, awsLocation, azureLocation } =
    require('./utils');

const awsBucket = 'multitester555';
const azureClient = getAzureClient();
const azureContainerName = getAzureContainerName();
const bucket = 'testmultbackendtagging';
const body = Buffer.from('I am a body', 'utf8');
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const emptyMD5 = 'd41d8cd98f00b204e9800998ecf8427e';
const mpuMD5 = 'e4c2438a8f503658547a77959890dcab-1';

const cloudTimeout = 10000;

let bucketUtil;
let s3;

const putParams = { Bucket: bucket, Body: body };
const testBackends = [memLocation, fileLocation, awsLocation, azureLocation];
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

function awsGet(key, tagCheck, isEmpty, isMpu, callback) {
    process.stdout.write('Getting object from AWS\n');
    awsS3.getObject({ Bucket: awsBucket, Key: key },
    (err, res) => {
        assert.equal(err, null);
        if (isEmpty) {
            assert.strictEqual(res.ETag, `"${emptyMD5}"`);
        } else if (isMpu) {
            assert.strictEqual(res.ETag, `"${mpuMD5}"`);
        } else {
            assert.strictEqual(res.ETag, `"${correctMD5}"`);
        }
        if (tagCheck) {
            assert.strictEqual(res.TagCount, '2');
        } else {
            assert.strictEqual(res.TagCount, undefined);
        }
        return callback();
    });
}

function azureGet(key, tagCheck, isEmpty, callback) {
    process.stdout.write('Getting object from Azure\n');
    azureClient.getBlobProperties(azureContainerName, key,
    (err, res) => {
        assert.equal(err, null);
        const resMD5 = convertMD5(res.contentSettings.contentMD5);
        if (isEmpty) {
            assert.strictEqual(resMD5, `${emptyMD5}`);
        } else {
            assert.strictEqual(resMD5, `${correctMD5}`);
        }
        if (tagCheck) {
            assert.strictEqual(res.metadata.tags,
                JSON.stringify(tagObj));
        } else {
            assert.strictEqual(res.metadata.tags, undefined);
        }
        return callback();
    });
}

function getObject(key, backend, tagCheck, isEmpty, isMpu, callback) {
    function get(cb) {
        process.stdout.write('Getting object\n');
        s3.getObject({ Bucket: bucket, Key: key }, (err, res) => {
            assert.equal(err, null);
            if (isEmpty) {
                assert.strictEqual(res.ETag, `"${emptyMD5}"`);
            } else if (isMpu) {
                assert.strictEqual(res.ETag, `"${mpuMD5}"`);
            } else {
                assert.strictEqual(res.ETag, `"${correctMD5}"`);
            }
            assert.strictEqual(res.Metadata['scal-location-constraint'],
                backend);
            if (tagCheck) {
                assert.strictEqual(res.TagCount, '2');
            } else {
                assert.strictEqual(res.TagCount, undefined);
            }
            return cb();
        });
    }
    if (backend === 'aws-test') {
        setTimeout(() => {
            get(() => awsGet(key, tagCheck, isEmpty, isMpu, callback));
        }, cloudTimeout);
    } else if (backend === 'azuretest') {
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
            assert.equal(err, null);
            next(null, data.UploadId);
        }),
        (uploadId, next) => {
            const partParams = { Bucket: bucket, Key: params.Key, PartNumber: 1,
                UploadId: uploadId, Body: body };
            s3.uploadPart(partParams, (err, result) => {
                assert.equal(err, null);
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
                assert.equal(err, null);
                next();
            });
        },
    ], err => {
        assert.equal(err, null);
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
                const itSkipIfAzure = backend === 'azuretest' ? it.skip : it;
                it(`should put an object with tags to ${backend} backend`,
                done => {
                    const key = `somekey-${Date.now()}`;
                    const params = Object.assign({ Key: key, Tagging: tagString,
                        Metadata: { 'scal-location-constraint': backend } },
                         putParams);
                    process.stdout.write('Putting object\n');
                    s3.putObject(params, err => {
                        assert.equal(err, null);
                        getObject(key, backend, true, false, false, done);
                    });
                });

                it(`should put a 0 byte object with tags to ${backend} backend`,
                done => {
                    const key = `somekey-${Date.now()}`;
                    const params = {
                        Bucket: bucket,
                        Key: key,
                        Tagging: tagString,
                        Metadata: { 'scal-location-constraint': backend },
                    };
                    process.stdout.write('Putting object\n');
                    s3.putObject(params, err => {
                        assert.equal(err, null);
                        getObject(key, backend, true, true, false, done);
                    });
                });

                it(`should put tags to preexisting object in ${backend} ` +
                'backend', done => {
                    const key = `somekey-${Date.now()}`;
                    const params = Object.assign({ Key: key, Metadata:
                        { 'scal-location-constraint': backend } }, putParams);
                    process.stdout.write('Putting object\n');
                    s3.putObject(params, err => {
                        assert.equal(err, null);
                        const putTagParams = { Bucket: bucket, Key: key,
                            Tagging: putTags };
                        process.stdout.write('Putting object tags\n');
                        s3.putObjectTagging(putTagParams, err => {
                            assert.equal(err, null);
                            getObject(key, backend, true, false, false, done);
                        });
                    });
                });

                it('should put tags to preexisting 0 byte object in ' +
                `${backend} backend`, done => {
                    const key = `somekey-${Date.now()}`;
                    const params = {
                        Bucket: bucket,
                        Key: key,
                        Metadata: { 'scal-location-constraint': backend },
                    };
                    process.stdout.write('Putting object\n');
                    s3.putObject(params, err => {
                        assert.equal(err, null);
                        const putTagParams = { Bucket: bucket, Key: key,
                            Tagging: putTags };
                        process.stdout.write('Putting object tags\n');
                        s3.putObjectTagging(putTagParams, err => {
                            assert.equal(err, null);
                            getObject(key, backend, true, true, false, done);
                        });
                    });
                });

                itSkipIfAzure('should put tags to completed MPU object in ' +
                `${backend}`, done => {
                    const key = `somekey-${Date.now()}`;
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
                            assert.equal(err, null);
                            getObject(key, backend, true, false, true, done);
                        });
                    });
                });
            });

            it('should not return error on putting tags to object ' +
            'that has had a delete marker put directly on from AWS',
            done => {
                const key = `somekey-${Date.now()}`;
                const params = Object.assign({ Key: key, Metadata:
                    { 'scal-location-constraint': awsLocation } }, putParams);
                process.stdout.write('Putting object\n');
                s3.putObject(params, err => {
                    assert.equal(err, null);
                    process.stdout.write('Deleting object from AWS\n');
                    awsS3.deleteObject({ Bucket: awsBucket, Key: key }, err => {
                        assert.equal(err, null);
                        const putTagParams = { Bucket: bucket, Key: key,
                            Tagging: putTags };
                        process.stdout.write('Putting object tags\n');
                        s3.putObjectTagging(putTagParams, err => {
                            assert.strictEqual(err, null);
                            done();
                        });
                    });
                });
            });
        });

        describe('getObjectTagging', () => {
            testBackends.forEach(backend => {
                it(`should get tags from object on ${backend} backend`,
                done => {
                    const key = `somekey-${Date.now()}`;
                    const params = Object.assign({ Key: key, Tagging: tagString,
                        Metadata: { 'scal-location-constraint': backend } },
                        putParams);
                    process.stdout.write('Putting object\n');
                    s3.putObject(params, err => {
                        assert.equal(err, null);
                        const tagParams = { Bucket: bucket, Key: key };
                        s3.getObjectTagging(tagParams, (err, res) => {
                            assert.strictEqual(res.TagSet.length, 2);
                            assert.strictEqual(res.TagSet[0].Key,
                                putTags.TagSet[0].Key);
                            assert.strictEqual(res.TagSet[0].Value,
                                putTags.TagSet[0].Value);
                            assert.strictEqual(res.TagSet[1].Key,
                                putTags.TagSet[1].Key);
                            assert.strictEqual(res.TagSet[1].Value,
                                putTags.TagSet[1].Value);
                            done();
                        });
                    });
                });
            });

            it('should not return error on getting tags from object that has ' +
            'had a delete marker put directly on AWS', done => {
                const key = `somekey-${Date.now()}`;
                const params = Object.assign({ Key: key, Tagging: tagString,
                    Metadata: { 'scal-location-constraint': awsLocation } },
                    putParams);
                process.stdout.write('Putting object\n');
                s3.putObject(params, err => {
                    assert.equal(err, null);
                    process.stdout.write('Deleting object from AWS\n');
                    awsS3.deleteObject({ Bucket: awsBucket, Key: key }, err => {
                        assert.equal(err, null);
                        const tagParams = { Bucket: bucket, Key: key };
                        s3.getObjectTagging(tagParams, err => {
                            assert.equal(err, null);
                            done();
                        });
                    });
                });
            });
        });

        describe('deleteObjectTagging', () => {
            testBackends.forEach(backend => {
                it(`should delete tags from object on ${backend} backend`,
                done => {
                    const key = `somekey-${Date.now()}`;
                    const params = Object.assign({ Key: key, Tagging: tagString,
                        Metadata: { 'scal-location-constraint': backend } },
                        putParams);
                    process.stdout.write('Putting object\n');
                    s3.putObject(params, err => {
                        assert.equal(err, null);
                        const tagParams = { Bucket: bucket, Key: key };
                        s3.deleteObjectTagging(tagParams, err => {
                            assert.equal(err, null);
                            getObject(key, backend, false, false, false, done);
                        });
                    });
                });
            });

            it('should not return error on deleting tags from object that ' +
            'has had delete markers put directly on AWS', done => {
                const key = `somekey-${Date.now()}`;
                const params = Object.assign({ Key: key, Tagging: tagString,
                    Metadata: { 'scal-location-constraint': awsLocation } },
                    putParams);
                process.stdout.write('Putting object\n');
                s3.putObject(params, err => {
                    assert.equal(err, null);
                    process.stdout.write('Deleting object from AWS\n');
                    awsS3.deleteObject({ Bucket: awsBucket, Key: key }, err => {
                        assert.equal(err, null);
                        const tagParams = { Bucket: bucket, Key: key };
                        s3.deleteObjectTagging(tagParams, err => {
                            assert.strictEqual(err, null);
                            done();
                        });
                    });
                });
            });
        });
    });
});
