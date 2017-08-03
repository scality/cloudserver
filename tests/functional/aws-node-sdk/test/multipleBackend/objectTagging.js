const assert = require('assert');
const AWS = require('aws-sdk');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { config } = require('../../../../../lib/Config');
const { getRealAwsConfig } = require('../support/awsConfig');
const { getAzureClient, getAzureContainerName, convertMD5 } =
    require('./utils');

const awsLocation = 'aws-test';
const awsBucket = 'multitester555';
const azureClient = getAzureClient();
const azureContainerName = getAzureContainerName();
const bucket = 'testmultbackendtagging';
const key = `somekey-${Date.now()}`;
const body = Buffer.from('I am a body', 'utf8');
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const emptyMD5 = 'd41d8cd98f00b204e9800998ecf8427e';

const cloudTimeout = 50000;

let bucketUtil;
let s3;
let awsS3;
const describeSkipIfNotMultiple = (config.backends.data !== 'multiple'
    || process.env.S3_END_TO_END) ? describe.skip : describe;

const putParams = { Bucket: bucket, Key: key, Body: body };
const noBodyParams = { Bucket: bucket, Key: key };
const tagParams = { Bucket: bucket, Key: key };
const awsDelParams = { Bucket: awsBucket, Key: key };
const testBackends = ['mem', 'file', 'aws-test', 'azuretest'];
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

function awsGet(tagCheck, isEmpty, callback) {
    awsS3.getObject({ Bucket: awsBucket, Key: key },
    (err, res) => {
        assert.equal(err, null);
        if (isEmpty) {
            assert.strictEqual(res.ETag, `"${emptyMD5}"`);
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

function azureGet(tagCheck, isEmpty, callback) {
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

function getObject(backend, tagCheck, isEmpty, callback) {
    function get(cb) {
        s3.getObject({ Bucket: bucket, Key: key }, (err, res) => {
            assert.equal(err, null);
            if (isEmpty) {
                assert.strictEqual(res.ETag, `"${emptyMD5}"`);
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
            get(() => awsGet(tagCheck, isEmpty, callback));
        }, cloudTimeout);
    } else if (backend === 'azuretest') {
        setTimeout(() => {
            get(() => azureGet(tagCheck, isEmpty, callback));
        }, cloudTimeout);
    } else {
        get(callback);
    }
}

describeSkipIfNotMultiple('Object tagging with multiple backends',
function testSuite() {
    this.timeout(80000);
    withV4(sigCfg => {
        beforeEach(() => {
            const awsConfig = getRealAwsConfig(awsLocation);
            awsS3 = new AWS.S3(awsConfig);
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
                it(`should put an object with tags to ${backend} backend`,
                done => {
                    const params = Object.assign({ Tagging: tagString, Metadata:
                        { 'scal-location-constraint': backend } }, putParams);
                    s3.putObject(params, err => {
                        assert.equal(err, null);
                        getObject(backend, true, false, done);
                    });
                });

                it(`should put a 0 byte object with tags to ${backend} backend`,
                done => {
                    const params = Object.assign({ Tagging: tagString,
                        Metadata: { 'scal-location-constraint': backend } },
                        noBodyParams);
                    s3.putObject(params, err => {
                        assert.equal(err, null);
                        getObject(backend, true, true, done);
                    });
                });

                it(`should put tags to preexisting object in ${backend} ` +
                'backend', done => {
                    const params = Object.assign({ Metadata:
                        { 'scal-location-constraint': backend } }, putParams);
                    s3.putObject(params, err => {
                        assert.equal(err, null);
                        const putTagParams = { Bucket: bucket, Key: key,
                            Tagging: putTags };
                        s3.putObjectTagging(putTagParams, err => {
                            assert.equal(err, null);
                            getObject(backend, true, false, done);
                        });
                    });
                });

                it('should put tags to preexisting 0 byte object in ' +
                `${backend} backend`, done => {
                    const params = Object.assign({ Metadata:
                        { 'scal-location-constraint': backend } },
                        noBodyParams);
                    s3.putObject(params, err => {
                        assert.equal(err, null);
                        const putTagParams = { Bucket: bucket, Key: key,
                            Tagging: putTags };
                        s3.putObjectTagging(putTagParams, err => {
                            assert.equal(err, null);
                            getObject(backend, true, true, done);
                        });
                    });
                });
            });

            it('should return error on putting tags to object deleted from AWS',
            done => {
                const params = Object.assign({ Metadata:
                    { 'scal-location-constraint': awsLocation } }, putParams);
                s3.putObject(params, err => {
                    assert.equal(err, null);
                    awsS3.deleteObject(awsDelParams, err => {
                        assert.equal(err, null);
                        const putTagParams = { Bucket: bucket, Key: key,
                            Tagging: putTags };
                        s3.putObjectTagging(putTagParams, err => {
                            assert.strictEqual(err.code, 'InternalError');
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
                    const params = Object.assign({ Tagging: tagString, Metadata:
                        { 'scal-location-constraint': backend } }, putParams);
                    s3.putObject(params, err => {
                        assert.equal(err, null);
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

            it('should not return error on getting tags from object deleted ' +
            'from AWS', done => {
                const params = Object.assign({ Tagging: tagString, Metadata:
                    { 'scal-location-constraint': awsLocation } }, putParams);
                s3.putObject(params, err => {
                    assert.equal(err, null);
                    awsS3.deleteObject(awsDelParams, err => {
                        assert.equal(err, null);
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
                    const params = Object.assign({ Tagging: tagString, Metadata:
                        { 'scal-location-constraint': backend } }, putParams);
                    s3.putObject(params, err => {
                        assert.equal(err, null);
                        s3.deleteObjectTagging(tagParams, err => {
                            assert.equal(err, null);
                            getObject(backend, false, false, done);
                        });
                    });
                });
            });

            it('should return error on deleting tags from object deleted ' +
            'from AWS', done => {
                const params = Object.assign({ Tagging: tagString, Metadata:
                    { 'scal-location-constraint': awsLocation } }, putParams);
                s3.putObject(params, err => {
                    assert.equal(err, null);
                    awsS3.deleteObject(awsDelParams, err => {
                        assert.equal(err, null);
                        s3.deleteObjectTagging(tagParams, err => {
                            assert.strictEqual(err.code, 'InternalError');
                            done();
                        });
                    });
                });
            });
        });
    });
});
