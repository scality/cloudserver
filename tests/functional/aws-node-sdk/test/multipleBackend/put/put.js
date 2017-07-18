const assert = require('assert');
const AWS = require('aws-sdk');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { config } = require('../../../../../../lib/Config');
const { getRealAwsConfig } = require('../../support/awsConfig');
const { createEncryptedBucketPromise } =
    require('../../../lib/utility/createEncryptedBucket');

const awsLocation = 'aws-test';
const bucket = 'buckettestmultiplebackendput';
const key = `somekey-${Date.now()}`;
const emptyKey = `emptykey-${Date.now()}`;
const bigKey = `bigkey-${Date.now()}`;
const body = Buffer.from('I am a body', 'utf8');
const bigBody = new Buffer(10485760);
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const emptyMD5 = 'd41d8cd98f00b204e9800998ecf8427e';
// AWS handles objects larger than 5MB as MPUs, so returned ETag differs
const bigS3MD5 = 'f1c9645dbc14efddc7d8a322685f26eb';
const bigAWSMD5 = 'a7d414b9133d6483d9a1c4e04e856e3b-2';

let bucketUtil;
let s3;
let awsS3;
const describeSkipIfNotMultiple = (config.backends.data !== 'multiple'
    || process.env.S3_END_TO_END) ? describe.skip : describe;

const awsTimeout = 20000;
const retryTimeout = 10000;

function awsGetCheck(objectKey, s3MD5, awsMD5, cb) {
    const awsBucket = config.locationConstraints[awsLocation].
        details.bucketName;
    s3.getObject({ Bucket: bucket, Key: objectKey },
    function s3GetCallback(err, res) {
        if (err && err.code === 'NetworkingError') {
            return setTimeout(() => {
                s3.getObject({ Bucket: bucket, Key: objectKey }, s3GetCallback);
            }, retryTimeout);
        }
        assert.strictEqual(err, null, 'Expected success, got error ' +
        `on call to AWS through S3: ${err}`);
        assert.strictEqual(res.ETag, `"${s3MD5}"`);
        assert.strictEqual(res.Metadata['scal-location-constraint'],
            awsLocation);
        return awsS3.getObject({ Bucket: awsBucket, Key: objectKey },
        (err, res) => {
            assert.strictEqual(err, null, 'Expected success, got error ' +
            `on direct AWS call: ${err}`);
            if (process.env.ENABLE_KMS_ENCRYPTION === 'true') {
                // doesn't check ETag because it's different
                // with every PUT with encryption
                assert.strictEqual(res.ServerSideEncryption, 'AES256');
            } else {
                assert.strictEqual(res.ETag, `"${awsMD5}"`);
            }
            assert.strictEqual(res.Metadata
                ['x-amz-meta-scal-location-constraint'],
                awsLocation);
            return cb(res);
        });
    });
}

describe('MultipleBackend put object', function testSuite() {
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

        it('should return an error to put request without a valid bucket name',
            done => {
                s3.putObject({ Bucket: '', Key: key }, err => {
                    assert.notEqual(err, null,
                        'Expected failure but got success');
                    assert.strictEqual(err.code, 'MethodNotAllowed');
                    done();
                });
            });

        // SKIP because no mem, file, or AWS location constraints in E2E.
        describeSkipIfNotMultiple('with set location from "x-amz-meta-scal-' +
            'location-constraint" header', () => {
            before(() => {
                const awsConfig = getRealAwsConfig(awsLocation);
                awsS3 = new AWS.S3(awsConfig);
            });

            it('should return an error to put request without a valid ' +
                'location constraint', done => {
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': 'fail-region' } };
                s3.putObject(params, err => {
                    assert.notEqual(err, null, 'Expected failure but got ' +
                        'success');
                    assert.strictEqual(err.code, 'InvalidArgument');
                    done();
                });
            });

            it('should put an object to mem', done => {
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': 'mem' },
                };
                s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${err}`);
                    s3.getObject({ Bucket: bucket, Key: key }, (err, res) => {
                        assert.strictEqual(err, null, 'Expected success, ' +
                        `got error ${err}`);
                        assert.strictEqual(res.ETag, `"${correctMD5}"`);
                        done();
                    });
                });
            });

            it('should put a 0-byte object to mem', done => {
                const params = { Bucket: bucket, Key: emptyKey,
                    Metadata: { 'scal-location-constraint': 'mem' },
                };
                s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${err}`);
                    s3.getObject({ Bucket: bucket, Key: emptyKey },
                    (err, res) => {
                        assert.strictEqual(err, null, 'Expected success, ' +
                        `got error ${err}`);
                        assert.strictEqual(res.ETag, `"${emptyMD5}"`);
                        done();
                    });
                });
            });

            it('should put a 0-byte object to AWS', done => {
                const params = { Bucket: bucket, Key: emptyKey,
                    Metadata: { 'scal-location-constraint': awsLocation },
                };
                s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                    `got error ${err}`);
                    setTimeout(() => {
                        awsGetCheck(emptyKey, emptyMD5, emptyMD5, () => done());
                    }, awsTimeout);
                });
            });

            it('should put an object to file', done => {
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': 'file' },
                };
                s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${err}`);
                    s3.getObject({ Bucket: bucket, Key: key }, (err, res) => {
                        assert.strictEqual(err, null, 'Expected success, ' +
                        `got error ${err}`);
                        assert.strictEqual(res.ETag, `"${correctMD5}"`);
                        done();
                    });
                });
            });

            it('should put an object to AWS', done => {
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': awsLocation } };
                s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${err}`);
                    setTimeout(() => {
                        awsGetCheck(key, correctMD5, correctMD5, () => done());
                    }, awsTimeout);
                });
            });

            it('should put a large object to AWS', done => {
                const params = { Bucket: bucket, Key: bigKey,
                    Body: bigBody,
                    Metadata: { 'scal-location-constraint': awsLocation } };
                s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected sucess, ' +
                        `got error ${err}`);
                    setTimeout(() => {
                        awsGetCheck(bigKey, bigS3MD5, bigAWSMD5, () => done());
                    }, awsTimeout);
                });
            });

            it('should put objects with same key to AWS ' +
            'then file, and object should only be present in file', done => {
                const awsBucket = config.locationConstraints[awsLocation].
                    details.bucketName;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': awsLocation } };
                s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${err}`);
                    params.Metadata = { 'scal-location-constraint': 'file' };
                    s3.putObject(params, err => {
                        assert.equal(err, null, 'Expected success, ' +
                            `got error ${err}`);
                        setTimeout(() => {
                            s3.getObject({ Bucket: bucket, Key: key },
                            (err, res) => {
                                assert.equal(err, null, 'Expected success, ' +
                                    `got error ${err}`);
                                assert.strictEqual(
                                    res.Metadata['scal-location-constraint'],
                                    'file');
                                awsS3.getObject({ Bucket: awsBucket,
                                Key: key }, err => {
                                    assert.strictEqual(err.code, 'NoSuchKey');
                                    done();
                                });
                            });
                        }, awsTimeout);
                    });
                });
            });

            it('should put objects with same key to file ' +
            'then AWS, and object should only be present on AWS', done => {
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': 'file' } };
                s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${err}`);
                    params.Metadata = {
                        'scal-location-constraint': awsLocation };
                    s3.putObject(params, err => {
                        assert.equal(err, null, 'Expected success, ' +
                            `got error ${err}`);
                        setTimeout(() => {
                            awsGetCheck(key, correctMD5, correctMD5,
                                () => done());
                        }, awsTimeout);
                    });
                });
            });

            it('should put two objects to AWS with same ' +
            'key, and newest object should be returned', done => {
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': awsLocation,
                                'unique-header': 'first object' } };
                s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${err}`);
                    params.Metadata = { 'scal-location-constraint': awsLocation,
                        'unique-header': 'second object' };
                    s3.putObject(params, err => {
                        assert.equal(err, null, 'Expected success, ' +
                            `got error ${err}`);
                        setTimeout(() => {
                            awsGetCheck(key, correctMD5, correctMD5, result => {
                                assert.strictEqual(result.Metadata
                                    ['x-amz-meta-unique-header'],
                                    'second object');
                                done();
                            });
                        }, awsTimeout);
                    });
                });
            });
        });
    });
});

describeSkipIfNotMultiple('MultipleBackend put object based on bucket location',
() => {
    withV4(sigCfg => {
        const params = { Bucket: bucket, Key: key, Body: body };
        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
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

        it('should put an object to mem with no location header',
        done => {
            process.stdout.write('Creating bucket\n');
            return s3.createBucket({ Bucket: bucket,
                CreateBucketConfiguration: {
                    LocationConstraint: 'mem',
                },
            }, err => {
                assert.equal(err, null, `Error creating bucket: ${err}`);
                process.stdout.write('Putting object\n');
                return s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${JSON.stringify(err)}`);
                    s3.getObject({ Bucket: bucket, Key: key }, (err, res) => {
                        assert.strictEqual(err, null, 'Expected success, ' +
                        `got error ${JSON.stringify(err)}`);
                        assert.strictEqual(res.ETag, `"${correctMD5}"`);
                        done();
                    });
                });
            });
        });

        it('should put an object to file with no location header', done => {
            process.stdout.write('Creating bucket\n');
            return s3.createBucket({ Bucket: bucket,
                CreateBucketConfiguration: {
                    LocationConstraint: 'file',
                },
            }, err => {
                assert.equal(err, null, `Error creating bucket: ${err}`);
                process.stdout.write('Putting object\n');
                return s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${JSON.stringify(err)}`);
                    s3.getObject({ Bucket: bucket, Key: key }, (err, res) => {
                        assert.strictEqual(err, null, 'Expected success, ' +
                        `got error ${JSON.stringify(err)}`);
                        assert.strictEqual(res.ETag, `"${correctMD5}"`);
                        done();
                    });
                });
            });
        });

        it('should put an object to AWS with no location header', done => {
            process.stdout.write('Creating bucket\n');
            return s3.createBucket({ Bucket: bucket,
                CreateBucketConfiguration: {
                    LocationConstraint: awsLocation,
                },
            }, err => {
                assert.equal(err, null, `Error creating bucket: ${err}`);
                process.stdout.write('Putting object\n');
                const awsBucket = config.locationConstraints[awsLocation].
                    details.bucketName;
                return s3.putObject(params, err => {
                    assert.equal(err, null,
                        `Expected success, got error ${err}`);
                    setTimeout(() => {
                        s3.getObject({ Bucket: bucket, Key: key },
                        (err, res) => {
                            assert.strictEqual(err, null,
                                `Expected success, got error ${err}`);
                            assert.strictEqual(res.ETag, `"${correctMD5}"`);
                            awsS3.getObject({ Bucket: awsBucket, Key: key },
                            (err, res) => {
                                assert.strictEqual(err, null,
                                    `Expected success, got error ${err}`);
                                assert.strictEqual(res.ETag, `"${correctMD5}"`);
                                done();
                            });
                        });
                    }, awsTimeout);
                });
            });
        });
    });
});

describe('MultipleBackend put based on request endpoint',
() => {
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

        it('should create bucket in corresponding backend', done => {
            process.stdout.write('Creating bucket');
            const request = s3.createBucket({ Bucket: bucket });
            request.on('build', () => {
                request.httpRequest.body = '';
            });
            request.send(err => {
                assert.strictEqual(err, null, `Error creating bucket: ${err}`);
                s3.putObject({ Bucket: bucket, Key: key, Body: body }, err => {
                    assert.strictEqual(err, null, 'Expected succes, ' +
                        `got error ${JSON.stringify(err)}`);
                    const host = request.service.endpoint.hostname;
                    const endpoint = config.restEndpoints[host];
                    s3.getBucketLocation({ Bucket: bucket }, (err, data) => {
                        assert.strictEqual(err, null, 'Expected succes, ' +
                            `got error ${JSON.stringify(err)}`);
                        assert.strictEqual(data.LocationConstraint, endpoint);
                        s3.getObject({ Bucket: bucket, Key: key },
                        (err, res) => {
                            assert.strictEqual(err, null, 'Expected succes, ' +
                                `got error ${JSON.stringify(err)}`);
                            assert.strictEqual(res.ETag, `"${correctMD5}"`);
                            done();
                        });
                    });
                });
            });
        });
    });
});
