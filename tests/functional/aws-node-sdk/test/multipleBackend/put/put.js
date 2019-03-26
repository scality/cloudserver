const assert = require('assert');
const async = require('async');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { config } = require('../../../../../../lib/Config');
const { createEncryptedBucketPromise } =
    require('../../../lib/utility/createEncryptedBucket');
const { versioningEnabled } = require('../../../lib/utility/versioning-util');

const { describeSkipIfNotMultiple, getAwsRetry, awsLocation,
    awsLocationEncryption, memLocation, fileLocation, genUniqID, isCEPH }
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

const retryTimeout = 10000;

function getAwsSuccess(key, awsMD5, location, cb) {
    return getAwsRetry({ key }, 0, (err, res) => {
        expect(err).toBe(null);
        if (location === awsLocationEncryption) {
            // doesn't check ETag because it's different
            // with every PUT with encryption
            expect(res.ServerSideEncryption).toBe('AES256');
        }
        if (process.env.ENABLE_KMS_ENCRYPTION !== 'true') {
            expect(res.ETag).toBe(`"${awsMD5}"`);
        }
        expect(res.Metadata['scal-location-constraint']).toBe(location);
        return cb(res);
    });
}

function getAwsError(key, expectedError, cb) {
    return getAwsRetry({ key }, 0, err => {
        expect(err).not.toBe(undefined);
        expect(err.code).toBe(expectedError);
        cb();
    });
}

function awsGetCheck(objectKey, s3MD5, awsMD5, location, cb) {
    process.stdout.write('Getting object\n');
    s3.getObject({ Bucket: bucket, Key: objectKey },
    function s3GetCallback(err, res) {
        if (err && err.code === 'NetworkingError') {
            return setTimeout(() => {
                process.stdout.write('Getting object retry\n');
                s3.getObject({ Bucket: bucket, Key: objectKey }, s3GetCallback);
            }, retryTimeout);
        }
        expect(err).toBe(null);
        expect(res.ETag).toBe(`"${s3MD5}"`);
        expect(res.Metadata['scal-location-constraint']).toBe(location);
        process.stdout.write('Getting object from AWS\n');
        return getAwsSuccess(objectKey, awsMD5, location, cb);
    });
}

describe('MultipleBackend put object', () => {
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

        test(
            'should return an error to put request without a valid bucket name',
            done => {
                const key = `somekey-${genUniqID()}`;
                s3.putObject({ Bucket: '', Key: key }, err => {
                    expect(err).not.toEqual(null);
                    expect(err.code).toBe('MethodNotAllowed');
                    done();
                });
            }
        );

        describeSkipIfNotMultiple('with set location from "x-amz-meta-scal-' +
            'location-constraint" header', function describe() {
            if (!process.env.S3_END_TO_END) {
                this.retries(2);
            }

            test('should return an error to put request without a valid ' +
                'location constraint', done => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': 'fail-region' } };
                s3.putObject(params, err => {
                    expect(err).not.toEqual(null);
                    expect(err.code).toBe('InvalidArgument');
                    done();
                });
            });

            test('should put an object to mem', done => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': memLocation },
                };
                s3.putObject(params, err => {
                    expect(err).toEqual(null);
                    s3.getObject({ Bucket: bucket, Key: key }, (err, res) => {
                        expect(err).toBe(null);
                        expect(res.ETag).toBe(`"${correctMD5}"`);
                        done();
                    });
                });
            });

            test('should put a 0-byte object to mem', done => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key,
                    Metadata: { 'scal-location-constraint': memLocation },
                };
                s3.putObject(params, err => {
                    expect(err).toEqual(null);
                    s3.getObject({ Bucket: bucket, Key: key },
                    (err, res) => {
                        expect(err).toBe(null);
                        expect(res.ETag).toBe(`"${emptyMD5}"`);
                        done();
                    });
                });
            });

            test('should put a 0-byte object to AWS', done => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key,
                    Metadata: { 'scal-location-constraint': awsLocation },
                };
                return s3.putObject(params, err => {
                    expect(err).toEqual(null);
                    return awsGetCheck(key, emptyMD5, emptyMD5, awsLocation,
                      () => done());
                });
            });

            test('should put an object to file', done => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': fileLocation },
                };
                s3.putObject(params, err => {
                    expect(err).toEqual(null);
                    s3.getObject({ Bucket: bucket, Key: key }, (err, res) => {
                        expect(err).toBe(null);
                        expect(res.ETag).toBe(`"${correctMD5}"`);
                        done();
                    });
                });
            });

            test('should put an object to AWS', done => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': awsLocation } };
                return s3.putObject(params, err => {
                    expect(err).toEqual(null);
                    return awsGetCheck(key, correctMD5, correctMD5, awsLocation,
                      () => done());
                });
            });

            test('should encrypt body only if bucket encrypted putting ' +
            'object to AWS', done => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': awsLocation } };
                return s3.putObject(params, err => {
                    expect(err).toEqual(null);
                    return getAwsSuccess(key, correctMD5, awsLocation,
                        () => done());
                });
            });


            test('should put an object to AWS with encryption', done => {
                // Test refuses to skip using itSkipCeph so just mark it passed
                if (isCEPH) {
                    return done();
                }
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint':
                    awsLocationEncryption } };
                return s3.putObject(params, err => {
                    expect(err).toEqual(null);
                    return awsGetCheck(key, correctMD5, correctMD5,
                      awsLocationEncryption, () => done());
                });
            });

            test('should return a version id putting object to ' +
            'to AWS with versioning enabled', done => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key, Body: body,
                    Metadata: { 'scal-location-constraint': awsLocation } };
                async.waterfall([
                    next => s3.putBucketVersioning({
                        Bucket: bucket,
                        VersioningConfiguration: versioningEnabled,
                    }, err => next(err)),
                    next => s3.putObject(params, (err, res) => {
                        expect(err).toBe(null);
                        expect(res.VersionId).toBeTruthy();
                        next(null, res.ETag);
                    }),
                    (eTag, next) => getAwsSuccess(key, correctMD5, awsLocation,
                        () => next()),
                ], done);
            });

            test('should put a large object to AWS', done => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: bigBody,
                    Metadata: { 'scal-location-constraint': awsLocation } };
                return s3.putObject(params, err => {
                    expect(err).toEqual(null);
                    return awsGetCheck(key, bigS3MD5, bigAWSMD5, awsLocation,
                      () => done());
                });
            });

            test('should put objects with same key to AWS ' +
            'then file, and object should only be present in file', done => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': awsLocation } };
                return s3.putObject(params, err => {
                    expect(err).toEqual(null);
                    params.Metadata =
                        { 'scal-location-constraint': fileLocation };
                    return s3.putObject(params, err => {
                        expect(err).toEqual(null);
                        return s3.getObject({ Bucket: bucket, Key: key },
                        (err, res) => {
                            expect(err).toEqual(null);
                            expect(res.Metadata['scal-location-constraint']).toBe(fileLocation);
                            return getAwsError(key, 'NoSuchKey', done);
                        });
                    });
                });
            });

            test('should put objects with same key to file ' +
            'then AWS, and object should only be present on AWS', done => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': fileLocation } };
                return s3.putObject(params, err => {
                    expect(err).toEqual(null);
                    params.Metadata = {
                        'scal-location-constraint': awsLocation };
                    return s3.putObject(params, err => {
                        expect(err).toEqual(null);
                        return awsGetCheck(key, correctMD5, correctMD5,
                            awsLocation, () => done());
                    });
                });
            });

            test('should put two objects to AWS with same ' +
            'key, and newest object should be returned', done => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': awsLocation,
                        'unique-header': 'first object' } };
                return s3.putObject(params, err => {
                    expect(err).toEqual(null);
                    params.Metadata = { 'scal-location-constraint': awsLocation,
                        'unique-header': 'second object' };
                    return s3.putObject(params, err => {
                        expect(err).toEqual(null);
                        return awsGetCheck(key, correctMD5, correctMD5,
                        awsLocation, result => {
                            expect(result.Metadata
                                ['unique-header']).toBe('second object');
                            done();
                        });
                    });
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

        test('should put an object to mem with no location header', done => {
            process.stdout.write('Creating bucket\n');
            return s3.createBucket({ Bucket: bucket,
                CreateBucketConfiguration: {
                    LocationConstraint: memLocation,
                },
            }, err => {
                expect(err).toEqual(null);
                process.stdout.write('Putting object\n');
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key, Body: body };
                return s3.putObject(params, err => {
                    expect(err).toEqual(null);
                    s3.getObject({ Bucket: bucket, Key: key }, (err, res) => {
                        expect(err).toBe(null);
                        expect(res.ETag).toBe(`"${correctMD5}"`);
                        done();
                    });
                });
            });
        });

        test('should put an object to file with no location header', done => {
            process.stdout.write('Creating bucket\n');
            return s3.createBucket({ Bucket: bucket,
                CreateBucketConfiguration: {
                    LocationConstraint: fileLocation,
                },
            }, err => {
                expect(err).toEqual(null);
                process.stdout.write('Putting object\n');
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key, Body: body };
                return s3.putObject(params, err => {
                    expect(err).toEqual(null);
                    s3.getObject({ Bucket: bucket, Key: key }, (err, res) => {
                        expect(err).toBe(null);
                        expect(res.ETag).toBe(`"${correctMD5}"`);
                        done();
                    });
                });
            });
        });

        test('should put an object to AWS with no location header', done => {
            process.stdout.write('Creating bucket\n');
            return s3.createBucket({ Bucket: bucket,
                CreateBucketConfiguration: {
                    LocationConstraint: awsLocation,
                },
            }, err => {
                expect(err).toEqual(null);
                process.stdout.write('Putting object\n');
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key, Body: body };
                return s3.putObject(params, err => {
                    expect(err).toEqual(null);
                    return awsGetCheck(key, correctMD5, correctMD5, undefined,
                        () => done());
                });
            });
        });
    });
});

describe('MultipleBackend put based on request endpoint', () => {
    withV4(sigCfg => {
        beforeAll(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });
        afterAll(() => {
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

        test('should create bucket in corresponding backend', done => {
            process.stdout.write('Creating bucket');
            const request = s3.createBucket({ Bucket: bucket });
            request.on('build', () => {
                request.httpRequest.body = '';
            });
            request.send(err => {
                expect(err).toBe(null);
                const key = `somekey-${genUniqID()}`;
                s3.putObject({ Bucket: bucket, Key: key, Body: body }, err => {
                    expect(err).toBe(null);
                    const host = request.service.endpoint.hostname;
                    let endpoint = config.restEndpoints[host];
                    // s3 returns '' for us-east-1
                    if (endpoint === 'us-east-1') {
                        endpoint = '';
                    }
                    s3.getBucketLocation({ Bucket: bucket }, (err, data) => {
                        expect(err).toBe(null);
                        expect(data.LocationConstraint).toBe(endpoint);
                        s3.getObject({ Bucket: bucket, Key: key },
                        (err, res) => {
                            expect(err).toBe(null);
                            expect(res.ETag).toBe(`"${correctMD5}"`);
                            done();
                        });
                    });
                });
            });
        });
    });
});
