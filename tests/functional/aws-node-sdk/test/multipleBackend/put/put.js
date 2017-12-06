const assert = require('assert');
const async = require('async');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { config } = require('../../../../../../lib/Config');
const { createEncryptedBucketPromise } =
    require('../../../lib/utility/createEncryptedBucket');
const { versioningEnabled } = require('../../../lib/utility/versioning-util');

const { describeSkipIfNotMultiple, getAwsRetry,
    awsLocationEncryption, memLocation, fileLocation,
    withGCP, s3Locations } = require('../utils');
const bucket = 'buckettestmultiplebackendput';
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

function getAwsSuccess(key, awsMD5, location, s3Type, cb) {
    return getAwsRetry({ key, s3Type }, 0, (err, res) => {
        assert.strictEqual(err, null, 'Expected success, got error ' +
        `on direct ${s3Type} call: ${err}`);
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
        return cb(res);
    });
}

function getAwsError(key, expectedError, s3Type, cb) {
    return getAwsRetry({ key, s3Type }, 0, err => {
        assert.notStrictEqual(err, undefined,
            'Expected error but did not find one');
        assert.strictEqual(err.code, expectedError,
            `Expected error code ${expectedError} but got ${err.code}`);
        cb();
    });
}

function awsGetCheck(objectKey, s3MD5, awsMD5, location, s3Type, cb) {
    process.stdout.write('Getting object\n');
    s3.getObject({ Bucket: bucket, Key: objectKey },
    function s3GetCallback(err, res) {
        if (err && err.code === 'NetworkingError') {
            return setTimeout(() => {
                process.stdout.write('Getting object retry\n');
                s3.getObject({ Bucket: bucket, Key: objectKey }, s3GetCallback);
            }, retryTimeout);
        }
        assert.strictEqual(err, null, 'Expected success, got error ' +
        `on call to ${s3Type} through S3: ${err}`);
        assert.strictEqual(res.ETag, `"${s3MD5}"`);
        assert.strictEqual(res.Metadata['scal-location-constraint'],
            location);
        process.stdout.write(`Getting object from ${s3Type}\n`);
        return getAwsSuccess(objectKey, awsMD5, location, s3Type, cb);
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
                const key = `somekey-${Date.now()}`;
                s3.putObject({ Bucket: '', Key: key }, err => {
                    assert.notEqual(err, null,
                        'Expected failure but got success');
                    assert.strictEqual(err.code, 'MethodNotAllowed');
                    done();
                });
            });

        describeSkipIfNotMultiple('with set location from "x-amz-meta-scal-' +
        'location-constraint" header', withGCP(s3Type => {
            if (!process.env.S3_END_TO_END) {
                this.retries(2);
            }
            const { s3Location, s3LocationEncryption }
                = s3Locations[s3Type];
            const skipIfGCP = s3Type === 'GCP' ? it.skip : it;

            skipIfGCP('should return an error to put request without a valid ' +
                'location constraint', done => {
                const key = `somekey-${Date.now()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: {
                        'scal-location-constraint': 'fail-region' } };
                s3.putObject(params, err => {
                    assert.notEqual(err, null, 'Expected failure but got ' +
                        'success');
                    assert.strictEqual(err.code, 'InvalidArgument');
                    done();
                });
            });

            skipIfGCP('should put an object to mem', done => {
                const key = `somekey-${Date.now()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': memLocation },
                };
                s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${err}`);
                    s3.getObject({ Bucket: bucket, Key: key },
                    (err, res) => {
                        assert.strictEqual(err, null, 'Expected success, ' +
                        `got error ${err}`);
                        assert.strictEqual(res.ETag, `"${correctMD5}"`);
                        done();
                    });
                });
            });

            skipIfGCP('should put a 0-byte object to mem', done => {
                const key = `somekey-${Date.now()}`;
                const params = { Bucket: bucket, Key: key,
                    Metadata: { 'scal-location-constraint': memLocation },
                };
                s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${err}`);
                    s3.getObject({ Bucket: bucket, Key: key },
                    (err, res) => {
                        assert.strictEqual(err, null, 'Expected success, ' +
                        `got error ${err}`);
                        assert.strictEqual(res.ETag, `"${emptyMD5}"`);
                        done();
                    });
                });
            });

            skipIfGCP('should put an object to file', done => {
                const key = `somekey-${Date.now()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': fileLocation },
                };
                s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${err}`);
                    s3.getObject({ Bucket: bucket, Key: key },
                    (err, res) => {
                        assert.strictEqual(err, null, 'Expected success, ' +
                        `got error ${err}`);
                        assert.strictEqual(res.ETag, `"${correctMD5}"`);
                        done();
                    });
                });
            });

            it(`should put a 0-byte object to ${s3Type}`, done => {
                const key = `somekey-${Date.now()}`;
                const params = { Bucket: bucket, Key: key,
                    Metadata: { 'scal-location-constraint': s3Location },
                };
                return s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                    `got error ${err}`);
                    return awsGetCheck(key, emptyMD5, emptyMD5, s3Location,
                        s3Type, () => done());
                });
            });


            it(`should put an object to ${s3Type}`, done => {
                const key = `somekey-${Date.now()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': s3Location } };
                return s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${err}`);
                    return awsGetCheck(key, correctMD5, correctMD5, s3Location,
                        s3Type, () => done());
                });
            });

            it('should encrypt body only if bucket encrypted putting ' +
            `object to ${s3Type}`,
            done => {
                const key = `somekey-${Date.now()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': s3Location } };
                return s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${err}`);
                    return getAwsSuccess(key, correctMD5, s3Location,
                        s3Type, () => done());
                });
            });

            it(`should put an object to ${s3Type} with encryption`, done => {
                const key = `somekey-${Date.now()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint':
                    s3LocationEncryption } };
                return s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${err}`);
                    return awsGetCheck(key, correctMD5, correctMD5,
                      s3LocationEncryption, s3Type, () => done());
                });
            });

            it('should return a version id putting object to ' +
            `to ${s3Type} with versioning enabled`, done => {
                const key = `somekey-${Date.now()}`;
                const params = { Bucket: bucket, Key: key, Body: body,
                    Metadata: { 'scal-location-constraint': s3Location } };
                async.waterfall([
                    next => s3.putBucketVersioning({
                        Bucket: bucket,
                        VersioningConfiguration: versioningEnabled,
                    }, err => next(err)),
                    next => s3.putObject(params, (err, res) => {
                        assert.strictEqual(err, null, 'Expected success ' +
                            `putting object, got error ${err}`);
                        assert(res.VersionId);
                        next(null, res.ETag);
                    }),
                    (eTag, next) => getAwsSuccess(key, correctMD5, s3Location,
                        s3Type, () => next()),
                ], done);
            });

            skipIfGCP(`should put a large object to ${s3Type}`, done => {
                const key = `somekey-${Date.now()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: bigBody,
                    Metadata: { 'scal-location-constraint': s3Location } };
                return s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected sucess, ' +
                        `got error ${err}`);
                    return awsGetCheck(key, bigS3MD5, bigAWSMD5, s3Location,
                        s3Type, () => done());
                });
            });

            it(`should put objects with same key to ${s3Type} ` +
            'then file, and object should only be present in file', done => {
                const key = `somekey-${Date.now()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': s3Location } };
                return s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${err}`);
                    params.Metadata =
                        { 'scal-location-constraint': fileLocation };
                    return s3.putObject(params, err => {
                        assert.equal(err, null, 'Expected success, ' +
                            `got error ${err}`);
                        return s3.getObject({ Bucket: bucket, Key: key },
                        (err, res) => {
                            assert.equal(err, null, 'Expected success, ' +
                                `got error ${err}`);
                            assert.strictEqual(
                                res.Metadata['scal-location-constraint'],
                                fileLocation);
                            return getAwsError(key, 'NoSuchKey', s3Type, done);
                        });
                    });
                });
            });

            it(`should put objects with same key to file then ${s3Type}, ` +
            `and object should only be present on ${s3Type}`, done => {
                const key = `somekey-${Date.now()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': fileLocation } };
                return s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${err}`);
                    params.Metadata = {
                        'scal-location-constraint': s3Location };
                    return s3.putObject(params, err => {
                        assert.equal(err, null, 'Expected success, ' +
                            `got error ${err}`);
                        return awsGetCheck(key, correctMD5, correctMD5,
                            s3Location, s3Type, () => done());
                    });
                });
            });

            it(`should put two objects to ${s3Type} with same ` +
            'key, and newest object should be returned', done => {
                const key = `somekey-${Date.now()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': s3Location,
                        'unique-header': 'first object' } };
                return s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${err}`);
                    params.Metadata = { 'scal-location-constraint': s3Location,
                        'unique-header': 'second object' };
                    return s3.putObject(params, err => {
                        assert.equal(err, null, 'Expected success, ' +
                            `got error ${err}`);
                        return awsGetCheck(key, correctMD5, correctMD5,
                        s3Location, s3Type, result => {
                            assert.strictEqual(result.Metadata
                                ['unique-header'], 'second object');
                            done();
                        });
                    });
                });
            });
        }));
    });
});

describeSkipIfNotMultiple('MultipleBackend put object based on bucket location',
withGCP(s3Type => {
    withV4(sigCfg => {
        const { s3Location } = s3Locations[s3Type];
        const skipIfGCP = s3Type === 'GCP' ? it.skip : it;

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

        skipIfGCP('should put an object to mem with no location header',
        done => {
            process.stdout.write('Creating bucket\n');
            return s3.createBucket({ Bucket: bucket,
                CreateBucketConfiguration: {
                    LocationConstraint: memLocation,
                },
            }, err => {
                assert.equal(err, null, `Error creating bucket: ${err}`);
                process.stdout.write('Putting object\n');
                const key = `somekey-${Date.now()}`;
                const params = { Bucket: bucket, Key: key, Body: body };
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

        skipIfGCP('should put an object to file with no location header',
        done => {
            process.stdout.write('Creating bucket\n');
            return s3.createBucket({ Bucket: bucket,
                CreateBucketConfiguration: {
                    LocationConstraint: fileLocation,
                },
            }, err => {
                assert.equal(err, null, `Error creating bucket: ${err}`);
                process.stdout.write('Putting object\n');
                const key = `somekey-${Date.now()}`;
                const params = { Bucket: bucket, Key: key, Body: body };
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

        it(`should put an object to ${s3Type} with no location header`,
        done => {
            process.stdout.write('Creating bucket\n');
            return s3.createBucket({ Bucket: bucket,
                CreateBucketConfiguration: {
                    LocationConstraint: s3Location,
                },
            }, err => {
                assert.equal(err, null, `Error creating bucket: ${err}`);
                process.stdout.write('Putting object\n');
                const key = `somekey-${Date.now()}`;
                const params = { Bucket: bucket, Key: key, Body: body };
                return s3.putObject(params, err => {
                    assert.equal(err, null,
                        `Expected success, got error ${err}`);
                    return awsGetCheck(key, correctMD5, correctMD5, undefined,
                        s3Type, () => done());
                });
            });
        });
    });
}));

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

        it('should create bucket in corresponding backend', done => {
            process.stdout.write('Creating bucket');
            const request = s3.createBucket({ Bucket: bucket });
            request.on('build', () => {
                request.httpRequest.body = '';
            });
            request.send(err => {
                assert.strictEqual(err, null, `Error creating bucket: ${err}`);
                const key = `somekey-${Date.now()}`;
                s3.putObject({ Bucket: bucket, Key: key, Body: body }, err => {
                    assert.strictEqual(err, null, 'Expected succes, ' +
                        `got error ${JSON.stringify(err)}`);
                    const host = request.service.endpoint.hostname;
                    let endpoint = config.restEndpoints[host];
                    // s3 returns '' for us-east-1
                    if (endpoint === 'us-east-1') {
                        endpoint = '';
                    }
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
