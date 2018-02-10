const assert = require('assert');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { describeSkipIfNotMultiple, gcpClient, gcpBucket,
    gcpLocation, fileLocation } = require('../utils');

const bucket = 'buckettestmultiplebackendput-gcp';
const body = Buffer.from('I am a body', 'utf8');
const bigBody = Buffer.alloc(10485760);
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const emptyMD5 = 'd41d8cd98f00b204e9800998ecf8427e';
const bigS3MD5 = 'f1c9645dbc14efddc7d8a322685f26eb';
const bigGCPMD5 = 'a7d414b9133d6483d9a1c4e04e856e3b-2';

let bucketUtil;
let s3;

const retryTimeout = 10000;

function checkGcp(key, gcpMD5, location, callback) {
    gcpClient.getObject({
        Bucket: gcpBucket,
        Key: key,
    }, (err, res) => {
        assert.equal(err, null, `Expected success, got error ${err}`);
        if (res.Metadata && res.Metadata['scal-etag']) {
            assert.strictEqual(res.Metadata['scal-etag'], gcpMD5);
        } else {
            assert.strictEqual(
                res.ETag.substring(1, res.ETag.length - 1), gcpMD5);
        }
        assert.strictEqual(res.Metadata['scal-location-constraint'],
            location);
        callback(res);
    });
}

function checkGcpError(key, expectedError, callback) {
    setTimeout(() => {
        gcpClient.getObject({
            Bucket: gcpBucket,
            Key: key,
        }, err => {
            assert.notStrictEqual(err, undefined,
                'Expected error but did not find one');
            assert.strictEqual(err.code, expectedError,
                `Expected error code ${expectedError} but got ${err.code}`);
            callback();
        });
    }, 1000);
}

function gcpGetCheck(objectKey, s3MD5, gcpMD5, location, callback) {
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
        `on call to GCP through S3: ${err}`);
        assert.strictEqual(res.ETag, `"${s3MD5}"`);
        assert.strictEqual(res.Metadata['scal-location-constraint'],
            location);
        process.stdout.write('Getting object from GCP\n');
        return checkGcp(objectKey, gcpMD5, location, callback);
    });
}

describeSkipIfNotMultiple('MultipleBackend put object to GCP', function
describeFn() {
    this.timeout(250000);
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

        describe('with set location from "x-amz-meta-scal-' +
            'location-constraint" header', function describe() {
            if (!process.env.S3_END_TO_END) {
                this.retries(2);
            }

            it('should put a 0-byte object to GCP', done => {
                const key = `somekey-${Date.now()}`;
                const params = { Bucket: bucket, Key: key,
                    Metadata: { 'scal-location-constraint': gcpLocation },
                };
                return s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                    `got error ${err}`);
                    return gcpGetCheck(key, emptyMD5, emptyMD5,
                        gcpLocation, () => done());
                });
            });

            it('should put an object to GCP', done => {
                const key = `somekey-${Date.now()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': gcpLocation },
                };
                return s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                    `got error ${err}`);
                    return gcpGetCheck(key, correctMD5, correctMD5,
                        gcpLocation, () => done());
                });
            });

            it('should put a large object to GCP', done => {
                const key = `somekey-${Date.now()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: bigBody,
                    Metadata: { 'scal-location-constraint': gcpLocation } };
                return s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected sucess, ' +
                        `got error ${err}`);
                    return gcpGetCheck(key, bigS3MD5, bigGCPMD5,
                        gcpLocation, () => done());
                });
            });

            it('should put objects with same key to GCP ' +
            'then file, and object should only be present in file', done => {
                const key = `somekey-${Date.now()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': gcpLocation } };
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
                            return checkGcpError(key, 'NoSuchKey',
                                () => done());
                        });
                    });
                });
            });

            it('should put objects with same key to file ' +
            'then GCP, and object should only be present on GCP', done => {
                const key = `somekey-${Date.now()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': fileLocation } };
                return s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${err}`);
                    params.Metadata = {
                        'scal-location-constraint': gcpLocation };
                    return s3.putObject(params, err => {
                        assert.equal(err, null, 'Expected success, ' +
                            `got error ${err}`);
                        return gcpGetCheck(key, correctMD5, correctMD5,
                            gcpLocation, () => done());
                    });
                });
            });

            it('should put two objects to GCP with same ' +
            'key, and newest object should be returned', done => {
                const key = `somekey-${Date.now()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': gcpLocation,
                        'unique-header': 'first object' } };
                return s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${err}`);
                    params.Metadata = { 'scal-location-constraint': gcpLocation,
                        'unique-header': 'second object' };
                    return s3.putObject(params, err => {
                        assert.equal(err, null, 'Expected success, ' +
                            `got error ${err}`);
                        return gcpGetCheck(key, correctMD5, correctMD5,
                            gcpLocation, result => {
                                assert.strictEqual(result.Metadata
                                    ['unique-header'], 'second object');
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

        it('should put an object to GCP with no location header', done => {
            process.stdout.write('Creating bucket\n');
            return s3.createBucket({ Bucket: bucket,
                CreateBucketConfiguration: {
                    LocationConstraint: gcpLocation,
                },
            }, err => {
                assert.equal(err, null, `Error creating bucket: ${err}`);
                process.stdout.write('Putting object\n');
                const key = `somekey-${Date.now()}`;
                const params = { Bucket: bucket, Key: key, Body: body };
                return s3.putObject(params, err => {
                    assert.equal(err, null,
                        `Expected success, got error ${err}`);
                    return gcpGetCheck(key, correctMD5, correctMD5, undefined,
                        () => done());
                });
            });
        });
    });
});

