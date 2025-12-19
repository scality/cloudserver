const assert = require('assert');

const {
    CreateBucketCommand,
    PutObjectCommand,
    GetObjectCommand,
} = require('@aws-sdk/client-s3');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { describeSkipIfNotMultipleOrCeph, gcpClient, gcpBucket,
    gcpLocation, fileLocation, genUniqID } = require('../utils');

const bucket = `putgcp${genUniqID()}`;
const body = Buffer.from('I am a body', 'utf8');
const bigBody = Buffer.alloc(10485760);
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const emptyMD5 = 'd41d8cd98f00b204e9800998ecf8427e';
const bigS3MD5 = 'f1c9645dbc14efddc7d8a322685f26eb';
const bigGCPMD5 = 'a7d414b9133d6483d9a1c4e04e856e3b-2';

let bucketUtil;
let s3;

const retryTimeout = 10000;
const maxGetRetries = 3;

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
    const params = { Bucket: bucket, Key: objectKey };
    let retries = 0;
    function attempt() {
        s3.send(new GetObjectCommand(params))
            .then(res => {
                assert.strictEqual(res.ETag, `"${s3MD5}"`);
                const metadataLocation = res.Metadata &&
                    res.Metadata['scal-location-constraint'];
                assert.strictEqual(metadataLocation, location);
                process.stdout.write('Getting object from GCP\n');
                checkGcp(objectKey, gcpMD5, location, callback);
            })
            .catch(err => {
                if (err && err.name === 'NetworkingError') {
                    if (retries >= maxGetRetries) {
                        assert.fail(`GetObject retry limit reached (${maxGetRetries}) due to NetworkingError: ${err}`);
                    }
                    retries += 1;
                    setTimeout(() => {
                        process.stdout.write('Getting object retry\n');
                        attempt();
                    }, retryTimeout);
                    return;
                }
                assert.strictEqual(err, null, 'Expected success, got error ' +
                    `on call to GCP through S3: ${err}`);
            });
    }
    attempt();
}

describeSkipIfNotMultipleOrCeph('MultipleBackend put object to GCP', function
describeFn() {
    this.timeout(250000);
    withV4(sigCfg => {
        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.send(new CreateBucketCommand({ Bucket: bucket }))
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

            const putTests = [
                {
                    msg: 'should put a 0-byte object to GCP',
                    input: { Body: null, location: gcpLocation },
                    output: { s3MD5: emptyMD5, gcpMD5: emptyMD5 },
                },
                {
                    msg: 'should put an object to GCP',
                    input: { Body: body, location: gcpLocation },
                    output: { s3MD5: correctMD5, gcpMD5: correctMD5 },
                },
                {
                    msg: 'should put a large object to GCP',
                    input: { Body: bigBody, location: gcpLocation },
                    output: { s3MD5: bigS3MD5, gcpMD5: bigGCPMD5 },
                },
            ];
            putTests.forEach(test => {
                const { location, Body } = test.input;
                const { s3MD5, gcpMD5 } = test.output;
                it(test.msg, done => {
                    const key = `somekey-${genUniqID()}`;
                    const params = { Bucket: bucket, Key: key, Body,
                        Metadata: { 'scal-location-constraint': location },
                    };
                    return s3.send(new PutObjectCommand(params))
                        .then(() => {
                            gcpGetCheck(key, s3MD5, gcpMD5, location,
                                () => done());
                        })
                        .catch(done);
                });
            });
        });

        describe('with object rewrites', function describe() {
            if (!process.env.S3_END_TO_END) {
                this.retries(2);
            }

            it('should put objects with same key to GCP ' +
            'then file, and object should only be present in file', done => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': gcpLocation } };
                return s3.send(new PutObjectCommand(params))
                    .then(() => {
                        params.Metadata =
                            { 'scal-location-constraint': fileLocation };
                        return s3.send(new PutObjectCommand(params));
                    })
                    .then(() => s3.send(new GetObjectCommand({
                        Bucket: bucket,
                        Key: key,
                    })))
                    .then(res => {
                        assert.strictEqual(
                            res.Metadata['scal-location-constraint'],
                            fileLocation);
                        checkGcpError(key, 'NoSuchKey',
                            () => done());
                    })
                    .catch(done);
            });

            it('should put objects with same key to file ' +
            'then GCP, and object should only be present on GCP', done => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': fileLocation } };
                return s3.send(new PutObjectCommand(params))
                    .then(() => {
                        params.Metadata = {
                            'scal-location-constraint': gcpLocation };
                        return s3.send(new PutObjectCommand(params));
                    })
                    .then(() => {
                        gcpGetCheck(key, correctMD5, correctMD5,
                            gcpLocation, () => done());
                    })
                    .catch(done);
            });

            it('should put two objects to GCP with same ' +
            'key, and newest object should be returned', done => {
                const key = `somekey-${genUniqID()}`;
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': gcpLocation,
                        'unique-header': 'first object' } };
                return s3.send(new PutObjectCommand(params))
                    .then(() => {
                        params.Metadata = { 'scal-location-constraint': gcpLocation,
                            'unique-header': 'second object' };
                        return s3.send(new PutObjectCommand(params));
                    })
                    .then(() => {
                        gcpGetCheck(key, correctMD5, correctMD5,
                            gcpLocation, result => {
                                assert.strictEqual(result.Metadata
                                    ['unique-header'], 'second object');
                                done();
                            });
                    })
                    .catch(done);
            });
        });
    });
});

describeSkipIfNotMultipleOrCeph('MultipleBackend put object' +
                                'based on bucket location', () => {
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
            const key = `somekey-${genUniqID()}`;
            const params = { Bucket: bucket, Key: key, Body: body };
            return s3.send(new CreateBucketCommand({ Bucket: bucket,
                CreateBucketConfiguration: {
                    LocationConstraint: gcpLocation,
                },
            }))
                .then(() => {
                    process.stdout.write('Putting object\n');
                    return s3.send(new PutObjectCommand(params));
                })
                .then(() => {
                    gcpGetCheck(key, correctMD5, correctMD5, undefined,
                        () => done());
                })
                .catch(done);
        });
    });
});
