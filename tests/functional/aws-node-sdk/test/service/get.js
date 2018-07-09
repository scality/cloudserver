const assert = require('assert');
const tv4 = require('tv4');
const Promise = require('bluebird');
const async = require('async');
const { S3 } = require('aws-sdk');

const BucketUtility = require('../../lib/utility/bucket-util');
const getConfig = require('../support/config');
const withV4 = require('../support/withV4');
const svcSchema = require('../../schema/service');
const testBucket = 'testbucket';

const describeFn = process.env.AWS_ON_AIR
    ? describe.skip
    : describe;

describeFn('GET Service - AWS.S3.listBuckets', function getService() {
    this.timeout(600000);

    describe('When user is unauthorized', () => {
        let s3;
        let config;

        beforeEach(() => {
            config = getConfig('default');
            s3 = new S3(config);
        });

        it('should return 403 and AccessDenied', done => {
            s3.makeUnauthenticatedRequest('listBuckets', error => {
                assert(error);

                assert.strictEqual(error.statusCode, 403);
                assert.strictEqual(error.code, 'AccessDenied');

                done();
            });
        });
    });

    describe('List Objects V2', () => {
        let s3;
        let config;

        beforeEach(done => {
            config = getConfig('default');
            s3 = new S3(config);
            s3.createBucket({ Bucket: testBucket }, done);
        });

        it('should return NotImplemented', done => {
            s3.listObjectsV2({ Bucket: testBucket }, error => {
                assert(error);
                assert.strictEqual(error.statusCode, 501);
                assert.strictEqual(error.code, 'NotImplemented');
                done();
            });
        });
    });

    withV4(sigCfg => {
        describe('when user has invalid credential', () => {
            let testFn;

            before(() => {
                testFn = function testFn(config, code, statusCode, done) {
                    const s3 = new S3(config);
                    s3.listBuckets((err, data) => {
                        assert(err);
                        assert.ifError(data);

                        assert.strictEqual(err.statusCode, statusCode);
                        assert.strictEqual(err.code, code);
                        done();
                    });
                };
            });

            it('should return 403 and InvalidAccessKeyId ' +
                'if accessKeyId is invalid', done => {
                const invalidAccess = getConfig('default',
                    Object.assign({},
                        {
                            credentials: null,
                            accessKeyId: 'wrong',
                            secretAccessKey: 'wrong again',
                        },
                        sigCfg
                    )
                );
                const expectedCode = 'InvalidAccessKeyId';
                const expectedStatus = 403;

                testFn(invalidAccess, expectedCode, expectedStatus, done);
            });

            it('should return 403 and SignatureDoesNotMatch ' +
                'if credential is polluted', done => {
                const pollutedConfig = getConfig('default', sigCfg);
                pollutedConfig.credentials.secretAccessKey = 'wrong';

                const expectedCode = 'SignatureDoesNotMatch';
                const expectedStatus = 403;

                testFn(pollutedConfig, expectedCode, expectedStatus, done);
            });
        });

        describe('when user has credential', () => {
            let bucketUtil;
            let s3;
            const bucketsNumber = 1001;
            process.stdout
                .write(`testing listing with ${bucketsNumber} buckets\n`);
            const createdBuckets = Array.from(Array(bucketsNumber).keys())
                .map(i => `getservicebuckets-${i}`);

            before(done => {
                bucketUtil = new BucketUtility('default', sigCfg);
                s3 = bucketUtil.s3;
                s3.config.update({ maxRetries: 0 });
                s3.config.update({ httpOptions: { timeout: 0 } });
                async.eachLimit(createdBuckets, 10, (bucketName, moveOn) => {
                    s3.createBucket({ Bucket: bucketName }, err => {
                        if (bucketName.endsWith('000')) {
                            // log to keep ci alive
                            process.stdout
                                .write(`creating bucket: ${bucketName}\n`);
                        }
                        moveOn(err);
                    });
                },
                err => {
                    if (err) {
                        process.stdout.write(`err creating buckets: ${err}`);
                    }
                    done(err);
                });
            });

            after(done => {
                async.eachLimit(createdBuckets, 10, (bucketName, moveOn) => {
                    s3.deleteBucket({ Bucket: bucketName }, err => {
                        if (bucketName.endsWith('000')) {
                            // log to keep ci alive
                            process.stdout
                            .write(`deleting bucket: ${bucketName}\n`);
                        }
                        moveOn(err);
                    });
                },
                err => {
                    if (err) {
                        process.stdout.write(`err deleting buckets: ${err}`);
                    }
                    done(err);
                });
            });

            it('should list buckets concurrently', done => {
                async.times(20, (n, next) => {
                    s3.listBuckets((err, result) => {
                        assert.equal(result.Buckets.length,
                            createdBuckets.length,
                            'Created buckets are missing in response');
                        next(err);
                    });
                },
                err => {
                    assert.ifError(err, `error listing buckets: ${err}`);
                    done();
                });
            });

            it('should list buckets', done => {
                s3
                    .listBucketsAsync()
                    .then(data => {
                        const isValidResponse = tv4.validate(data, svcSchema);
                        if (!isValidResponse) {
                            throw new Error(tv4.error);
                        }
                        assert.ok(data.Buckets[0].CreationDate instanceof Date);

                        return data;
                    })
                    .then(data => {
                        const buckets = data.Buckets.filter(bucket =>
                            createdBuckets.indexOf(bucket.Name) > -1
                        );

                        assert.equal(buckets.length, createdBuckets.length,
                            'Created buckets are missing in response');

                        return buckets;
                    })
                    .then(buckets => {
                        // Sort createdBuckets in alphabetical order
                        createdBuckets.sort();

                        const isCorrectOrder = buckets
                            .reduce(
                                (prev, bucket, idx) =>
                                prev && bucket.Name === createdBuckets[idx]
                            , true);

                        assert.ok(isCorrectOrder,
                            'Not returning created buckets by alphabetically');
                        done();
                    })
                    .catch(done);
            });

            const filterFn = bucket => createdBuckets.indexOf(bucket.name) > -1;

            describe('two accounts are given', () => {
                let anotherS3;

                before(() => {
                    anotherS3 = Promise.promisifyAll(new S3(getConfig('lisa')));
                });

                it('should not return other accounts bucket list', done => {
                    anotherS3
                        .listBucketsAsync()
                        .then(data => {
                            const hasSameBuckets = data.Buckets
                                .filter(filterFn)
                                .length;

                            assert.strictEqual(hasSameBuckets, 0,
                                'It has other buddies bucket');
                            done();
                        })
                        .catch(done);
                });
            });
        });
    });
});
