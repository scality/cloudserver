const assert = require('assert');
const tv4 = require('tv4');
const Promise = require('bluebird');
const async = require('async');
const { S3 } = require('aws-sdk');

const BucketUtility = require('../../lib/utility/bucket-util');
const getConfig = require('../support/config');
const withV4 = require('../support/withV4');
const svcSchema = require('../../schema/service');

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

        test('should return 403 and AccessDenied', done => {
            s3.makeUnauthenticatedRequest('listBuckets', error => {
                expect(error).toBeTruthy();

                expect(error.statusCode).toBe(403);
                expect(error.code).toBe('AccessDenied');

                done();
            });
        });
    });

    withV4(sigCfg => {
        describe('when user has invalid credential', () => {
            let testFn;

            beforeAll(() => {
                testFn = function testFn(config, code, statusCode, done) {
                    const s3 = new S3(config);
                    s3.listBuckets((err, data) => {
                        expect(err).toBeTruthy();
                        assert.ifError(data);

                        expect(err.statusCode).toBe(statusCode);
                        expect(err.code).toBe(code);
                        done();
                    });
                };
            });

            test('should return 403 and InvalidAccessKeyId ' +
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

            test('should return 403 and SignatureDoesNotMatch ' +
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

            beforeAll(done => {
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

            afterAll(done => {
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

            test('should list buckets concurrently', done => {
                async.times(20, (n, next) => {
                    s3.listBuckets((err, result) => {
                        expect(result.Buckets.length).toEqual(createdBuckets.length);
                        next(err);
                    });
                },
                err => {
                    assert.ifError(err, `error listing buckets: ${err}`);
                    done();
                });
            });

            test('should list buckets', done => {
                s3
                    .listBucketsAsync()
                    .then(data => {
                        const isValidResponse = tv4.validate(data, svcSchema);
                        if (!isValidResponse) {
                            throw new Error(tv4.error);
                        }
                        expect(data.Buckets[0].CreationDate instanceof Date).toBeTruthy();

                        return data;
                    })
                    .then(data => {
                        const buckets = data.Buckets.filter(bucket =>
                            createdBuckets.indexOf(bucket.Name) > -1
                        );

                        expect(buckets.length).toEqual(createdBuckets.length);

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

                        expect(isCorrectOrder).toBeTruthy();
                        done();
                    })
                    .catch(done);
            });

            const filterFn = bucket => createdBuckets.indexOf(bucket.name) > -1;

            describe('two accounts are given', () => {
                let anotherS3;

                beforeAll(() => {
                    anotherS3 = Promise.promisifyAll(new S3(getConfig('lisa')));
                });

                test('should not return other accounts bucket list', done => {
                    anotherS3
                        .listBucketsAsync()
                        .then(data => {
                            const hasSameBuckets = data.Buckets
                                .filter(filterFn)
                                .length;

                            expect(hasSameBuckets).toBe(0);
                            done();
                        })
                        .catch(done);
                });
            });
        });
    });
});
