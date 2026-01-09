const assert = require('assert');
const tv4 = require('tv4');
const async = require('async');
const {
    S3Client,
    ListBucketsCommand,
    CreateBucketCommand,
    DeleteBucketCommand,
} = require('@aws-sdk/client-s3');

const BucketUtility = require('../../lib/utility/bucket-util');
const getConfig = require('../support/config');
const withV4 = require('../support/withV4');
const svcSchema = require('../../schema/service');

const describeFn = process.env.AWS_ON_AIR
    ? describe.skip
    : describe;

async function cleanBucket(bucketUtils, s3, Bucket) {
    try {
        await bucketUtils.empty(Bucket, true);
        await bucketUtils.deleteOne(Bucket);
    } catch (error) {
        process.stdout
            .write(`Error emptying and deleting bucket: ${error}\n`);
        // ignore the error and continue
    }
}

async function cleanAllBuckets(bucketUtils, s3) {
    process.stdout.write('Try cleaning all buckets before running the test\n');

    // ListBuckets doesn't support pagination, it returns all buckets at once
    const list = await s3.send(new ListBucketsCommand({}));

    if (list.Buckets && list.Buckets.length) {
        process.stdout
            .write(`Found ${list.Buckets.length} buckets to clean:\n${
                JSON.stringify(list.Buckets, null, 2)}\n`);

        // clean sequentially to avoid overloading
        for (const bucket of list.Buckets) {
            await cleanBucket(bucketUtils, s3, bucket.Name);
        }
    }
}

describeFn('GET Service - AWS.S3.listBuckets', function getService() {
    this.timeout(600000);
    let unauthenticatedBucketUtil;

    describe('When user is unauthorized', () => {

        beforeEach(() => {
            const config = getConfig('default');
            unauthenticatedBucketUtil = new BucketUtility('default', config, true);
        });

        it('should return 403 and AccessDenied', async () => {
            const s3Unauth = unauthenticatedBucketUtil.s3;

            try {
                await s3Unauth.send(new ListBucketsCommand({}));
                throw new Error('Should have thrown an error');
            } catch (error) {
                assert.strictEqual(error.$metadata?.httpStatusCode || error.statusCode, 403);
                assert.strictEqual(error.name, 'AccessDenied');
            }
        });
    });

    withV4(sigCfg => {
        describe('when user has invalid credential', () => {
            let testFn;

            before(() => {
                testFn = async function testFn(config, code, statusCode) {
                    const s3 = new S3Client(config);
                    try {
                        await s3.send(new ListBucketsCommand({}));
                        throw new Error('Should have thrown an error');
                    } catch (err) {
                        assert.strictEqual(err.$metadata?.httpStatusCode || err.statusCode, statusCode);
                        assert.strictEqual(err.name, code);
                    }
                };
            });

            it('should return 403 and InvalidAccessKeyId ' +
                'if accessKeyId is invalid', async () => {
                const invalidAccess = getConfig('default',
                    Object.assign({},
                        {
                            credentials: {
                                accessKeyId: 'wrong',
                                secretAccessKey: 'wrong again',
                            },
                        },
                        sigCfg
                    )
                );
                const expectedCode = 'InvalidAccessKeyId';
                const expectedStatus = 403;

                await testFn(invalidAccess, expectedCode, expectedStatus);
            });

            it('should return 403 and SignatureDoesNotMatch ' +
                'if credential is polluted', async () => {
                const pollutedConfig = getConfig('default', sigCfg);
                pollutedConfig.credentials.secretAccessKey = 'wrong';

                const expectedCode = 'SignatureDoesNotMatch';
                const expectedStatus = 403;

                await testFn(pollutedConfig, expectedCode, expectedStatus);
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
                async.series([
                    next => cleanAllBuckets(bucketUtil, s3).then(() => next()).catch(next),
                    next =>
                        async.eachLimit(createdBuckets, 10, (bucketName, moveOn) => {
                            s3.send(new CreateBucketCommand({ Bucket: bucketName }))
                                .then(() => {
                                    if (bucketName.endsWith('000')) {
                                        process.stdout
                                            .write(`creating bucket: ${bucketName}\n`);
                                    }
                                    moveOn();
                                })
                                .catch(err => {
                                    moveOn(err);
                                });
                        },
                        err => {
                            if (err) {
                                process.stdout.write(`err creating buckets: ${err}\n`);
                                return next(err);
                            }
                            return next(err);
                        })
                ], done);
            });

            after(done => {
                async.eachLimit(createdBuckets, 10, (bucketName, moveOn) => {
                    s3.send(new DeleteBucketCommand({ Bucket: bucketName }))
                        .then(() => {
                            if (bucketName.endsWith('000')) {
                                // log to keep ci alive
                                process.stdout
                                    .write(`deleting bucket: ${bucketName}\n`);
                            }
                            moveOn();
                        })
                        .catch(() => {
                            moveOn();
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
                    s3.send(new ListBucketsCommand({}))
                        .then(result => {
                            // Filter for our test buckets only
                            const ourBuckets = result.Buckets.filter(bucket => 
                                bucket.Name.startsWith('getservicebuckets-')
                            );
                            assert.equal(ourBuckets.length,
                                createdBuckets.length,
                                'Created buckets are missing in response');
                            next();
                        })
                        .catch(next);
                },
                err => {
                    assert.ifError(err, `error listing buckets: ${err}`);
                    done();
                });
            });

            it('should list buckets', done => {
                s3.send(new ListBucketsCommand({}))
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

            const filterFn = bucket => createdBuckets.indexOf(bucket.Name) > -1;

            describe('two accounts are given', () => {
                let anotherS3;

                before(() => {
                    anotherS3 = new S3Client(getConfig('lisa'));
                });

                it('should not return other accounts bucket list', done => {
                    anotherS3.send(new ListBucketsCommand({}))
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
