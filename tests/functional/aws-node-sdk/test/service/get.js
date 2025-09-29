const assert = require('assert');
const tv4 = require('tv4');
const { 
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    ListBucketsCommand
} = require('@aws-sdk/client-s3');

const BucketUtility = require('../../lib/utility/bucket-util');
const getConfig = require('../support/config');
const withV4 = require('../support/withV4');
const svcSchema = require('../../schema/service');

const describeFn = process.env.AWS_ON_AIR
    ? describe.skip
    : describe;

async function cleanBucket(bucketUtils, s3, Bucket) {
    await bucketUtils.empty(Bucket);
    await s3.send(new DeleteBucketCommand({ Bucket }));
}

async function cleanAllBuckets(bucketUtils, s3) {
    let listingLoop = true;
    let ContinuationToken;

    process.stdout.write('Try cleaning all buckets before running the test\n');

    while (listingLoop) {
        const list = await s3.send(new ListBucketsCommand({ ContinuationToken }));
        ContinuationToken = list.ContinuationToken;
        listingLoop = !!ContinuationToken;

        if (list.Buckets.length) {
            // process.stdout
            //     .write(`Found ${list.Buckets.length} buckets to clean:\n${
            //         JSON.stringify(list.Buckets, null, 2)}\n`);
        }

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
            try {
                const data = await unauthenticatedBucketUtil.s3.send(new ListBucketsCommand());
                assert.ifError(data);
            } catch (err) {
                assert(err);
                assert.strictEqual(err.$metadata.httpStatusCode, 403);
                assert.strictEqual(err.Code, 'AccessDenied');
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
                        const data = await s3.send(new ListBucketsCommand());
                        assert.ifError(data);
                    } catch (err) {
                        assert(err);
                        assert.strictEqual(err.$metadata.httpStatusCode, statusCode);
                        assert.strictEqual(err.Code, code);
                    }
                };
            });

            it('should return 403 and InvalidAccessKeyId ' +
                'if accessKeyId is invalid', async () => {
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
            const timestamp = Date.now();
            const createdBuckets = Array.from(Array(bucketsNumber).keys())
                .map(i => `getservicebuckets-${timestamp}-${i}`);

            before(async () => {
                // eslint-disable-next-line no-param-reassign
                sigCfg = {
                    maxRetries: 0,
                    httpOptions: { timeout: 0 },
                    ...sigCfg,
                };
                bucketUtil = new BucketUtility('default', sigCfg);
                s3 = bucketUtil.s3;
                
                // if other tests failed to delete their buckets, listings might be wrong
                // try to clean all buckets before running the test
                await cleanAllBuckets(bucketUtil, s3);
                
                // Create buckets in batches of 10
                for (let i = 0; i < createdBuckets.length; i += 10) {
                    const batch = createdBuckets.slice(i, i + 10);
                    await Promise.all(batch.map(async bucketName => {
                        try {
                            await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
                            if (bucketName.endsWith('000')) {
                                // log to keep ci alive
                                process.stdout.write(`creating bucket: ${bucketName}\n`);
                            }
                        } catch (err) {
                            process.stdout.write(`err creating bucket ${bucketName}: ${err}\n`);
                            throw err;
                        }
                    }));
                }
            });

            after(async () => {
                // Delete buckets in batches of 10
                for (let i = 0; i < createdBuckets.length; i += 10) {
                    const batch = createdBuckets.slice(i, i + 10);
                    await Promise.all(batch.map(async bucketName => {
                        try {
                            await s3.send(new DeleteBucketCommand({ Bucket: bucketName }));
                            if (bucketName.endsWith('000')) {
                                // log to keep ci alive
                                process.stdout.write(`deleting bucket: ${bucketName}\n`);
                            }
                        } catch (err) {
                            process.stdout.write(`err deleting bucket ${bucketName}: ${err}\n`);
                            // Continue with other deletions even if one fails
                        }
                    }));
                }
            });


            it('should list buckets concurrently', async () => {
                const promises = Array.from({ length: 20 }, async () => {
                    const result = await s3.send(new ListBucketsCommand());
                    assert.equal(result.Buckets.length,
                        createdBuckets.length,
                        'Created buckets are missing in response');
                });
                
                await Promise.all(promises);
            });

            it('should list buckets', async () => {
                const data = await s3.send(new ListBucketsCommand());
                
                const isValidResponse = tv4.validate(data, svcSchema);
                if (!isValidResponse) {
                    throw new Error(tv4.error);
                }
                assert.ok(data.Buckets[0].CreationDate instanceof Date);

                const buckets = data.Buckets.filter(bucket =>
                    createdBuckets.indexOf(bucket.Name) > -1
                );

                assert.equal(buckets.length, createdBuckets.length,
                    'Created buckets are missing in response');

                // Sort createdBuckets in alphabetical order
                createdBuckets.sort();

                const isCorrectOrder = buckets
                    .reduce(
                        (prev, bucket, idx) =>
                        prev && bucket.Name === createdBuckets[idx]
                    , true);

                assert.ok(isCorrectOrder,
                    'Not returning created buckets by alphabetically');
            });

            const filterFn = bucket => createdBuckets.indexOf(bucket.Name) > -1;

            describe('two accounts are given', () => {
                let anotherS3;

                before(() => {
                    anotherS3 = new S3Client(getConfig('lisa'));
                });

                it('should not return other accounts bucket list', async () => {
                    const data = await anotherS3.send(new ListBucketsCommand());
                    const hasSameBuckets = data.Buckets
                        .filter(filterFn)
                        .length;

                    assert.strictEqual(hasSameBuckets, 0,
                        'It has other buddies bucket');
                });
            });
        });
    });
});
