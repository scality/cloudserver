const assert = require('assert');
const tv4 = require('tv4');
const { S3Client, ListBucketsCommand, CreateBucketCommand, DeleteBucketCommand } = require('@aws-sdk/client-s3');

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
        let config;

        beforeEach(() => {
            config = getConfig('default');
        });

        it('should return 403 and AccessDenied', async () => {
            // v3 does not support unauthenticated requests directly, so simulate with invalid credentials
            const badS3 = new S3Client({ ...config, credentials: 
                { accessKeyId: 'invalid', secretAccessKey: 'invalid' } });
            try {
                await badS3.send(new ListBucketsCommand({}));
                assert.fail('Expected error');
            } catch (error) {
                assert(error);
                assert.strictEqual(error.$metadata?.httpStatusCode, 403);
                // v3 error code may be in error.Code or error.name
                assert(
                    error.Code === 'AccessDenied' || error.name === 'AccessDenied' 
                    || error.message.includes('AccessDenied'),
                    'Expected AccessDenied error'
                );
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
                        assert.fail('Expected error');
                    } catch (err) {
                        assert(err);
                        assert.strictEqual(err.$metadata?.httpStatusCode, statusCode);
                        // v3 error code may be in err.Code or err.name
                        assert(
                            err.Code === code || err.name === code || (err.message && err.message.includes(code)),
                            `Expected error code ${code}`
                        );
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
            const createdBuckets = Array.from(Array(bucketsNumber).keys())
                .map(i => `getservicebuckets-${i}`);

            before(async () => {
                bucketUtil = new BucketUtility('default', sigCfg);
                s3 = bucketUtil.s3;
                // No config.update in v3; set timeouts in client config if needed
                for (let i = 0; i < createdBuckets.length; i += 1) {
                    const bucketName = createdBuckets[i];
                    await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
                    if (bucketName.endsWith('000')) {
                        process.stdout.write(`creating bucket: ${bucketName}\n`);
                    }
                }
            });

            after(async () => {
                for (let i = 0; i < createdBuckets.length; i += 1) {
                    const bucketName = createdBuckets[i];
                    await s3.send(new DeleteBucketCommand({ Bucket: bucketName }));
                    if (bucketName.endsWith('000')) {
                        process.stdout.write(`deleting bucket: ${bucketName}\n`);
                    }
                }
            });

            it('should list buckets concurrently', async () => {
                for (let n = 0; n < 20; n += 1) {
                    const result = await s3.send(new ListBucketsCommand({}));
                    assert.equal(result.Buckets.length,
                        createdBuckets.length,
                        'Created buckets are missing in response');
                }
            });

            it('should list buckets', async () => {
                const data = await s3.send(new ListBucketsCommand({}));
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

            const filterFn = bucket => createdBuckets.indexOf(bucket.name) > -1;

            describe('two accounts are given', () => {
                let anotherS3;

                before(() => {
                    anotherS3 = new S3Client(getConfig('lisa'));
                    // No config.setPromisesDependency in v3
                });

                it('should not return other accounts bucket list', async () => {
                    const data = await anotherS3.send(new ListBucketsCommand({}));
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
