import assert from 'assert';
import tv4 from 'tv4';
import Promise from 'bluebird';
import { S3 } from 'aws-sdk';

import BucketUtility from '../../lib/utility/bucket-util';
import getConfig from '../support/config';
import withV4 from '../support/withV4';
import svcSchema from '../../schema/service';

describe('GET Service - AWS.S3.listBuckets', () => {
    describe('When user is unauthorized', () => {
        let s3;
        let config;

        beforeEach(() => {
            config = getConfig('default');
            s3 = new S3(config);
        });

        const itSkipAWS = process.env.AWS_ON_AIR
            ? it.skip
            : it;

        itSkipAWS('should return 403 and AccessDenied', done => {
            s3.makeUnauthenticatedRequest('listBuckets', error => {
                assert(error);

                assert.strictEqual(error.statusCode, 403);
                assert.strictEqual(error.code, 'AccessDenied');

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
            let createdBuckets;

            const bucketsNumber = 5;

            before(() => {
                bucketUtil = new BucketUtility('default', sigCfg);
                s3 = bucketUtil.s3;
            });

            before(done => {
                bucketUtil
                    .createRandom(bucketsNumber)
                    .then(data => {
                        createdBuckets = data;
                        done();
                    })
                    .catch(done);
            });

            after(done => {
                bucketUtil
                    .deleteMany(createdBuckets)
                    .then(() => done())
                    .catch(done);
            });

            it('should return no error, owner info, ' +
                'and created buckets list in alphabetical order', done => {
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

            const describeFn = process.env.AWS_ON_AIR
                ? describe.skip
                : describe;
            const filterFn = bucket => createdBuckets.indexOf(bucket.name) > -1;

            describeFn('two accounts are given', () => {
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
