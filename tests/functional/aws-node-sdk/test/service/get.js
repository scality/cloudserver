import assert from 'assert';
import tv4 from 'tv4';
import { S3 } from 'aws-sdk';
import Promise from 'bluebird';
import getConfig from '../support/config';
import withV4 from '../support/withV4';
import svcSchema from '../../schema/service';

describe('GET Service - AWS.S3.listBuckets', () => {
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
            let s3;
            let anotherS3;

            const random = Math.round(Math.random() * 100).toString();
            const baseName = `ft-awsnodesdk-bucket-${random}`;
            const bucketsNumber = 5;

            let createdBuckets;

            before(() => {
                s3 = new S3(getConfig('default', sigCfg));
                anotherS3 = new S3(getConfig('lisa'));

                s3 = Promise.promisifyAll(s3);
                anotherS3 = Promise.promisifyAll(anotherS3);
            });

            before(done => {
                let promises;

                promises = Array.from(Array(bucketsNumber).keys()).reverse()
                    .map(i => {
                        const bucketName = `${baseName}-${i}`;
                        return s3
                            .createBucketAsync({ Bucket: bucketName })
                            .then(() => bucketName);
                    });

                Promise.all(promises)
                    .catch(done)
                    .then(data => {
                        createdBuckets = data;
                        done();
                    });
            });

            after(done => {
                let promises;

                promises = createdBuckets.map(bucketName => {
                    return s3.deleteBucketAsync({ Bucket: bucketName });
                });

                Promise.all(promises)
                    .catch(done)
                    .then(() => done());
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
                        const buckets = data.Buckets.filter(bucket => {
                            return createdBuckets.indexOf(bucket.Name) > -1;
                        });

                        assert.equal(buckets.length, createdBuckets.length,
                            'Created buckets are missing in response');

                        return buckets;
                    })
                    .then(buckets => {
                        let isCorrectOrder;

                        // Sort createdBuckets in alphabetical order
                        createdBuckets.sort();

                        isCorrectOrder = buckets
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
