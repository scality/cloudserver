import assert from 'assert';
import tv4 from 'tv4';
import { S3 } from 'aws-sdk';
import { promisify } from 'bluebird';
import getConfig from '../support/config';
import serviceSchema from '../../schema/service';

describe('GET Service - AWS.S3.listBuckets', () => {
    // List all available buckets
    it('callback function\'s context should be AWS.Response type', done => {
        (new S3()).listBuckets(function callback() {
            assert.ok(this instanceof require('aws-sdk').Response);
            done();
        });
    });

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

        it('should return CredentialsError ' +
            'if user is unidentified', done => {
            const missingCredential = getConfig('default', {
                credentials: null,
            });
            const expectedCode = 'CredentialsError';

            testFn(missingCredential, expectedCode, undefined, done);
        });

        const itFn = process.env.AWS_ON_AIR ? it : it.skip;
        itFn('should return 403 and InvalidAccessKeyId ' +
            'if accessKeyId is invalid', done => {
            const invalidAccess = getConfig('default', {
                credentials: null,
                accessKeyId: 'wrong',
                secretAccessKey: 'wrong again',
            });
            const expectedCode = 'InvalidAccessKeyId';
            const expectedStatus = 403;

            testFn(invalidAccess, expectedCode, expectedStatus, done);
        });

        it('should return 403 and SignatureDoesNotMatch ' +
            'if credential is polluted', done => {
            const pollutedConfig = getConfig('default');
            pollutedConfig.credentials.secretAccessKey = 'wrong';

            const expectedCode = 'SignatureDoesNotMatch';
            const expectedStatus = 403;

            testFn(pollutedConfig, expectedCode, expectedStatus, done);
        });
    });

    describe('when user has credential', () => {
        let s3;
        let anotherS3;
        let listBuckets;
        let createBucket;
        let deleteBucket;

        before(() => {
            s3 = new S3(getConfig());
            anotherS3 = new S3(getConfig('lisa'));
            listBuckets = promisify(s3.listBuckets);
            createBucket = promisify(s3.createBucket);
            deleteBucket = promisify(s3.deleteBucket);
        });

        it('should return no error and owner and available buckets', done => {
            listBuckets.call(s3)
                .then(data => {
                    const isValid = tv4.validate(data, serviceSchema);

                    assert.ok(isValid);
                    if (!isValid) {
                        throw new Error(tv4.error);
                    }

                    done();
                })
                .catch(done);
        });

        const describeFn = process.env.AWS_ON_AIR ? describe.skip : describe;
        describeFn('two accounts are given', () => {
            const random = Math.round(Math.random() * 100).toString();
            const bucketName = `fttest-awsnodesdk-bucket-${random}`;

            before(done => {
                createBucket
                    .call(anotherS3, { Bucket: bucketName })
                    .then(() => done())
                    .catch(done);
            });

            after(done => {
                deleteBucket
                    .call(anotherS3, { Bucket: bucketName })
                    .then(() => done())
                    .catch(done);
            });

            it('should not return other account bucket list', done => {
                listBuckets.call(s3)
                    .then((data) => {
                        const buckets = data.Buckets;

                        const hasSameBuckets = buckets
                            .filter(b => b.Name === bucketName)
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
