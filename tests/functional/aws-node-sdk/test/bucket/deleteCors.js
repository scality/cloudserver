const assert = require('assert');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucketName = 'testdeletecorsbucket';
const sampleCors = { CORSRules: [
    { AllowedMethods: ['PUT', 'POST', 'DELETE'],
        AllowedOrigins: ['http://www.example.com'],
        AllowedHeaders: ['*'],
        MaxAgeSeconds: 3000,
        ExposeHeaders: ['x-amz-server-side-encryption'] },
    { AllowedMethods: ['GET'],
        AllowedOrigins: ['*'],
        AllowedHeaders: ['*'],
        MaxAgeSeconds: 3000 },
] };

const itSkipIfAWS = process.env.AWS_ON_AIR ? it.skip : it;

describe('DELETE bucket cors', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const otherAccountBucketUtility = new BucketUtility('lisa', {});
        const otherAccountS3 = otherAccountBucketUtility.s3;

        describe('without existing bucket', () => {
            test('should return NoSuchBucket', done => {
                s3.deleteBucketCors({ Bucket: bucketName }, err => {
                    expect(err).toBeTruthy();
                    expect(err.code).toBe('NoSuchBucket');
                    expect(err.statusCode).toBe(404);
                    return done();
                });
            });
        });

        describe('with existing bucket', () => {
            beforeEach(() => s3.createBucketAsync({ Bucket: bucketName }));
            afterEach(() => bucketUtil.deleteOne(bucketName));

            describe('without existing cors configuration', () => {
                test('should return a 204 response', done => {
                    s3.deleteBucketCors({ Bucket: bucketName },
                    function deleteBucketCors(err) {
                        const statusCode = this.httpResponse.statusCode;
                        expect(statusCode).toBe(204);
                        expect(err).toBe(null);
                        return done();
                    });
                });
            });

            describe('with existing cors configuration', () => {
                beforeEach(done => {
                    s3.putBucketCors({ Bucket: bucketName,
                        CORSConfiguration: sampleCors }, done);
                });

                test('should delete bucket configuration successfully', done => {
                    s3.deleteBucketCors({ Bucket: bucketName },
                    function deleteBucketCors(err) {
                        const statusCode = this.httpResponse.statusCode;
                        expect(statusCode).toBe(204);
                        expect(err).toBe(null);
                        s3.getBucketCors({ Bucket: bucketName }, err => {
                            expect(err.code).toBe('NoSuchCORSConfiguration');
                            expect(err.statusCode).toBe(404);
                            return done();
                        });
                    });
                });

                // Skip if AWS because AWS Node SDK raises CredentialsError
                // before letting the request hit the API
                // If you want to run this test against AWS_ON_AIR, make sure
                // to add a second set of real aws credentials under a profile
                // named 'lisa' in ~/.aws/scality, then rename 'itSkipIfAWS' to
                // 'it'.
                itSkipIfAWS('should return AccessDenied if user is not bucket' +
                'owner', done => {
                    otherAccountS3.deleteBucketCors({ Bucket: bucketName },
                    err => {
                        expect(err).toBeTruthy();
                        expect(err.code).toBe('AccessDenied');
                        expect(err.statusCode).toBe(403);
                        return done();
                    });
                });
            });
        });
    });
});
