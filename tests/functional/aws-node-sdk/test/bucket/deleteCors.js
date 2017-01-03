import assert from 'assert';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

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
            it('should return NoSuchBucket', done => {
                s3.deleteBucketCors({ Bucket: bucketName }, err => {
                    assert(err);
                    assert.strictEqual(err.code, 'NoSuchBucket');
                    assert.strictEqual(err.statusCode, 404);
                    return done();
                });
            });
        });

        describe('with existing bucket', () => {
            beforeEach(() => s3.createBucketAsync({ Bucket: bucketName }));
            afterEach(() => bucketUtil.deleteOne(bucketName));

            describe('without existing cors configuration', () => {
                it('should return a 200 response', done => {
                    s3.deleteBucketCors({ Bucket: bucketName }, err => {
                        assert.strictEqual(err, null,
                            `Found unexpected err ${err}`);
                        return done();
                    });
                });
            });

            describe('with existing cors configuration', () => {
                beforeEach(done => {
                    s3.putBucketCors({ Bucket: bucketName,
                    CORSConfiguration: sampleCors }, done);
                });

                it('should delete bucket configuration successfully', done => {
                    s3.deleteBucketCors({ Bucket: bucketName }, err => {
                        assert.strictEqual(err, null,
                            `Found unexpected err ${err}`);
                        s3.getBucketCors({ Bucket: bucketName }, err => {
                            assert.strictEqual(err.code,
                                'NoSuchCORSConfiguration');
                            assert.strictEqual(err.statusCode, 404);
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
                        assert(err);
                        assert.strictEqual(err.code, 'AccessDenied');
                        assert.strictEqual(err.statusCode, 403);
                        return done();
                    });
                });
            });
        });
    });
});
