const assert = require('assert');
const { S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    DeleteBucketCorsCommand,
    PutBucketCorsCommand,
    GetBucketCorsCommand } = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const getConfig = require('../support/config');

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

function deleteBucket(s3, bucket) {
    return s3.send(new DeleteBucketCommand({ Bucket: bucket }));
}

describe('DELETE bucket cors', () => {
    withV4(sigCfg => {
        const config = getConfig('default', sigCfg);
        const s3 = new S3Client(config);
        const otherAccountConfig = getConfig('lisa', {});
        const otherAccountS3 = new S3Client(otherAccountConfig);

        describe('without existing bucket', () => {
            it('should return NoSuchBucket', async () => {
                try {
                    await s3.send(new DeleteBucketCorsCommand({ Bucket: bucketName }));
                    throw new Error('Expected NoSuchBucket error');
                } catch (err) {
                    assert.strictEqual(err.name, 'NoSuchBucket');
                    assert.strictEqual(err.$metadata.httpStatusCode, 404);
                }
            });
        });

        describe('with existing bucket', () => {
            beforeEach(() => s3.send(new CreateBucketCommand({ Bucket: bucketName })));
            
            afterEach(() => deleteBucket(s3, bucketName));

            describe('without existing cors configuration', () => {
                it('should return a 204 response', async () => {
                    const res = await s3.send(new DeleteBucketCorsCommand({ Bucket: bucketName }));
                    const statusCode = res?.$metadata?.httpStatusCode;
                    assert.strictEqual(statusCode, 204,
                        `Found unexpected statusCode ${statusCode}`);
                });
            });

            describe('with existing cors configuration', () => {
                beforeEach(() => s3.send(new PutBucketCorsCommand({ 
                    Bucket: bucketName,
                    CORSConfiguration: sampleCors 
                })));


                it('should delete bucket configuration successfully', async () => {
                    const res = await s3.send(new DeleteBucketCorsCommand({ Bucket: bucketName }));
                    const statusCode = res?.$metadata?.httpStatusCode;
                    assert.strictEqual(statusCode, 204,
                        `Found unexpected statusCode ${statusCode}`);
                    try {
                        await s3.send(new GetBucketCorsCommand({ Bucket: bucketName }));
                        throw new Error('Expected NoSuchCORSConfiguration error');
                    } catch (err) {
                        assert.strictEqual(err.name, 'NoSuchCORSConfiguration');
                        assert.strictEqual(err.$metadata.httpStatusCode, 404);
                    }
                });

                // Skip if AWS because AWS Node SDK raises CredentialsError
                // before letting the request hit the API
                // If you want to run this test against AWS_ON_AIR, make sure
                // to add a second set of real aws credentials under a profile
                // named 'lisa' in ~/.aws/scality, then rename 'itSkipIfAWS' to
                // 'it'.
                itSkipIfAWS('should return AccessDenied if user is not bucket' +
                'owner', async () => {
                    try {
                        await otherAccountS3.send(new DeleteBucketCorsCommand({ Bucket: bucketName }));
                        throw new Error('Expected AccessDenied error');
                    } catch (err) {
                        assert.strictEqual(err.name, 'AccessDenied');
                        assert.strictEqual(err.$metadata.httpStatusCode, 403);
                    }
                });
            });
        });
    });
});
