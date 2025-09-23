const assert = require('assert');
const { S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    GetBucketCorsCommand,
    PutBucketCorsCommand } = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const getConfig = require('../support/config');

const bucketName = 'testgetcorsbucket';

// Helper function to delete bucket
async function deleteBucket(s3, bucket) {
    try {
        await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
    } catch (err) {
        // eslint-disable-next-line no-console
        console.log(err);
    }
}

describe('GET bucket cors', () => {
    withV4(sigCfg => {
        const config = getConfig('default', sigCfg);
        const s3 = new S3Client(config);

        afterEach(() => deleteBucket(s3, bucketName));

        describe('on bucket with existing cors configuration', () => {
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
            
            before(async () => {
                await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
                await s3.send(new PutBucketCorsCommand({
                    Bucket: bucketName,
                    CORSConfiguration: sampleCors,
                }));
            });

            it('should return cors configuration successfully', async () => {
                const data = await s3.send(new GetBucketCorsCommand({ Bucket: bucketName }));
                assert.deepStrictEqual(data.CORSRules, sampleCors.CORSRules);
            });
        });

        describe('mixed case for AllowedHeader', () => {
            const testValue = 'tEsTvAlUe';
            const sampleCors = { CORSRules: [
                { AllowedMethods: ['PUT', 'POST', 'DELETE'],
                    AllowedOrigins: ['http://www.example.com'],
                    AllowedHeaders: [testValue] },
            ] };
            
            before(async () => {
                await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
                await s3.send(new PutBucketCorsCommand({
                    Bucket: bucketName,
                    CORSConfiguration: sampleCors,
                }));
            });

            it('should be preserved when putting / getting cors resource', async () => {
                const data = await s3.send(new GetBucketCorsCommand({ Bucket: bucketName }));
                assert.deepStrictEqual(data.CORSRules[0].AllowedHeaders,
                    sampleCors.CORSRules[0].AllowedHeaders);
            });
        });

        describe('uppercase for AllowedMethod', () => {
            const sampleCors = { CORSRules: [
                { AllowedMethods: ['PUT', 'POST', 'DELETE'],
                    AllowedOrigins: ['http://www.example.com'] },
            ] };
            
            before(async () => {
                await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
                await s3.send(new PutBucketCorsCommand({
                    Bucket: bucketName,
                    CORSConfiguration: sampleCors,
                }));
            });

            it('should be preserved when retrieving cors resource', async () => {
                const data = await s3.send(new GetBucketCorsCommand({ Bucket: bucketName }));
                assert.deepStrictEqual(data.CORSRules[0].AllowedMethods,
                    sampleCors.CORSRules[0].AllowedMethods);
            });
        });

        describe('on bucket without cors configuration', () => {
            before(async () => {
                process.stdout.write('about to create bucket\n');
                try {
                    await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
                } catch (err) {
                    process.stdout.write('error creating bucket', err);
                    throw err;
                }
            });

            it('should return NoSuchCORSConfiguration', async () => {
                try {
                    await s3.send(new GetBucketCorsCommand({ Bucket: bucketName }));
                    throw new Error('Expected NoSuchCORSConfiguration error');
                } catch (err) {
                    assert(err);
                    assert.strictEqual(err.Code, 'NoSuchCORSConfiguration');
                    assert.strictEqual(err.$metadata.httpStatusCode, 404);
                }
            });
        });
    });
});
