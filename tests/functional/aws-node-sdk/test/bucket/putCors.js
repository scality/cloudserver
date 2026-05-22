const assert = require('assert');
const { S3Client, CreateBucketCommand, DeleteBucketCommand, PutBucketCorsCommand } = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const getConfig = require('../support/config');

const bucketName = 'testcorsbucket';

const sampleCors = {
    CORSRules: [
        {
            AllowedMethods: ['PUT', 'POST', 'DELETE'],
            AllowedOrigins: ['http://www.example.com'],
            AllowedHeaders: ['*'],
            MaxAgeSeconds: 3000,
            ExposeHeaders: ['x-amz-server-side-encryption'],
        },
        { AllowedMethods: ['GET'], AllowedOrigins: ['*'], AllowedHeaders: ['*'], MaxAgeSeconds: 3000 },
    ],
};

function _corsTemplate(params) {
    const sampleRule = {
        AllowedMethods: ['PUT', 'POST', 'DELETE'],
        AllowedOrigins: ['http://www.example.com'],
        AllowedHeaders: ['*'],
        MaxAgeSeconds: 3000,
        ExposeHeaders: ['x-amz-server-side-encryption'],
    };
    ['AllowedMethods', 'AllowedOrigins', 'AllowedHeaders', 'MaxAgeSeconds', 'ExposeHeaders'].forEach(prop => {
        if (params[prop]) {
            sampleRule[prop] = params[prop];
        }
    });
    return { CORSRules: [sampleRule] };
}

describe('PUT bucket cors', () => {
    withV4(sigCfg => {
        const config = getConfig('default', sigCfg);
        const s3 = new S3Client(config);

        async function _testPutBucketCors(rules, statusCode, errMsg) {
            try {
                await s3.send(
                    new PutBucketCorsCommand({
                        Bucket: bucketName,
                        CORSConfiguration: rules,
                    }),
                );
                throw new Error('Expected error but found none');
            } catch (err) {
                assert.strictEqual(err.name, errMsg);
                assert.strictEqual(err.$metadata.httpStatusCode, statusCode);
            }
        }

        beforeEach(() => s3.send(new CreateBucketCommand({ Bucket: bucketName })));

        afterEach(() => s3.send(new DeleteBucketCommand({ Bucket: bucketName })));

        it('should put a bucket cors successfully', async () => {
            await s3.send(
                new PutBucketCorsCommand({
                    Bucket: bucketName,
                    CORSConfiguration: sampleCors,
                }),
            );
        });

        it('should return InvalidRequest if more than 100 rules', async () => {
            const sampleRule = {
                AllowedMethods: ['PUT', 'POST', 'DELETE'],
                AllowedOrigins: ['http://www.example.com'],
                AllowedHeaders: ['*'],
                MaxAgeSeconds: 3000,
                ExposeHeaders: ['x-amz-server-side-encryption'],
            };
            const testCors = { CORSRules: [] };
            for (let i = 0; i < 101; i++) {
                testCors.CORSRules.push(sampleRule);
            }
            await _testPutBucketCors(testCors, 400, 'InvalidRequest');
        });

        it('should return MalformedXML if missing AllowedOrigin', async () => {
            const testCors = _corsTemplate({ AllowedOrigins: [] });
            await _testPutBucketCors(testCors, 400, 'MalformedXML');
        });

        it('should return InvalidRequest if more than one asterisk in ' + 'AllowedOrigin', async () => {
            const testCors = _corsTemplate({ AllowedOrigins: ['http://*.*.com'] });
            await _testPutBucketCors(testCors, 400, 'InvalidRequest');
        });

        it('should return MalformedXML if missing AllowedMethod', async () => {
            const testCors = _corsTemplate({ AllowedMethods: [] });
            await _testPutBucketCors(testCors, 400, 'MalformedXML');
        });

        it('should return InvalidRequest if AllowedMethod is not a valid ' + 'method', async () => {
            const testCors = _corsTemplate({ AllowedMethods: ['test'] });
            await _testPutBucketCors(testCors, 400, 'InvalidRequest');
        });

        it('should return InvalidRequest for lowercase value for ' + 'AllowedMethod', async () => {
            const testCors = _corsTemplate({ AllowedMethods: ['put', 'get'] });
            await _testPutBucketCors(testCors, 400, 'InvalidRequest');
        });

        it('should return InvalidRequest if more than one asterisk in ' + 'AllowedHeader', async () => {
            const testCors = _corsTemplate({ AllowedHeaders: ['*-amz-*'] });
            await _testPutBucketCors(testCors, 400, 'InvalidRequest');
        });

        it(
            'should return InvalidRequest if ExposeHeader has character ' + 'that is not dash or alphanumeric',
            async () => {
                const testCors = _corsTemplate({ ExposeHeaders: ['test header'] });
                await _testPutBucketCors(testCors, 400, 'InvalidRequest');
            },
        );

        it('should return InvalidRequest if ExposeHeader has wildcard', async () => {
            const testCors = _corsTemplate({ ExposeHeaders: ['x-amz-*'] });
            await _testPutBucketCors(testCors, 400, 'InvalidRequest');
        });
    });
});
