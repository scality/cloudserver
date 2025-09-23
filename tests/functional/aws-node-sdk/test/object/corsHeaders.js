const { S3Client,
    ListObjectsCommand, 
    GetBucketAclCommand, 
    GetBucketCorsCommand, 
    GetBucketVersioningCommand, 
    GetBucketLocationCommand, 
    GetBucketWebsiteCommand, 
    ListMultipartUploadsCommand, 
    GetObjectCommand, 
    GetObjectAclCommand, 
    ListPartsCommand, 
    HeadBucketCommand, 
    HeadObjectCommand, 
    CreateBucketCommand, 
    PutBucketAclCommand, 
    PutBucketVersioningCommand, 
    PutBucketWebsiteCommand, 
    PutBucketCorsCommand, 
    PutObjectCommand, 
    PutObjectAclCommand, 
    CopyObjectCommand, 
    UploadPartCommand, 
    UploadPartCopyCommand, 
    CreateMultipartUploadCommand, 
    CompleteMultipartUploadCommand, 
    DeleteObjectsCommand, 
    DeleteBucketCommand, 
    DeleteBucketWebsiteCommand, 
    DeleteBucketCorsCommand, 
    DeleteObjectCommand, 
    AbortMultipartUploadCommand, 
    ListBucketsCommand } = require('@aws-sdk/client-s3');
const { promisify } = require('util');
const assert = require('assert');

const getConfig = require('../support/config');
const { methodRequest } = require('../../lib/utility/cors-util');
const { generateCorsParams } = require('../../lib/utility/cors-util');
const { WebsiteConfigTester } = require('../../lib/utility/website-util');
const { removeAllVersions } = require('../../lib/utility/versioning-util');

const methodRequestPromise = promisify(methodRequest);

const config = getConfig('default', { signatureVersion: 'v4' });
const s3 = new S3Client(config);

const bucket = 'bucketcorsheadertest';
const objectKey = 'objectKeyName';
const allowedOrigin = 'http://www.allowedwebsite.com';
const notAllowedOrigin = 'http://www.notallowedwebsite.com';
const vary = 'Origin, Access-Control-Request-Headers, ' +
    'Access-Control-Request-Method';
const defaultOptions = {
    allowedMethods: ['GET'],
    allowedOrigins: [allowedOrigin],
};

const apiMethods = [
    {
        description: 'GET bucket (list objects)',
        action: ListObjectsCommand,
        params: { Bucket: bucket },
    },
    {
        description: 'GET bucket ACL',
        action: GetBucketAclCommand,
        params: { Bucket: bucket },
    },
    {
        description: 'GET bucket CORS',
        action: GetBucketCorsCommand,
        params: { Bucket: bucket },
    },
    {
        description: 'GET bucket versioning',
        action: GetBucketVersioningCommand,
        params: { Bucket: bucket },
    },
    {
        description: 'GET bucket location',
        action: GetBucketLocationCommand,
        params: { Bucket: bucket },
    },
    {
        description: 'GET bucket website',
        action: GetBucketWebsiteCommand,
        params: { Bucket: bucket },
    },
    {
        description: 'GET bucket uploads (list multipart uploads)',
        action: ListMultipartUploadsCommand,
        params: { Bucket: bucket },
    },
    {
        description: 'GET object',
        action: GetObjectCommand,
        params: { Bucket: bucket, Key: objectKey },
    },
    {
        description: 'GET object ACL',
        action: GetObjectAclCommand,
        params: { Bucket: bucket, Key: objectKey },
    },
    {
        description: 'GET object uploadId (list multipart upload parts)',
        action: ListPartsCommand,
        params: { Bucket: bucket, Key: objectKey, UploadId: 'testId' },
    },
    {
        description: 'HEAD bucket',
        action: HeadBucketCommand,
        params: { Bucket: bucket },
    },
    {
        description: 'HEAD object',
        action: HeadObjectCommand,
        params: { Bucket: bucket, Key: objectKey },
    },
    {
        description: 'PUT bucket (create bucket)',
        action: CreateBucketCommand,
        params: { Bucket: bucket },
    },
    {
        description: 'PUT bucket ACL',
        action: PutBucketAclCommand,
        params: { Bucket: bucket, ACL: 'private' },
    },
    {
        description: 'PUT bucket versioning',
        action: PutBucketVersioningCommand,
        params: {
            Bucket: bucket,
            VersioningConfiguration: {
                Status: 'Enabled',
            },
        },
    },
    {
        description: 'PUT bucket website',
        action: PutBucketWebsiteCommand,
        params: {
            Bucket: bucket,
            WebsiteConfiguration: {
                IndexDocument: { Suffix: 'index.html' },
            },
        },
    },
    {
        description: 'PUT bucket CORS',
        action: PutBucketCorsCommand,
        params: {
            Bucket: bucket,
            CORSConfiguration: {
                CORSRules: [{
                    AllowedOrigins: [allowedOrigin],
                    AllowedMethods: ['PUT'],
                }],
            },
        },
    },
    {
        description: 'PUT object',
        action: PutObjectCommand,
        params: { Bucket: bucket, Key: objectKey },
    },
    {
        description: 'PUT object ACL',
        action: PutObjectAclCommand,
        params: {
            Bucket: bucket,
            Key: objectKey,
            ACL: 'private',
        },
    },
    {
        description: 'PUT object copy (copy object)',
        action: CopyObjectCommand,
        params: {
            Bucket: bucket,
            CopySource: `${bucket}/${objectKey}`,
            Key: objectKey,
        },
    },
    {
        description: 'PUT object part (upload part)',
        action: UploadPartCommand,
        params: {
            Bucket: bucket,
            Key: objectKey,
            PartNumber: 1,
            UploadId: 'testId',
        },
    },
    {
        description: 'PUT object part copy (upload part copy)',
        action: UploadPartCopyCommand,
        params: {
            Bucket: bucket,
            CopySource: `${bucket}/${objectKey}`,
            Key: objectKey,
            PartNumber: 1,
            UploadId: 'testId',
        },
    },
    {
        description: 'POST uploads (create multipart upload)',
        action: CreateMultipartUploadCommand,
        params: { Bucket: bucket, Key: objectKey },
    },
    {
        description: 'POST uploadId (complete multipart upload)',
        action: CompleteMultipartUploadCommand,
        params: { Bucket: bucket, Key: objectKey, UploadId: 'testId' },
    },
    {
        description: 'POST delete (multi object delete)',
        action: DeleteObjectsCommand,
        params: {
            Bucket: bucket,
            Delete: {
                Objects: [
                    { Key: objectKey },
                ],
            },
        },
    },
    {
        description: 'DELETE bucket',
        action: DeleteBucketCommand,
        params: { Bucket: bucket },
    },
    {
        description: 'DELETE bucket website',
        action: DeleteBucketWebsiteCommand,
        params: { Bucket: bucket },
    },
    {
        description: 'DELETE bucket CORS',
        action: DeleteBucketCorsCommand,
        params: { Bucket: bucket },
    },
    {
        description: 'DELETE object',
        action: DeleteObjectCommand,
        params: { Bucket: bucket, Key: objectKey },
    },
    {
        description: 'DELETE object uploadId (abort multipart upload)',
        action: AbortMultipartUploadCommand,
        params: { Bucket: bucket, Key: objectKey, UploadId: 'testId' },
    },
];

async function _checkHeaders(action, params, origin, expectedHeaders) {
    function _runAssertions(resHeaders) {
        if (expectedHeaders) {
            Object.keys(expectedHeaders).forEach(key => {
                assert.deepEqual(resHeaders[key], expectedHeaders[key], `error header: ${key}`);
            });
        } else {
            // if no expectedHeaders provided, should not have these headers in the response
            ['access-control-allow-origin', 
            'access-control-allow-methods', 
            'access-control-allow-credentials', 
            'vary'].forEach(key => {
                assert.strictEqual(resHeaders[key], undefined, `Error: ${key} should not have value`);
            });
        }
    }

    // Create a new S3 client for each request to avoid middleware conflicts
    const testS3 = new S3Client(config);
    let capturedHeaders = {};

    // Add middleware to capture response headers (similar to AWS SDK v2's event approach)
    testS3.middlewareStack.add(
        next => async args => {
            if (origin) {
                if (!args.request.headers) {
                    // eslint-disable-next-line no-param-reassign
                    args.request.headers = {};
                }
                // eslint-disable-next-line no-param-reassign
                args.request.headers['origin'] = origin;
            }

            try {
                const result = await next(args);

                // Capture response headers (equivalent to request.on('success'))
                if (result.response && result.response.headers) {
                    capturedHeaders = result.response.headers;
                } else if (result.output && result.output.$metadata && result.output.$metadata.httpHeaders) {
                    capturedHeaders = result.output.$metadata.httpHeaders;
                }

                return result;
            } catch (error) {
                // Capture headers from error response (equivalent to request.on('error'))
                if (error.$response && error.$response.headers) {
                    capturedHeaders = error.$response.headers;
                } else if (error.$metadata && error.$metadata.httpHeaders) {
                    capturedHeaders = error.$metadata.httpHeaders;
                }
                throw error;
            }
        },
        {
            step: 'finalizeRequest',
            name: 'captureHeaders',
            priority: 'high'
        }
    );

    try {
        // eslint-disable-next-line new-cap
        const command = new action(params);
        const response = await testS3.send(command);

        // Clean up multipart upload if needed (equivalent to the original cleanup logic)
        if (response.UploadId) {
            await testS3.send(new AbortMultipartUploadCommand({
                Bucket: bucket,
                Key: objectKey,
                UploadId: response.UploadId
            }));
        }

        _runAssertions(capturedHeaders);

    } catch {
        // CORS headers should still be sent in case of errors as long as
        // request matches CORS configuration
        _runAssertions(capturedHeaders);
    }
}

describe('Cross Origin Resource Sharing requests', () => {
    beforeEach(async () => {
        try {
            await s3.send(new CreateBucketCommand({ Bucket: bucket, ACL: 'public-read-write' }));
        } catch (err) {
            process.stdout.write(`Error in beforeEach ${err}`);
            throw err;
        }
    });

    afterEach(async () => {
        try {
            await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
        } catch (err) {
            if (err.name !== 'NoSuchBucket') {
                process.stdout.write(`Error in afterEach ${err}`);
                throw err;
            }
        }
    });

    describe('on non-existing bucket', () => {
        it('should not respond to request with CORS headers, even if request was sent with Origin header',
            async () => {
            await _checkHeaders(ListObjectsCommand, { Bucket: 'nonexistingbucket' }, allowedOrigin, null);
        });
    });

    describe('on bucket without CORS configuration', () => {
        it('should not respond to request with CORS headers,' +
            ' even if request was sent with Origin header', async () => {
            await _checkHeaders(ListObjectsCommand, { Bucket: bucket }, allowedOrigin, null);
        });
    });

    describe('on bucket with CORS configuration: ' +
            'allow one origin and all methods', () => {
        const corsParams = generateCorsParams(bucket, {
            allowedMethods: ['GET', 'PUT', 'HEAD', 'POST', 'DELETE'],
            allowedOrigins: [allowedOrigin],
        });
        const expectedHeaders = {
            'access-control-allow-origin': allowedOrigin,
            'access-control-allow-methods': corsParams.CORSConfiguration
                .CORSRules[0].AllowedMethods.join(', '),
            'access-control-allow-credentials': 'true',
            vary,
        };

        beforeEach(async () => {
            await s3.send(new PutBucketCorsCommand(corsParams));
        });

        afterEach(async () => {
            try {
                await removeAllVersions({ Bucket: bucket });
            } catch (err) {
                if (err.name !== 'NoSuchKey' && err.name !== 'NoSuchBucket') {
                    process.stdout.write(`Unexpected err in afterEach: ${err}`);
                    throw err;
                }
            }
        });

        describe('when request Origin/method match CORS configuration', () => {
            it('should not respond with CORS headers to GET service (list buckets), ' +
                'even if Origin/method match CORS rule', async () => {
                await _checkHeaders(ListBucketsCommand, {}, allowedOrigin, null);
            });

            it('should not respond with CORS headers after deleting bucket, ' +
                'even if Origin/method match CORS rule', async () => {
                await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
                await _checkHeaders(ListObjectsCommand, { Bucket: bucket }, allowedOrigin, null);
            });

            apiMethods.forEach(method => {
                it(`should respond to ${method.description} with CORS headers (access-control-allow-origin, 
                    access-control-allow-methods, access-control-allow-credentials and vary)`, async () => {
                    await _checkHeaders(method.action, method.params, allowedOrigin, expectedHeaders);
                });
            });
        });

        describe('when request Origin does not match CORS rule', () => {
            apiMethods.forEach(method => {
                it(`should not respond to ${method.description} with CORS headers`, async () => {
                    await _checkHeaders(method.action, method.params, notAllowedOrigin, null);
                });
            });
        });
    });

    describe('on bucket with CORS configuration: allow PUT method and one origin', () => {
        const corsParams = generateCorsParams(bucket, {
            allowedMethods: ['PUT'],
            allowedOrigins: [allowedOrigin],
        });

        beforeEach(async () => {
            await s3.send(new PutBucketCorsCommand(corsParams));
        });

        afterEach(async () => {
            await s3.send(new DeleteBucketCorsCommand({ Bucket: bucket }));
        });

        it('when request method does not match CORS rule should not respond with CORS headers', async () => {
            await _checkHeaders(ListObjectsCommand, { Bucket: bucket }, allowedOrigin, null);
        });
    });

    describe('on bucket with CORS configuration and website configuration',
        () => {
        const bucket = process.env.AWS_ON_AIR ? 'awsbucketwebsitetester' : 
        'bucketwebsitetester';
        const corsParams = generateCorsParams(bucket, {
            allowedMethods: ['GET', 'HEAD'],
            allowedOrigins: [allowedOrigin],
        });
        const headersResponse = {
            'access-control-allow-origin': allowedOrigin,
            'access-control-allow-methods': 'GET, HEAD',
            'access-control-allow-credentials': 'true',
            vary,
        };
        const webConfig = new WebsiteConfigTester('index.html');
        const condition = { KeyPrefixEquals: 'redirect' };
        const redirect = { HostName: 'www.google.com' };
        webConfig.addRoutingRule(redirect, condition);

        beforeEach(async () => {
            await s3.send(new CreateBucketCommand({ Bucket: bucket, ACL: 'public-read' }));
            await s3.send(new PutBucketCorsCommand(corsParams));
            await s3.send(new PutBucketWebsiteCommand({ Bucket: bucket, WebsiteConfiguration: webConfig }));
            await s3.send(new PutObjectCommand({ Bucket: bucket, Key: 'index.html', 
                ACL: 'public-read', 
                Body: 'test content' }));
            if (process.env.AWS_ON_AIR) {
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
        });

        afterEach(async () => {
            try {
                await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: 'index.html' }));
                await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
            } catch (err) {
                process.stdout.write(`Error in afterEach ${err}`);
                throw err;
            }
        });

        it('should respond with CORS headers at website endpoint (GET)', async () => {
            const headers = { Origin: allowedOrigin };
            await methodRequestPromise({ method: 'GET', bucket, 
                headers, headersResponse, code: 200, isWebsite: true });
        });

        it('should respond with CORS headers at website endpoint (GET) even in case of error', async () => {
            const headers = { Origin: allowedOrigin };
            await methodRequestPromise({ method: 'GET', bucket, objectKey: 'test', 
                headers, headersResponse, code: 404, isWebsite: true });
        });

        it('should respond with CORS headers at website endpoint (GET) even in case of redirect', async () => {
            const headers = { Origin: allowedOrigin };
            await methodRequestPromise({ method: 'GET', bucket, objectKey: 'redirect', 
                headers, headersResponse, code: 301, isWebsite: true });
        });

        it('should respond with CORS headers at website endpoint (HEAD)', async () => {
            const headers = { Origin: allowedOrigin };
            await methodRequestPromise({ method: 'HEAD', bucket, headers, headersResponse, 
                code: 200, isWebsite: true });
        });
    });

    describe('on bucket with additional cors configuration', () => {
        afterEach(async () => {
            await s3.send(new DeleteBucketCorsCommand({ Bucket: bucket }));
        });

        describe('cors configuration : AllowedHeaders', () => {
            const corsParams = generateCorsParams(bucket, defaultOptions);
            corsParams.CORSConfiguration.CORSRules[0].AllowedHeaders = ['Content-Type'];

            const headersResponse = {
                'access-control-allow-origin': allowedOrigin,
                'access-control-allow-methods': 'GET',
                'access-control-allow-credentials': 'true',
                vary,
            };

            beforeEach(async () => {
                await s3.send(new PutBucketCorsCommand(corsParams));
            });

            it('should not return access-control-allow-headers response header ' +
                'even if request matches CORS rule and other access-control headers are returned', async () => {
                const headers = {
                    'Origin': allowedOrigin,
                    'Content-Type': 'testvalue',
                };
                const headersOmitted = ['access-control-allow-headers'];
                await methodRequestPromise({ method: 'GET', bucket, headers, headersResponse, 
                    headersOmitted, code: 200 });
            });

            it('Request with matching Origin/method but additional headers that violate CORS rule:\n\t should still ' +
                'respond with access-control headers (headers are only checked in preflight requests)', async () => {
                const headers = {
                    Origin: allowedOrigin,
                    Test: 'test',
                    Expires: 86400,
                };
                await methodRequestPromise({ method: 'GET', bucket, headers, 
                    headersResponse, code: 200 });
            });
        });

        [
            {
                name: 'MaxAgeSeconds',
                header: 'access-control-max-age',
                testValue: '86400',
            },
            {
                name: 'ExposeHeaders',
                header: 'access-control-expose-headers',
                testValue: ['Content-Type'],
            },
        ].forEach(elem => {
            describe(`cors configuration : ${elem.name}`, () => {
                const corsParams = generateCorsParams(bucket, defaultOptions);
                corsParams.CORSConfiguration.CORSRules[0][elem.name] = elem.testValue;

                beforeEach(async () => {
                    await s3.send(new PutBucketCorsCommand(corsParams));
                });

                it(`should respond with ${elem.header} header if request matches CORS rule`, async () => {
                    const headers = { Origin: allowedOrigin };
                    const headersResponse = {
                        'access-control-allow-origin': allowedOrigin,
                        'access-control-allow-methods': 'GET',
                        'access-control-allow-credentials': 'true',
                        vary,
                    };
                    headersResponse[elem.header] = Array.isArray(elem.testValue) ? elem.testValue[0] : elem.testValue;
                    await methodRequestPromise({ method: 'GET', bucket, headers, headersResponse, code: 200 });
                });
            });
        });
    });
});
