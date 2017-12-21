const { S3 } = require('aws-sdk');
const assert = require('assert');
const async = require('async');

const getConfig = require('../support/config');
const { methodRequest } = require('../../lib/utility/cors-util');
const { generateCorsParams } = require('../../lib/utility/cors-util');
const { WebsiteConfigTester } = require('../../lib/utility/website-util');
const { removeAllVersions } = require('../../lib/utility/versioning-util');

const config = getConfig('default', { signatureVersion: 'v4' });
const s3 = new S3(config);

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
        action: s3.listObjects,
        params: { Bucket: bucket },
    },
    {
        description: 'GET bucket ACL',
        action: s3.getBucketAcl,
        params: { Bucket: bucket },
    },
    {
        description: 'GET bucket CORS',
        action: s3.getBucketCors,
        params: { Bucket: bucket },
    },
    {
        description: 'GET bucket versioning',
        action: s3.getBucketVersioning,
        params: { Bucket: bucket },
    },
    {
        description: 'GET bucket location',
        action: s3.getBucketLocation,
        params: { Bucket: bucket },
    },
    {
        description: 'GET bucket website',
        action: s3.getBucketWebsite,
        params: { Bucket: bucket },
    },
    {
        description: 'GET bucket uploads (list multipart uploads)',
        action: s3.listMultipartUploads,
        params: { Bucket: bucket },
    },
    {
        description: 'GET object',
        action: s3.getObject,
        params: { Bucket: bucket, Key: objectKey },
    },
    {
        description: 'GET object ACL',
        action: s3.getObjectAcl,
        params: { Bucket: bucket, Key: objectKey },
    },
    {
        description: 'GET object uploadId (list multipart upload parts)',
        action: s3.listParts,
        params: { Bucket: bucket, Key: objectKey, UploadId: 'testId' },
    },
    {
        description: 'HEAD bucket',
        action: s3.headBucket,
        params: { Bucket: bucket },
    },
    {
        description: 'HEAD object',
        action: s3.headObject,
        params: { Bucket: bucket, Key: objectKey },
    },
    {
        description: 'PUT bucket (create bucket)',
        action: s3.createBucket,
        params: { Bucket: bucket },
    },
    {
        description: 'PUT bucket ACL',
        action: s3.putBucketAcl,
        params: { Bucket: bucket, ACL: 'private' },
    },
    {
        description: 'PUT bucket versioning',
        action: s3.putBucketVersioning,
        params: {
            Bucket: bucket,
            VersioningConfiguration: {
                Status: 'Enabled',
            },
        },
    },
    {
        description: 'PUT bucket website',
        action: s3.putBucketWebsite,
        params: {
            Bucket: bucket,
            WebsiteConfiguration: {
                IndexDocument: { Suffix: 'index.html' },
            },
        },
    },
    {
        description: 'PUT bucket CORS',
        action: s3.putBucketCors,
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
        action: s3.putObject,
        params: { Bucket: bucket, Key: objectKey },
    },
    {
        description: 'PUT object ACL',
        action: s3.putObjectAcl,
        params: {
            Bucket: bucket,
            Key: objectKey,
            ACL: 'private',
        },
    },
    {
        description: 'PUT object copy (copy object)',
        action: s3.copyObject,
        params: {
            Bucket: bucket,
            CopySource: `${bucket}/${objectKey}`, // 'sourceBucket/testSource',
            Key: objectKey,
        },
    },
    {
        description: 'PUT object part (upload part)',
        action: s3.uploadPart,
        params: {
            Bucket: bucket,
            Key: objectKey,
            PartNumber: 1,
            UploadId: 'testId',
        },
    },
    {
        description: 'PUT object part copy (upload part copy)',
        action: s3.uploadPartCopy,
        params: {
            Bucket: bucket,
            CopySource: `${bucket}/${objectKey}`, // 'sourceBucket/testSource',
            Key: objectKey,
            PartNumber: 1,
            UploadId: 'testId',
        },
    },
    {
        description: 'POST uploads (create multipart upload)',
        action: s3.createMultipartUpload,
        params: { Bucket: bucket, Key: objectKey },
    },
    {
        description: 'POST uploadId (complete multipart upload)',
        action: s3.completeMultipartUpload,
        params: { Bucket: bucket, Key: objectKey, UploadId: 'testId' },
    },
    {
        description: 'POST delete (multi object delete)',
        action: s3.deleteObjects,
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
        action: s3.deleteBucket,
        params: { Bucket: bucket },
    },
    {
        description: 'DELETE bucket website',
        action: s3.deleteBucketWebsite,
        params: { Bucket: bucket },
    },
    {
        description: 'DELETE bucket CORS',
        action: s3.deleteBucketCors,
        params: { Bucket: bucket },
    },
    {
        description: 'DELETE object',
        action: s3.deleteObject,
        params: { Bucket: bucket, Key: objectKey },
    },
    {
        description: 'DELETE object uploadId (abort multipart upload)',
        action: s3.abortMultipartUpload,
        params: { Bucket: bucket, Key: objectKey, UploadId: 'testId' },
    },
];

// AWS seems to take a bit long so sometimes by the time we send the request
// the bucket has not yet been created or the bucket has been deleted.
function _waitForAWS(callback, err) {
    if (process.env.AWS_ON_AIR) {
        setTimeout(() => callback(err), 1000);
    } else {
        callback(err);
    }
}

function _checkHeaders(action, params, origin, expectedHeaders, callback) {
    function _runAssertions(resHeaders, cb) {
        if (expectedHeaders) {
            Object.keys(expectedHeaders).forEach(key => {
                assert.deepEqual(resHeaders[key], expectedHeaders[key],
                  `error header: ${key}`);
            });
        } else {
            // if no headersResponse provided, should not have these headers
            // in the request
            ['access-control-allow-origin',
                'access-control-allow-methods',
                'access-control-allow-credentials',
                'vary'].forEach(key => {
                    assert.strictEqual(resHeaders[key], undefined,
                    `Error: ${key} should not have value`);
                });
        }
        cb();
    }
    const method = action.bind(s3);
    const request = method(params);
    // modify underlying http request object created by aws sdk to add
    // origin header
    request.on('build', () => {
        request.httpRequest.headers.origin = origin;
    });
    request.on('success', response => {
        const resHeaders = response.httpResponse.headers;
        _runAssertions(resHeaders, () => {
            if (response.data.UploadId) {
                // abort multipart upload before deleting bucket in afterEach
                return s3.abortMultipartUpload({ Bucket: bucket, Key: objectKey,
                    UploadId: response.data.UploadId }, callback);
            }
            return callback();
        });
    });
    // CORS headers should still be sent in case of errors as long as
    // request matches CORS configuration
    request.on('error', () => {
        const resHeaders = request.response.httpResponse.headers;
        _runAssertions(resHeaders, callback);
    });
    request.send();
}

describe('Cross Origin Resource Sharing requests', () => {
    beforeEach(done => {
        s3.createBucket({ Bucket: bucket, ACL: 'public-read-write' }, err => {
            if (err) {
                process.stdout.write(`Error in beforeEach ${err}`);
            }
            return _waitForAWS(done, err);
        });
    });

    afterEach(done => {
        s3.deleteBucket({ Bucket: bucket }, err => {
            if (err && err.code !== 'NoSuchBucket') {
                process.stdout.write(`Error in afterEach ${err}`);
                return _waitForAWS(done, err);
            }
            return _waitForAWS(done);
        });
    });

    describe('on non-existing bucket', () => {
        it('should not respond to request with CORS headers, even ' +
            'if request was sent with Origin header', done => {
            _checkHeaders(s3.listObjects, { Bucket: 'nonexistingbucket' },
            allowedOrigin, null, done);
        });
    });

    describe('on bucket without CORS configuration', () => {
        it('should not respond to request with CORS headers, even ' +
            'if request was sent with Origin header', done => {
            _checkHeaders(s3.listObjects, { Bucket: bucket },
            allowedOrigin, null, done);
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

        beforeEach(done => s3.putBucketCors(corsParams, done));

        afterEach(done => {
            removeAllVersions({ Bucket: bucket }, err => {
                if (err && err.code !== 'NoSuchKey' &&
                err.code !== 'NoSuchBucket') {
                    process.stdout.write(`Unexpected err in afterEach: ${err}`);
                    return done(err);
                }
                return done();
            });
        });

        describe('when request Origin/method match CORS configuration', () => {
            it('should not respond with CORS headers to GET service (list ' +
            'buckets), even if Origin/method match CORS rule', done => {
                // no bucket specified in this request
                _checkHeaders(s3.listBuckets, {}, allowedOrigin,
                    null, done);
            });

            it('should not respond with CORS headers after deleting bucket, ' +
            'even if Origin/method match CORS rule', done => {
                s3.deleteBucket({ Bucket: bucket }, err => {
                    assert.strictEqual(err, null, `Unexpected err ${err}`);
                    _checkHeaders(s3.listObjects, { Bucket: bucket },
                    allowedOrigin, null, done);
                });
            });

            apiMethods.forEach(method => {
                it(`should respond to ${method.description} with CORS ` +
                'headers (access-control-allow-origin, access-control-allow-' +
                'methods, access-control-allow-credentials and vary)', done => {
                    _checkHeaders(method.action, method.params, allowedOrigin,
                    expectedHeaders, done);
                });
            });
        });

        describe('when request Origin does not match CORS rule', () => {
            apiMethods.forEach(method => {
                it(`should not respond to ${method.description} with ` +
                'CORS headers', done => {
                    _checkHeaders(method.action, method.params,
                    notAllowedOrigin, null, done);
                });
            });
        });
    });

    describe('on bucket with CORS configuration: ' +
            'allow PUT method and one origin', () => {
        const corsParams = generateCorsParams(bucket, {
            allowedMethods: ['PUT'],
            allowedOrigins: [allowedOrigin],
        });

        beforeEach(done => {
            s3.putBucketCors(corsParams, done);
        });

        afterEach(done => {
            s3.deleteBucketCors({ Bucket: bucket }, done);
        });

        it('when request method does not match CORS rule ' +
        'should not respond with CORS headers', done => {
            _checkHeaders(s3.listObjects, { Bucket: bucket },
            allowedOrigin, null, done);
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

        beforeEach(done =>
            async.series([
                next => s3.createBucket({
                    Bucket: bucket,
                    ACL: 'public-read',
                }, next),
                next => s3.putBucketCors(corsParams, next),
                next => s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, next),
                next => s3.putObject({
                    Bucket: bucket,
                    Key: 'index.html',
                    ACL: 'public-read',
                }, next),
            ], err => {
                assert.strictEqual(err, null,
                    `Unexpected err ${err} in beforeEach`);
                done(err);
            })
        );

        afterEach(done =>
            s3.deleteObject({ Bucket: bucket, Key: 'index.html' }, err => {
                assert.strictEqual(err, null,
                    `Unexpected err ${err} in afterEach`);
                s3.deleteBucket({ Bucket: bucket }, err => {
                    if (err) {
                        process.stdout.write(`Error in afterEach ${err}`);
                        return _waitForAWS(done, err);
                    }
                    return _waitForAWS(done);
                });
            })
        );

        it('should respond with CORS headers at website endpoint (GET)',
        done => {
            const headers = { Origin: allowedOrigin };
            methodRequest({ method: 'GET', bucket, headers, headersResponse,
                code: 200, isWebsite: true }, done);
        });

        it('should respond with CORS headers at website endpoint (GET) ' +
        'even in case of error',
        done => {
            const headers = { Origin: allowedOrigin };
            methodRequest({ method: 'GET', bucket, objectKey: 'test',
                headers, headersResponse, code: 404, isWebsite: true }, done);
        });

        it('should respond with CORS headers at website endpoint (GET) ' +
        'even in case of redirect',
        done => {
            const headers = { Origin: allowedOrigin };
            methodRequest({ method: 'GET', bucket, objectKey: 'redirect',
                headers, headersResponse, code: 301, isWebsite: true }, done);
        });

        it('should respond with CORS headers at website endpoint (HEAD)',
        done => {
            const headers = { Origin: allowedOrigin };
            methodRequest({ method: 'HEAD', bucket, headers, headersResponse,
                code: 200, isWebsite: true }, done);
        });
    });

    describe('on bucket with additional cors configuration',
    () => {
        afterEach(done => {
            s3.deleteBucketCors({ Bucket: bucket }, done);
        });

        describe('cors configuration : AllowedHeaders', () => {
            const corsParams = generateCorsParams(bucket, defaultOptions);
            corsParams.CORSConfiguration.CORSRules[0]
                .AllowedHeaders = ['Content-Type'];

            const headersResponse = {
                'access-control-allow-origin': allowedOrigin,
                'access-control-allow-methods': 'GET',
                'access-control-allow-credentials': 'true',
                vary,
            };

            beforeEach(done => {
                s3.putBucketCors(corsParams, done);
            });

            it('should not return access-control-allow-headers response ' +
            'header even if request matches CORS rule and other access-' +
            'control headers are returned', done => {
                const headers = {
                    'Origin': allowedOrigin,
                    'Content-Type': 'testvalue',
                };
                const headersOmitted = ['access-control-allow-headers'];
                methodRequest({ method: 'GET', bucket, headers, headersResponse,
                    headersOmitted, code: 200 }, done);
            });

            it('Request with matching Origin/method but additional headers ' +
            'that violate CORS rule:\n\t should still respond with access-' +
            'control headers (headers are only checked in preflight requests)',
            done => {
                const headers = {
                    Origin: allowedOrigin,
                    Test: 'test',
                    Expires: 86400,
                };
                methodRequest({ method: 'GET', bucket, headers, headersResponse,
                    code: 200 }, done);
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
                corsParams.CORSConfiguration.CORSRules[0][elem.name] =
                    elem.testValue;

                beforeEach(done => {
                    s3.putBucketCors(corsParams, done);
                });

                it(`should respond with ${elem.header} header ` +
                'if request matches CORS rule', done => {
                    const headers = { Origin: allowedOrigin };
                    const headersResponse = {
                        'access-control-allow-origin': allowedOrigin,
                        'access-control-allow-methods': 'GET',
                        'access-control-allow-credentials': 'true',
                        vary,
                    };
                    headersResponse[elem.header] =
                        Array.isArray(elem.testValue) ? elem.testValue[0] :
                        elem.testValue;
                    methodRequest({ method: 'GET', bucket, headers,
                        headersResponse, code: 200 }, done);
                });
            });
        });
    });
});
