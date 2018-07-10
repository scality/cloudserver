const { S3 } = require('aws-sdk');

const getConfig = require('../support/config');
const { methodRequest } = require('../../lib/utility/cors-util');

const config = getConfig('default', { signatureVersion: 'v4' });
const s3 = new S3(config);

const bucket = 'bucketcorstester';

const methods = ['PUT', 'POST', 'DELETE', 'GET'];
const originsWithWildcards = [
    '*.allowedorigin.com',
    'http://*.allowedorigin.com',
    'http://www.allowedorigin.*',
];
const allowedOrigin = 'http://www.allowedwebsite.com';
const vary = 'Origin, Access-Control-Request-Headers, ' +
    'Access-Control-Request-Method';

// AWS seems to take a bit long so sometimes by the time we send the request
// the bucket has not yet been created or the bucket has been deleted.
function _waitForAWS(callback, err) {
    if (process.env.AWS_ON_AIR) {
        setTimeout(() => callback(err), 5000);
    } else {
        callback(err);
    }
}

describe('Preflight CORS request on non-existing bucket', () => {
    it('should respond no such bucket if bucket does not exist', done => {
        const headers = {
            Origin: allowedOrigin,
        };
        methodRequest({ method: 'GET', bucket, headers, code: 'NoSuchBucket',
            headersResponse: null }, done);
    });
    it('should return BadRequest for OPTIONS request without origin', done => {
        const headers = {};
        methodRequest({ method: 'OPTIONS', bucket, headers, code: 'BadRequest',
            headersResponse: null }, done);
    });
    it('should return BadRequest for OPTIONS request without ' +
    'Access-Control-Request-Method', done => {
        const headers = {
            Origin: allowedOrigin,
        };
        methodRequest({ method: 'OPTIONS', bucket, headers, code: 'BadRequest',
            headersResponse: null }, done);
    });
});

describe('Preflight CORS request with existing bucket', () => {
    beforeEach(done => {
        s3.createBucket({ Bucket: bucket, ACL: 'public-read' }, err => {
            _waitForAWS(done, err);
        });
    });
    afterEach(done => {
        s3.deleteBucket({ Bucket: bucket }, err => {
            _waitForAWS(done, err);
        });
    });

    it('should allow GET on bucket without cors configuration even if ' +
    'Origin header sent', done => {
        const headers = {
            Origin: allowedOrigin,
        };
        methodRequest({ method: 'GET', bucket, headers, code: 200,
            headersResponse: null }, done);
    });
    it('should allow HEAD on bucket without cors configuration even if ' +
    'Origin header sent', done => {
        const headers = {
            Origin: allowedOrigin,
        };
        methodRequest({ method: 'HEAD', bucket, headers, code: 200,
            headersResponse: null }, done);
    });
    it('should respond AccessForbidden for OPTIONS request on bucket without ' +
    'CORSConfiguration', done => {
        const headers = {
            'Origin': allowedOrigin,
            'Access-Control-Request-Method': 'GET',
        };
        methodRequest({ method: 'OPTIONS', bucket, headers,
            code: 'AccessForbidden', headersResponse: null }, done);
    });

    describe('allow PUT, POST, DELETE, GET methods and allow only ' +
    'one origin', () => {
        const corsParams = {
            Bucket: bucket,
            CORSConfiguration: {
                CORSRules: [
                    {
                        AllowedMethods: [
                            'PUT', 'POST', 'DELETE', 'GET',
                        ],
                        AllowedOrigins: [
                            allowedOrigin,
                        ],
                    },
                ],
            },
        };
        beforeEach(done => {
            s3.putBucketCors(corsParams, done);
        });

        afterEach(done => {
            s3.deleteBucketCors({ Bucket: bucket }, done);
        });

        methods.forEach(method => {
            it('should respond with 200 and access control headers to ' +
            'OPTIONS request from allowed origin and allowed method ' +
            `"${method}"`, done => {
                const headers = {
                    'Origin': allowedOrigin,
                    'Access-Control-Request-Method': method,
                };
                const headersResponse = {
                    'access-control-allow-origin': allowedOrigin,
                    'access-control-allow-methods': 'PUT, POST, DELETE, GET',
                    'access-control-allow-credentials': 'true',
                    vary,
                };
                methodRequest({ method: 'OPTIONS', bucket, headers, code: 200,
                    headersResponse }, done);
            });
        });
        it('should respond AccessForbidden to OPTIONS request from ' +
        'not allowed origin', done => {
            const headers = {
                'Origin': allowedOrigin,
                'Access-Control-Request-Method': 'GET',
                'Access-Control-Request-Headers': 'Origin, Accept, ' +
                'Content-Type',
            };
            methodRequest({ method: 'OPTIONS', bucket, headers,
                code: 'AccessForbidden', headersResponse: null }, done);
        });
        it('should respond AccessForbidden to OPTIONS request with ' +
        'not allowed Access-Control-Request-Headers', done => {
            const headers = {
                'Origin': 'http://www.forbiddenwebsite.com',
                'Access-Control-Request-Method': 'GET',
            };
            methodRequest({ method: 'OPTIONS', bucket, headers,
                code: 'AccessForbidden', headersResponse: null }, done);
        });
    });

    describe('CORS allows method GET and allows one origin', () => {
        const corsParams = {
            Bucket: bucket,
            CORSConfiguration: {
                CORSRules: [
                    {
                        AllowedMethods: [
                            'GET',
                        ],
                        AllowedOrigins: [
                            allowedOrigin,
                        ],
                    },
                ],
            },
        };
        beforeEach(done => {
            s3.putBucketCors(corsParams, done);
        });

        afterEach(done => {
            s3.deleteBucketCors({ Bucket: bucket }, done);
        });

        it('should respond with 200 and access control headers to OPTIONS ' +
        'request from allowed origin and method "GET"', done => {
            const headers = {
                'Origin': allowedOrigin,
                'Access-Control-Request-Method': 'GET',
            };
            const headersResponse = {
                'access-control-allow-origin': allowedOrigin,
                'access-control-allow-methods': 'GET',
                'access-control-allow-credentials': 'true',
                vary,
            };
            methodRequest({ method: 'OPTIONS', bucket, headers, code: 200,
                headersResponse }, done);
        });
        it('should respond AccessForbidden to OPTIONS request with allowed ' +
        'method but not from allowed origin', done => {
            const headers = {
                'Origin': 'http://www.forbiddenwebsite.com',
                'Access-Control-Request-Method': 'GET',
            };
            methodRequest({ method: 'OPTIONS', bucket, headers,
                code: 'AccessForbidden', headersResponse: null }, done);
        });
        it('should respond AccessForbidden to OPTIONS request from allowed ' +
        'origin and method but with not allowed Access-Control-Request-Headers',
        done => {
            const headers = {
                'Origin': allowedOrigin,
                'Access-Control-Request-Method': 'GET',
                'Access-Control-Request-Headers': 'Origin, Accept, ' +
                'Content-Type',
            };
            methodRequest({ method: 'OPTIONS', bucket, headers,
                code: 'AccessForbidden', headersResponse: null }, done);
        });
        ['PUT', 'POST', 'DELETE'].forEach(method => {
            it('should respond AccessForbidden to OPTIONS request from ' +
            `allowed origin but not allowed method "${method}"`, done => {
                const headers = {
                    'Origin': allowedOrigin,
                    'Access-Control-Request-Method': method,
                };
                methodRequest({ method: 'OPTIONS', bucket, headers,
                    code: 'AccessForbidden', headersResponse: null }, done);
            });
        });
    });

    methods.forEach(allowedMethod => {
        const corsParams = {
            Bucket: bucket,
            CORSConfiguration: {
                CORSRules: [
                    {
                        AllowedMethods: [allowedMethod],
                        AllowedOrigins: ['*'],
                    },
                ],
            },
        };
        describe(`CORS allows method "${allowedMethod}" and allows all origins`,
        () => {
            beforeEach(done => {
                s3.putBucketCors(corsParams, done);
            });

            afterEach(done => {
                s3.deleteBucketCors({ Bucket: bucket }, done);
            });

            it('should respond with 200 and access control headers to ' +
            `OPTIONS request from allowed origin and method "${allowedMethod}"`,
            done => {
                const headers = {
                    'Origin': allowedOrigin,
                    'Access-Control-Request-Method': allowedMethod,
                };
                const headersResponse = {
                    'access-control-allow-origin': '*',
                    'access-control-allow-methods': allowedMethod,
                    vary,
                };
                methodRequest({ method: 'OPTIONS', bucket, headers, code: 200,
                    headersResponse }, done);
            });
            it('should respond AccessForbidden to OPTIONS request from ' +
            'allowed origin and method but with not allowed Access-Control-' +
            'Request-Headers', done => {
                const headers = {
                    'Origin': allowedOrigin,
                    'Access-Control-Request-Method': allowedMethod,
                    'Access-Control-Request-Headers': 'Origin, Accept, ' +
                    'Content-Type',
                };
                methodRequest({ method: 'OPTIONS', bucket, headers,
                    code: 'AccessForbidden', headersResponse: null }, done);
            });
            methods.filter(method => method !== allowedMethod)
            .forEach(method => {
                it('should respond AccessForbidden to OPTIONS request from ' +
                `allowed origin but not allowed method "${method}"`, done => {
                    const headers = {
                        'Origin': allowedOrigin,
                        'Access-Control-Request-Method': method,
                    };
                    methodRequest({ method: 'OPTIONS', bucket, headers,
                        code: 'AccessForbidden', headersResponse: null }, done);
                });
            });
        });
    });

    originsWithWildcards.forEach(origin => {
        const corsParams = {
            Bucket: bucket,
            CORSConfiguration: {
                CORSRules: [
                    {
                        AllowedMethods: ['GET'],
                        AllowedOrigins: [origin],
                    },
                ],
            },
        };
        const originWithoutWildcard = origin.replace('*', '');
        const originReplaceWildcard = origin.replace('*', 'test');

        describe(`CORS allows method GET and origin "${origin}"`, () => {
            beforeEach(done => {
                s3.putBucketCors(corsParams, done);
            });

            afterEach(done => {
                s3.deleteBucketCors({ Bucket: bucket }, done);
            });

            [originWithoutWildcard, originReplaceWildcard]
            .forEach(acceptableOrigin => {
                it('should return 200 and CORS header to OPTIONS request ' +
                `from allowed method and origin "${acceptableOrigin}"`,
                done => {
                    const headers = {
                        'Origin': acceptableOrigin,
                        'Access-Control-Request-Method': 'GET',
                    };
                    const headersResponse = {
                        'access-control-allow-origin': acceptableOrigin,
                        'access-control-allow-methods': 'GET',
                        'access-control-allow-credentials': 'true',
                        vary,
                    };
                    methodRequest({ method: 'OPTIONS', bucket, headers,
                        code: 200, headersResponse }, done);
                });
            });
            if (!origin.endsWith('*')) {
                it('should respond AccessForbidden to OPTIONS request from ' +
                `allowed method and origin "${originWithoutWildcard}test"`,
                done => {
                    const headers = {
                        'Origin': `${originWithoutWildcard}test`,
                        'Access-Control-Request-Method': 'GET',
                    };
                    methodRequest({ method: 'OPTIONS', bucket, headers,
                        code: 'AccessForbidden', headersResponse: null }, done);
                });
            }
            if (!origin.startsWith('*')) {
                it('should respond AccessForbidden to OPTIONS request from ' +
                `allowed method and origin "test${originWithoutWildcard}"`,
                done => {
                    const headers = {
                        'Origin': `test${originWithoutWildcard}`,
                        'Access-Control-Request-Method': 'GET',
                    };
                    methodRequest({ method: 'OPTIONS', bucket, headers,
                        code: 'AccessForbidden', headersResponse: null }, done);
                });
            }
        });
    });

    describe('CORS response access-control-allow-origin header value',
    () => {
        const anotherOrigin = 'http://www.anotherorigin.com';
        const originContainingWildcard = 'http://www.originwith*.com';
        const corsParams = {
            Bucket: bucket,
            CORSConfiguration: {
                CORSRules: [
                    {
                        AllowedMethods: [
                            'GET',
                        ],
                        AllowedOrigins: [
                            allowedOrigin,
                            originContainingWildcard,
                        ],
                    },
                    {
                        AllowedMethods: [
                            'GET',
                        ],
                        AllowedOrigins: [
                            '*',
                        ],
                    },
                ],
            },
        };
        beforeEach(done => {
            s3.putBucketCors(corsParams, done);
        });

        afterEach(done => {
            s3.deleteBucketCors({ Bucket: bucket }, done);
        });

        it('if OPTIONS request matches rule with multiple origins, response ' +
        'access-control-request-origin header value should be request Origin ' +
        '(not list of AllowedOrigins)', done => {
            const headers = {
                'Origin': allowedOrigin,
                'Access-Control-Request-Method': 'GET',
            };
            const headersResponse = {
                'access-control-allow-origin': allowedOrigin,
                'access-control-allow-methods': 'GET',
                'access-control-allow-credentials': 'true',
                vary,
            };
            methodRequest({ method: 'OPTIONS', bucket, headers, code: 200,
                headersResponse }, done);
        });
        it('if OPTIONS request matches rule with origin containing wildcard, ' +
        'response access-control-request-origin header value should be ' +
        'request Origin (not value containing wildcard)', done => {
            const requestOrigin = originContainingWildcard.replace('*', 'test');
            const headers = {
                'Origin': requestOrigin,
                'Access-Control-Request-Method': 'GET',
            };
            const headersResponse = {
                'access-control-allow-origin': requestOrigin,
                'access-control-allow-methods': 'GET',
                'access-control-allow-credentials': 'true',
                vary,
            };
            methodRequest({ method: 'OPTIONS', bucket, headers, code: 200,
                headersResponse }, done);
        });
        it('if OPTIONS request matches rule that allows all origins, ' +
        'e.g. "*", response access-control-request-origin header should ' +
        'return "*"', done => {
            const headers = {
                'Origin': anotherOrigin,
                'Access-Control-Request-Method': 'GET',
            };
            const headersResponse = {
                'access-control-allow-origin': '*',
                'access-control-allow-methods': 'GET',
                vary,
            };
            methodRequest({ method: 'OPTIONS', bucket, headers, code: 200,
                headersResponse }, done);
        });
    });

    describe('CORS allows method GET, allows all origins and allows ' +
    'header Content-Type', () => {
        const corsParams = {
            Bucket: bucket,
            CORSConfiguration: {
                CORSRules: [
                    {
                        AllowedMethods: [
                            'GET',
                        ],
                        AllowedOrigins: [
                            '*',
                        ],
                        AllowedHeaders: [
                            'content-type',
                        ],
                    },
                ],
            },
        };
        beforeEach(done => {
            s3.putBucketCors(corsParams, done);
        });

        afterEach(done => {
            s3.deleteBucketCors({ Bucket: bucket }, done);
        });

        it('should respond with 200 and access control headers to OPTIONS ' +
        'request from allowed origin and method, even without request ' +
        'Access-Control-Request-Headers header value', done => {
            const headers = {
                'Origin': allowedOrigin,
                'Access-Control-Request-Method': 'GET',
            };
            const headersResponse = {
                'access-control-allow-origin': '*',
                'access-control-allow-methods': 'GET',
                vary,
            };
            methodRequest({ method: 'OPTIONS', bucket, headers, code: 200,
                headersResponse }, done);
        });
        it('should respond with 200 and access control headers to OPTIONS ' +
        'request from allowed origin and method with Access-Control-' +
        'Request-Headers \'Content-Type\'', done => {
            const headers = {
                'Origin': allowedOrigin,
                'Access-Control-Request-Method': 'GET',
                'Access-Control-Request-Headers': 'content-type',
            };
            const headersResponse = {
                'access-control-allow-origin': '*',
                'access-control-allow-methods': 'GET',
                'access-control-allow-headers': 'content-type',
                vary,
            };
            methodRequest({ method: 'OPTIONS', bucket, headers, code: 200,
                headersResponse }, done);
        });
        it('should respond AccessForbidden to OPTIONS request from allowed ' +
        'origin and method but not allowed Access-Control-Request-Headers ' +
        'in addition to Content-Type',
        done => {
            const headers = {
                'Origin': allowedOrigin,
                'Access-Control-Request-Method': 'GET',
                'Access-Control-Request-Headers': 'Origin, Accept, ' +
                'content-type',
            };
            methodRequest({ method: 'OPTIONS', bucket, headers,
                code: 'AccessForbidden', headersResponse: null }, done);
        });
    });

    describe('CORS response Access-Control-Allow-Headers header value',
    () => {
        const corsParams = {
            Bucket: bucket,
            CORSConfiguration: {
                CORSRules: [
                    {
                        AllowedMethods: [
                            'GET',
                        ],
                        AllowedOrigins: [
                            '*',
                        ],
                        AllowedHeaders: [
                            'Content-Type', 'amz-*', 'Expires',
                        ],
                    },
                    {
                        AllowedMethods: [
                            'GET',
                        ],
                        AllowedOrigins: [
                            '*',
                        ],
                        AllowedHeaders: [
                            '*',
                        ],
                    },
                ],
            },
        };
        beforeEach(done => {
            s3.putBucketCors(corsParams, done);
        });

        afterEach(done => {
            s3.deleteBucketCors({ Bucket: bucket }, done);
        });

        it('should return request access-control-request-headers value, ' +
        'not list of AllowedHeaders from rule or corresponding AllowedHeader ' +
        'value containing wildcard',
        done => {
            const requestHeaderValue = 'amz-meta-header-test, content-type';
            const headers = {
                'Origin': allowedOrigin,
                'Access-Control-Request-Method': 'GET',
                'Access-Control-Request-Headers': requestHeaderValue,
            };
            const headersResponse = {
                'access-control-allow-origin': '*',
                'access-control-allow-methods': 'GET',
                'access-control-allow-headers': requestHeaderValue,
                vary,
            };
            methodRequest({ method: 'OPTIONS', bucket, headers, code: 200,
                headersResponse }, done);
        });
        it('should return lowercase version of request Access-Control-' +
        'Request-Method header value if it contains any upper-case values',
        done => {
            const requestHeaderValue = 'Content-Type';
            const headers = {
                'Origin': allowedOrigin,
                'Access-Control-Request-Method': 'GET',
                'Access-Control-Request-Headers': requestHeaderValue,
            };
            const headersResponse = {
                'access-control-allow-origin': '*',
                'access-control-allow-methods': 'GET',
                'access-control-allow-headers':
                requestHeaderValue.toLowerCase(),
                vary,
            };
            methodRequest({ method: 'OPTIONS', bucket, headers, code: 200,
                headersResponse }, done);
        });
        it('should remove empty comma-separated values derived from request ' +
        'Access-Control-Request-Method header and separate values with ' +
        'spaces when responding with Access-Control-Allow-Headers header',
        done => {
            const requestHeaderValue = 'content-type,,expires';
            const expectedValue = 'content-type, expires';
            const headers = {
                'Origin': allowedOrigin,
                'Access-Control-Request-Method': 'GET',
                'Access-Control-Request-Headers': requestHeaderValue,
            };
            const headersResponse = {
                'access-control-allow-origin': '*',
                'access-control-allow-methods': 'GET',
                'access-control-allow-headers': expectedValue,
                vary,
            };
            methodRequest({ method: 'OPTIONS', bucket, headers, code: 200,
                headersResponse }, done);
        });
        it('should return request Access-Control-Request-Headers value ' +
        'even if rule allows all headers (e.g. "*"), unlike access-control-' +
        'allow-origin value', done => {
            const requestHeaderValue = 'puppies';
            const headers = {
                'Origin': allowedOrigin,
                'Access-Control-Request-Method': 'GET',
                'Access-Control-Request-Headers': requestHeaderValue,
            };
            const headersResponse = {
                'access-control-allow-origin': '*',
                'access-control-allow-methods': 'GET',
                'access-control-allow-headers': requestHeaderValue,
                vary,
            };
            methodRequest({ method: 'OPTIONS', bucket, headers, code: 200,
                headersResponse }, done);
        });
    });

    describe('CORS and OPTIONS request with object keys', () => {
        const corsParams = {
            Bucket: bucket,
            CORSConfiguration: {
                CORSRules: [
                    {
                        AllowedMethods: [
                            'GET',
                        ],
                        AllowedOrigins: [
                            allowedOrigin,
                        ],
                    },
                ],
            },
        };
        const objectKey = 'testobject';
        beforeEach(done => {
            s3.putObject({ Key: objectKey, Bucket: bucket }, err => {
                if (err) {
                    process.stdout.write(`err in beforeEach ${err}`);
                    done(err);
                }
                s3.putBucketCors(corsParams, done);
            });
        });

        afterEach(done => {
            s3.deleteBucketCors({ Bucket: bucket }, err => {
                if (err) {
                    process.stdout.write(`err in afterEach ${err}`);
                    done(err);
                }
                s3.deleteObject({ Key: objectKey, Bucket: bucket }, done);
            });
        });

        it('should respond with 200 and access control headers to OPTIONS ' +
        'request from allowed origin, allowed method and existing object key',
        done => {
            const headers = {
                'Origin': allowedOrigin,
                'Access-Control-Request-Method': 'GET',
            };
            const headersResponse = {
                'access-control-allow-origin': allowedOrigin,
                'access-control-allow-methods': 'GET',
                'access-control-allow-credentials': 'true',
                vary,
            };
            methodRequest({ method: 'OPTIONS', objectKey, bucket, headers,
                code: 200, headersResponse }, done);
        });
        it('should respond with 200 and access control headers to OPTIONS ' +
        'request from allowed origin, allowed method, even with non-existing ' +
        'object key', done => {
            const headers = {
                'Origin': allowedOrigin,
                'Access-Control-Request-Method': 'GET',
            };
            const headersResponse = {
                'access-control-allow-origin': allowedOrigin,
                'access-control-allow-methods': 'GET',
                'access-control-allow-credentials': 'true',
                vary,
            };
            methodRequest({ method: 'OPTIONS', bucket, objectKey:
            'anotherObjectKey', headers, code: 200, headersResponse }, done);
        });
    });

    describe('CORS and OPTIONS request', () => {
        const corsParams = {
            Bucket: bucket,
            CORSConfiguration: {
                CORSRules: [
                    {
                        AllowedMethods: ['GET'],
                        AllowedOrigins: ['*'],
                    },
                ],
            },
        };
        beforeEach(done => {
            s3.putBucketCors(corsParams, done);
        });

        afterEach(done => {
            s3.deleteBucketCors({ Bucket: bucket }, done);
        });

        it('with fake auth credentials: should respond with 200 and access ' +
        'control headers even if request has fake auth credentials', done => {
            const headers = {
                'Origin': allowedOrigin,
                'Access-Control-Request-Method': 'GET',
                'Authorization': 'AWS fakeKey:fakesignature',
            };
            const headersResponse = {
                'access-control-allow-origin': '*',
                'access-control-allow-methods': 'GET',
                vary,
            };
            methodRequest({ method: 'OPTIONS', bucket, headers, code: 200,
                headersResponse }, done);
        });

        it('with cookies: should send identical response as to request ' +
        'without cookies (200 and access control headers)', done => {
            const headers = {
                'Origin': allowedOrigin,
                'Access-Control-Request-Method': 'GET',
                'Cookie': 'testcookie=1',
            };
            const headersResponse = {
                'access-control-allow-origin': '*',
                'access-control-allow-methods': 'GET',
                vary,
            };
            methodRequest({ method: 'OPTIONS', bucket, headers, code: 200,
                headersResponse }, done);
        });
    });

    describe('CORS exposes headers', () => {
        const corsParams = {
            Bucket: bucket,
            CORSConfiguration: {
                CORSRules: [
                    {
                        AllowedMethods: [
                            'GET',
                        ],
                        AllowedOrigins: [
                            '*',
                        ],
                        ExposeHeaders: [
                            'x-amz-server-side-encryption',
                            'x-amz-request-id',
                            'x-amz-id-2',
                        ],
                    },
                ],
            },
        };
        beforeEach(done => {
            s3.putBucketCors(corsParams, done);
        });

        afterEach(done => {
            s3.deleteBucketCors({ Bucket: bucket }, done);
        });

        it('if OPTIONS request matches CORS rule with ExposeHeader\'s, ' +
        'response should include Access-Control-Expose-Headers header',
        done => {
            const headers = {
                'Origin': allowedOrigin,
                'Access-Control-Request-Method': 'GET',
            };
            const headersResponse = {
                'access-control-allow-origin': '*',
                'access-control-allow-methods': 'GET',
                'access-control-expose-headers':
                'x-amz-server-side-encryption, x-amz-request-id, x-amz-id-2',
                vary,
            };
            methodRequest({ method: 'OPTIONS', bucket, headers, code: 200,
                headersResponse }, done);
        });
    });

    describe('CORS max age seconds', () => {
        const corsParams = {
            Bucket: bucket,
            CORSConfiguration: {
                CORSRules: [
                    {
                        AllowedMethods: [
                            'GET',
                        ],
                        AllowedOrigins: [
                            '*',
                        ],
                        MaxAgeSeconds: 86400,
                    },
                ],
            },
        };
        beforeEach(done => {
            s3.putBucketCors(corsParams, done);
        });

        afterEach(done => {
            s3.deleteBucketCors({ Bucket: bucket }, done);
        });

        it('if OPTIONS request matches CORS rule with max age seconds, ' +
        'response should include Access-Control-Max-Age header', done => {
            const headers = {
                'Origin': allowedOrigin,
                'Access-Control-Request-Method': 'GET',
            };
            const headersResponse = {
                'access-control-allow-origin': '*',
                'access-control-allow-methods': 'GET',
                'access-control-max-age': '86400',
                vary,
            };
            methodRequest({ method: 'OPTIONS', bucket, headers, code: 200,
                headersResponse }, done);
        });
    });
});
