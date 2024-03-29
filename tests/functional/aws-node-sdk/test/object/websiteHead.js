const assert = require('assert');
const async = require('async');
const fs = require('fs');
const path = require('path');

const { S3 } = require('aws-sdk');

const conf = require('../../../../../lib/Config').config;
const getConfig = require('../support/config');
const { WebsiteConfigTester } = require('../../lib/utility/website-util');

const config = getConfig('default', { signatureVersion: 'v4' });
const s3 = new S3(config);

// Note: To run these tests locally, you may need to edit the machine's
// /etc/hosts file to include the following line:
// `127.0.0.1 bucketwebsitetester.s3-website-us-east-1.amazonaws.com`

const transport = conf.https ? 'https' : 'http';
const bucket = process.env.AWS_ON_AIR ? `awsbucketwebsitetester-${Date.now()}` :
    'bucketwebsitetester';
const hostname = process.env.S3_END_TO_END ?
    `${bucket}.s3-website-us-east-1.scality.com` :
    `${bucket}.s3-website-us-east-1.amazonaws.com`;
const endpoint = process.env.AWS_ON_AIR ? `${transport}://${hostname}` :
    `${transport}://${hostname}:8000`;
const redirectEndpoint = conf.https ? 'https://www.google.com/' :
    'http://www.google.com/';

const indexDocETag = '"95a589c37a2df74b062fb4d5a6f64197"';
const indexExpectedHeaders = {
    'etag': indexDocETag,
    'x-amz-meta-test': 'value',
};

// Basic Expected Behavior:

// 1) If error, respond with error headers:

// HTTP/1.1 403 Forbidden
// Date: Wed, 21 Dec 2016 20:33:00 GMT
// Server: AmazonS3
// Transfer-Encoding: chunked
// x-amz-error-code: AccessDenied
// x-amz-error-message: Access Denied
// x-amz-id-2: HteVr4cPi9iddwUdAifVTxLZcidlh
// e1yxO6mtr1lqaGRSW/lZlQMGLbIztWOTUcDP3vMA6PwpNE=
// x-amz-request-id: FD4EA11DD16537F5

// 2) If redirect rule, redirect with 301 status code.

// 3) If success, respond with object's headers:

// HTTP/1.1 200 OK
// Content-Length: 314
// Content-Type: text/html
// Date: Wed, 21 Dec 2016 20:32:52 GMT
// ETag: "6af6552e24eb4bdcc83f1b227dfc3dda"
// Last-Modified: Fri, 02 Dec 2016 18:06:55 GMT
// Server: AmazonS3
// x-amz-id-2: o2wNnLjNiRhiL83ji34IeV5vLV/kXoIN+nzh41XE/cz7+
// 8C5P3O3SIRj7PYNmhhJqE2NyDA2h5g=
// x-amz-meta-somekey: mymetadata!!!
// x-amz-request-id: 89BBCC95D4738024

// 4) If there is a redirect rule for an error, and get such error,
// redirect in accordance with rule:

// Called head with prefix zzz and got 403 so satisfied conditions for redirect
// HTTP/1.1 301 Moved Permanently
// Content-Length: 0
// Date: Thu, 22 Dec 2016 02:47:48 GMT
// Location: http://whatever.com.s3-website-us-east-1.amazonaws.com/zzz
// Server: AmazonS3
// x-amz-id-2: 5HXWYfdLQS8ZRBZJcKV1cxqxgKMTJzg74rGl61kRLA
// KX/MgqE4dZCJ4d9eF59Wbg/kza40cWcoA=
// x-amz-request-id: 0073330F58C7137C


describe('Head request on bucket website endpoint', () => {
    it('should return 404 when no such bucket', done => {
        const expectedHeaders = {
            'x-amz-error-code': 'NoSuchBucket',
            // Need arsenal fixed to remove period at the end
            // so compatible with aws
            'x-amz-error-message': 'The specified bucket does not exist.',
        };
        WebsiteConfigTester.makeHeadRequest(undefined, endpoint, 404,
            expectedHeaders, done);
    });

    describe('with existing bucket', () => {
        beforeEach(done => s3.createBucket({ Bucket: bucket }, done));

        afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

        it('should return 404 when no website configuration', done => {
            const expectedHeaders = {
                'x-amz-error-code': 'NoSuchWebsiteConfiguration',
                'x-amz-error-message': 'The specified bucket does not ' +
                    'have a website configuration',
            };
            WebsiteConfigTester.makeHeadRequest(undefined, endpoint, 404,
                expectedHeaders, done);
        });

        describe('with existing configuration', () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html');
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, err => {
                    assert.strictEqual(err,
                        null, `Found unexpected err ${err}`);
                    s3.putObject({ Bucket: bucket, Key: 'index.html',
                        ACL: 'public-read',
                        Body: fs.readFileSync(path.join(__dirname,
                            '/websiteFiles/index.html')),
                        ContentType: 'text/html',
                        Metadata: {
                            test: 'value',
                        },
                    },
                        err => {
                            assert.strictEqual(err, null);
                            done();
                        });
                });
            });

            afterEach(done => {
                s3.deleteObject({ Bucket: bucket, Key: 'index.html' },
                err => done(err));
            });

            it('should return indexDocument headers if no key ' +
                'requested', done => {
                WebsiteConfigTester.makeHeadRequest(undefined, endpoint,
                    200, indexExpectedHeaders, done);
            });

            it('should return indexDocument headers if key requested', done => {
                WebsiteConfigTester.makeHeadRequest(undefined,
                    `${endpoint}/index.html`, 200, indexExpectedHeaders, done);
            });
        });

        describe('with path prefix in request with/without key', () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html');
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, err => {
                    assert.strictEqual(err,
                        null, `Found unexpected err ${err}`);
                    s3.putObject({ Bucket: bucket,
                        Key: 'pathprefix/index.html',
                        ACL: 'public-read',
                        Body: fs.readFileSync(path.join(__dirname,
                            '/websiteFiles/index.html')),
                        ContentType: 'text/html',
                        Metadata: {
                            test: 'value',
                        },
                    }, done);
                });
            });

            afterEach(done => {
                s3.deleteObject({ Bucket: bucket, Key:
                    'pathprefix/index.html' },
                done);
            });

            it('should serve indexDocument if path request without key',
            done => {
                WebsiteConfigTester.makeHeadRequest(undefined,
                    `${endpoint}/pathprefix/`, 200, indexExpectedHeaders, done);
            });

            it('should serve indexDocument if path request with key',
            done => {
                WebsiteConfigTester.makeHeadRequest(undefined,
                    `${endpoint}/pathprefix/index.html`, 200,
                    indexExpectedHeaders, done);
            });
        });

        describe('with private key', () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html');
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, err => {
                    assert.strictEqual(err,
                        null, `Found unexpected err ${err}`);
                    s3.putObject({ Bucket: bucket,
                        Key: 'index.html',
                        ACL: 'private',
                        Body: fs.readFileSync(path.join(__dirname,
                            '/websiteFiles/index.html')),
                        ContentType: 'text/html' }, done);
                });
            });

            afterEach(done => {
                s3.deleteObject({ Bucket: bucket, Key: 'index.html' }, done);
            });

            it('should return 403 if key is private', done => {
                const expectedHeaders = {
                    'x-amz-error-code': 'AccessDenied',
                    'x-amz-error-message': 'Access Denied',
                };
                WebsiteConfigTester.makeHeadRequest(undefined, endpoint, 403,
                    expectedHeaders, done);
            });
        });

        describe('with nonexisting index document key', () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html');
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, done);
            });

            it('should return 403 if nonexisting index document key', done => {
                const expectedHeaders = {
                    'x-amz-error-code': 'AccessDenied',
                    'x-amz-error-message': 'Access Denied',
                };
                WebsiteConfigTester.makeHeadRequest(undefined, endpoint, 403,
                    expectedHeaders, done);
            });
        });

        describe(`redirect all requests to ${redirectEndpoint}`, () => {
            beforeEach(done => {
                const redirectAllTo = {
                    HostName: 'www.google.com',
                };
                const webConfig = new WebsiteConfigTester(null, null,
                  redirectAllTo);
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, done);
            });

            it(`should redirect to ${redirectEndpoint}`, done => {
                const expectedHeaders = {
                    location: redirectEndpoint,
                };
                WebsiteConfigTester.makeHeadRequest(undefined,
                    endpoint, 301, expectedHeaders, done);
            });

            it(`should redirect to ${redirectEndpoint}about`, done => {
                const expectedHeaders = {
                    location: `${redirectEndpoint}about/`,
                };
                WebsiteConfigTester.makeHeadRequest(undefined,
                    `${endpoint}/about/`, 301, expectedHeaders, done);
            });
        });

        describe('redirect all requests to https://www.google.com ' +
            'since https protocol set in website config', () => {
            // Note: these tests will all redirect to https even if
            // conf does not have https since protocol in website config
            // specifies https
            beforeEach(done => {
                const redirectAllTo = {
                    HostName: 'www.google.com',
                    Protocol: 'https',
                };
                const webConfig = new WebsiteConfigTester(null, null,
                  redirectAllTo);
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, done);
            });

            it('should redirect to https://google.com', done => {
                const expectedHeaders = {
                    location: 'https://www.google.com/',
                };
                WebsiteConfigTester.makeHeadRequest(undefined, endpoint,
                    301, expectedHeaders, done);
            });

            it('should redirect to https://google.com/about', done => {
                const expectedHeaders = {
                    location: 'https://www.google.com/about/',
                };
                WebsiteConfigTester.makeHeadRequest(undefined,
                    `${endpoint}/about/`, 301, expectedHeaders, done);
            });
        });

        describe('with custom error document', () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html',
                'error.html');
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, err => {
                    assert.strictEqual(err,
                        null, `Found unexpected err ${err}`);
                    s3.putObject({ Bucket: bucket,
                        Key: 'error.html',
                        ACL: 'public-read',
                        Body: fs.readFileSync(path.join(__dirname,
                            '/websiteFiles/error.html')),
                        ContentType: 'text/html' }, done);
                });
            });

            afterEach(done => {
                s3.deleteObject({ Bucket: bucket, Key: 'error.html' }, done);
            });

            it('should return regular error headers regardless of whether ' +
                'custom error document', done => {
                const expectedHeaders = {
                    'x-amz-error-code': 'AccessDenied',
                    'x-amz-error-message': 'Access Denied',
                };
                WebsiteConfigTester.makeHeadRequest(undefined,
                    `${endpoint}/madeup`, 403, expectedHeaders, done);
            });
        });

        describe('redirect to hostname with error code condition', () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html');
                const condition = {
                    HttpErrorCodeReturnedEquals: '403',
                };
                const redirect = {
                    HostName: 'www.google.com',
                };
                webConfig.addRoutingRule(redirect, condition);
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, done);
            });

            it(`should redirect to ${redirectEndpoint} if error 403` +
            ' occured', done => {
                const expectedHeaders = {
                    location: redirectEndpoint,
                };
                WebsiteConfigTester.makeHeadRequest(undefined, endpoint, 301,
                    expectedHeaders, done);
            });
        });

        describe('redirect to hostname with prefix condition', () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html');
                const condition = {
                    KeyPrefixEquals: 'about/',
                };
                const redirect = {
                    HostName: 'www.google.com',
                };
                webConfig.addRoutingRule(redirect, condition);
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, done);
            });

            it(`should redirect to ${redirectEndpoint}about if ` +
            'key prefix is equal to "about"', done => {
                const expectedHeaders = {
                    location: `${redirectEndpoint}about/`,
                };
                WebsiteConfigTester.makeHeadRequest(undefined,
                    `${endpoint}/about/`, 301, expectedHeaders, done);
            });
        });

        describe('redirect to hostname with prefix and error condition',
        () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html');
                const condition = {
                    KeyPrefixEquals: 'about/',
                    HttpErrorCodeReturnedEquals: '403',
                };
                const redirect = {
                    HostName: 'www.google.com',
                };
                webConfig.addRoutingRule(redirect, condition);
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, done);
            });

            it(`should redirect to ${redirectEndpoint} if ` +
            'key prefix is equal to "about" AND error code 403', done => {
                const expectedHeaders = {
                    location: `${redirectEndpoint}about/`,
                };
                WebsiteConfigTester.makeHeadRequest(undefined,
                    `${endpoint}/about/`, 301, expectedHeaders, done);
            });
        });

        describe('redirect with multiple redirect rules', () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html');
                const conditions = {
                    KeyPrefixEquals: 'about/',
                };
                const redirectOne = {
                    HostName: 'www.google.com',
                };
                const redirectTwo = {
                    HostName: 's3.google.com',
                };
                webConfig.addRoutingRule(redirectOne, conditions);
                webConfig.addRoutingRule(redirectTwo, conditions);
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, done);
            });

            it('should redirect based on first rule', done => {
                const expectedHeaders = {
                    location: `${redirectEndpoint}about/`,
                };
                WebsiteConfigTester.makeHeadRequest(undefined,
                    `${endpoint}/about/`, 301, expectedHeaders, done);
            });
        });

        describe('redirect with protocol',
        () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html');
                const condition = {
                    KeyPrefixEquals: 'about/',
                };
                const redirect = {
                    Protocol: 'https',
                    HostName: 'www.google.com',
                };
                webConfig.addRoutingRule(redirect, condition);
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, done);
            });

            it('should redirect to https://www.google.com/about if ' +
            'https protocol specified', done => {
                const expectedHeaders = {
                    location: 'https://www.google.com/about/',
                };
                WebsiteConfigTester.makeHeadRequest(undefined,
                    `${endpoint}/about/`, 301, expectedHeaders, done);
            });
        });

        describe('redirect to key using ReplaceKeyWith', () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html');
                const condition = {
                    HttpErrorCodeReturnedEquals: '403',
                };
                const redirect = {
                    ReplaceKeyWith: 'redirect.html',
                };
                webConfig.addRoutingRule(redirect, condition);
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, done);
            });

            afterEach(done => {
                s3.deleteObject({ Bucket: bucket, Key: 'redirect.html' },
                err => done(err));
            });

            it('should redirect to specified file if 403 error ' +
                'error occured', done => {
                const expectedHeaders = {
                    location: `${endpoint}/redirect.html`,
                };
                WebsiteConfigTester.makeHeadRequest(undefined, endpoint, 301,
                    expectedHeaders, done);
            });
        });

        describe('redirect using ReplaceKeyPrefixWith', () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html');
                const condition = {
                    HttpErrorCodeReturnedEquals: '403',
                };
                const redirect = {
                    HostName: 'www.google.com',
                    ReplaceKeyPrefixWith: 'about',
                };
                webConfig.addRoutingRule(redirect, condition);
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, done);
            });

            it(`should redirect to ${redirectEndpoint}about if ` +
            'ReplaceKeyPrefixWith equals "about"', done => {
                const expectedHeaders = {
                    location: `${redirectEndpoint}about`,
                };
                WebsiteConfigTester.makeHeadRequest(undefined, endpoint, 301,
                    expectedHeaders, done);
            });
        });

        describe('redirect requests with prefix /about to redirect/',
        () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html');
                const condition = {
                    KeyPrefixEquals: 'about/',
                };
                const redirect = {
                    ReplaceKeyPrefixWith: 'redirect/',
                };
                webConfig.addRoutingRule(redirect, condition);
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, done);
            });

            afterEach(done => {
                s3.deleteObject({ Bucket: bucket, Key: 'redirect/index.html' },
                err => done(err));
            });

            it('should redirect to "redirect/" object if key prefix is equal ' +
                'to "about/"', done => {
                const expectedHeaders = {
                    location: `${endpoint}/redirect/`,
                };
                WebsiteConfigTester.makeHeadRequest(undefined,
                    `${endpoint}/about/`, 301, expectedHeaders, done);
            });
        });

        describe('redirect requests, with both prefix and error code ' +
            'condition', () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html');
                const condition = {
                    KeyPrefixEquals: 'about/',
                    HttpErrorCodeReturnedEquals: '403',
                };
                const redirect = {
                    ReplaceKeyPrefixWith: 'redirect/',
                };
                webConfig.addRoutingRule(redirect, condition);
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, done);
            });

            afterEach(done => {
                s3.deleteObject({ Bucket: bucket, Key: 'redirect/index.html' },
                err => done(err));
            });

            it('should redirect to "redirect" object if key prefix is equal ' +
                'to "about/" and there is a 403 error satisfying the ' +
                'condition in the redirect rule',
            done => {
                const expectedHeaders = {
                    location: `${endpoint}/redirect/`,
                };
                WebsiteConfigTester.makeHeadRequest(undefined,
                    `${endpoint}/about/`, 301, expectedHeaders, done);
            });
        });

        describe('object redirect to /', () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html');
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, err => {
                    assert.strictEqual(err,
                        null, `Found unexpected err ${err}`);
                    s3.putObject({ Bucket: bucket, Key: 'index.html',
                        ACL: 'public-read',
                        Body: fs.readFileSync(path.join(__dirname,
                            '/websiteFiles/index.html')),
                        ContentType: 'text/html',
                        Metadata: {
                            test: 'value',
                        },
                        WebsiteRedirectLocation: '/',
                    },
                        err => {
                            assert.strictEqual(err, null);
                            done();
                        });
                });
            });

            afterEach(done => {
                s3.deleteObject({ Bucket: bucket, Key: 'index.html' },
                err => done(err));
            });

            it('should redirect to /', done => {
                const expectedHeaders = {
                    location: '/',
                };
                WebsiteConfigTester.makeHeadRequest(undefined,
                    `${endpoint}/index.html`, 301, expectedHeaders, done);
            });
        });

        describe('with bucket policy', () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html');
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, err => {
                    assert.strictEqual(err,
                        null, `Found unexpected err ${err}`);
                    s3.putBucketPolicy({ Bucket: bucket, Policy: JSON.stringify(
                        {
                            Version: '2012-10-17',
                            Statement: [{
                                Sid: 'PublicReadGetObject',
                                Effect: 'Allow',
                                Principal: '*',
                                Action: ['s3:GetObject'],
                                Resource: [
                                    `arn:aws:s3:::${bucket}/index.html`,
                                    `arn:aws:s3:::${bucket}/access.html`,
                                ],
                            }],
                        }
                    ) }, err => {
                        assert.strictEqual(err,
                            null, `Found unexpected err ${err}`);
                        s3.putObject({ Bucket: bucket, Key: 'index.html',
                            Body: fs.readFileSync(path.join(__dirname,
                                '/websiteFiles/index.html')),
                            ContentType: 'text/html',
                            Metadata: {
                                test: 'value',
                            } },
                            err => {
                                assert.strictEqual(err, null);
                                done();
                            });
                    });
                });
            });

            afterEach(done => {
                s3.deleteObject({ Bucket: bucket, Key: 'index.html' },
                err => done(err));
            });

            it('should return indexDocument headers if no key ' +
                'requested', done => {
                WebsiteConfigTester.makeHeadRequest(undefined, endpoint,
                    200, indexExpectedHeaders, done);
            });

            it('should serve error 403 with no access to key', done => {
                const expectedHeaders = {
                    'x-amz-error-code': 'AccessDenied',
                    'x-amz-error-message': 'Access Denied',
                };
                WebsiteConfigTester.makeHeadRequest(undefined,
                    `${endpoint}/non_existing.html`, 403, expectedHeaders,
                    done);
            });

            it('should serve error 404 with access to key', done => {
                const expectedHeaders = {
                    'x-amz-error-code': 'NoSuchKey',
                    'x-amz-error-message': 'The specified key does not exist.',
                };
                WebsiteConfigTester.makeHeadRequest(undefined,
                    `${endpoint}/access.html`, 404, expectedHeaders,
                    done);
            });
        });

        describe('with routing rule on index', () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html');
                const condition = {
                    KeyPrefixEquals: 'index.html',
                };
                const redirect = {
                    ReplaceKeyWith: 'whatever.html',
                };
                webConfig.addRoutingRule(redirect, condition);
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, err => {
                    assert.strictEqual(err,
                        null, `Found unexpected err ${err}`);
                    s3.putObject({ Bucket: bucket, Key: 'index.html',
                        ACL: 'public-read',
                        Body: fs.readFileSync(path.join(__dirname,
                            '/websiteFiles/index.html')),
                        ContentType: 'text/html',
                        Metadata: {
                            test: 'value',
                        },
                    },
                        err => {
                            assert.strictEqual(err, null);
                            done();
                        });
                });
            });

            afterEach(done => {
                s3.deleteObject({ Bucket: bucket, Key: 'index.html' },
                err => done(err));
            });

            it('should not redirect if index key is not explicit', done => {
                WebsiteConfigTester.makeHeadRequest(undefined, endpoint,
                    200, indexExpectedHeaders, done);
            });
        });

        describe('without trailing / for recursive index check', () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html');
                const object = {
                    Bucket: bucket,
                    Body: fs.readFileSync(path.join(__dirname,
                    '/websiteFiles/index.html')),
                    ContentType: 'text/html',
                };
                async.waterfall([
                    next => s3.putBucketWebsite({ Bucket: bucket,
                        WebsiteConfiguration: webConfig }, next),
                    (data, next) => s3.putBucketPolicy({ Bucket: bucket,
                        Policy: JSON.stringify({
                            Version: '2012-10-17',
                            Statement: [{
                                Sid: 'PublicReadGetObject',
                                Effect: 'Allow',
                                Principal: '*',
                                Action: ['s3:GetObject'],
                                Resource: [
                                    `arn:aws:s3:::${bucket}/original_key_file`,
                                    `arn:aws:s3:::${bucket}/original_key_nofile`,
                                    `arn:aws:s3:::${bucket}/file/*`,
                                    `arn:aws:s3:::${bucket}/nofile/*`,
                                ],
                            }],
                        }),
                    }, next),
                    (data, next) => s3.putObject(Object.assign({}, object,
                        { Key: 'original_key_file/index.html' }), next),
                    (data, next) => s3.putObject(Object.assign({}, object,
                        { Key: 'file/index.html' }), next), // the redirect 302
                    (data, next) => s3.putObject(Object.assign({}, object,
                        { Key: 'no_access_file/index.html' }), next),
                ], err => {
                    assert.ifError(err);
                    done();
                });
            });

            afterEach(done => {
                async.waterfall([
                    next => s3.deleteObject({ Bucket: bucket,
                        Key: 'original_key_file/index.html' }, next),
                    (data, next) => s3.deleteObject({ Bucket: bucket,
                            Key: 'file/index.html' }, next),
                    (data, next) => s3.deleteObject({ Bucket: bucket,
                        Key: 'no_access_file/index.html' }, next),
                ], err => {
                    assert.ifError(err);
                    done();
                });
            });

            it('should redirect 302 with trailing / on folder with index', done => {
                const expectedHeaders = {
                    'location': '/file/',
                    'x-amz-error-code': 'Found',
                    'x-amz-error-message': 'Resource Found',
                };
                WebsiteConfigTester.makeHeadRequest(undefined,
                    `${endpoint}/file`, 302, expectedHeaders, done);
            });

            it('should return 404 on original key access without index',
            done => {
                const expectedHeaders = {
                    'x-amz-error-code': 'NoSuchKey',
                    'x-amz-error-message': 'The specified key does not exist.',
                };
                WebsiteConfigTester.makeHeadRequest(undefined,
                    `${endpoint}/original_key_nofile`, 404,
                    expectedHeaders, done);
            });

            describe('should return 403', () => {
                [
                    {
                        it: 'on original key access with index no access',
                        key: 'original_key_file',
                    },
                    {
                        it: 'on folder access without index',
                        key: 'nofile',
                    },
                    {
                        it: 'on no access with index',
                        key: 'no_access_file',
                    },
                ].forEach(test =>
                    it(test.it, done => {
                        const expectedHeaders = {
                            'x-amz-error-code': 'AccessDenied',
                            'x-amz-error-message': 'Access Denied',
                        };
                        WebsiteConfigTester.makeHeadRequest(undefined,
                            `${endpoint}/${test.key}`, 403,
                            expectedHeaders, done);
                    }));
            });
        });
    });
});
