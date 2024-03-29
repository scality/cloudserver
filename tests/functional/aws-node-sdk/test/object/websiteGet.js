const assert = require('assert');
const async = require('async');
const fs = require('fs');
const path = require('path');

const { S3 } = require('aws-sdk');

const conf = require('../../../../../lib/Config').config;
const getConfig = require('../support/config');
const { makeRequest } = require('../../../raw-node/utils/makeRequest');
const { WebsiteConfigTester } = require('../../lib/utility/website-util');

const config = getConfig('default', { signatureVersion: 'v4' });
const s3 = new S3(config);

const transport = conf.https ? 'https' : 'http';
const bucket = process.env.AWS_ON_AIR ? 'awsbucketwebsitetester' :
    'bucketwebsitetester';
const port = process.env.AWS_ON_AIR ? 80 : 8000;
const hostname = process.env.S3_END_TO_END ?
    `${bucket}.s3-website-us-east-1.scality.com` :
    `${bucket}.s3-website-us-east-1.amazonaws.com`;
const endpoint = `${transport}://${hostname}:${port}`;
const redirectEndpoint = `${transport}://www.google.com`;

// Note: To run these tests locally, you may need to edit the machine's
// /etc/hosts file to include the following line:
// `127.0.0.1 bucketwebsitetester.s3-website-us-east-1.amazonaws.com`

function putBucketWebsiteAndPutObjectRedirect(redirect, condition, key, done) {
    const webConfig = new WebsiteConfigTester('index.html');
    webConfig.addRoutingRule(redirect, condition);
    s3.putBucketWebsite({ Bucket: bucket,
        WebsiteConfiguration: webConfig }, err => {
        if (err) {
            done(err);
        }
        return s3.putObject({ Bucket: bucket,
            Key: key,
            ACL: 'public-read',
            Body: fs.readFileSync(path.join(__dirname,
            '/websiteFiles/redirect.html')),
            ContentType: 'text/html' }, done);
    });
}

describe('User visits bucket website endpoint', () => {
    it('should return 404 when no such bucket', done => {
        WebsiteConfigTester.checkHTML({
            method: 'GET',
            url: endpoint,
            responseType: '404-no-such-bucket',
        }, done);
    });

    describe('with existing bucket', () => {
        beforeEach(done => s3.createBucket({ Bucket: bucket }, done));

        afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

        it('should return 404 when no website configuration', done => {
            WebsiteConfigTester.checkHTML({
                method: 'GET',
                url: endpoint,
                responseType: '404-no-such-website-configuration',
            }, done);
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
                        ContentType: 'text/html' },
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

            it('should return 405 when user requests method other than get ' +
            'or head', done => {
                makeRequest({
                    hostname,
                    port,
                    method: 'POST',
                }, (err, res) => {
                    assert.strictEqual(err, null,
                        `Err with request ${err}`);
                    assert.strictEqual(res.statusCode, 405);
                    assert(res.body.indexOf('<head><title>405 ' +
                        'Method Not Allowed</title></head>') > -1);
                    return done();
                });
            });

            it('should serve indexDocument if no key requested', done => {
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: endpoint,
                    responseType: 'index-user',
                }, done);
            });
            it('should serve indexDocument if key requested', done => {
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: `${endpoint}/index.html`,
                    responseType: 'index-user',
                }, done);
            });
        });
        describe('with path in request with/without key', () => {
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
                        ContentType: 'text/html' }, done);
                });
            });

            afterEach(done => {
                s3.deleteObject({ Bucket: bucket, Key:
                    'pathprefix/index.html' },
                done);
            });

            it('should serve indexDocument if path request without key',
            done => {
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: `${endpoint}/pathprefix/`,
                    responseType: 'index-user',
                }, done);
            });

            it('should serve indexDocument if path request with key',
            done => {
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: `${endpoint}/pathprefix/index.html`,
                    responseType: 'index-user',
                }, done);
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
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: endpoint,
                    responseType: '403-access-denied',
                }, done);
            });
        });

        describe('with nonexisting index document key', () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html');
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, done);
            });

            it('should return 403 if nonexisting index document key', done => {
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: endpoint,
                    responseType: '403-access-denied',
                }, done);
            });
        });

        describe.skip(`redirect all requests to ${redirectEndpoint}`, () => {
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
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: endpoint,
                    responseType: 'redirect',
                    redirectUrl: `${redirectEndpoint}/`,
                }, done);
            });

            it(`should redirect to ${redirectEndpoint}/about`, done => {
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: `${endpoint}/about`,
                    responseType: 'redirect',
                    redirectUrl: `${redirectEndpoint}/about`,
                }, done);
            });
        });

        describe.skip('redirect all requests to https://www.google.com ' +
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

            it('should redirect to https://google.com/', done => {
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: endpoint,
                    responseType: 'redirect',
                    redirectUrl: 'https://www.google.com/',
                }, done);
            });

            it('should redirect to https://google.com/about', done => {
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: `${endpoint}/about`,
                    responseType: 'redirect',
                    redirectUrl: 'https://www.google.com/about',
                }, done);
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

            it('should serve custom error document if an error occurred',
            done => {
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: endpoint,
                    responseType: 'error-user',
                }, done);
            });

            it('should serve custom error document with redirect',
            done => {
                s3.putObject({ Bucket: bucket,
                    Key: 'error.html',
                    ACL: 'public-read',
                    Body: fs.readFileSync(path.join(__dirname,
                        '/websiteFiles/error.html')),
                    ContentType: 'text/html',
                    WebsiteRedirectLocation: 'https://scality.com/test',
                }, err => {
                    assert.ifError(err);
                    WebsiteConfigTester.checkHTML({
                        method: 'GET',
                        url: endpoint,
                        responseType: 'redirect-error',
                        redirectUrl: 'https://scality.com/test',
                        expectedHeaders: {
                            'x-amz-error-code': 'AccessDenied',
                            'x-amz-error-message': 'Access Denied',
                        },
                    }, done);
                });
            });
        });

        describe('unfound custom error document', () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html',
                'error.html');
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, done);
            });

            it('should serve s3 error file if unfound custom error document ' +
            'and an error occurred', done => {
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: endpoint,
                    responseType: '403-retrieve-error-document',
                }, done);
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
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: endpoint,
                    responseType: 'redirect',
                    redirectUrl: `${redirectEndpoint}/`,
                }, done);
            });
        });

        describe.skip('redirect to hostname with prefix condition', () => {
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

            it(`should redirect to ${redirectEndpoint}/about/ if ` +
            'key prefix is equal to "about"', done => {
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: `${endpoint}/about/`,
                    responseType: 'redirect',
                    redirectUrl: `${redirectEndpoint}/about/`,
                }, done);
            });
        });

        describe.skip('redirect to hostname with prefix and error condition',
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
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: `${endpoint}/about/`,
                    responseType: 'redirect',
                    redirectUrl: `${redirectEndpoint}/about/`,
                }, done);
            });
        });

        describe.skip('redirect with multiple redirect rules', () => {
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

            it('should redirect to the first one', done => {
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: `${endpoint}/about/`,
                    responseType: 'redirect',
                    redirectUrl: `${redirectEndpoint}/about/`,
                }, done);
            });
        });

        describe.skip('redirect with protocol',
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
            'https protocols', done => {
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: `${endpoint}/about/`,
                    responseType: 'redirect',
                    redirectUrl: 'https://www.google.com/about/',
                }, done);
            });
        });

        describe('redirect to key using ReplaceKeyWith', () => {
            beforeEach(done => {
                const condition = {
                    HttpErrorCodeReturnedEquals: '403',
                };
                const redirect = {
                    ReplaceKeyWith: 'redirect.html',
                };
                putBucketWebsiteAndPutObjectRedirect(redirect, condition,
                  'redirect.html', done);
            });

            afterEach(done => {
                s3.deleteObject({ Bucket: bucket, Key: 'redirect.html' },
                err => done(err));
            });

            it('should serve redirect file if error 403 error occured',
            done => {
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: endpoint,
                    responseType: 'redirect-user',
                    redirectUrl: `${endpoint}/redirect.html`,
                }, done);
            });
        });

        describe.skip('redirect using ReplaceKeyPrefixWith', () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html');
                const condition = {
                    HttpErrorCodeReturnedEquals: '403',
                };
                const redirect = {
                    HostName: 'www.google.com',
                    ReplaceKeyPrefixWith: 'about/',
                };
                webConfig.addRoutingRule(redirect, condition);
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, done);
            });

            it(`should redirect to ${redirectEndpoint}/about/ if ` +
            'ReplaceKeyPrefixWith equals "about/"', done => {
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: endpoint,
                    responseType: 'redirect',
                    redirectUrl: `${redirectEndpoint}/about/`,
                }, done);
            });
        });

        describe.skip('redirect requests with prefix /about to redirect/',
        () => {
            beforeEach(done => {
                const condition = {
                    KeyPrefixEquals: 'about/',
                };
                const redirect = {
                    ReplaceKeyPrefixWith: 'redirect/',
                };
                putBucketWebsiteAndPutObjectRedirect(redirect, condition,
                  'redirect/index.html', done);
            });

            afterEach(done => {
                s3.deleteObject({ Bucket: bucket, Key: 'redirect/index.html' },
                err => done(err));
            });

            it('should serve redirect file if key prefix is equal to "about"',
            done => {
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: `${endpoint}/about/`,
                    responseType: 'redirect-user',
                    redirectUrl: `${endpoint}/redirect/`,
                }, done);
            });
        });

        describe.skip('redirect requests, with prefix /about and that return ' +
        '403 error, to prefix redirect/', () => {
            beforeEach(done => {
                const condition = {
                    KeyPrefixEquals: 'about/',
                    HttpErrorCodeReturnedEquals: '403',
                };
                const redirect = {
                    ReplaceKeyPrefixWith: 'redirect/',
                };
                putBucketWebsiteAndPutObjectRedirect(redirect, condition,
                  'redirect/index.html', done);
            });

            afterEach(done => {
                s3.deleteObject({ Bucket: bucket, Key: 'redirect/index.html' },
                err => done(err));
            });

            it('should serve redirect file if key prefix is equal to ' +
            '"about" and error 403',
            done => {
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: `${endpoint}/about/`,
                    responseType: 'redirect-user',
                    redirectUrl: `${endpoint}/redirect/`,
                }, done);
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
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: `${endpoint}/index.html`,
                    responseType: 'redirect',
                    redirectUrl: '/',
                }, done);
            });
        });

        describe('with bucket policy', () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html',
                'error.html');

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
                                    `arn:aws:s3:::${bucket}/index.html`,
                                    `arn:aws:s3:::${bucket}/error.html`,
                                    `arn:aws:s3:::${bucket}/access.html`,
                                ],
                            },
                            {
                                Sid: 'DenyUnrelatedObj',
                                Effect: 'Deny',
                                Principal: '*',
                                Action: ['s3:GetObject'],
                                Resource: [
                                    `arn:aws:s3:::${bucket}/unrelated_obj.html`,
                                ],
                            }],
                        }),
                    }, next),
                    (data, next) => s3.putObject({
                        Bucket: bucket, Key: 'index.html',
                        Body: fs.readFileSync(path.join(__dirname,
                        '/websiteFiles/index.html')),
                        ContentType: 'text/html',
                    }, next),
                    (data, next) => s3.putObject({
                        Bucket: bucket, Key: 'error.html',
                        Body: fs.readFileSync(path.join(__dirname,
                        '/websiteFiles/error.html')),
                        ContentType: 'text/html',
                    }, next),

                ], err => {
                    assert.ifError(err);
                    done();
                });
            });

            afterEach(done => {
                async.waterfall([
                    next => s3.deleteObject({ Bucket: bucket,
                        Key: 'index.html' }, next),
                    (data, next) => s3.deleteObject({ Bucket: bucket,
                            Key: 'error.html' }, next),
                ], err => {
                    assert.ifError(err);
                    done();
                });
            });

            it('should serve indexDocument if no key requested', done => {
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: endpoint,
                    responseType: 'index-user',
                }, done);
            });

            it('should serve custom error 403 with deny on unrelated object ' +
            'and no access to key', done => {
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: `${endpoint}/non_existing.html`,
                    responseType: 'error-user',
                }, done);
            });

            it('should serve custom error 404 with deny on unrelated object ' +
            'and access to key', done => {
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: `${endpoint}/access.html`,
                    responseType: 'error-user-404',
                }, done);
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
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: endpoint,
                    responseType: 'index-user',
                }, done);
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
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: `${endpoint}/file`,
                    responseType: 'redirect-error-found',
                    redirectUrl: '/file/',
                }, done);
            });

            it('should return 404 on original key access without index',
            done => {
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: `${endpoint}/original_key_nofile`,
                    responseType: '404-not-found',
                }, done);
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
                        WebsiteConfigTester.checkHTML({
                            method: 'GET',
                            url: `${endpoint}/${test.key}`,
                            responseType: '403-access-denied',
                        }, done);
                    }));
            });
        });
    });
});
