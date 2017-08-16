const assert = require('assert');
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
const hostname = `${bucket}.s3-website-us-east-1.amazonaws.com`;
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
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: `${endpoint}/about/`,
                    responseType: 'redirect',
                    redirectUrl: `${redirectEndpoint}/about/`,
                }, done);
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

            it('should redirect to the first one', done => {
                WebsiteConfigTester.checkHTML({
                    method: 'GET',
                    url: `${endpoint}/about/`,
                    responseType: 'redirect',
                    redirectUrl: `${redirectEndpoint}/about/`,
                }, done);
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

        describe('redirect using ReplaceKeyPrefixWith', () => {
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

        describe('redirect requests with prefix /about to redirect/',
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

        describe('redirect requests, with prefix /about and that return ' +
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
    });
});
