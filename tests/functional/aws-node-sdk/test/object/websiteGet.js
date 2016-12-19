import assert from 'assert';
import fs from 'fs';
import http from 'http';
import path from 'path';

import { S3 } from 'aws-sdk';
import Browser from 'zombie';

import conf from '../../../../../lib/Config';
import getConfig from '../support/config';
import { WebsiteConfigTester } from '../../lib/utility/website-util';

const config = getConfig('default', { signatureVersion: 'v4' });
const s3 = new S3(config);

const transport = conf.https ? 'https' : 'http';
const bucket = process.env.AWS_ON_AIR ? 'awsbucketwebsitetester' :
    'bucketwebsitetester';
const hostname = `${bucket}.s3-website-us-east-1.amazonaws.com`;

const endpoint = process.env.AWS_ON_AIR ? `${transport}://${hostname}` :
    `${transport}://${hostname}:8000`;

// TODO: Add this endpoint in Integration for CI

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

describe.only('User visits bucket website endpoint', () => {
    const browser = new Browser();

    // Have not manage to reproduce agains AWS
    it.skip('should return 405 when user requests method other than get or ' +
    ' head',
        done => {
            const options = {
                hostname,
                port: 8000,
                method: 'POST',
            };
            const req = http.request(options, res => {
                const body = [];
                res.on('data', chunk => {
                    body.push(chunk);
                });
                res.on('end', () => {
                    assert.strictEqual(res.statusCode, 405);
                    const total = body.join('');
                    assert(total.indexOf('<head><title>405 ' +
                        'Method Not Allowed</title></head>') > -1);
                    done();
                });
            });
            req.end();
        });

    it('should return 404 when no such bucket', done => {
        browser.visit(endpoint, () => {
            WebsiteConfigTester.checkHTML(browser, '404-no-such-bucket', null,
              bucket);
            done();
        });
    });

    describe('with existing bucket', () => {
        beforeEach(done => {
            s3.createBucket({ Bucket: bucket }, err => {
                if (err) {
                    return done(err);
                }
                return setTimeout(() => done(), 5000);
            });
        });

        afterEach(done => {
            s3.deleteBucket({ Bucket: bucket }, done);
        });

        it('should return 404 when no website configuration', done => {
            browser.visit(endpoint, () => {
                WebsiteConfigTester.checkHTML(browser,
                  '404-no-such-website-configuration', null, bucket);
                done();
            });
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

            it('should serve indexDocument if no key requested', done => {
                browser.visit(endpoint, () => {
                    WebsiteConfigTester.checkHTML(browser, 'index-user');
                    done();
                });
            });
            it('should serve indexDocument if key requested', done => {
                browser.visit(`${endpoint}/index.html`, () => {
                    WebsiteConfigTester.checkHTML(browser, 'index-user');
                    done();
                });
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
                        Key: 'www/index.html',
                        ACL: 'public-read',
                        Body: fs.readFileSync(path.join(__dirname,
                            '/websiteFiles/index.html')),
                        ContentType: 'text/html' }, done);
                });
            });

            afterEach(done => {
                s3.deleteObject({ Bucket: bucket, Key: 'www/index.html' },
                done);
            });

            it('should serve indexDocument if path request without key',
            done => {
                browser.visit(`${endpoint}/www/`, () => {
                    WebsiteConfigTester.checkHTML(browser, 'index-user');
                    done();
                });
            });

            it('should serve indexDocument if path request with key',
            done => {
                browser.visit(`${endpoint}/www/index.html`, () => {
                    WebsiteConfigTester.checkHTML(browser, 'index-user');
                    done();
                });
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
                browser.visit(endpoint, () => {
                    WebsiteConfigTester.checkHTML(browser, '403-access-denied');
                    done();
                });
            });
        });

        describe('with nonexisting index document key', () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html');
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, done);
            });

            it('should return 403 if nonexisting index document key', done => {
                browser.visit(endpoint, () => {
                    WebsiteConfigTester.checkHTML(browser, '403-access-denied');
                    done();
                });
            });
        });

        describe('redirect all requests to http://www.scality.com', () => {
            beforeEach(done => {
                const redirectAllTo = {
                    HostName: 'www.scality.com',
                };
                const webConfig = new WebsiteConfigTester(null, null,
                  redirectAllTo);
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, done);
            });

            it('should redirect to http://www.scality.com', done => {
                browser.visit(endpoint, () => {
                    WebsiteConfigTester.checkHTML(browser, '200',
                      'http://www.scality.com');
                    done();
                });
            });

            it('should redirect to http://www.scality.com/about-us', done => {
                browser.visit(`${endpoint}/about-us`, () => {
                    WebsiteConfigTester.checkHTML(browser, '200',
                      'http://www.scality.com/about-us/');
                    done();
                });
            });
        });

        // 10
        describe('redirect all requests to https://www.scality.com', () => {
            beforeEach(done => {
                const redirectAllTo = {
                    HostName: 'www.scality.com',
                    Protocol: 'https',
                };
                const webConfig = new WebsiteConfigTester(null, null,
                  redirectAllTo);
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, done);
            });

            it('should redirect to https://scality.com', done => {
                browser.visit(endpoint, () => {
                    WebsiteConfigTester.checkHTML(browser, '200',
                      'https://www.scality.com');
                    done();
                });
            });

            it('should redirect to https://scality.com/about-us', done => {
                browser.visit(`${endpoint}/about-us`, () => {
                    WebsiteConfigTester.checkHTML(browser, '200',
                      'https://www.scality.com/about-us/');
                    done();
                });
            });
        });
        // 11
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
                browser.visit(endpoint, () => {
                    WebsiteConfigTester.checkHTML(browser, 'error-user');
                    done();
                });
            });
        });
        // 12
        describe('unfound custom error document', () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html',
                'error.html');
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, done);
            });

            it('should serve s3 error file if unfound custom error document ' +
            'and an error occurred', done => {
                browser.visit(endpoint, () => {
                    WebsiteConfigTester.checkHTML(browser,
                      '403-retrieve-error-document');
                    done();
                });
            });
        });

        // 13
        describe('redirect to hostname with error code condition', () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html');
                const condition = {
                    HttpErrorCodeReturnedEquals: '403',
                };
                const redirect = {
                    HostName: 'www.scality.com',
                };
                webConfig.addRoutingRule(redirect, condition);
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, done);
            });

            it('should redirect to http://www.scality.com if error 403' +
            ' occured', done => {
                browser.visit(endpoint, () => {
                    WebsiteConfigTester.checkHTML(browser, '200',
                    'http://www.scality.com');
                    done();
                });
            });
        });

        // 14
        describe('redirect to hostname with prefix condition', () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html');
                const condition = {
                    KeyPrefixEquals: 'about-us/',
                };
                const redirect = {
                    HostName: 'www.scality.com',
                };
                webConfig.addRoutingRule(redirect, condition);
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, done);
            });

            it('should redirect to https://www.scality.com/about-us if ' +
            'key prefix is equal to "about-us"', done => {
                browser.visit(`${endpoint}/about-us/`, () => {
                    WebsiteConfigTester.checkHTML(browser, '200',
                    'http://www.scality.com/about-us/');
                    done();
                });
            });
        });

        // 15
        describe('redirect to hostname with prefix and error condition',
        () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html');
                const condition = {
                    KeyPrefixEquals: 'about-us/',
                    HttpErrorCodeReturnedEquals: '403',
                };
                const redirect = {
                    HostName: 'www.scality.com',
                };
                webConfig.addRoutingRule(redirect, condition);
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, done);
            });

            it('should redirect to http://www.scality.com if ' +
            'key prefix is equal to "about-us" AND error code 403', done => {
                browser.visit(`${endpoint}/about-us/`, () => {
                    WebsiteConfigTester.checkHTML(browser, '200',
                      'http://www.scality.com/about-us/');
                    done();
                });
            });
        });

        // 16 redirect with multiple redirect rules and show that first one wins
        describe('redirect with multiple redirect rules', () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html');
                const conditions = {
                    KeyPrefixEquals: 'about-us/',
                };
                const redirectOne = {
                    HostName: 'www.scality.com',
                };
                const redirectTwo = {
                    HostName: 's3.scality.com',
                };
                webConfig.addRoutingRule(redirectOne, conditions);
                webConfig.addRoutingRule(redirectTwo, conditions);
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, done);
            });

            it('should redirect to the first one', done => {
                browser.visit(`${endpoint}/about-us/`, () => {
                    WebsiteConfigTester.checkHTML(browser, '200',
                      'http://www.scality.com/about-us/');
                    done();
                });
            });
        });

        // 17
        describe('redirect with protocol',
        () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html');
                const condition = {
                    KeyPrefixEquals: 'about-us/',
                };
                const redirect = {
                    Protocol: 'https',
                    HostName: 'www.scality.com',
                };
                webConfig.addRoutingRule(redirect, condition);
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, done);
            });

            it('should redirect to https://www.scality.com/about-us if ' +
            'https protocols', done => {
                browser.visit(`${endpoint}/about-us/`, () => {
                    WebsiteConfigTester.checkHTML(browser, '200',
                      'https://www.scality.com/about-us/');
                    done();
                });
            });
        });

        // 18 SKIP because redirect to hostname is the default redirection

        // 19
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
                browser.visit(endpoint, () => {
                    WebsiteConfigTester.checkHTML(browser, 'redirect-user');
                    done();
                });
            });
        });

        // 20
        describe('redirect using ReplaceKeyPrefixWith', () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html');
                const condition = {
                    HttpErrorCodeReturnedEquals: '403',
                };
                const redirect = {
                    HostName: 'www.scality.com',
                    ReplaceKeyPrefixWith: '/about-us',
                };
                webConfig.addRoutingRule(redirect, condition);
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, done);
            });

            it('should redirect to www.scality.com/about-us if ' +
            'ReplaceKeyPrefixWith equals "about-us/"', done => {
                browser.visit(endpoint, () => {
                    WebsiteConfigTester.checkHTML(browser, '200',
                    'http://www.scality.com/about-us/');
                    done();
                });
            });
        });


        // MIXING

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
                browser.visit(`${endpoint}/about/`, () => {
                    WebsiteConfigTester.checkHTML(browser, 'redirect-user');
                    done();
                });
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
                browser.visit(`${endpoint}/about/`, () => {
                    WebsiteConfigTester.checkHTML(browser, 'redirect-user');
                    done();
                });
            });
        });
    });
});

// Tests:
// 1) website endpoint method other than get or head X
// 2) website endpoint without a bucket name (would need separate etc/hosts
// entry -- SKIP it)
// 3) no such bucket X
// 4) no website configuration X
// 5) no key in request -- uses index document X
// 6) path in request without key (for example: docs/) -- uses index document
//  a) put website config like in prior test
//  b) put key called docs/index.html in bucket (must be public).  the key value
//  should be some small document file that you save in websiteFiles.
//  c) use zombie to call endpoint/docs/
//  d) should get the document file
//
//
// 7) key is not public
// 8) no such key error from metadata
// 9) redirect all requests with no protocol specified (should use
// same as request)
// 10) redirect all requests with protocol specified
// 11) return user's errordocument
// 12) return our error page for when user's error document can't be retrieved
// 13) redirect with just error code condition
// 14) redirect with just prefix condition
// 15) redirect with error code and prefix condition
// 16) redirect with multiple condition rules and show that first one wins
// 17) redirect with protocol specified
// DEFAULT 18) redirect with hostname specified
// 19) redirect with replaceKeyWith specified
// 20) redirect with replaceKeyPrefixWith specified
// 21) redirect with httpRedirect Code specified
// 22) redirect with combination of redirect items applicable
