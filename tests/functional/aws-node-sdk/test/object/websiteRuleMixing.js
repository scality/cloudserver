import assert from 'assert';
import fs from 'fs';
import http from 'http';
import https from 'https';
import path from 'path';

import Browser from 'zombie';

import BucketUtility from '../../lib/utility/bucket-util';
import conf from '../../../../../lib/Config';
import getConfig from '../support/config';
import { WebsiteConfigTester } from '../../lib/utility/website-util';

const config = getConfig('default', { signatureVersion: 'v4' });
const bucketUtil = new BucketUtility('default', config);
const s3 = bucketUtil.s3;

const transport = conf.https ? 'https' : 'http';
const bucket = process.env.AWS_ON_AIR ? 'awsbucketwebsitetester' :
    'bucketwebsitetester';
const hostname = `${bucket}.s3-website-us-east-1.amazonaws.com`;

const endpoint = process.env.AWS_ON_AIR ? `${transport}://${hostname}` :
    `${transport}://${hostname}:8000`;

const redirectEndpoint = conf.https ? 'https://www.google.com/' :
    'http://www.google.com/';

const redirectWaitingPeriod = 12000;

function _makeRequest(path, method, callback) {
    const options = {
        hostname,
        port: process.env.AWS_ON_AIR ? 80 : 8000,
        method,
        rejectUnauthorized: false,
    };
    if (path) {
        options.path = path;
    }
    const module = conf.https ? https : http;
    const req = module.request(options, res => {
        const body = [];
        res.on('data', chunk => {
            body.push(chunk);
        });
        res.on('error', err => {
            process.stdout.write(`err on ${method} response`);
            return callback(err);
        });
        res.on('end', () => callback(null, {
            body: body.toString('UTF-8'),
            statusCode: res.statusCode,
            headers: res.headers,
        }));
    });
    req.on('error', err => {
        process.stdout.write(`err from ${method} request`);
        return callback(err);
    });
    req.end();
}

// Note: Timeouts are set on tests with redirects to a URL as they are flaky
// without them. If they still fail, consider increasing the timeout or using
// mocha's this.retries method to auto-retry the test after failure.
describe('User visits bucket website endpoint and requests resource ' +
'that has x-amz-website-redirect-location header ::', () => {
    const browser = new Browser({ strictSSL: false });
    browser.on('error', err => {
        process.stdout.write('zombie encountered err loading resource or ' +
            'evaluating javascript:', err);
    });

    before(done => {
        // so that redirects will not time out, have zombie visit
        // redirectEndpoint first
        browser.visit(redirectEndpoint);
        done();
    });

    beforeEach(done => s3.createBucket({ Bucket: bucket }, done));

    afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

    describe('when x-amz-website-redirect-location: /redirect.html', () => {
        beforeEach(() => {
            const webConfig = new WebsiteConfigTester('index.html');
            return s3.putBucketWebsiteAsync({ Bucket: bucket,
                WebsiteConfiguration: webConfig })
            .then(() => s3.putObjectAsync({ Bucket: bucket,
                Key: 'index.html',
                ACL: 'public-read',
                Body: fs.readFileSync(path.join(__dirname,
                    '/websiteFiles/index.html')),
                ContentType: 'text/html',
                WebsiteRedirectLocation: '/redirect.html' }))
            .then(() => s3.putObjectAsync({ Bucket: bucket,
                Key: 'redirect.html',
                ACL: 'public-read',
                Body: fs.readFileSync(path.join(__dirname,
                    '/websiteFiles/redirect.html')),
                ContentType: 'text/html' }));
        });

        afterEach(() => bucketUtil.empty(bucket));

        it('should serve redirect file on GET request', done => {
            browser.visit(`${endpoint}`, () => {
                WebsiteConfigTester.checkHTML(browser, 'redirect-user');
                done();
            });
        });

        it('should redirect to redirect file on HEAD request', done => {
            _makeRequest(undefined, 'HEAD', (err, res) => {
                if (err) {
                    return done(err);
                }
                assert.strictEqual(res.statusCode, 301);
                assert.strictEqual(res.headers.location,
                '/redirect.html');
                return done();
            });
        });
    });

    describe('when x-amz-website-redirect-location: https://www.google.com',
    () => {
        beforeEach(() => {
            const webConfig = new WebsiteConfigTester('index.html');
            return s3.putBucketWebsiteAsync({ Bucket: bucket,
                WebsiteConfiguration: webConfig })
            .then(() => s3.putObjectAsync({ Bucket: bucket,
                Key: 'index.html',
                ACL: 'public-read',
                Body: fs.readFileSync(path.join(__dirname,
                    '/websiteFiles/index.html')),
                ContentType: 'text/html',
                WebsiteRedirectLocation: 'https://www.google.com' }));
        });

        afterEach(() => bucketUtil.empty(bucket));

        it('should redirect to https://www.google.com', done => {
            browser.visit(endpoint, () => setTimeout(() => {
                WebsiteConfigTester.checkHTML(browser, '200',
                'https://www.google.com');
                done();
            }, redirectWaitingPeriod));
        });

        it('should redirect to https://www.google.com on HEAD request',
            done => {
                _makeRequest(undefined, 'HEAD', (err, res) => {
                    if (err) {
                        return done(err);
                    }
                    assert.strictEqual(res.statusCode, 301);
                    assert.strictEqual(res.headers.location,
                    'https://www.google.com');
                    return done();
                });
            });
    });

    describe('when key with header is private', () => {
        beforeEach(() => {
            const webConfig = new WebsiteConfigTester('index.html');
            return s3.putBucketWebsiteAsync({ Bucket: bucket,
                WebsiteConfiguration: webConfig })
            .then(() => s3.putObjectAsync({ Bucket: bucket,
                Key: 'index.html',
                Body: fs.readFileSync(path.join(__dirname,
                    '/websiteFiles/index.html')),
                ContentType: 'text/html',
                WebsiteRedirectLocation: 'https://www.google.com' }));
        });

        afterEach(() => bucketUtil.empty(bucket));

        it('should return 403 instead of x-amz-website-redirect-location ' +
        'header location', done => {
            browser.visit(endpoint, () => {
                WebsiteConfigTester.checkHTML(browser, '403-access-denied');
                done();
            });
        });

        it('should return 403 instead of x-amz-website-redirect-location ' +
        'header location on HEAD request',
            done => {
                _makeRequest(undefined, 'HEAD', (err, res) => {
                    if (err) {
                        return done(err);
                    }
                    assert.strictEqual(res.statusCode, 403);
                    return done();
                });
            });
    });

    describe('when key with header is private' +
    'and website config has error condition routing rule', () => {
        beforeEach(() => {
            const webConfig = new WebsiteConfigTester('index.html');
            const condition = {
                HttpErrorCodeReturnedEquals: '403',
            };
            const redirect = {
                HostName: 'www.google.com',
            };
            webConfig.addRoutingRule(redirect, condition);
            return s3.putBucketWebsiteAsync({ Bucket: bucket,
                WebsiteConfiguration: webConfig })
            .then(() => s3.putObjectAsync({ Bucket: bucket,
                Key: 'index.html',
                Body: fs.readFileSync(path.join(__dirname,
                    '/websiteFiles/index.html')),
                ContentType: 'text/html',
                WebsiteRedirectLocation: '/redirect.html' }))
            .then(() => s3.putObjectAsync({ Bucket: bucket,
                Key: 'redirect.html',
                ACL: 'public-read',
                Body: fs.readFileSync(path.join(__dirname,
                    '/websiteFiles/redirect.html')),
                ContentType: 'text/html' }));
        });

        afterEach(() => bucketUtil.empty(bucket));

        it(`should redirect to ${redirectEndpoint} since error 403 ` +
        'occurred instead of x-amz-website-redirect-location header ' +
        'location on GET request', done => {
            browser.visit(endpoint, () => setTimeout(() => {
                WebsiteConfigTester.checkHTML(browser, '200',
                redirectEndpoint);
                done();
            }, redirectWaitingPeriod));
        });

        it(`should redirect to ${redirectEndpoint} since error 403 ` +
        'occurred instead of x-amz-website-redirect-location header ' +
        'location on HEAD request',
            done => {
                _makeRequest(undefined, 'HEAD', (err, res) => {
                    if (err) {
                        return done(err);
                    }
                    assert.strictEqual(res.statusCode, 301);
                    assert.strictEqual(res.headers.location,
                    redirectEndpoint);
                    return done();
                });
            });
    });

    describe(`with redirect all requests to ${redirectEndpoint}`, () => {
        beforeEach(() => {
            const redirectAllTo = {
                HostName: 'www.google.com',
            };
            const webConfig = new WebsiteConfigTester(null, null,
              redirectAllTo);
            return s3.putBucketWebsiteAsync({ Bucket: bucket,
                WebsiteConfiguration: webConfig })
            .then(() => s3.putObjectAsync({ Bucket: bucket,
                Key: 'index.html',
                ACL: 'public-read',
                Body: fs.readFileSync(path.join(__dirname,
                    '/websiteFiles/index.html')),
                ContentType: 'text/html',
                WebsiteRedirectLocation: '/redirect.html' }));
        });

        afterEach(() => bucketUtil.empty(bucket));

        it(`should redirect to ${redirectEndpoint} instead of ` +
        'x-amz-website-redirect-location header location on GET request',
        done => {
            browser.visit(endpoint, () => setTimeout(() => {
                WebsiteConfigTester.checkHTML(browser, '200',
                  redirectEndpoint);
                done();
            }, redirectWaitingPeriod));
        });

        it(`should redirect to ${redirectEndpoint} instead of ` +
        'x-amz-website-redirect-location header location on HEAD request',
            done => {
                _makeRequest(undefined, 'HEAD', (err, res) => {
                    if (err) {
                        return done(err);
                    }
                    assert.strictEqual(res.statusCode, 301);
                    assert.strictEqual(res.headers.location,
                    redirectEndpoint);
                    return done();
                });
            });
    });

    describe('with routing rule redirect to hostname with prefix condition',
    () => {
        beforeEach(() => {
            const webConfig = new WebsiteConfigTester('index.html');
            const condition = {
                KeyPrefixEquals: 'about/',
            };
            const redirect = {
                HostName: 'www.google.com',
            };
            webConfig.addRoutingRule(redirect, condition);
            return s3.putBucketWebsiteAsync({ Bucket: bucket,
                WebsiteConfiguration: webConfig })
            .then(() => s3.putObjectAsync({ Bucket: bucket,
                Key: 'about/index.html',
                ACL: 'public-read',
                Body: fs.readFileSync(path.join(__dirname,
                    '/websiteFiles/index.html')),
                ContentType: 'text/html',
                WebsiteRedirectLocation: '/redirect.html' }));
        });

        afterEach(() => bucketUtil.empty(bucket));

        it(`should redirect GET request to ${redirectEndpoint}about ` +
            'instead of about/ key x-amz-website-redirect-location ' +
            'header location', done => {
            _makeRequest('/about/', 'GET', (err, res) => {
                if (err) {
                    return done(err);
                }
                assert.strictEqual(res.statusCode, 301);
                assert.strictEqual(res.headers.location,
                `${redirectEndpoint}about/`);
                return done();
            });
        });

        it(`should redirect HEAD request to ${redirectEndpoint}about ` +
            'instead of about/ key x-amz-website-redirect-location ' +
            'header location', done => {
            _makeRequest('/about/', 'HEAD', (err, res) => {
                if (err) {
                    return done(err);
                }
                assert.strictEqual(res.statusCode, 301);
                assert.strictEqual(res.headers.location,
                `${redirectEndpoint}about/`);
                return done();
            });
        });
    });

    describe('with routing rule replaceKeyWith', () => {
        beforeEach(() => {
            const webConfig = new WebsiteConfigTester('index.html');
            const condition = {
                KeyPrefixEquals: 'index.html',
            };
            const redirect = {
                ReplaceKeyWith: 'redirect.html',
            };
            webConfig.addRoutingRule(redirect, condition);
            return s3.putBucketWebsiteAsync({ Bucket: bucket,
                WebsiteConfiguration: webConfig })
            .then(() => s3.putObjectAsync({ Bucket: bucket,
                Key: 'index.html',
                ACL: 'public-read',
                Body: fs.readFileSync(path.join(__dirname,
                    '/websiteFiles/index.html')),
                ContentType: 'text/html',
                WebsiteRedirectLocation: 'https://www.google.com' }))
            .then(() => s3.putObjectAsync({ Bucket: bucket,
                Key: 'redirect.html',
                ACL: 'public-read',
                Body: fs.readFileSync(path.join(__dirname,
                    '/websiteFiles/redirect.html')),
                ContentType: 'text/html' }));
        });

        afterEach(() => bucketUtil.empty(bucket));

        it('should replace key instead of redirecting to key ' +
        'x-amz-website-redirect-location header location on GET request',
        done => {
            browser.visit(`${endpoint}/index.html`, () => {
                WebsiteConfigTester.checkHTML(browser, 'redirect-user');
                done();
            });
        });

        it('should replace key instead of redirecting to key ' +
        'x-amz-website-redirect-location header location on HEAD request',
            done => {
                _makeRequest('/index.html', 'HEAD', (err, res) => {
                    if (err) {
                        return done(err);
                    }
                    assert.strictEqual(res.statusCode, 301);
                    assert.strictEqual(res.headers.location,
                    `${endpoint}/redirect.html`);
                    return done();
                });
            });
    });
});
