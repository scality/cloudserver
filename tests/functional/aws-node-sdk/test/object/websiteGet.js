import assert from 'assert';
import fs from 'fs';
import path from 'path';

import { S3 } from 'aws-sdk';

import conf from '../../../../../lib/Config';
import getConfig from '../support/config';
import makeRequest from '../../../raw-node/utils/makeRequest';
import { WebsiteConfigTester } from '../../lib/utility/website-util';

const config = getConfig('default', { signatureVersion: 'v4' });
const s3 = new S3(config);

const transport = conf.https ? 'https' : 'http';
const bucket = process.env.AWS_ON_AIR ? 'awsbucketwebsitetester' :
    'bucketwebsitetester';
const hostname = `${bucket}.s3-website-us-east-1.amazonaws.com`;
const endpoint = process.env.AWS_ON_AIR ? `${transport}://${hostname}` :
    `${transport}://${hostname}:8000`;
const redirectEndpoint = conf.https ? 'https://www.google.com' :
    'http://www.google.com';


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

// Note: To run these tests locally, you may need to edit the /etc/hosts file
// to include `127.0.0.1 bucketwebsitetester.s3-website-us-east-1.amazonaws.com`
describe('User visits bucket website endpoint', () => {
    it('should return 404 when no such bucket', done => {
        WebsiteConfigTester.checkHTML('GET', endpoint, '404-no-such-bucket',
        null, bucket, done);
    });

    describe('with existing bucket', () => {
        beforeEach(done => s3.createBucket({ Bucket: bucket }, done));

        afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

        it('should return 404 when no website configuration', done => {
            WebsiteConfigTester.checkHTML('GET', endpoint,
            '404-no-such-website-configuration', null, bucket, done);
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
                    port: process.env.AWS_ON_AIR ? 80 : 8000,
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
                WebsiteConfigTester.checkHTML('GET', endpoint, 'index-user',
                null, null, done);
            });
            it('should serve indexDocument if key requested', done => {
                WebsiteConfigTester.checkHTML('GET', `${endpoint}/index.html`,
                'index-user', null, null, done);
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
                WebsiteConfigTester.checkHTML('GET', `${endpoint}/pathprefix/`,
                'index-user', null, null, done);
            });

            it('should serve indexDocument if path request with key',
            done => {
                WebsiteConfigTester.checkHTML('GET',
                `${endpoint}/pathprefix/index.html`,
                'index-user', null, null, done);
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
                WebsiteConfigTester.checkHTML('GET', endpoint,
                '403-access-denied', null, null, done);
            });
        });

        describe('with nonexisting index document key', () => {
            beforeEach(done => {
                const webConfig = new WebsiteConfigTester('index.html');
                s3.putBucketWebsite({ Bucket: bucket,
                    WebsiteConfiguration: webConfig }, done);
            });

            it('should return 403 if nonexisting index document key', done => {
                WebsiteConfigTester.checkHTML('GET', endpoint,
                '403-access-denied', null, null, done);
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
                WebsiteConfigTester.checkHTML('GET', endpoint, 'redirect',
                `${redirectEndpoint}/`, null, done);
            });

            it(`should redirect to ${redirectEndpoint}/about`, done => {
                WebsiteConfigTester.checkHTML('GET', `${endpoint}/about`,
                'redirect', `${redirectEndpoint}/about`, null, done);
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
                WebsiteConfigTester.checkHTML('GET', endpoint, 'redirect',
                'https://www.google.com/', null, done);
            });

            it('should redirect to https://google.com/about', done => {
                WebsiteConfigTester.checkHTML('GET', `${endpoint}/about`,
                'redirect', 'https://www.google.com/about', null, done);
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
                WebsiteConfigTester.checkHTML('GET', endpoint, 'error-user',
                null, null, done);
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
                WebsiteConfigTester.checkHTML('GET', endpoint,
                '403-retrieve-error-document', null, null, done);
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
                WebsiteConfigTester.checkHTML('GET', endpoint,
                'redirect', `${redirectEndpoint}/`, null, done);
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
                WebsiteConfigTester.checkHTML('GET', `${endpoint}/about/`,
                'redirect', `${redirectEndpoint}/about/`, null, done);
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
                WebsiteConfigTester.checkHTML('GET', `${endpoint}/about/`,
                'redirect', `${redirectEndpoint}/about/`, null, done);
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
                WebsiteConfigTester.checkHTML('GET', `${endpoint}/about/`,
                'redirect', `${redirectEndpoint}/about/`, null, done);
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
                WebsiteConfigTester.checkHTML('GET', `${endpoint}/about/`,
                'redirect', 'https://www.google.com/about/', null, done);
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
                WebsiteConfigTester.checkHTML('GET', endpoint, 'redirect-user',
                `${endpoint}/redirect.html`, null, done);
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
            'ReplaceKeyPrefixWith equals "about"', done => {
                WebsiteConfigTester.checkHTML('GET', endpoint, 'redirect',
                `${redirectEndpoint}/about/`, null, done);
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
                WebsiteConfigTester.checkHTML('GET', `${endpoint}/about/`,
                'redirect-user', `${endpoint}/redirect/`, null, done);
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
                WebsiteConfigTester.checkHTML('GET', `${endpoint}/about/`,
                'redirect-user', `${endpoint}/redirect/`, null, done);
            });
        });
    });
});
