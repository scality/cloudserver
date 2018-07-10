const fs = require('fs');
const path = require('path');

const BucketUtility = require('../../lib/utility/bucket-util');
const conf = require('../../../../../lib/Config').config;
const getConfig = require('../support/config');
const { WebsiteConfigTester } = require('../../lib/utility/website-util');

const config = getConfig('default', { signatureVersion: 'v4' });
const bucketUtil = new BucketUtility('default', config);
const s3 = bucketUtil.s3;

// Note: To run these tests locally, you may need to edit the machine's
// /etc/hosts file to include the following line:
// `127.0.0.1 bucketwebsitetester.s3-website-us-east-1.amazonaws.com`

const transport = conf.https ? 'https' : 'http';
const bucket = process.env.AWS_ON_AIR ? 'awsbucketwebsitetester' :
    'bucketwebsitetester';
const hostname = process.env.S3_END_TO_END ?
    `${bucket}.s3-website-us-east-1.scality.com` :
    `${bucket}.s3-website-us-east-1.amazonaws.com`;
const endpoint = process.env.AWS_ON_AIR ? `${transport}://${hostname}` :
    `${transport}://${hostname}:8000`;
const redirectEndpoint = conf.https ? 'https://www.google.com/' :
    'http://www.google.com/';

describe('User visits bucket website endpoint and requests resource ' +
'that has x-amz-website-redirect-location header ::', () => {
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
            WebsiteConfigTester.checkHTML({
                method: 'GET',
                url: endpoint,
                responseType: 'redirect',
                redirectUrl: '/redirect.html',
            }, done);
        });

        it('should redirect to redirect file on HEAD request', done => {
            WebsiteConfigTester.checkHTML({
                method: 'HEAD',
                url: endpoint,
                responseType: 'redirect',
                redirectUrl: '/redirect.html',
            }, done);
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
            WebsiteConfigTester.checkHTML({
                method: 'GET',
                url: endpoint,
                responseType: 'redirect',
                redirectUrl: 'https://www.google.com',
            }, done);
        });

        it('should redirect to https://www.google.com on HEAD request',
            done => {
                WebsiteConfigTester.checkHTML({
                    method: 'HEAD',
                    url: endpoint,
                    responseType: 'redirect',
                    redirectUrl: 'https://www.google.com',
                }, done);
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
            WebsiteConfigTester.checkHTML({
                method: 'GET',
                url: endpoint,
                responseType: '403-access-denied',
            }, done);
        });

        it('should return 403 instead of x-amz-website-redirect-location ' +
        'header location on HEAD request', done => {
            WebsiteConfigTester.checkHTML({
                method: 'HEAD',
                url: endpoint,
                responseType: '403-access-denied',
            }, done);
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
            WebsiteConfigTester.checkHTML({
                method: 'GET',
                url: endpoint,
                responseType: 'redirect',
                redirectUrl: redirectEndpoint,
            }, done);
        });

        it(`should redirect to ${redirectEndpoint} since error 403 ` +
        'occurred instead of x-amz-website-redirect-location header ' +
        'location on HEAD request',
        done => {
            WebsiteConfigTester.checkHTML({
                method: 'HEAD',
                url: endpoint,
                responseType: 'redirect',
                redirectUrl: redirectEndpoint,
            }, done);
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
            WebsiteConfigTester.checkHTML({
                method: 'GET',
                url: endpoint,
                responseType: 'redirect',
                redirectUrl: redirectEndpoint,
            }, done);
        });

        it(`should redirect to ${redirectEndpoint} instead of ` +
        'x-amz-website-redirect-location header location on HEAD request',
        done => {
            WebsiteConfigTester.checkHTML({
                method: 'HEAD',
                url: endpoint,
                responseType: 'redirect',
                redirectUrl: redirectEndpoint,
            }, done);
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

        it(`should redirect GET request to ${redirectEndpoint}about/ ` +
            'instead of about/ key x-amz-website-redirect-location ' +
            'header location', done => {
            WebsiteConfigTester.checkHTML({
                method: 'GET',
                url: `${endpoint}/about/`,
                responseType: 'redirect',
                redirectUrl: `${redirectEndpoint}about/`,
            }, done);
        });

        it(`should redirect HEAD request to ${redirectEndpoint}about ` +
            'instead of about/ key x-amz-website-redirect-location ' +
            'header location', done => {
            WebsiteConfigTester.checkHTML({
                method: 'HEAD',
                url: `${endpoint}/about/`,
                responseType: 'redirect',
                redirectUrl: `${redirectEndpoint}about/`,
            }, done);
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
            WebsiteConfigTester.checkHTML({
                method: 'GET',
                url: `${endpoint}/index.html`,
                responseType: 'redirect-user',
                redirectUrl: `${endpoint}/redirect.html`,
            }, done);
        });

        it('should replace key instead of redirecting to key ' +
        'x-amz-website-redirect-location header location on HEAD request',
            done => {
                WebsiteConfigTester.checkHTML({
                    method: 'HEAD',
                    url: `${endpoint}/index.html`,
                    responseType: 'redirect-user',
                    redirectUrl: `${endpoint}/redirect.html`,
                }, done);
            });
    });
});
