import fs from 'fs';
import path from 'path';

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
            WebsiteConfigTester.checkHTML('GET', endpoint, 'redirect',
            '/redirect.html', null, done);
        });

        it('should redirect to redirect file on HEAD request', done => {
            WebsiteConfigTester.checkHTML('HEAD', endpoint, 'redirect',
            '/redirect.html', null, done);
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
            WebsiteConfigTester.checkHTML('GET', endpoint, 'redirect',
            'https://www.google.com', null, done);
        });

        it('should redirect to https://www.google.com on HEAD request',
            done => {
                WebsiteConfigTester.checkHTML('HEAD', endpoint, 'redirect',
                'https://www.google.com', null, done);
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
            WebsiteConfigTester.checkHTML('GET', endpoint, '403-access-denied',
            null, null, done);
        });

        it('should return 403 instead of x-amz-website-redirect-location ' +
        'header location on HEAD request', done => {
            WebsiteConfigTester.checkHTML('HEAD', endpoint, '403-access-denied',
            null, null, done);
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
            WebsiteConfigTester.checkHTML('GET', endpoint, 'redirect',
            redirectEndpoint, null, done);
        });

        it(`should redirect to ${redirectEndpoint} since error 403 ` +
        'occurred instead of x-amz-website-redirect-location header ' +
        'location on HEAD request',
        done => {
            WebsiteConfigTester.checkHTML('HEAD', endpoint, 'redirect',
            redirectEndpoint, null, done);
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
            WebsiteConfigTester.checkHTML('GET', endpoint, 'redirect',
            redirectEndpoint, null, done);
        });

        it(`should redirect to ${redirectEndpoint} instead of ` +
        'x-amz-website-redirect-location header location on HEAD request',
        done => {
            WebsiteConfigTester.checkHTML('HEAD', endpoint, 'redirect',
            redirectEndpoint, null, done);
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
            WebsiteConfigTester.checkHTML('GET', `${endpoint}/about/`,
            'redirect', `${redirectEndpoint}about/`, null, done);
        });

        it(`should redirect HEAD request to ${redirectEndpoint}about ` +
            'instead of about/ key x-amz-website-redirect-location ' +
            'header location', done => {
            WebsiteConfigTester.checkHTML('HEAD', `${endpoint}/about/`,
            'redirect', `${redirectEndpoint}about/`, null, done);
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
            WebsiteConfigTester.checkHTML('GET', `${endpoint}/index.html`,
            'redirect-user', `${endpoint}/redirect.html`, null, done);
        });

        it('should replace key instead of redirecting to key ' +
        'x-amz-website-redirect-location header location on HEAD request',
            done => {
                WebsiteConfigTester.checkHTML('HEAD', `${endpoint}/index.html`,
                'redirect-user', `${endpoint}/redirect.html`, null, done);
            });
    });
});
