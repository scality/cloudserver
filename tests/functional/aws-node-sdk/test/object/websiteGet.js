import assert from 'assert';
import fs from 'fs';
import http from 'http';
import path from 'path';

import { S3 } from 'aws-sdk';
import Browser from 'zombie';

import conf from '../../../../../lib/Config';
import getConfig from '../support/config';
import { WebsiteConfigTester } from '../../lib/utility/website-util';

const transport = conf.https ? 'https' : 'http';
const bucket = 'bucketwebsitetester';
const hostname = `${bucket}.s3-website-us-east-1.amazonaws.com`;

const endpoint = `${transport}://${hostname}:8000`;

// TODO: Add this endpoint in Integration for CI

describe('User visits bucket website endpoint', () => {
    const browser = new Browser();

    it('should return 405 when user requests method other than get or head',
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
            browser.assert.status(404);
            browser.assert.text('title', '404 Not Found');
            browser.assert.text('h1', '404 Not Found');
            browser.assert.element('#code');
            browser.assert.text('#code', 'Code: NoSuchBucket');
            browser.assert.text('#message',
                'Message: The specified bucket does not exist.');
            done();
        });
    });

    describe('with existing bucket', () => {
        let s3;
        beforeEach(done => {
            const config = getConfig('default', { signatureVersion: 'v4' });
            s3 = new S3(config);
            s3.createBucket({ Bucket: bucket }, err => done(err));
        });

        afterEach(done => {
            s3.deleteBucket({ Bucket: bucket }, err => done(err));
        });

        it('should return 404 when no website configuration', done => {
            browser.visit(endpoint, () => {
                browser.assert.status(404);
                browser.assert.text('title', '404 Not Found');
                browser.assert.text('h1', '404 Not Found');
                browser.assert.text('#code',
                    'Code: NoSuchWebsiteConfiguration');
                browser.assert.text('#message',
                    'Message: The specified bucket does not ' +
                    'have a website configuration');
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
                    browser.assert.status(200);
                    browser.assert.text('title',
                        'Best testing website ever');
                    browser.assert.text('h1', 'Welcome to my ' +
                        'extraordinary bucket website testing page');
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
// 18) redirect with hostname specified
// 19) reirect with replaceKeyWith specified
// 20) redirect with replaceKeyPrefixWith specified
// 21) redirect with httpRedirect Code specified
// 22) redirect with combination of redirect items applicable
