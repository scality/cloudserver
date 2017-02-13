import async from 'async';
import fs from 'fs';
import path from 'path';

export class WebsiteConfigTester {
    constructor(indexDocument, errorDocument, redirectAllReqTo) {
        if (indexDocument) {
            this.IndexDocument = {};
            this.IndexDocument.Suffix = indexDocument;
        }
        if (errorDocument) {
            this.ErrorDocument = {};
            this.ErrorDocument.Key = errorDocument;
        }
        if (redirectAllReqTo) {
            this.RedirectAllRequestsTo = redirectAllReqTo;
        }
    }
    addRoutingRule(redirectParams, conditionParams) {
        const newRule = {};
        if (!this.RoutingRules) {
            this.RoutingRules = [];
        }
        if (redirectParams) {
            newRule.Redirect = {};
            Object.keys(redirectParams).forEach(key => {
                newRule.Redirect[key] = redirectParams[key];
            });
        }
        if (conditionParams) {
            newRule.Condition = {};
            Object.keys(conditionParams).forEach(key => {
                newRule.Condition[key] = conditionParams[key];
            });
        }
        this.RoutingRules.push(newRule);
    }

    static checkHTML(browser, type, url, bucketName) {
        // 404 error
        if (url) {
            browser.assert.url(url);
        }
        if (type) {
            if (type.startsWith('404')) {
                browser.assert.status(404);
                browser.assert.text('title', '404 Not Found');
                browser.assert.text('h1', '404 Not Found');
                // ul section
                if (type === '404-no-such-bucket') {
                    browser.assert.text('ul:first-of-type > li:first-child',
                    'Code: NoSuchBucket');
                    browser.assert.text('ul:first-of-type > li:nth-child(2)',
                    'Message: The specified bucket does not exist.');
                    browser.assert.text('ul:first-of-type > li:nth-child(3)',
                    `BucketName: ${bucketName}`);
                } else if (type === '404-no-such-website-configuration') {
                    browser.assert.text('ul:first-of-type > li:first-child',
                    'Code: NoSuchWebsiteConfiguration');
                    browser.assert.text('ul:first-of-type > li:nth-child(2)',
                    'Message: The specified bucket does not have a website ' +
                    'configuration');
                    browser.assert.text('ul:first-of-type > li:nth-child(3)',
                    `BucketName: ${bucketName}`);
                } else if (type === '404-not-found') {
                    browser.assert.text('ul:first-of-type > li:first-child',
                    'Code: NoSuchKey');
                    browser.assert.text('ul:first-of-type > li:nth-child(2)',
                    'Message: The specified key does not exist.');
                } else {
                    throw new Error('This 404 error is not checked in ' +
                    'checkHTML()');
                }
            // 403 error
            } else if (type.startsWith('403')) {
                browser.assert.status(403);
                browser.assert.text('title', '403 Forbidden');
                browser.assert.text('h1', '403 Forbidden');
                if (type === '403-access-denied') {
                    browser.assert.text('ul:first-of-type > li:first-child',
                    'Code: AccessDenied');
                    browser.assert.text('ul:first-of-type > li:nth-child(2)',
                    'Message: Access Denied');
                } else if (type === '403-retrieve-error-document') {
                    browser.assert.text('ul:first-of-type > li:first-child',
                    'Code: AccessDenied');
                    browser.assert.text('ul:first-of-type > li:nth-child(2)',
                    'Message: Access Denied');
                    browser.assert.text('h3', 'An Error Occurred While ' +
                    'Attempting to Retrieve a Custom Error Document');
                    browser.assert.text('ul:nth-of-type(2) > li:first-child',
                    'Code: AccessDenied');
                    browser.assert.text('ul:nth-of-type(2) > li:nth-child(2)',
                    'Message: Access Denied');
                } else {
                    throw new Error('This 403 error is not checked in ' +
                    'checkHTML()');
                }
            } else if (type === 'index-user') {
                browser.assert.status(200);
                browser.assert.text('title',
                    'Best testing website ever');
                browser.assert.text('h1', 'Welcome to my ' +
                    'extraordinary bucket website testing page');
            } else if (type === '200') {
                browser.assert.status(200);
            } else if (type === 'error-user') {
                browser.assert.status(403);
                browser.assert.text('title', 'Error!!');
                browser.assert.text('h1', 'It appears you messed up');
            } else if (type === 'error-user-404') {
                browser.assert.status(404);
                browser.assert.text('title', 'Error!!');
                browser.assert.text('h1', 'It appears you messed up');
            } else if (type === 'redirect-user') {
                browser.assert.status(200);
                browser.assert.text('title', 'Best redirect link ever');
                browser.assert.text('h1', 'Welcome to your redirection file');
            } else {
                throw new Error('This is not checked in checkHTML()');
            }
        }
    }
    static createPutBucketWebsite(s3, bucket, bucketACL, objects, done) {
        s3.createBucket({ Bucket: bucket, ACL: bucketACL },
        err => {
            if (err) {
                return done(err);
            }
            const webConfig = new WebsiteConfigTester('index.html',
              'error.html');
            return s3.putBucketWebsite({ Bucket: bucket,
            WebsiteConfiguration: webConfig }, err => {
                if (err) {
                    return done(err);
                }
                return async.forEachOf(objects,
                (acl, object, next) => {
                    s3.putObject({ Bucket: bucket,
                        Key: `${object}.html`,
                        ACL: acl,
                        Body: fs.readFileSync(path.join(__dirname,
                            `/../../test/object/websiteFiles/${object}.html`)),
                        },
                        next);
                }, done);
            });
        });
    }

    static deleteObjectsThenBucket(s3, bucket, objects, done) {
        async.forEachOf(objects, (acl, object, next) => {
            s3.deleteObject({ Bucket: bucket,
                Key: `${object}.html` }, next);
        }, err => {
            if (err) {
                return done(err);
            }
            return s3.deleteBucket({ Bucket: bucket }, done);
        });
    }
}
