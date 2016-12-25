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
            } else if (type === 'redirect-user') {
                browser.assert.status(200);
                browser.assert.text('title', 'Best redirect link ever');
                browser.assert.text('h1', 'Welcome to your redirection file');
            } else {
                throw new Error('This is not checked in checkHTML()');
            }
        }
    }
}
