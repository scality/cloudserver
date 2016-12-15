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

// <------ *404-no-such-bucket* ------>

// <html>
// <head><title>404 Not Found</title></head>
// <body>
// <h1>404 Not Found</h1>
// <ul>
// <li>Code: NoSuchBucket</li>
// <li>Message: The specified bucket does not exist</li>
// <li>BucketName: mybucketwebsite</li>
// <li>RequestId: F412857B3F6BF85C</li>
// <li>HostId: 9MSmu0rhfRamKVTX3kWdoCKW/w4uRZ4XhGax+W/grmBt/...=</li>
// </ul>
// <hr/>
// </body>
// </html>


// <------ *404-no-such-website-configuration* ------>

// <html>
//     <head>
//         <title>404 Not Found</title>
//     </head>
//     <body>
//         <h1>404 Not Found</h1>
//         <ul>
//             <li>Code: NoSuchWebsiteConfiguration</li>
//             <li>Message: The specified bucket does not have a website
//             configuration</li>
//             <li>BucketName: mybucketwebsite</li>
//             <li>RequestId: 9C6F2197F2EC7CC9</li>
//             <li>HostId: 3XfU0YUgXWo4KdTarTojbuy9h4ZhCyIuD+aucW4...</li>
//         </ul>
//         <hr/>
//     </body>
// </html>

// <------  *403-access-denied* ------>

// <html>
// <head><title>403 Forbidden</title></head>
// <body>
// <h1>403 Forbidden</h1>
// <ul>
// <li>Code: AccessDenied</li>
// <li>Message: Access Denied</li>
// <li>RequestId: 33C9B5C2192A9641</li>
// <li>HostId: rJFQetB8DrneRJF813B3YQuFmjieU+wHzMi7vRKNiL7dVZIP...</li>
// </ul>
// <hr/>
// </body>
// </html>

// <------  *403-retrieve-error-document* ------>

// <html>
// <head><title>403 Forbidden</title></head>
// <body>
// <h1>403 Forbidden</h1>
// <ul>
// <li>Code: AccessDenied</li>
// <li>Message: Access Denied</li>
// <li>RequestId: 47D87AA0ABFA2014</li>
// <li>HostId: +WscOLrdOB9JqB2ohkMShZhkR+Q7DGSLP8Pc+h</li>
// </ul>
// <h3>An Error Occurred While Attempting to Retrieve a Custom Error
// Document</h3>
// <ul>
// <li>Code: AccessDenied</li>
// <li>Message: Access Denied</li>
// </ul>
// <hr/>
// </body>
// </html>
