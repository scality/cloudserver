const assert = require('assert');
const collectResponseHeaders =
    require('../../../lib/utilities/collectResponseHeaders');

describe('Middleware: Collect Response Headers', () => {
    it('should return an undefined value when x-amz-website-redirect-location' +
       ' is empty', () => {
        const objectMD = { 'x-amz-website-redirect-location': '' };
        const headers = collectResponseHeaders(objectMD);
        assert.strictEqual(headers['x-amz-website-redirect-location'],
          undefined);
    });

    it('should return the (nonempty) value of WebsiteRedirectLocation', () => {
        const obj = { 'x-amz-website-redirect-location': 'google.com' };
        const headers = collectResponseHeaders(obj);
        assert.strictEqual(headers['x-amz-website-redirect-location'],
            'google.com');
    });
});
