import assert from 'assert';

import utils from '../../../lib/utils';

const bucketName = 'bucketname';
const objName = 'testObject';

describe('normalizeRequest utility', () => {
    it('should parse bucket name from path', () => {
        const request = {
            url: `/${bucketName}`,
            headers: {host: `s3.amazonaws.com`},
        };
        const result = utils.normalizeRequest(request);
        assert.strictEqual(result.bucketName, bucketName);
        assert.strictEqual(result.parsedHost, 's3.amazonaws.com');
    });

    it('should parse bucket name from host', () => {
        const request = {
            url: '/',
            headers: {host: `${bucketName}.s3.amazonaws.com`},
        };
        const result = utils.normalizeRequest(request);
        assert.strictEqual(result.bucketName, bucketName);
        assert.strictEqual(result.parsedHost, 's3.amazonaws.com');
    });

    it('should parse bucket and object name from path', () => {
        const request = {
            url: `/${bucketName}/${objName}`,
            headers: {host: `s3.amazonaws.com`},
        };
        const result = utils.normalizeRequest(request);
        assert.strictEqual(result.bucketName, bucketName);
        assert.strictEqual(result.objectKey, objName);
        assert.strictEqual(result.parsedHost, 's3.amazonaws.com');
    });

    it('should parse bucket name from host ' +
        'and object name from path', () => {
        const request = {
            url: `/${objName}`,
            headers: {host: `${bucketName}.s3.amazonaws.com`},
        };
        const result = utils.normalizeRequest(request);
        assert.strictEqual(result.bucketName, bucketName);
        assert.strictEqual(result.objectKey, objName);
        assert.strictEqual(result.parsedHost, 's3.amazonaws.com');
    });
});
