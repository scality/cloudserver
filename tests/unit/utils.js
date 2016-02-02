import assert from 'assert';

import utils from '../../lib/utils';

describe('utils.isValidBucketName', () => {
    it('should return false if bucketname is fewer than ' +
        '3 characters long', () => {
        const result = utils.isValidBucketName('no');
        assert.strictEqual(result, false);
    });

    it('should return false if bucketname is greater than ' +
        '63 characters long', () => {
        const longString = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' +
            'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
        const result = utils.isValidBucketName(longString);
        assert.strictEqual(result, false);
    });

    it('should return false if bucketname contains ' +
        'capital letters', () => {
        const result = utils.isValidBucketName('noSHOUTING');
        assert.strictEqual(result, false);
    });

    it('should return false if bucketname is an IP address', () => {
        const result = utils.isValidBucketName('172.16.254.1');
        assert.strictEqual(result, false);
    });

    it('should return false if bucketname is not DNS compatible', () => {
        const result = utils.isValidBucketName('*notvalid*');
        assert.strictEqual(result, false);
    });

    it('should return true if bucketname does not break rules', () => {
        const result = utils.isValidBucketName('okay');
        assert.strictEqual(result, true);
    });
});

const bucketName = 'bucketname';
const objName = 'testObject';

describe('utils.normalizeRequest', () => {
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
