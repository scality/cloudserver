import assert from 'assert';

import utils from '../../../lib/utils';

describe('isValidBucketName utility', () => {
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
