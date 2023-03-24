const assert = require('assert');

const { models } = require('arsenal');
const { BucketInfo } = models;
const { DummyRequestLogger, makeAuthInfo } = require('../helpers');

const creationDate = new Date().toJSON();
const authInfo = makeAuthInfo('accessKey');
const otherAuthInfo = makeAuthInfo('otherAccessKey');
const ownerCanonicalId = authInfo.getCanonicalID();

const bucket = new BucketInfo('niftyBucket', ownerCanonicalId,
    authInfo.getAccountDisplayName(), creationDate);
const log = new DummyRequestLogger();

const { validateBucket } = require('../../../lib/metadata/metadataUtils');

describe('validateBucket', () => {
    it('action bucketPutPolicy by bucket owner', () => {
        const validationResult = validateBucket(bucket, {
            authInfo,
            requestType: 'bucketPutPolicy',
            request: null,
        }, log);
        assert.ifError(validationResult);
    });
    it('action bucketPutPolicy by other than bucket owner', () => {
        const validationResult = validateBucket(bucket, {
            authInfo: otherAuthInfo,
            requestType: 'bucketPutPolicy',
            request: null,
        }, log);
        assert(validationResult);
        assert(validationResult.is.MethodNotAllowed);
    });

    it('action bucketGet by bucket owner', () => {
        const validationResult = validateBucket(bucket, {
            authInfo,
            requestType: 'bucketGet',
            request: null,
        }, log);
        assert.ifError(validationResult);
    });

    it('action bucketGet by other than bucket owner', () => {
        const validationResult = validateBucket(bucket, {
            authInfo: otherAuthInfo,
            requestType: 'bucketGet',
            request: null,
        }, log);
        assert(validationResult);
        assert(validationResult.is.AccessDenied);
    });
});
