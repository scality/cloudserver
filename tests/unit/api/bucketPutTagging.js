const assert = require('assert');

const { bucketPut } = require('../../../lib/api/bucketPut');
const { cleanup,
    DummyRequestLogger,
    makeAuthInfo,
    TaggingConfigTester,
} = require('../helpers');
const bucketPutTagging = require('../../../lib/api/bucketPutTagging');
const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketGetTaggingTest';

const testBucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
    actionImplicitDenies: false,
};

describe('putBucketTagging API', () => {
    beforeEach(done => {
        cleanup();
        bucketPut(authInfo, testBucketPutRequest, log, done);
    });

    afterEach(() => cleanup());

    it('should set tags resource', done => {
        const taggingUtil = new TaggingConfigTester();
        const testBucketPutTaggingRequest = taggingUtil
            .createBucketTaggingRequest('PUT', bucketName);
        bucketPutTagging(authInfo, testBucketPutTaggingRequest, log, err => {
            if (err) {
                process.stdout.write(`Err putting object tagging ${err}`);
                return done(err);
            }
            assert.strictEqual(err, undefined);
            return done();
        });
    });

    it('should return access denied if the authorization check fails', done => {
        const taggingUtil = new TaggingConfigTester();
        const testBucketPutTaggingRequest = taggingUtil
            .createBucketTaggingRequest('PUT', bucketName);
        const authInfo = makeAuthInfo('accessKey2');
        bucketPutTagging(authInfo, testBucketPutTaggingRequest, log, err => {
            assert(err.AccessDenied);
            return done();
        });
    });
});
