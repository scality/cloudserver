const assert = require('assert');

const { bucketPut } = require('../../../lib/api/bucketPut');
const { cleanup,
    DummyRequestLogger,
    makeAuthInfo,
    TaggingConfigTester }
    = require('../helpers');
const bucketPutTagging = require('../../../lib/api/bucketPutTagging');
const bucketGetTagging = require('../../../lib/api/bucketGetTagging');
const bucketDeleteTagging = require('../../../lib/api/bucketDeleteTagging');
const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketGetTaggingTest';

const testBucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
    actionImplicitDenies: false,
};

describe('deleteBucketTagging API', () => {
    beforeEach(done => {
        cleanup();
        bucketPut(authInfo, testBucketPutRequest, log, done);
    });

    afterEach(() => cleanup());

    it('should delete tags resource', done => {
        const taggingUtil = new TaggingConfigTester();
        const testBucketPutTaggingRequest = taggingUtil
            .createBucketTaggingRequest('PUT', bucketName);
        bucketPutTagging(authInfo, testBucketPutTaggingRequest, log, err => {
            assert.strictEqual(err, undefined);

            const testBucketGetTaggingRequest = taggingUtil
                .createBucketTaggingRequest('GET', bucketName);
            return bucketGetTagging(authInfo, testBucketGetTaggingRequest, log,
                (err, xml) => {
                    assert.ifError(err);
                    assert.strictEqual(xml, taggingUtil.constructXml());
                    const testBucketDeleteTaggingRequest = taggingUtil
                        .createBucketTaggingRequest('DELETE', bucketName);
                    return bucketDeleteTagging(authInfo, testBucketDeleteTaggingRequest,
                        log, err => {
                            assert.ifError(err);
                            return bucketGetTagging(authInfo, testBucketGetTaggingRequest,
                                log, err => {
                                    assert(err.NoSuchTagSet);
                                    return done();
                                });
                        });
                });
        });
    });

    it('should return access denied if the authorization check fails', done => {
        const taggingUtil = new TaggingConfigTester();
        const testBucketPutTaggingRequest = taggingUtil
            .createBucketTaggingRequest('PUT', bucketName);
        bucketPutTagging(authInfo, testBucketPutTaggingRequest, log, err => {
            assert.ifError(err);
            const testBucketDeleteTaggingRequest = taggingUtil
                .createBucketTaggingRequest('DELETE', bucketName, null, true);
            return bucketDeleteTagging(authInfo, testBucketDeleteTaggingRequest,
                log, err => {
                    assert(err.AccessDenied);
                    return done();
                });
        });
    });
});
