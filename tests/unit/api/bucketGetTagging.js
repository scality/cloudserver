const assert = require('assert');

const { bucketPut } = require('../../../lib/api/bucketPut');
const { cleanup,
    DummyRequestLogger,
    makeAuthInfo,
    TaggingConfigTester,
} = require('../helpers');
const bucketPutTagging = require('../../../lib/api/bucketPutTagging');
const bucketGetTagging = require('../../../lib/api/bucketGetTagging');
const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketGetTaggingTest';

const testBucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
    actionImplicitDenies: false,
};

describe('getBucketTagging API', () => {
    beforeEach(done => {
        cleanup();
        bucketPut(authInfo, testBucketPutRequest, log, done);
    });

    afterEach(() => cleanup());

    it('should return tags resource', done => {
        const taggingUtil = new TaggingConfigTester();
        const testBucketPutTaggingRequest = taggingUtil
            .createBucketTaggingRequest('PUT', bucketName);
        bucketPutTagging(authInfo, testBucketPutTaggingRequest, log, err => {
            assert.strictEqual(err, undefined);
            const testBucketGetTaggingRequest = taggingUtil
                .createBucketTaggingRequest('GET', bucketName);
            return bucketGetTagging(authInfo, testBucketGetTaggingRequest, log,
            (err, xml) => {
                if (err) {
                    process.stdout.write(`Err getting object tagging ${err}`);
                    return done(err);
                }
                assert.strictEqual(xml, taggingUtil.constructXml());
                return done();
            });
        });
    });

    it('should return access denied if the authorization check fails', done => {
        const taggingUtil = new TaggingConfigTester();
        const testBucketPutTaggingRequest = taggingUtil
            .createBucketTaggingRequest('PUT', bucketName);
        bucketPutTagging(authInfo, testBucketPutTaggingRequest, log, err => {
            assert.strictEqual(err, undefined);
            const testBucketGetTaggingRequest = taggingUtil
                .createBucketTaggingRequest('GET', bucketName, true);
            const badAuthInfo = makeAuthInfo('accessKey2');
            return bucketGetTagging(badAuthInfo, testBucketGetTaggingRequest, log,
            err => {
                assert.strictEqual(err.AccessDenied, true);
                return done();
            });
        });
    });
});

