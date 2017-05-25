const assert = require('assert');
const async = require('async');

const { bucketPut } = require('../../../lib/api/bucketPut');
const objectPut = require('../../../lib/api/objectPut');
const objectPutTagging = require('../../../lib/api/objectPutTagging');
const objectDeleteTagging = require('../../../lib/api/objectDeleteTagging');
const metadata = require('../../../lib/metadata/wrapper');
const { cleanup,
    DummyRequestLogger,
    makeAuthInfo,
    TaggingConfigTester } = require('../helpers');
const DummyRequest = require('../DummyRequest');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const namespace = 'default';
const bucketName = 'bucketname';
const objectName = 'objectName';
const postBody = Buffer.from('I am a body', 'utf8');
const testBucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
};

const testPutObjectRequest = new DummyRequest({
    bucketName,
    namespace,
    objectKey: objectName,
    headers: {},
    url: `/${bucketName}/${objectName}`,
}, postBody);

describe('deleteObjectTagging API', () => {
    beforeEach(done => {
        cleanup();
        bucketPut(authInfo, testBucketPutRequest, log, err => {
            if (err) {
                return done(err);
            }
            return objectPut(authInfo, testPutObjectRequest, undefined, log,
              done);
        });
    });

    afterEach(() => cleanup());

    it('should delete tag set', done => {
        const taggingUtil = new TaggingConfigTester();
        const testObjectPutTaggingRequest = taggingUtil
            .createObjectTaggingRequest('PUT', bucketName, objectName);
        const testObjectDeleteTaggingRequest = taggingUtil
            .createObjectTaggingRequest('DELETE', bucketName, objectName);
        async.waterfall([
            next => objectPutTagging(authInfo, testObjectPutTaggingRequest, log,
              err => next(err)),
            next => objectDeleteTagging(authInfo,
              testObjectDeleteTaggingRequest, log, err => next(err)),
            next => metadata.getObjectMD(bucketName, objectName, {}, log,
            (err, objectMD) => next(err, objectMD)),
        ], (err, objectMD) => {
            const uploadedTags = objectMD.tags;
            assert.deepStrictEqual(uploadedTags, {});
            return done();
        });
    });
});
