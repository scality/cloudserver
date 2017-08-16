const assert = require('assert');

const { bucketPut } = require('../../../lib/api/bucketPut');
const objectPut = require('../../../lib/api/objectPut');
const objectPutTagging = require('../../../lib/api/objectPutTagging');
const objectGetTagging = require('../../../lib/api/objectGetTagging');
const { cleanup,
    DummyRequestLogger,
    makeAuthInfo,
    TaggingConfigTester }
    = require('../helpers');
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

describe('getObjectTagging API', () => {
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

    it('should return tags resource', done => {
        const taggingUtil = new TaggingConfigTester();
        const testObjectPutTaggingRequest = taggingUtil
            .createObjectTaggingRequest('PUT', bucketName, objectName);
        objectPutTagging(authInfo, testObjectPutTaggingRequest, log, err => {
            if (err) {
                process.stdout.write(`Err putting object tagging ${err}`);
                return done(err);
            }
            const testObjectGetTaggingRequest = taggingUtil
                .createObjectTaggingRequest('GET', bucketName, objectName);
            return objectGetTagging(authInfo, testObjectGetTaggingRequest, log,
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
});
