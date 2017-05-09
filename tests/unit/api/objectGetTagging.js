import assert from 'assert';

import bucketPut from '../../../lib/api/bucketPut';
import objectPut from '../../../lib/api/objectPut';
import objectPutTagging from '../../../lib/api/objectPutTagging';
import objectGetTagging from '../../../lib/api/objectGetTagging';
import { cleanup,
    DummyRequestLogger,
    makeAuthInfo,
    TaggingConfigTester } from '../helpers';
import DummyRequest from '../DummyRequest';

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
