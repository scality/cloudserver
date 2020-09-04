const assert = require('assert');

const DummyRequest = require('../../DummyRequest');
const {
    cleanup,
    DummyRequestLogger,
    makeAuthInfo,
    TaggingConfigTester,
    createRequestContext,
} = require('../../helpers');
const { tagConditionKeyAuth, updateRequestContexts, makeTagQuery } =
    require('../../../../lib/api/apiUtils/authorization/tagConditionKeys');
const { bucketPut } = require('../../../../lib/api/bucketPut');

const log = new DummyRequestLogger();
const bucketName = 'tagconditionkeybuckettester';
const objectKey = 'tagconditionkeykeytester';
const namespace = 'default';
const postBody = Buffer.from('I am a body', 'utf8');
const authInfo = makeAuthInfo('accessKey1');

const bucketPutReq = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
};

const taggingUtil = new TaggingConfigTester();

const objectPutReq = new DummyRequest({
    bucketName,
    namespace,
    objectKey,
    headers: {
        'host': `${bucketName}.s3.amazonaws.com`,
        'x-amz-tagging': makeTagQuery(taggingUtil.getTags()),
    },
    url: `/${bucketName}/${objectKey}`,
    calculatedHash: 'vnR+tLdVF79rPPfF+7YvOg==',
}, postBody);

const requestContexts = [
    createRequestContext('objectPut', objectPutReq),
];

describe('tagConditionKeyAuth', () => {
    it('should return empty if no previous auth results', done => {
        tagConditionKeyAuth([], objectPutReq, requestContexts, log, err => {
            assert.ifError(err);
            done();
        });
    });
    it('should return empty if auth results do not contain checkTagConditions key', done => {
        const authResults = [{ isAllowed: true }, { isAllowed: true }];
        tagConditionKeyAuth(authResults, objectPutReq, requestContexts, log, err => {
            assert.ifError(err);
            done();
        });
    });
});

describe('updateRequestContexts', () => {
    before(done => {
        cleanup();
        bucketPut(authInfo, bucketPutReq, log, done);
    });

    after(cleanup);

    it('should update request context with request object tags', done => {
        updateRequestContexts(objectPutReq, requestContexts, log, (err, newRequestContexts) => {
            assert.ifError(err);
            assert(newRequestContexts[0].getNeedTagEval());
            assert.strictEqual(newRequestContexts[0].getRequestObjTags(), makeTagQuery(taggingUtil.getTags()));
            done();
        });
    });
});
