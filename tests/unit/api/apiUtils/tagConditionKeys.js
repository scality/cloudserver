const assert = require('assert');

const DummyRequest = require('../../DummyRequest');
const {
    cleanup,
    DummyRequestLogger,
    makeAuthInfo,
    TaggingConfigTester,
    createRequestContext,
} = require('../../helpers');
const { tagConditionKeyAuth, updateRequestContextsWithTags, makeTagQuery } =
    require('../../../../lib/api/apiUtils/authorization/tagConditionKeys');
const { bucketPut } = require('../../../../lib/api/bucketPut');
const objectPut = require('../../../../lib/api/objectPut');

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

const objectPutRequestContexts = [
    createRequestContext('objectPut', objectPutReq),
];

const objectGetReq = {
    bucketName,
    headers: {
        host: `${bucketName}.s3.amazonaws.com`,
    },
    objectKey,
    url: `/${bucketName}/${objectKey}`,
    query: {},
};
const objectGetRequestContexts = [
    createRequestContext('objectGet', objectGetReq),
    createRequestContext('objectGetTagging', objectGetReq),
];

describe('tagConditionKeyAuth', () => {
    it('should return empty if no previous auth results', done => {
        tagConditionKeyAuth([], objectPutReq, objectPutRequestContexts, 'bucketPut', log, err => {
            assert.ifError(err);
            done();
        });
    });
    it('should return empty if auth results do not contain checkTagConditions key', done => {
        const authResults = [{ isAllowed: true }, { isAllowed: true }];
        tagConditionKeyAuth(authResults, objectPutReq, objectPutRequestContexts, 'bucketPut', log, err => {
            assert.ifError(err);
            done();
        });
    });
});

describe('updateRequestContextsWithTags', () => {
    before(done => {
        cleanup();
        bucketPut(authInfo, bucketPutReq, log, done);
    });

    after(cleanup);

    it('should update request context with request object tags', done => {
        updateRequestContextsWithTags(objectPutReq, objectPutRequestContexts, 'objectPut', log, err => {
            assert.ifError(err);
            assert(objectPutRequestContexts[0].getNeedTagEval());
            assert.strictEqual(objectPutRequestContexts[0].getRequestObjTags(),
                               makeTagQuery(taggingUtil.getTags()));
            assert.strictEqual(objectPutRequestContexts[0].getExistingObjTag(), null);
            done();
        });
    });

    it('should update multiple request contexts with existing object tags', done => {
        objectPut(authInfo, objectPutReq, 'foobar', log, err => {
            assert.ifError(err);
            updateRequestContextsWithTags(objectGetReq, objectGetRequestContexts, 'objectGet', log,
            err => {
                assert.ifError(err);
                for (const requestContext of objectGetRequestContexts) {
                    assert(requestContext.getNeedTagEval());
                    assert.strictEqual(requestContext.getExistingObjTag(),
                                       makeTagQuery(taggingUtil.getTags()));
                    assert.strictEqual(requestContext.getRequestObjTags(), null);
                }
                done();
            });
        });
    });
});
