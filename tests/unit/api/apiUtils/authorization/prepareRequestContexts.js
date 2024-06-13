const assert = require('assert');
const DummyRequest = require('../../../DummyRequest');
const  prepareRequestContexts =
      require('../../../../../lib/api/apiUtils/authorization/prepareRequestContexts.js');

const makeRequest = (headers, query) => new DummyRequest({
    headers,
    url: '/',
    parsedHost: 'localhost',
    socket: {},
    query,
});
const sourceBucket = 'bucketsource';
const sourceObject = 'objectsource';
const sourceVersionId = 'vid1';

describe('prepareRequestContexts', () => {
    it('should return s3:DeleteObject if multiObjectDelete method', () => {
        const apiMethod = 'multiObjectDelete';
        const request = makeRequest();
        const results = prepareRequestContexts(apiMethod, request, sourceBucket,
        sourceObject, sourceVersionId);

        assert.strictEqual(results.length, 1);
        const expectedAction = 's3:DeleteObject';
        assert.strictEqual(results[0].getAction(), expectedAction);
    });

    it('should return s3:PutObjectVersion request context action for objectPut method with x-scal-s3-version-id' +
    ' header', () => {
        const apiMethod = 'objectPut';
        const request = makeRequest({
            'x-scal-s3-version-id': 'vid',
        });
        const results = prepareRequestContexts(apiMethod, request, sourceBucket,
        sourceObject, sourceVersionId);

        assert.strictEqual(results.length, 1);
        const expectedAction = 's3:PutObjectVersion';
        assert.strictEqual(results[0].getAction(), expectedAction);
    });

    it('should return s3:PutObjectVersion request context action for objectPut method with empty x-scal-s3-version-id' +
    ' header', () => {
        const apiMethod = 'objectPut';
        const request = makeRequest({
            'x-scal-s3-version-id': '',
        });
        const results = prepareRequestContexts(apiMethod, request, sourceBucket,
        sourceObject, sourceVersionId);

        assert.strictEqual(results.length, 1);
        const expectedAction = 's3:PutObjectVersion';
        assert.strictEqual(results[0].getAction(), expectedAction);
    });

    it('should return s3:PutObject request context action for objectPut method and no header', () => {
        const apiMethod = 'objectPut';
        const request = makeRequest({});
        const results = prepareRequestContexts(apiMethod, request, sourceBucket,
        sourceObject, sourceVersionId);

        assert.strictEqual(results.length, 1);
        const expectedAction = 's3:PutObject';
        assert.strictEqual(results[0].getAction(), expectedAction);
    });

    it('should return s3:PutObject and s3:PutObjectTagging actions for objectPut method with' +
    ' x-amz-tagging header', () => {
        const apiMethod = 'objectPut';
        const request = makeRequest({
            'x-amz-tagging': 'key1=value1&key2=value2',
        });
        const results = prepareRequestContexts(apiMethod, request, sourceBucket,
        sourceObject, sourceVersionId);

        assert.strictEqual(results.length, 2);
        const expectedAction1 = 's3:PutObject';
        const expectedAction2 = 's3:PutObjectTagging';
        assert.strictEqual(results[0].getAction(), expectedAction1);
        assert.strictEqual(results[1].getAction(), expectedAction2);
    });

    it('should return s3:PutObject and s3:PutObjectAcl actions for objectPut method with ACL header', () => {
        const apiMethod = 'objectPut';
        const request = makeRequest({
            'x-amz-acl': 'private',
        });
        const results = prepareRequestContexts(apiMethod, request, sourceBucket,
        sourceObject, sourceVersionId);

        assert.strictEqual(results.length, 2);
        const expectedAction1 = 's3:PutObject';
        const expectedAction2 = 's3:PutObjectAcl';
        assert.strictEqual(results[0].getAction(), expectedAction1);
        assert.strictEqual(results[1].getAction(), expectedAction2);
    });

    ['initiateMultipartUpload', 'objectPutPart', 'completeMultipartUpload'].forEach(apiMethod => {
        it(`should return s3:PutObjectVersion request context action for ${apiMethod} method ` +
        'with x-scal-s3-version-id header', () => {
            const request = makeRequest({
                'x-scal-s3-version-id': '',
            });
            const results = prepareRequestContexts(apiMethod, request, sourceBucket,
            sourceObject, sourceVersionId);

            assert.strictEqual(results.length, 1);
            const expectedAction = 's3:PutObjectVersion';
            assert.strictEqual(results[0].getAction(), expectedAction);
        });

        it(`should return s3:PutObjectVersion request context action for ${apiMethod} method` +
        'with empty x-scal-s3-version-id header', () => {
            const request = makeRequest({
                'x-scal-s3-version-id': '',
            });
            const results = prepareRequestContexts(apiMethod, request, sourceBucket,
            sourceObject, sourceVersionId);

            assert.strictEqual(results.length, 1);
            const expectedAction = 's3:PutObjectVersion';
            assert.strictEqual(results[0].getAction(), expectedAction);
        });

        it(`should return s3:PutObject request context action for ${apiMethod} method and no header`, () => {
            const request = makeRequest({});
            const results = prepareRequestContexts(apiMethod, request, sourceBucket,
            sourceObject, sourceVersionId);

            assert.strictEqual(results.length, 1);
            const expectedAction = 's3:PutObject';
            assert.strictEqual(results[0].getAction(), expectedAction);
        });
    });
});
