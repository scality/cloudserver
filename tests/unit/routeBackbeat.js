const sinon = require('sinon');
const async = require('async');
const assert = require('assert');
const DummyRequest = require('./DummyRequest');
const { routeBackbeat, backbeatRoutes } = require('../../lib/routes/routeBackbeat');
const { bucketPut } = require('../../lib/api/bucketPut');
const { makeAuthInfo, versioningTestUtils, DummyRequestLogger } = require('./helpers');
const objectPut = require('../../lib/api/objectPut');
const { auth, errors } = require('arsenal');
const { default: AuthInfo } = require('arsenal/build/lib/auth/AuthInfo');
const bucketPutVersioning = require('../../lib/api/bucketPutVersioning');

const log = new DummyRequestLogger();
const bucketName = 'bucketname';
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const objectName = 'objectName';
const postBody = Buffer.from('I am a body', 'utf8');

const testBucket = {
    bucketName,
    namespace,
    headers: {
        'host': `${bucketName}.s3.amazonaws.com`,
    },
    url: `/${bucketName}`,
    actionImplicitDenies: false,
};

const testObject = new DummyRequest({
    bucketName,
    namespace,
    objectKey: objectName,
    headers: {
        'x-amz-meta-test': 'some metadata',
        'content-length': '12',
    },
    parsedContentLength: 12,
    url: `/${bucketName}/${objectName}`,
}, postBody);

describe('routeBackbeat', () => {
    let request;
    let response;

    beforeEach(() => {
        sinon.stub(backbeatRoutes, 'PUT').returns({
            data: sinon.stub(),
            metadata: sinon.stub(),
            multiplebackenddata: {
                putobject: sinon.stub(),
                putpart: sinon.stub(),
            },
        });

        sinon.stub(backbeatRoutes, 'POST').returns({
            multiplebackenddata: {
                initiatempu: sinon.stub(),
                completempu: sinon.stub(),
                puttagging: sinon.stub(),
            },
            batchdelete: sinon.stub(),
            index: {
                add: sinon.stub(),
                delete: sinon.stub(),
            },
        });

        sinon.stub(backbeatRoutes, 'DELETE').returns({
            expiration: sinon.stub(),
            multiplebackenddata: {
                deleteobject: sinon.stub(),
                deleteobjecttagging: sinon.stub(),
                abortmpu: sinon.stub(),
            },
        });

        sinon.stub(backbeatRoutes, 'GET').returns({
            metadata: sinon.stub(),
            multiplebackendmetadata: sinon.stub(),
            lifecycle: sinon.stub(),
            index: sinon.stub(),
        });

        request = new DummyRequest(
            {
                method: 'GET',
                headers: { 'content-length': '123' },
                url: '/_/backbeat/multiplebackendmetadata/bucketName/objectKey?operation=putobject',
            },
            'body'
        );
        response = {
            setHeader: sinon.stub(),
            writeHead: sinon.stub(),
            end: sinon.stub().callsFake((body, format, cb) => cb()),
        };
    });

    afterEach(() => {
        sinon.restore();
    });

    it('should reject if the request is invalid', done => {
        request.url = '/_/backbeat//bucketName/objectKey?operation=putobject';
        // Cover the case invalidRequest === true
        routeBackbeat('127.0.0.1', request, response, log, err => {
            assert(err.is.MethodNotAllowed);
            done();
        });
    });

    it('should reject if the route is invalid', done => {
        request.url = '/_/backbeat/wrong/bucketName/objectKey?operation=putobject';
        // Cover the case invalidRoute === true
        routeBackbeat('127.0.0.1', request, response, log, err => {
            assert(err.is.MethodNotAllowed);
            done();
        });
    });

    [
        {
            method: 'PUT',
            resourceType: 'metadata',
            target: `${bucketName}/${objectName}`,
            operation: null,
            versionId: false,
            expect: errors.MalformedPOSTRequest,
        },
        {
            method: 'GET',
            resourceType: 'metadata',
            target: `${bucketName}/${objectName}`,
            operation: null,
            versionId: true,
        },
        {
            method: 'PUT',
            resourceType: 'data',
            target: `${bucketName}/${objectName}`,
            operation: null,
            versionId: false,
            expect: errors.BadRequest,
        },
        {
            method: 'PUT',
            resourceType: 'multiplebackenddata',
            target: `${bucketName}/${objectName}`,
            operation: 'putobject',
            versionId: false,
            expect: errors.BadRequest,
        },
        {
            method: 'PUT',
            resourceType: 'multiplebackenddata',
            target: `${bucketName}/${objectName}`,
            operation: 'putpart',
            versionId: false,
            expect: errors.BadRequest,
        },
        {
            method: 'DELETE',
            resourceType: 'multiplebackenddata',
            target: `${bucketName}/${objectName}`,
            operation: 'deleteobject',
            versionId: false,
            expect: errors.BadRequest,
        },
        {
            method: 'DELETE',
            resourceType: 'multiplebackenddata',
            target: `${bucketName}/${objectName}`,
            operation: 'abortmpu',
            versionId: false,
            expect: errors.BadRequest,
        },
        {
            method: 'DELETE',
            resourceType: 'multiplebackenddata',
            target: `${bucketName}/${objectName}`,
            operation: 'deleteobjecttagging',
            versionId: false,
            expect: errors.BadRequest,
        },
        {
            method: 'POST',
            resourceType: 'multiplebackenddata',
            target: `${bucketName}/${objectName}`,
            operation: 'initiatempu',
            versionId: false,
            expect: errors.BadRequest,
        },
        {
            method: 'POST',
            resourceType: 'multiplebackenddata',
            target: `${bucketName}/${objectName}`,
            operation: 'completempu',
            versionId: false,
            expect: errors.BadRequest,
        },
        {
            method: 'POST',
            resourceType: 'multiplebackenddata',
            target: `${bucketName}/${objectName}`,
            operation: 'puttagging',
            versionId: false,
            expect: errors.BadRequest,
        },
        {
            method: 'GET',
            resourceType: 'multiplebackendmetadata',
            target: `${bucketName}/${objectName}`,
            operation: null,
            versionId: false,
            expect: errors.BadRequest,
        },
        {
            method: 'POST',
            resourceType: 'batchdelete',
            target: null,
            operation: null,
            versionId: false,
            expect: errors.MalformedPOSTRequest,
        },
        {
            method: 'GET',
            resourceType: 'lifecycle',
            target: `${bucketName}?list-type=wrong`,
            operation: null,
            versionId: false,
            expect: errors.BadRequest,
        },
        {
            method: 'POST',
            resourceType: 'index',
            target: bucketName,
            operation: 'add',
            versionId: false,
            expect: errors.BadRequest,
        },
        {
            method: 'POST',
            resourceType: 'index',
            target: bucketName,
            operation: 'delete',
            versionId: false,
            expect: errors.BadRequest,
        },
        {
            method: 'GET',
            resourceType: 'index',
            target: null,
            operation: 'delete',
            versionId: false,
            expect: errors.NotImplemented,
        }
    ].forEach(testCase => {
        it(`should call method ${testCase.method} ${testCase.resourceType}`, done => {
            let hasQuery = false;
            let versionIdParsed = null;
            request.method = testCase.method;
            request.url = `/_/backbeat/${testCase.resourceType}/${testCase.target}`;
            if (testCase.operation) {
                request.url += `?operation=${testCase.operation}`;
                hasQuery = true;
            }

            // Mock auth server to ignore auth in this test
            sinon.stub(auth.server, 'doAuth').callsFake((req, log, cb) =>
                cb(null, new AuthInfo({
                    canonicalID: 'abcdef/lifecycle',
                    accountDisplayName: 'Lifecycle Service Account',
                }), undefined, undefined, {
                    accountQuota: 1000,
                })
            );

            const enableVersioningRequest =
                versioningTestUtils.createBucketPutVersioningReq(bucketName, 'Enabled');

            return async.series([
                next => bucketPut(authInfo, testBucket, log, next),
                next => bucketPutVersioning(authInfo, enableVersioningRequest, log, next),
                next => objectPut(authInfo, testObject, undefined, log, (err, res) => {
                    versionIdParsed = res['x-amz-version-id'];
                    if (testCase.versionId) {
                        request.url += `${(hasQuery ? '&' : '?')}&versionId=${versionIdParsed}`;
                    }
                    next(err);
                }),
                next => routeBackbeat('127.0.0.1', request, response, log, next),
            ], err => {
                if (testCase.expect) {
                    assert.strictEqual(err.code, testCase.expect.code);
                    return done();
                }
                assert.ifError(err);
                assert.strictEqual(Array.isArray(request.finalizerHooks), true);
                assert.strictEqual(request.apiMethods[0], 'objectReplicate');
                assert.strictEqual(request.apiMethods.length, 1);
                assert.strictEqual(request.accountQuotas, 1000);
                return done();
            });
        });
    });

    // Although the authz result is by default an implicit deny, the
    // ACL should prevent any further processing for non-service or
    // non-account identities.
    it('should return access denied if doAuth returns an error', done => {
        request.method = 'PUT';
        request.url = `/_/backbeat/metadata/${bucketName}/${objectName}?operation=putobject`;

        routeBackbeat('127.0.0.1', request, response, log, err => {
            assert(err.is.AccessDenied);
            done();
        });
    });
});
