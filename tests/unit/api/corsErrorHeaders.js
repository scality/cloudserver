const assert = require('assert');
const async = require('async');
const sinon = require('sinon');
const { errors, auth } = require('arsenal');

const api = require('../../../lib/api/api');
const { bucketGet } = require('../../../lib/api/bucketGet');
const bucketGetCors = require('../../../lib/api/bucketGetCors');
const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutCors = require('../../../lib/api/bucketPutCors');
const metadata = require('../../../lib/metadata/wrapper');
const DummyRequest = require('../DummyRequest');
const { CorsConfigTester, DummyRequestLogger, cleanup, makeAuthInfo } = require('../helpers');

const endpoints = [
    { apiMethod: 'bucketGet', httpMethod: 'GET', url: '/', query: {} },
    { apiMethod: 'bucketHead', httpMethod: 'HEAD', url: '/', query: {} },
    { apiMethod: 'bucketDelete', httpMethod: 'DELETE', url: '/', query: {} },
    { apiMethod: 'bucketGetACL', httpMethod: 'GET', url: '/?acl', query: { acl: '' } },
    { apiMethod: 'bucketGetCors', httpMethod: 'GET', url: '/?cors', query: { cors: '' } },
    { apiMethod: 'bucketGetLifecycle', httpMethod: 'GET', url: '/?lifecycle', query: { lifecycle: '' } },
    { apiMethod: 'bucketGetReplication', httpMethod: 'GET', url: '/?replication', query: { replication: '' } },
    { apiMethod: 'bucketGetPolicy', httpMethod: 'GET', url: '/?policy', query: { policy: '' } },
    { apiMethod: 'bucketGetVersioning', httpMethod: 'GET', url: '/?versioning', query: { versioning: '' } },
    { apiMethod: 'bucketGetWebsite', httpMethod: 'GET', url: '/?website', query: { website: '' } },
    { apiMethod: 'bucketGetTagging', httpMethod: 'GET', url: '/?tagging', query: { tagging: '' } },
    { apiMethod: 'bucketGetEncryption', httpMethod: 'GET', url: '/?encryption', query: { encryption: '' } },
    { apiMethod: 'bucketGetNotification', httpMethod: 'GET', url: '/?notification', query: { notification: '' } },
    { apiMethod: 'bucketGetObjectLock', httpMethod: 'GET', url: '/?object-lock', query: { 'object-lock': '' } },
    { apiMethod: 'bucketGetLocation', httpMethod: 'GET', url: '/?location', query: { location: '' } },
    { apiMethod: 'objectGet', httpMethod: 'GET', url: '/obj', query: {}, objectKey: 'obj' },
    { apiMethod: 'objectHead', httpMethod: 'HEAD', url: '/obj', query: {}, objectKey: 'obj' },
    { apiMethod: 'objectDelete', httpMethod: 'DELETE', url: '/obj', query: {}, objectKey: 'obj' },
    {
        apiMethod: 'objectGetLegalHold',
        httpMethod: 'GET',
        url: '/obj?legal-hold',
        query: { 'legal-hold': '' },
        objectKey: 'obj',
    },
    {
        apiMethod: 'objectGetAttributes',
        httpMethod: 'GET',
        url: '/obj?attributes',
        query: { attributes: '' },
        objectKey: 'obj',
    },
    { apiMethod: 'listMultipartUploads', httpMethod: 'GET', url: '/?uploads', query: { uploads: '' } },
];

const bucketName = 'corserrorheaderstest';
const authInfo = makeAuthInfo('accessKey1');
const origin = 'http://foo.test';

function setupBucketWithCors(done) {
    cleanup();
    const putBucketReq = {
        bucketName,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: '/',
        actionImplicitDenies: false,
    };
    // Single rule allowing all tested HTTP methods from the test Origin.
    // Using CorsConfigTester's default rules would leave HEAD/DELETE
    // uncovered for our origin and mask the thing we want to assert.
    const corsUtil = new CorsConfigTester({
        allowedMethods: ['GET', 'HEAD', 'PUT', 'POST', 'DELETE'],
        allowedOrigins: [origin],
        allowedHeaders: ['*'],
    });
    const putCorsReq = corsUtil.createBucketCorsRequest('PUT', bucketName);
    const log = new DummyRequestLogger();
    return bucketPut(authInfo, putBucketReq, log, err => {
        if (err) {
            return done(err);
        }
        return bucketPutCors(authInfo, putCorsReq, log, done);
    });
}

function buildRequest(spec) {
    // DummyRequest is an http.IncomingMessage stream that emits 'end'
    // synchronously. We need that because callApiMethod's waterfall
    // waits for the request body on non-objectPut paths.
    return new DummyRequest(
        {
            bucketName,
            objectKey: spec.objectKey,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                origin,
            },
            url: spec.url,
            query: spec.query,
            method: spec.httpMethod,
        },
        Buffer.alloc(0),
    );
}

function buildResponseSpy(sandbox) {
    const headers = {};
    return {
        headers,
        setHeader: sandbox.spy((k, v) => {
            headers[k.toLowerCase()] = v;
        }),
        getHeader: k => headers[k.toLowerCase()],
    };
}

function buildLog(sandbox) {
    return {
        addDefaultFields: sandbox.stub(),
        trace: sandbox.stub(),
        debug: sandbox.stub(),
        info: sandbox.stub(),
        warn: sandbox.stub(),
        error: sandbox.stub(),
        fatal: sandbox.stub(),
        end() {
            return this;
        },
    };
}

describe('CORS headers on 403 auth failures (api.callApiMethod)', () => {
    let sandbox;

    before(done => setupBucketWithCors(done));

    beforeEach(() => {
        sandbox = sinon.createSandbox();
        const authServer = {
            doAuth: sandbox.stub().callsArgWith(2, errors.AccessDenied),
        };
        sandbox.stub(auth, 'server').value(authServer);
    });

    afterEach(() => sandbox.restore());

    endpoints.forEach(spec => {
        it(`attaches CORS headers to 403 response for ${spec.apiMethod}`, done => {
            const request = buildRequest(spec);
            const response = buildResponseSpy(sandbox);
            const log = buildLog(sandbox);

            api.callApiMethod(spec.apiMethod, request, response, log, err => {
                assert(err, 'expected an error');
                assert(err.is && err.is.AccessDenied, `expected AccessDenied, got ${err.code}`);
                // Either the callback surfaces CORS headers in one of
                // its trailing args OR they have been set directly on
                // the HTTP response. We assert on the response since
                // that is what the HTTP transport ultimately sends.
                const allowOrigin = response.getHeader('access-control-allow-origin');
                assert(allowOrigin, 'access-control-allow-origin missing from 403 ' + `response for ${spec.apiMethod}`);
                assert(response.getHeader('access-control-allow-methods'), 'access-control-allow-methods missing');
                done();
            });
        });
    });

    it('does not attach CORS headers when Origin header is absent', done => {
        const request = buildRequest({
            apiMethod: 'bucketGet',
            httpMethod: 'GET',
            url: '/',
            query: {},
        });
        delete request.headers.origin;
        const response = buildResponseSpy(sandbox);
        const log = buildLog(sandbox);

        api.callApiMethod('bucketGet', request, response, log, err => {
            assert(err && err.is.AccessDenied);
            assert.strictEqual(response.getHeader('access-control-allow-origin'), undefined);
            done();
        });
    });

    it('does not attach CORS headers when origin does not match any rule', done => {
        const request = buildRequest({
            apiMethod: 'bucketGet',
            httpMethod: 'GET',
            url: '/',
            query: {},
        });
        request.headers.origin = 'http://not-allowed.test';
        // The bucket's CORS config allows any method from foo.test
        // plus GET from *. Use PUT from a different origin so neither
        // condition matches.
        request.method = 'PUT';
        const response = buildResponseSpy(sandbox);
        const log = buildLog(sandbox);

        api.callApiMethod('bucketGet', request, response, log, err => {
            assert(err && err.is.AccessDenied);
            assert.strictEqual(response.getHeader('access-control-allow-origin'), undefined);
            done();
        });
    });
});

describe('CORS headers on 403 via handler (fast path)', () => {
    let sandbox;

    before(done => setupBucketWithCors(done));

    beforeEach(() => {
        sandbox = sinon.createSandbox();
        // Auth succeeds as accessKey2 so the handler runs and then
        // denies at its own ACL check (bucket is owned by accessKey1).
        const otherAuth = makeAuthInfo('accessKey2');
        const authServer = {
            doAuth: sandbox.stub().callsArgWith(2, null, otherAuth, [{ isAllowed: true, isImplicit: false }], null, {}),
        };
        sandbox.stub(auth, 'server').value(authServer);
    });

    afterEach(() => sandbox.restore());

    it('forwards handler-provided corsHeaders without setting headers ' + 'on the response directly', done => {
        const request = buildRequest({
            apiMethod: 'bucketGet',
            httpMethod: 'GET',
            url: '/',
            query: {},
        });
        const response = buildResponseSpy(sandbox);
        const log = buildLog(sandbox);

        api.callApiMethod('bucketGet', request, response, log, (err, xml, corsHeaders) => {
            assert(err, 'expected an error');
            assert(err.is && err.is.AccessDenied, `expected AccessDenied, got ${err.code}`);
            assert(corsHeaders, 'handler should have supplied corsHeaders');
            assert.strictEqual(corsHeaders['access-control-allow-origin'], origin);
            // Fast path: wrapper forwards corsHeaders via the callback
            // instead of setting them on the response directly.
            assert.strictEqual(response.getHeader('access-control-allow-origin'), undefined);
            done();
        });
    });

    it('makes at most 2 metadata.getBucket calls on the error path', done => {
        const getBucketSpy = sandbox.spy(metadata, 'getBucket');
        const request = buildRequest({
            apiMethod: 'bucketHead',
            httpMethod: 'HEAD',
            url: '/',
            query: {},
        });
        // Origin that matches no CORS rule -> handler emits empty
        // corsHeaders -> fast path misses -> wrapper falls back to a
        // second getBucket. The handler's own call (1) + the wrapper
        // fallback (1) is the documented ceiling - see the comment
        // on wrapCallbackWithErrorCorsHeaders in lib/api/api.js. Use
        // <= so this remains a future-proof ceiling: optimizations
        // that reduce the count are welcome.
        request.headers.origin = 'http://not-allowed.test';
        const response = buildResponseSpy(sandbox);
        const log = buildLog(sandbox);

        api.callApiMethod('bucketHead', request, response, log, err => {
            assert(err && err.is.AccessDenied);
            assert(
                getBucketSpy.callCount <= 2,
                'expected at most 2 metadata.getBucket calls, got ' + `${getBucketSpy.callCount}`,
            );
            done();
        });
    });
});

describe('CORS headers on copy operations', () => {
    // objectCopy / objectPutCopyPart call standardMetadataValidateBucketAndObj
    // twice (destination, then source). The wrapper must evaluate CORS
    // against the request's actual bucket (destination), not the source -
    // even though the source is loaded last. We exercise this by giving
    // dest a config that does NOT match the request and source a config
    // that DOES match, then asserting source headers don't leak.
    const destBucket = 'cors-copy-dest';
    const srcBucket = 'cors-copy-src';
    const reqOrigin = 'http://foo.test';
    let sandbox;

    before(done => {
        cleanup();
        const log = new DummyRequestLogger();
        const destPutReq = {
            bucketName: destBucket,
            headers: { host: `${destBucket}.s3.amazonaws.com` },
            url: '/',
            actionImplicitDenies: false,
        };
        const srcPutReq = {
            bucketName: srcBucket,
            headers: { host: `${srcBucket}.s3.amazonaws.com` },
            url: '/',
            actionImplicitDenies: false,
        };
        // Dest CORS: only GET from reqOrigin. The request is a PUT, so
        // dest's collectCorsHeaders returns {} - no headers should be
        // applied to the response.
        const destCors = new CorsConfigTester({
            allowedMethods: ['GET'],
            allowedOrigins: [reqOrigin],
        });
        // Source CORS: PUT from reqOrigin. If the wrapper mistakenly
        // evaluated against source, it would set real CORS headers.
        const srcCors = new CorsConfigTester({
            allowedMethods: ['PUT'],
            allowedOrigins: [reqOrigin],
        });
        async.series(
            [
                cb => bucketPut(authInfo, destPutReq, log, cb),
                cb => bucketPut(authInfo, srcPutReq, log, cb),
                cb => bucketPutCors(authInfo, destCors.createBucketCorsRequest('PUT', destBucket), log, cb),
                cb => bucketPutCors(authInfo, srcCors.createBucketCorsRequest('PUT', srcBucket), log, cb),
            ],
            done,
        );
    });

    beforeEach(() => {
        sandbox = sinon.createSandbox();
        const authServer = {
            doAuth: sandbox.stub().callsArgWith(
                2,
                null,
                authInfo,
                [
                    { isAllowed: true, isImplicit: false },
                    { isAllowed: true, isImplicit: false },
                ],
                null,
                {},
            ),
        };
        sandbox.stub(auth, 'server').value(authServer);
    });

    afterEach(() => sandbox.restore());

    it('does not leak source-bucket CORS headers on objectCopy errors', done => {
        const request = new DummyRequest(
            {
                bucketName: destBucket,
                objectKey: 'destkey',
                headers: {
                    host: `${destBucket}.s3.amazonaws.com`,
                    origin: reqOrigin,
                    'x-amz-copy-source': `/${srcBucket}/missing-source-key`,
                },
                url: `/${destBucket}/destkey`,
                query: {},
                method: 'PUT',
            },
            Buffer.alloc(0),
        );
        const response = buildResponseSpy(sandbox);
        const log = buildLog(sandbox);

        api.callApiMethod('objectCopy', request, response, log, err => {
            assert(err, 'expected an error');
            // Dest does not allow PUT from reqOrigin, so no CORS
            // headers should be set. If the wrapper used the source
            // bucket (which DOES allow PUT from reqOrigin) we would
            // see access-control-allow-origin: http://foo.test here.
            assert.strictEqual(
                response.getHeader('access-control-allow-origin'),
                undefined,
                'wrapper must not apply source-bucket CORS headers',
            );
            done();
        });
    });
});

describe('CORS headers on 200 successful responses (per-handler)', () => {
    before(done => setupBucketWithCors(done));

    it('bucketGet returns corsHeaders to callback on 200', done => {
        const log = new DummyRequestLogger();
        const request = {
            bucketName,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                origin,
            },
            method: 'GET',
            url: '/',
            query: {},
            actionImplicitDenies: false,
        };
        bucketGet(authInfo, request, log, (err, xml, corsHeaders) => {
            assert.ifError(err);
            assert(corsHeaders, 'expected corsHeaders to be set on successful bucketGet');
            assert(corsHeaders['access-control-allow-origin'], 'expected access-control-allow-origin on 200');
            done();
        });
    });

    it('bucketGetCors returns corsHeaders to callback on 200', done => {
        const log = new DummyRequestLogger();
        const request = {
            bucketName,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                origin,
            },
            method: 'GET',
            url: '/?cors',
            query: { cors: '' },
            actionImplicitDenies: false,
        };
        bucketGetCors(authInfo, request, log, (err, xml, corsHeaders) => {
            assert.ifError(err);
            assert(
                corsHeaders && corsHeaders['access-control-allow-origin'],
                'expected access-control-allow-origin on 200',
            );
            done();
        });
    });
});
