const { S3 } = require('aws-sdk');
const assert = require('assert');
const async = require('async');

const getConfig = require('../support/config');
const { methodRequest, generateCorsParams } =
    require('../../lib/utility/cors-util');

const config = getConfig('default', { signatureVersion: 'v4' });
const s3 = new S3(config);

const bucket = 'corserrorheadertest';
const objectKey = 'objectKey';
const allowedOrigin = 'http://www.allowed.test';
const vary = 'Origin, Access-Control-Request-Headers, '
    + 'Access-Control-Request-Method';

const expectedCorsHeaders = {
    'access-control-allow-origin': allowedOrigin,
    'access-control-allow-methods': 'GET, PUT, POST, DELETE, HEAD',
    'access-control-allow-credentials': 'true',
    vary,
};

const corsParams = generateCorsParams(bucket, {
    allowedMethods: ['GET', 'PUT', 'POST', 'DELETE', 'HEAD'],
    allowedOrigins: [allowedOrigin],
    allowedHeaders: ['*'],
});

// Raw unauthenticated requests - they always return 403.
// Each spec describes (method, path, query) against the bucket.
const unauthenticatedRequests = [
    { description: 'GET bucket (list objects)',
        method: 'GET', query: null, objectKey: null },
    { description: 'HEAD bucket',
        method: 'HEAD', query: null, objectKey: null },
    { description: 'DELETE bucket',
        method: 'DELETE', query: null, objectKey: null },
    { description: 'GET bucket ACL',
        method: 'GET', query: 'acl', objectKey: null },
    { description: 'GET bucket CORS',
        method: 'GET', query: 'cors', objectKey: null },
    { description: 'GET bucket versioning',
        method: 'GET', query: 'versioning', objectKey: null },
    { description: 'GET bucket website',
        method: 'GET', query: 'website', objectKey: null },
    { description: 'GET bucket tagging',
        method: 'GET', query: 'tagging', objectKey: null },
    { description: 'GET object',
        method: 'GET', query: null, objectKey },
    { description: 'HEAD object',
        method: 'HEAD', query: null, objectKey },
    { description: 'PUT object',
        method: 'PUT', query: null, objectKey },
    { description: 'DELETE object',
        method: 'DELETE', query: null, objectKey },
    { description: 'GET bucket uploads (list multipart uploads)',
        method: 'GET', query: 'uploads', objectKey: null },
    // GET bucket policy and POST multi-delete are not covered here: the
    // first returns 405 (method rejected pre-auth), the second returns 400
    // (missing XML body fails validation pre-auth). Neither reaches the
    // 403 path. Both are exercised via the unit test that stubs auth
    // failure directly.
];

function _waitForAWS(callback, err) {
    if (err) {
        return setTimeout(() => callback(err), 500);
    }
    return setTimeout(() => callback(), 500);
}

describe('CORS headers on 403 responses when bucket has CORS configured', () => {
    before(done => async.series([
        cb => s3.createBucket({ Bucket: bucket }, err => _waitForAWS(cb, err)),
        cb => s3.putBucketCors(corsParams, err => _waitForAWS(cb, err)),
    ], done));

    after(done => s3.deleteBucket({ Bucket: bucket },
        err => _waitForAWS(done, err)));

    unauthenticatedRequests.forEach(spec => {
        it(`returns CORS headers on 403 for ${spec.description} `
            + 'when Origin matches a rule', done => {
            methodRequest({
                method: spec.method,
                bucket,
                objectKey: spec.objectKey,
                query: spec.query,
                headers: { origin: allowedOrigin },
                // Use numeric status: HEAD responses have no body, and some
                // endpoints (bucket policy, multi-delete) can fail with a
                // non-AccessDenied body before auth even runs. We only care
                // about the 403 status and the CORS headers here.
                code: 403,
                headersResponse: expectedCorsHeaders,
            }, done);
        });
    });

    it('omits CORS headers on 403 when Origin does not match any rule',
        done => {
            methodRequest({
                method: 'GET',
                bucket,
                query: null,
                objectKey: null,
                headers: { origin: 'http://not-allowed.test' },
                code: 403,
                // headersResponse unset -> cors-util asserts CORS headers
                // are NOT present.
            }, done);
        });

    it('omits CORS headers on 403 when no Origin header is sent',
        done => {
            methodRequest({
                method: 'GET',
                bucket,
                query: null,
                objectKey: null,
                headers: {},
                code: 403,
            }, done);
        });
});

describe('CORS headers on 200 responses (regression guard)', () => {
    before(done => async.series([
        cb => s3.createBucket({ Bucket: bucket }, err => _waitForAWS(cb, err)),
        cb => s3.putBucketCors(corsParams, err => _waitForAWS(cb, err)),
    ], done));

    after(done => s3.deleteBucket({ Bucket: bucket },
        err => _waitForAWS(done, err)));

    it('returns CORS headers on a successful list objects (200)', done => {
        const request = s3.listObjects({ Bucket: bucket });
        request.on('build', () => {
            request.httpRequest.headers.origin = allowedOrigin;
        });
        request.on('success', response => {
            const h = response.httpResponse.headers;
            assert.strictEqual(h['access-control-allow-origin'],
                allowedOrigin);
            done();
        });
        request.on('error', err => done(err));
        request.send();
    });
});
