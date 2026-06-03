const {
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketCorsCommand,
    ListObjectsCommand,
} = require('@aws-sdk/client-s3');
const assert = require('assert');

const getConfig = require('../support/config');
const { methodRequest, generateCorsParams } = require('../../lib/utility/cors-util');

const config = getConfig('default', { signatureVersion: 'v4' });
const s3 = new S3Client({ ...config, forcePathStyle: true });

const bucket = 'corserrorheadertest';
const objectKey = 'objectKey';
const allowedOrigin = 'http://www.allowed.test';
const vary = 'Origin, Access-Control-Request-Headers, ' + 'Access-Control-Request-Method';

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
    { description: 'GET bucket (list objects)', method: 'GET', query: null, objectKey: null },
    { description: 'HEAD bucket', method: 'HEAD', query: null, objectKey: null },
    { description: 'DELETE bucket', method: 'DELETE', query: null, objectKey: null },
    { description: 'GET bucket ACL', method: 'GET', query: 'acl', objectKey: null },
    { description: 'GET bucket CORS', method: 'GET', query: 'cors', objectKey: null },
    { description: 'GET bucket versioning', method: 'GET', query: 'versioning', objectKey: null },
    { description: 'GET bucket website', method: 'GET', query: 'website', objectKey: null },
    { description: 'GET bucket tagging', method: 'GET', query: 'tagging', objectKey: null },
    { description: 'GET object', method: 'GET', query: null, objectKey },
    { description: 'HEAD object', method: 'HEAD', query: null, objectKey },
    { description: 'PUT object', method: 'PUT', query: null, objectKey },
    { description: 'DELETE object', method: 'DELETE', query: null, objectKey },
    { description: 'GET bucket uploads (list multipart uploads)', method: 'GET', query: 'uploads', objectKey: null },
    // GET bucket policy and POST multi-delete are not covered here: the
    // first returns 405 (method rejected pre-auth), the second returns 400
    // (missing XML body fails validation pre-auth). Neither reaches the
    // 403 path. Both are exercised via the unit test that stubs auth
    // failure directly.
];

describe('CORS headers on 403 responses when bucket has CORS configured', () => {
    before(async () => {
        await s3.send(new CreateBucketCommand({ Bucket: bucket }));
        await s3.send(new PutBucketCorsCommand(corsParams));
    });

    after(async () => {
        await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
    });

    unauthenticatedRequests.forEach(spec => {
        it(`returns CORS headers on 403 for ${spec.description} ` + 'when Origin matches a rule', done => {
            methodRequest(
                {
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
                },
                done,
            );
        });
    });

    it('omits CORS headers on 403 when Origin does not match any rule', done => {
        methodRequest(
            {
                method: 'GET',
                bucket,
                query: null,
                objectKey: null,
                headers: { origin: 'http://not-allowed.test' },
                code: 403,
                // headersResponse unset -> cors-util asserts CORS headers
                // are NOT present.
            },
            done,
        );
    });

    it('omits CORS headers on 403 when no Origin header is sent', done => {
        methodRequest(
            {
                method: 'GET',
                bucket,
                query: null,
                objectKey: null,
                headers: {},
                code: 403,
            },
            done,
        );
    });
});

describe('CORS headers on 200 responses (regression guard)', () => {
    before(async () => {
        await s3.send(new CreateBucketCommand({ Bucket: bucket }));
        await s3.send(new PutBucketCorsCommand(corsParams));
    });

    after(async () => {
        await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
    });

    it('returns CORS headers on a successful list objects (200)', async () => {
        const command = new ListObjectsCommand({ Bucket: bucket });
        // Inject Origin on the outgoing request and capture the raw
        // response headers via the deserialize step.
        command.middlewareStack.add(
            next => async args => {
                const headers = args.request && args.request.headers;
                if (headers) {
                    headers.origin = allowedOrigin;
                }
                return next(args);
            },
            { step: 'build' },
        );
        let responseHeaders;
        command.middlewareStack.add(
            next => async args => {
                const result = await next(args);
                responseHeaders = result.response && result.response.headers;
                return result;
            },
            { step: 'deserialize' },
        );
        await s3.send(command);
        assert.strictEqual(responseHeaders['access-control-allow-origin'], allowedOrigin);
    });
});
