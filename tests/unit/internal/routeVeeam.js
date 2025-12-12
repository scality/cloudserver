const assert = require('assert');
const { DummyRequestLogger } = require('../helpers');
const routeVeeam = require('../../../lib/routes/routeVeeam');
const DummyRequest = require('../DummyRequest');

const log = new DummyRequestLogger();

describe('RouteVeeam: checkBucketAndKey', () => {
    [
        [null, 'objectKey', null, 'POST', log],
        [null, 'objectKey', null, 'CONNECT', log],
        [null, 'objectKey', null, 'OPTIONS', log],
        [null, 'objectKey', null, 'PATCH', log],
        [null, 'objectKey', null, 'TRACE', log],
    ].forEach(test => {
        it(`should return MethodNotAllowed for ${test[2]}`, () => {
            assert.strictEqual(routeVeeam.checkBucketAndKey(...test).is.MethodNotAllowed, true);
        });
    });

    [
        ['bad_bucket', 'objectKey', null, 'GET', log],
        ['badbucket-', 'objectKey', null, 'GET', log],
        ['bad..bucket', 'objectKey', null, 'GET', log],
        ['bad_bucket', 'objectKey', null, 'POST', log],
        ['badbucket-', 'objectKey', null, 'POST', log],
        ['bad..bucket', 'objectKey', null, 'POST', log],
        ['bad_bucket', 'objectKey', null, 'PUT', log],
        ['badbucket-', 'objectKey', null, 'PUT', log],
        ['bad..bucket', 'objectKey', null, 'PUT', log],
    ].forEach(test => {
        it(`should return InvalidBucketName for "${test[0]}" bucket name`, () => {
            assert.strictEqual(routeVeeam.checkBucketAndKey(...test).is.InvalidBucketName, true);
        });
    });

    [
        ['bad_bucket', 'objectKey', null, 'DELETE', log],
        ['badbucket-', 'objectKey', null, 'DELETE', log],
        ['bad..bucket', 'objectKey', null, 'DELETE', log],
    ].forEach(test => {
        it(`should return NoSuchBucket for "${test[0]}" bucket name (DELETE)`, () => {
            assert.strictEqual(routeVeeam.checkBucketAndKey(...test).is.NoSuchBucket, true);
        });
    });

    [
        ['test', 'badObjectKey', null, 'GET', log],
    ].forEach(test => {
        it(`should return InvalidArgument for "${test[1]}" object name`, () => {
            assert.strictEqual(routeVeeam.checkBucketAndKey(...test).is.InvalidArgument, true);
        });
    });

    [
        ['test', '.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml', { random: 'queryparam' }, 'GET', log],
    ].forEach(test => {
        it(`should return InvalidRequest for "${test[1]}" object name`, () => {
            assert.strictEqual(routeVeeam.checkBucketAndKey(...test).is.InvalidRequest, true);
        });
    });

    [
        ['test', '.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml', null, 'GET', log],
        ['test', '.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml', null, 'GET', log],
    ].forEach(test => {
        it(`should return success for "${test[1]}" object name`, () => {
            assert.strictEqual(routeVeeam.checkBucketAndKey(...test), undefined);
        });
    });

    [
        ['test', '.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml', { tagging: undefined }, 'GET', log],
        ['test', '.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml', { 'X-Amz-Credential': 'a' }, 'GET', log],
    ].forEach(test => {
        it(`should return success for "${test[1]}" object name with supported query parameters`, () => {
            assert.strictEqual(routeVeeam.checkBucketAndKey(...test), undefined);
        });
    });

    it('should allow SigV4 presigned GET query parameters in mixed case', () => {
        const err = routeVeeam.checkBucketAndKey(
            'test',
            '.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml',
            {
                'X-Amz-Algorithm': 'AWS4-HMAC-SHA256',
                'X-Amz-Credential': 'cred',
                'X-Amz-Date': '20240101T000000Z',
                'X-Amz-Expires': '900',
                'X-Amz-SignedHeaders': 'host',
                'X-Amz-Signature': 'signature',
                'X-Amz-Security-Token': 'token',
            },
            'GET',
            log,
        );
        assert.strictEqual(err, undefined);
    });

    it('should allow SigV4-style query parameters on non-GET when they are presigned', () => {
        const err = routeVeeam.checkBucketAndKey(
            'test',
            '.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml',
            {
                'x-amz-algorithm': 'AWS4-HMAC-SHA256',
                'x-amz-credential': 'cred',
                'x-amz-date': '20240101T000000Z',
                'x-amz-expires': '900',
                'x-amz-signedheaders': 'host',
                'x-amz-signature': 'signature',
            },
            'DELETE',
            log,
        );
        assert.strictEqual(err, undefined);
    });

    it('should reject unexpected query parameters even when presigned GET keys are present', () => {
        const err = routeVeeam.checkBucketAndKey(
            'test',
            '.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml',
            {
                'X-Amz-Credential': 'a',
                extra: 'not-allowed',
            },
            'GET',
            log,
        );
        assert.strictEqual(err.is.InvalidRequest, true);
    });

    it('should allow AWS SDK x-id=PutObject query on PUT for system.xml', () => {
        const err = routeVeeam.checkBucketAndKey(
            'test',
            '.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml',
            { 'x-id': 'PutObject' },
            'PUT',
            log,
        );
        assert.strictEqual(err, undefined);
    });

    it('should allow AWS SDK auxiliary x-amz-sdk-* query params on PUT for system.xml', () => {
        const err = routeVeeam.checkBucketAndKey(
            'test',
            '.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml',
            {
                'x-id': 'PutObject',
                'x-amz-sdk-request': 'attempt=1',
                'x-amz-sdk-invocation-id': 'abc-123',
                'x-amz-user-agent': 'aws-sdk-js-v3',
            },
            'PUT',
            log,
        );
        assert.strictEqual(err, undefined);
    });

    it('should reject mismatched x-id value on PUT for system.xml', () => {
        const err = routeVeeam.checkBucketAndKey(
            'test',
            '.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml',
            { 'x-id': 'GetObject' },
            'PUT',
            log,
        );
        assert.strictEqual(err.is.InvalidRequest, true);
    });

    it('should reject mismatched x-id value on GET for system.xml', () => {
        const err = routeVeeam.checkBucketAndKey(
            'test',
            '.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml',
            { 'x-id': 'PutObject' },
            'GET',
            log,
        );
        assert.strictEqual(err.is.InvalidRequest, true);
    });

    it('should accept x-id with different casing when value matches action', () => {
        const err = routeVeeam.checkBucketAndKey(
            'test',
            '.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml',
            { 'X-Id': 'GetObject' },
            'GET',
            log,
        );
        assert.strictEqual(err, undefined);
    });
});

describe('RouteVeeam: checkBucketAndKey', () => {
    [
        [undefined, undefined, undefined, log],
        ['PATCH', undefined, undefined, log],
        ['PUT', undefined, undefined, log],
        ['GET', undefined, undefined, log],
        ['DELETE', undefined, undefined, log],
    ].forEach(test => {
        it(`should return InvalidArgument for "${test[0]}"`, () => {
            assert.strictEqual(routeVeeam.checkUnsupportedRoutes(...test).error.is.MethodNotAllowed, true);
        });
    });

    [
        ['PUT', {}, undefined, log],
        ['GET', undefined, {}, log],
        ['DELETE', {}, {}, log],
    ].forEach(test => {
        it(`should return success for "${test[0]}"`, () => {
            assert.strictEqual(routeVeeam.checkUnsupportedRoutes(...test).error, undefined);
        });
    });
});

describe('RouteVeeam: _normalizeVeeamRequest', () => {
    it('should normalize request', () => {
        const request = {
            url: 'url',
            headers: [],
        };
        assert.doesNotThrow(() => routeVeeam._normalizeVeeamRequest(request));
    });
});

describe('RouteVeeam: routeVeeam', () => {
    it('should return error for unsupported routes', done => {
        const req = new DummyRequest({
            method: 'PATCH',
            resourceType: 'bucket',
            subresource: 'veeam',
            apiMethod: 'routeVeeam',
            url: '/bucket/veeam',
        });
        req.method = 'PATCH';
        routeVeeam.routeVeeam('127.0.0.1', req, {
            setHeader: () => {},
            writeHead: () => {},
            end: data => {
                assert(data.includes('MethodNotAllowed'));
                done();
            },
            headersSent: false,
        }, log);
    });
});
