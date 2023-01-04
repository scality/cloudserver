const assert = require('assert');
const { DummyRequestLogger } = require('../helpers');
const routeVeeam = require('../../../lib/routes/routeVeeam');

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
