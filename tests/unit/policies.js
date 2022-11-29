const assert = require('assert');
const DummyRequest = require('./DummyRequest');
const { authBucketPut } = require('../../lib/api/bucketPut');

function prepareDummyRequest(headers = {}) {
    const request = new DummyRequest({
        hostname: 'localhost',
        port: 80,
        headers,
        socket: {
            remoteAddress: '0.0.0.0',
        },
    });
    return request;
}

describe('Policies: permission checks for S3 APIs', () => {
    describe('PutBucket', () => {
        function putBucketApiMethods(headers) {
            const request = prepareDummyRequest(headers);
            const result = authBucketPut(null, 'name', null, request, null);
            return result.map(req => req.apiMethod);
        }

        it('should return s3:PutBucket without any provided header', () => {
            assert.deepStrictEqual(
                putBucketApiMethods(),
                ['bucketPut'],
            );
        });

        it('should return s3:PutBucket and s3:PutBucketObjectLockConfiguration with ACL headers', () => {
            assert.deepStrictEqual(
                putBucketApiMethods({ 'x-amz-bucket-object-lock-enabled': 'true' }),
                ['bucketPut', 'bucketPutObjectLock', 'bucketPutVersioning'],
            );
        });
    });
});
