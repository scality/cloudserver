const assert = require('assert');
const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutLogging = require('../../../lib/api/bucketPutLogging');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const metadata = require('../metadataswitch');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const otherAuthInfo = makeAuthInfo('accessKey2');
const bucketName = 'bucketputloggingtest';
const targetBucket = 'loggingbucket';
const namespace = 'default';

const testBucketPutRequest = {
    bucketName,
    namespace,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
    actionImplicitDenies: false,
};

const testTargetBucketPutRequest = {
    bucketName: targetBucket,
    namespace,
    headers: { host: `${targetBucket}.s3.amazonaws.com` },
    url: '/',
    actionImplicitDenies: false,
};

function createLoggingRequest(bucketName, post, headers = {}) {
    return {
        bucketName,
        namespace,
        headers: {
            host: `${bucketName}.s3.amazonaws.com`,
            ...headers,
        },
        url: '/?logging',
        query: { logging: '' },
        post,
        actionImplicitDenies: false,
    };
}

function createValidLoggingXML(targetBucket, targetPrefix = 'logs/') {
    return '<?xml version="1.0" encoding="UTF-8"?>' +
        '<BucketLoggingStatus xmlns="http://doc.s3.amazonaws.com/2006-03-01">' +
        '<LoggingEnabled>' +
        `<TargetBucket>${targetBucket}</TargetBucket>` +
        `<TargetPrefix>${targetPrefix}</TargetPrefix>` +
        '</LoggingEnabled>' +
        '</BucketLoggingStatus>';
}

function createEmptyLoggingXML() {
    return '<?xml version="1.0" encoding="UTF-8"?>' +
        '<BucketLoggingStatus xmlns="http://doc.s3.amazonaws.com/2006-03-01" />';
}

describe('bucketPutLogging API', () => {
    beforeEach(done => {
        cleanup();
        return bucketPut(authInfo, testBucketPutRequest, log, err => {
            if (err) {
                return done(err);
            }
            return bucketPut(authInfo, testTargetBucketPutRequest, log, done);
        });
    });

    afterEach(() => cleanup());

    it('should set logging configuration on bucket', done => {
        const loggingXML = createValidLoggingXML(targetBucket);
        const request = createLoggingRequest(bucketName, loggingXML);

        bucketPutLogging(authInfo, request, log, err => {
            assert.ifError(err);
            metadata.getBucket(bucketName, log, (err, bucket) => {
                assert.ifError(err);
                const loggingConfig = bucket.getBucketLoggingStatus();
                assert(loggingConfig);
                assert.strictEqual(loggingConfig.getLoggingEnabled().TargetBucket, targetBucket);
                assert.strictEqual(loggingConfig.getLoggingEnabled().TargetPrefix, 'logs/');
                done();
            });
        });
    });

    it('should disable logging with empty BucketLoggingStatus', done => {
        // First enable logging
        const enableLoggingXML = createValidLoggingXML(targetBucket);
        const enableRequest = createLoggingRequest(bucketName, enableLoggingXML);

        bucketPutLogging(authInfo, enableRequest, log, err => {
            assert.ifError(err);

            // Now disable logging
            const disableLoggingXML = createEmptyLoggingXML();
            const disableRequest = createLoggingRequest(bucketName, disableLoggingXML);

            bucketPutLogging(authInfo, disableRequest, log, err => {
                assert.ifError(err);
                metadata.getBucket(bucketName, log, (err, bucket) => {
                    assert.ifError(err);
                    const loggingConfig = bucket.getBucketLoggingStatus();
                    assert(loggingConfig);
                    // Empty config should have no LoggingEnabled
                    assert.strictEqual(loggingConfig.getLoggingEnabled(), undefined);
                    done();
                });
            });
        });
    });

    it('should validate expected bucket owner header - matching account', done => {
        const loggingXML = createValidLoggingXML(targetBucket);
        // The in-memory backend has account ID '123456789012' hardcoded
        const accountId = '123456789012';
        const request = createLoggingRequest(bucketName, loggingXML, {
            'x-amz-expected-bucket-owner': accountId,
        });

        bucketPutLogging(authInfo, request, log, err => {
            assert.ifError(err);
            done();
        });
    });

    it('should return error for mismatched expected bucket owner', done => {
        const loggingXML = createValidLoggingXML(targetBucket);
        const wrongAccountId = '999999999999';
        const request = createLoggingRequest(bucketName, loggingXML, {
            'x-amz-expected-bucket-owner': wrongAccountId,
        });

        bucketPutLogging(authInfo, request, log, err => {
            assert(err);
            assert.strictEqual(err.is.AccessDenied, true);
            done();
        });
    });

    it('should handle empty request body', done => {
        const request = createLoggingRequest(bucketName, '');

        bucketPutLogging(authInfo, request, log, err => {
            assert(err);
            assert.strictEqual(err.is.MalformedXML, true);
            done();
        });
    });

    it('should handle missing request body', done => {
        const request = createLoggingRequest(bucketName, undefined);

        bucketPutLogging(authInfo, request, log, err => {
            assert(err);
            // Missing body should trigger XML parsing error
            assert(err.is.MalformedXML);
            done();
        });
    });

    it('should return error for unauthorized access', done => {
        const loggingXML = createValidLoggingXML(targetBucket);
        const request = createLoggingRequest(bucketName, loggingXML);

        // Try to set logging with different auth
        bucketPutLogging(otherAuthInfo, request, log, err => {
            assert(err);
            assert.strictEqual(err.is.MethodNotAllowed, true);
            done();
        });
    });

    it('should return error for non-existent bucket', done => {
        const loggingXML = createValidLoggingXML(targetBucket);
        const request = createLoggingRequest('nonexistentbucket', loggingXML);

        bucketPutLogging(authInfo, request, log, err => {
            assert(err);
            assert.strictEqual(err.is.NoSuchBucket, true);
            done();
        });
    });

    it('should return error for malformed XML - missing closing tag', done => {
        const malformedXML = '<?xml version="1.0" encoding="UTF-8"?>' +
            '<BucketLoggingStatus xmlns="http://doc.s3.amazonaws.com/2006-03-01">' +
            '<LoggingEnabled>' +
            `<TargetBucket>${targetBucket}</TargetBucket>` +
            '<TargetPrefix>logs/</TargetPrefix>' +
            // Missing </LoggingEnabled> and </BucketLoggingStatus>
            '';
        const request = createLoggingRequest(bucketName, malformedXML);

        bucketPutLogging(authInfo, request, log, err => {
            assert(err);
            assert.strictEqual(err.is.MalformedXML, true);
            done();
        });
    });

    it('should return error for malformed XML - invalid structure', done => {
        const malformedXML = '<?xml version="1.0" encoding="UTF-8"?>' +
            '<BucketLoggingStatus xmlns="http://doc.s3.amazonaws.com/2006-03-01">' +
            '<LoggingEnabled>' +
            '<InvalidTag>invalid</InvalidTag>' + // Invalid tag
            '</LoggingEnabled>' +
            '</BucketLoggingStatus>';
        const request = createLoggingRequest(bucketName, malformedXML);

        bucketPutLogging(authInfo, request, log, err => {
            assert(err);
            // Should fail validation
            assert(err.is.MalformedXML);
            done();
        });
    });

    it('should return error for malformed XML - not XML at all', done => {
        const malformedXML = 'This is not XML at all';
        const request = createLoggingRequest(bucketName, malformedXML);

        bucketPutLogging(authInfo, request, log, err => {
            assert(err);
            assert.strictEqual(err.is.MalformedXML, true);
            done();
        });
    });

    it('should return NotImplemented error when TargetGrants is present', done => {
        const loggingXMLWithGrants = '<?xml version="1.0" encoding="UTF-8"?>' +
            '<BucketLoggingStatus xmlns="http://doc.s3.amazonaws.com/2006-03-01">' +
            '<LoggingEnabled>' +
            `<TargetBucket>${targetBucket}</TargetBucket>` +
            '<TargetPrefix>logs/</TargetPrefix>' +
            '<TargetGrants>' +
            '<Grant>' +
            '<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" ' +
            'xsi:type="CanonicalUser">' +
            '<ID>79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be</ID>' +
            '<DisplayName>GranteeDisplayName</DisplayName>' +
            '</Grantee>' +
            '<Permission>READ</Permission>' +
            '</Grant>' +
            '</TargetGrants>' +
            '</LoggingEnabled>' +
            '</BucketLoggingStatus>';
        const request = createLoggingRequest(bucketName, loggingXMLWithGrants);

        bucketPutLogging(authInfo, request, log, err => {
            assert(err);
            assert.strictEqual(err.is.NotImplemented, true);
            done();
        });
    });

    it('should handle logging with custom TargetPrefix', done => {
        const customPrefix = 'my-app-logs/2025/';
        const loggingXML = createValidLoggingXML(targetBucket, customPrefix);
        const request = createLoggingRequest(bucketName, loggingXML);

        bucketPutLogging(authInfo, request, log, err => {
            assert.ifError(err);
            metadata.getBucket(bucketName, log, (err, bucket) => {
                assert.ifError(err);
                const loggingConfig = bucket.getBucketLoggingStatus();
                assert(loggingConfig);
                assert(loggingConfig.getLoggingEnabled());
                assert.strictEqual(loggingConfig.getLoggingEnabled().TargetPrefix, customPrefix);
                done();
            });
        });
    });

    it('should handle logging with empty TargetPrefix', done => {
        const loggingXML = createValidLoggingXML(targetBucket, '');
        const request = createLoggingRequest(bucketName, loggingXML);

        bucketPutLogging(authInfo, request, log, err => {
            assert.ifError(err);
            metadata.getBucket(bucketName, log, (err, bucket) => {
                assert.ifError(err);
                const loggingConfig = bucket.getBucketLoggingStatus();
                assert(loggingConfig);
                assert(loggingConfig.getLoggingEnabled());
                assert.strictEqual(loggingConfig.getLoggingEnabled().TargetPrefix, '');
                done();
            });
        });
    });

    it('should reject TargetPrefix if target bucket does not exist', done => {
        const loggingXML = createValidLoggingXML('non-existing-bucket', '');
        const request = createLoggingRequest(bucketName, loggingXML);

        bucketPutLogging(authInfo, request, log, err => {
            assert(err);
            assert.strictEqual(err.is.InvalidTargetBucketForLogging, true);
            done();
        });
    });
});
