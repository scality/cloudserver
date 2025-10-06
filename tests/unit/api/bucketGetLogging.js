const assert = require('assert');
const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutLogging = require('../../../lib/api/bucketPutLogging');
const bucketGetLogging = require('../../../lib/api/bucketGetLogging');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const { parseString } = require('xml2js');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const otherAuthInfo = makeAuthInfo('accessKey2');
const bucketName = 'bucketgetloggingtest';
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

function createGetLoggingRequest(bucketName, headers = {}) {
    return {
        bucketName,
        namespace,
        headers: {
            host: `${bucketName}.s3.amazonaws.com`,
            ...headers,
        },
        url: '/?logging',
        query: { logging: '' },
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

describe('bucketGetLogging API', () => {
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

    it('should return empty BucketLoggingStatus when logging is not configured', done => {
        const request = createGetLoggingRequest(bucketName);

        bucketGetLogging(authInfo, request, log, (err, xml) => {
            assert.ifError(err);
            assert(xml);
            // Should return empty BucketLoggingStatus
            const expectedXML = '<?xml version="1.0" encoding="UTF-8"?>\n' +
                '<BucketLoggingStatus xmlns="http://doc.s3.amazonaws.com/2006-03-01" />';
            assert.strictEqual(xml, expectedXML);
            done();
        });
    });

    it('should return logging configuration when logging is enabled', done => {
        const loggingXML = createValidLoggingXML(targetBucket, 'logs/');
        const putRequest = createLoggingRequest(bucketName, loggingXML);

        // First, set logging configuration
        bucketPutLogging(authInfo, putRequest, log, err => {
            assert.ifError(err);

            // Now get the logging configuration
            const getRequest = createGetLoggingRequest(bucketName);
            bucketGetLogging(authInfo, getRequest, log, (err, xml) => {
                assert.ifError(err);
                assert(xml);
                // Verify the XML contains the expected values
                assert(xml.includes('<LoggingEnabled>'));
                assert(xml.includes(`<TargetBucket>${targetBucket}</TargetBucket>`));
                assert(xml.includes('<TargetPrefix>logs/</TargetPrefix>'));
                done();
            });
        });
    });

    it('should return logging configuration with custom TargetPrefix', done => {
        const customPrefix = 'my-app-logs/2025/';
        const loggingXML = createValidLoggingXML(targetBucket, customPrefix);
        const putRequest = createLoggingRequest(bucketName, loggingXML);

        bucketPutLogging(authInfo, putRequest, log, err => {
            assert.ifError(err);

            const getRequest = createGetLoggingRequest(bucketName);
            bucketGetLogging(authInfo, getRequest, log, (err, xml) => {
                assert.ifError(err);
                assert(xml);
                assert(xml.includes(`<TargetPrefix>${customPrefix}</TargetPrefix>`));
                done();
            });
        });
    });

    it('should return logging configuration with empty TargetPrefix', done => {
        const loggingXML = createValidLoggingXML(targetBucket, '');
        const putRequest = createLoggingRequest(bucketName, loggingXML);

        bucketPutLogging(authInfo, putRequest, log, err => {
            assert.ifError(err);

            const getRequest = createGetLoggingRequest(bucketName);
            bucketGetLogging(authInfo, getRequest, log, (err, xml) => {
                assert.ifError(err);
                assert(xml);
                assert(xml.includes('<TargetPrefix></TargetPrefix>'));
                done();
            });
        });
    });

    it('should return empty status after logging is disabled', done => {
        const loggingXML = createValidLoggingXML(targetBucket);
        const putRequest = createLoggingRequest(bucketName, loggingXML);

        // First enable logging
        bucketPutLogging(authInfo, putRequest, log, err => {
            assert.ifError(err);

            // Disable logging
            const disableXML = '<?xml version="1.0" encoding="UTF-8"?>' +
                '<BucketLoggingStatus xmlns="http://doc.s3.amazonaws.com/2006-03-01" />';
            const disableRequest = createLoggingRequest(bucketName, disableXML);

            bucketPutLogging(authInfo, disableRequest, log, err => {
                assert.ifError(err);

                // Get logging status - should be empty
                const getRequest = createGetLoggingRequest(bucketName);
                bucketGetLogging(authInfo, getRequest, log, (err, xml) => {
                    assert.ifError(err);
                    assert(xml);

                    parseString(xml, { explicitArray: false }, (err, result) => {
                        assert.ifError(err);
                        // result should have BucketLoggingStatus, but no LoggingEnabled
                        assert(result);
                        assert(result.BucketLoggingStatus);
                        assert.strictEqual(result.BucketLoggingStatus.LoggingEnabled, undefined);
                        done();
                    });
                });
            });
        });
    });

    it('should validate expected bucket owner header - matching account', done => {
        const loggingXML = createValidLoggingXML(targetBucket);
        const putRequest = createLoggingRequest(bucketName, loggingXML);

        bucketPutLogging(authInfo, putRequest, log, err => {
            assert.ifError(err);

            // The in-memory backend has account ID '123456789012' hardcoded
            const accountId = '123456789012';
            const getRequest = createGetLoggingRequest(bucketName, {
                'x-amz-expected-bucket-owner': accountId,
            });

            bucketGetLogging(authInfo, getRequest, log, (err, xml) => {
                assert.ifError(err);
                assert(xml);
                done();
            });
        });
    });

    it('should return error for mismatched expected bucket owner', done => {
        const wrongAccountId = '999999999999';
        const getRequest = createGetLoggingRequest(bucketName, {
            'x-amz-expected-bucket-owner': wrongAccountId,
        });

        bucketGetLogging(authInfo, getRequest, log, err => {
            assert(err);
            assert.strictEqual(err.is.AccessDenied, true);
            done();
        });
    });

    it('should return error for unauthorized access', done => {
        const loggingXML = createValidLoggingXML(targetBucket);
        const putRequest = createLoggingRequest(bucketName, loggingXML);

        // Set logging with owner
        bucketPutLogging(authInfo, putRequest, log, err => {
            assert.ifError(err);

            // Try to get logging with different auth
            const getRequest = createGetLoggingRequest(bucketName);
            bucketGetLogging(otherAuthInfo, getRequest, log, err => {
                assert(err);
                assert.strictEqual(err.is.MethodNotAllowed, true);
                done();
            });
        });
    });

    it('should return error for non-existent bucket', done => {
        const getRequest = createGetLoggingRequest('nonexistentbucket');

        bucketGetLogging(authInfo, getRequest, log, err => {
            assert(err);
            assert.strictEqual(err.is.NoSuchBucket, true);
            done();
        });
    });

    it('should return well-formed XML response', done => {
        const loggingXML = createValidLoggingXML(targetBucket);
        const putRequest = createLoggingRequest(bucketName, loggingXML);

        bucketPutLogging(authInfo, putRequest, log, err => {
            assert.ifError(err);

            const getRequest = createGetLoggingRequest(bucketName);
            bucketGetLogging(authInfo, getRequest, log, (err, xml) => {
                assert.ifError(err);
                assert(xml);
                // Try to parse the XML, if no error, it's well-formed
                parseString(xml, (parseErr, result) => {
                    assert.strictEqual(parseErr, null, `XML is not well-formed: ${parseErr}`);
                    assert(result);
                    done();
                });
            });
        });
    });
});
