const assert = require('assert');
const sinon = require('sinon');
const {
    logServerAccess,
    setServerAccessLogger,
    getRemoteIPFromRequest,
    getOperation,
    getRequester,
    getURI,
    getObjectSize,
    getBytesSent,
    calculateTotalTime,
    calculateTurnAroundTime,
    timestampToDateTime643,
} = require('../../../lib/utilities/serverAccessLogger');

describe('serverAccessLogger utility functions', () => {
    describe('getRemoteIPFromRequest', () => {
        it('should return IP from x-forwarded-for header', () => {
            const request = {
                headers: {
                    'x-forwarded-for': '192.168.1.100',
                },
                connection: { remoteAddress: '10.0.0.1' },
            };
            const result = getRemoteIPFromRequest(request);
            assert.strictEqual(result, '192.168.1.100');
        });

        it('should return first IP from comma-separated x-forwarded-for', () => {
            const request = {
                headers: {
                    'x-forwarded-for': '192.168.1.100, 10.0.0.2, 10.0.0.3',
                },
                connection: { remoteAddress: '10.0.0.1' },
            };
            const result = getRemoteIPFromRequest(request);
            assert.strictEqual(result, '192.168.1.100');
        });

        it('should return IP from x-real-ip header when x-forwarded-for is not present', () => {
            const request = {
                headers: {
                    'x-real-ip': '172.16.0.50',
                },
                connection: { remoteAddress: '10.0.0.1' },
            };
            const result = getRemoteIPFromRequest(request);
            assert.strictEqual(result, '172.16.0.50');
        });

        it('should return IP from x-client-ip header', () => {
            const request = {
                headers: {
                    'x-client-ip': '203.0.113.45',
                },
                connection: { remoteAddress: '10.0.0.1' },
            };
            const result = getRemoteIPFromRequest(request);
            assert.strictEqual(result, '203.0.113.45');
        });

        it('should return IP from cf-connecting-ip header (Cloudflare)', () => {
            const request = {
                headers: {
                    'cf-connecting-ip': '198.51.100.23',
                },
                connection: { remoteAddress: '10.0.0.1' },
            };
            const result = getRemoteIPFromRequest(request);
            assert.strictEqual(result, '198.51.100.23');
        });

        it('should fallback to connection.remoteAddress', () => {
            const request = {
                headers: {},
                connection: { remoteAddress: '10.0.0.1' },
            };
            const result = getRemoteIPFromRequest(request);
            assert.strictEqual(result, '10.0.0.1');
        });

        it('should fallback to socket.remoteAddress', () => {
            const request = {
                headers: {},
                socket: { remoteAddress: '172.31.0.100' },
            };
            const result = getRemoteIPFromRequest(request);
            assert.strictEqual(result, '172.31.0.100');
        });

        it('should fallback to request.ip', () => {
            const request = {
                headers: {},
                ip: '192.0.2.42',
            };
            const result = getRemoteIPFromRequest(request);
            assert.strictEqual(result, '192.0.2.42');
        });

        it('should return null when no IP is available', () => {
            const request = {
                headers: {},
            };
            const result = getRemoteIPFromRequest(request);
            assert.strictEqual(result, null);
        });

        it('should prefer x-forwarded-for over connection.remoteAddress', () => {
            const request = {
                headers: {
                    'x-forwarded-for': '192.168.1.100',
                },
                connection: { remoteAddress: '10.0.0.1' },
            };
            const result = getRemoteIPFromRequest(request);
            assert.strictEqual(result, '192.168.1.100');
        });
    });

    describe('getOperation', () => {
        it('should return REST.GET.OBJECT for objectGet', () => {
            const req = { method: 'GET', apiMethod: 'objectGet' };
            const result = getOperation(req);
            assert.strictEqual(result, 'REST.GET.OBJECT');
        });

        it('should return REST.PUT.OBJECT for objectPut', () => {
            const req = { method: 'PUT', apiMethod: 'objectPut' };
            const result = getOperation(req);
            assert.strictEqual(result, 'REST.PUT.OBJECT');
        });

        it('should return REST.DELETE.OBJECT for objectDelete', () => {
            const req = { method: 'DELETE', apiMethod: 'objectDelete' };
            const result = getOperation(req);
            assert.strictEqual(result, 'REST.DELETE.OBJECT');
        });

        it('should return REST.GET.BUCKET for bucketGet', () => {
            const req = { method: 'GET', apiMethod: 'bucketGet' };
            const result = getOperation(req);
            assert.strictEqual(result, 'REST.GET.BUCKET');
        });

        it('should return REST.PUT.BUCKET for bucketPut', () => {
            const req = { method: 'PUT', apiMethod: 'bucketPut' };
            const result = getOperation(req);
            assert.strictEqual(result, 'REST.PUT.BUCKET');
        });

        it('should return REST.DELETE.BUCKET for bucketDelete', () => {
            const req = { method: 'DELETE', apiMethod: 'bucketDelete' };
            const result = getOperation(req);
            assert.strictEqual(result, 'REST.DELETE.BUCKET');
        });

        it('should return REST.GET.VERSIONING for bucketGetVersioning', () => {
            const req = { method: 'GET', apiMethod: 'bucketGetVersioning' };
            const result = getOperation(req);
            assert.strictEqual(result, 'REST.GET.VERSIONING');
        });

        it('should return REST.PUT.VERSIONING for bucketPutVersioning', () => {
            const req = { method: 'PUT', apiMethod: 'bucketPutVersioning' };
            const result = getOperation(req);
            assert.strictEqual(result, 'REST.PUT.VERSIONING');
        });

        it('should return REST.PUT.BUCKETPOLICY for bucketPutPolicy', () => {
            const req = { method: 'PUT', apiMethod: 'bucketPutPolicy' };
            const result = getOperation(req);
            assert.strictEqual(result, 'REST.PUT.BUCKETPOLICY');
        });

        it('should return REST.GET.LOGGING_STATUS for bucketGetLogging', () => {
            const req = { method: 'GET', apiMethod: 'bucketGetLogging' };
            const result = getOperation(req);
            assert.strictEqual(result, 'REST.GET.LOGGING_STATUS');
        });

        it('should return REST.POST.UPLOAD for completeMultipartUpload', () => {
            const req = { method: 'POST', apiMethod: 'completeMultipartUpload' };
            const result = getOperation(req);
            assert.strictEqual(result, 'REST.POST.UPLOAD');
        });

        it('should return REST.method.UNKNOWN for unknown apiMethod', () => {
            const req = { method: 'GET', apiMethod: 'unknownMethod' };
            const result = getOperation(req);
            assert.strictEqual(result, 'REST.GET.UNKNOWN');
        });

        it('should handle missing apiMethod', () => {
            const req = { method: 'GET' };
            const result = getOperation(req);
            assert.strictEqual(result, 'REST.GET.UNKNOWN');
        });

        it('should return REST.GET.BACKBEAT when backbeat is enabled for GET', () => {
            const req = {
                method: 'GET',
                apiMethod: 'objectGet',
                serverAccessLog: { backbeat: true },
            };
            const result = getOperation(req);
            assert.strictEqual(result, 'REST.GET.BACKBEAT');
        });

        it('should return REST.PUT.BACKBEAT when backbeat is enabled for PUT', () => {
            const req = {
                method: 'PUT',
                apiMethod: 'objectPut',
                serverAccessLog: { backbeat: true },
            };
            const result = getOperation(req);
            assert.strictEqual(result, 'REST.PUT.BACKBEAT');
        });

        it('should return REST.DELETE.BACKBEAT when backbeat is enabled for DELETE', () => {
            const req = {
                method: 'DELETE',
                apiMethod: 'objectDelete',
                serverAccessLog: { backbeat: true },
            };
            const result = getOperation(req);
            assert.strictEqual(result, 'REST.DELETE.BACKBEAT');
        });

        it('should return REST.POST.BACKBEAT when backbeat is enabled for POST', () => {
            const req = {
                method: 'POST',
                apiMethod: 'completeMultipartUpload',
                serverAccessLog: { backbeat: true },
            };
            const result = getOperation(req);
            assert.strictEqual(result, 'REST.POST.BACKBEAT');
        });

        it('should prioritize backbeat over normal apiMethod mapping', () => {
            const req = {
                method: 'GET',
                apiMethod: 'bucketGetVersioning',
                serverAccessLog: { backbeat: true },
            };
            const result = getOperation(req);
            // Should return BACKBEAT instead of normal REST.GET.VERSIONING
            assert.strictEqual(result, 'REST.GET.BACKBEAT');
        });

        it('should return REST.method.BACKBEAT even with unknown apiMethod', () => {
            const req = {
                method: 'GET',
                apiMethod: 'unknownMethod',
                serverAccessLog: { backbeat: true },
            };
            const result = getOperation(req);
            // Should return BACKBEAT instead of UNKNOWN
            assert.strictEqual(result, 'REST.GET.BACKBEAT');
        });
    });

    describe('getRequester', () => {
        it('should return null for public user', () => {
            const authInfo = {
                isRequesterPublicUser: () => true,
            };
            const result = getRequester(authInfo);
            assert.strictEqual(result, null);
        });

        it('should return IAM user name with account for IAM user', () => {
            const authInfo = {
                isRequesterPublicUser: () => false,
                isRequesterAnIAMUser: () => true,
                getIAMdisplayName: () => 'iamUser',
                getAccountDisplayName: () => 'accountName',
                getCanonicalID: () => 'canonicalID123',
            };
            const result = getRequester(authInfo);
            assert.strictEqual(result, 'iamUser:accountName');
        });

        it('should return canonical ID for IAM user if display names are missing', () => {
            const authInfo = {
                isRequesterPublicUser: () => false,
                isRequesterAnIAMUser: () => true,
                getIAMdisplayName: () => '',
                getAccountDisplayName: () => 'accountName',
                getCanonicalID: () => 'canonicalID123',
            };
            const result = getRequester(authInfo);
            assert.strictEqual(result, 'canonicalID123');
        });

        it('should return canonical ID for regular user', () => {
            const authInfo = {
                isRequesterPublicUser: () => false,
                isRequesterAnIAMUser: () => false,
                getCanonicalID: () => 'canonicalID456',
            };
            const result = getRequester(authInfo);
            assert.strictEqual(result, 'canonicalID456');
        });

        it('should return null for null authInfo', () => {
            const result = getRequester(null);
            assert.strictEqual(result, null);
        });

        it('should return null for undefined authInfo', () => {
            const result = getRequester(undefined);
            assert.strictEqual(result, null);
        });

        it('should return null if authInfo has no getCanonicalID method', () => {
            const authInfo = {
                isRequesterPublicUser: () => false,
                isRequesterAnIAMUser: () => false,
            };
            const result = getRequester(authInfo);
            assert.strictEqual(result, null);
        });
    });

    describe('getURI', () => {
        it('should return formatted URI with method, url, and HTTP version', () => {
            const request = {
                method: 'GET',
                url: '/bucket/key',
                httpVersion: '1.1',
            };
            const result = getURI(request);
            assert.strictEqual(result, 'GET /bucket/key HTTP/1.1');
        });

        it('should default to UNKNOWN for missing method', () => {
            const request = {
                url: '/bucket/key',
                httpVersion: '1.1',
            };
            const result = getURI(request);
            assert.strictEqual(result, 'UNKNOWN /bucket/key HTTP/1.1');
        });

        it('should default to / for missing url', () => {
            const request = {
                method: 'GET',
                httpVersion: '1.1',
            };
            const result = getURI(request);
            assert.strictEqual(result, 'GET / HTTP/1.1');
        });

        it('should default to 1.1 for missing httpVersion', () => {
            const request = {
                method: 'PUT',
                url: '/bucket',
            };
            const result = getURI(request);
            assert.strictEqual(result, 'PUT /bucket HTTP/1.1');
        });

        it('should return null for null request', () => {
            const result = getURI(null);
            assert.strictEqual(result, null);
        });

        it('should return null for undefined request', () => {
            const result = getURI(undefined);
            assert.strictEqual(result, null);
        });

        it('should handle HTTP/2', () => {
            const request = {
                method: 'GET',
                url: '/bucket/key',
                httpVersion: '2.0',
            };
            const result = getURI(request);
            assert.strictEqual(result, 'GET /bucket/key HTTP/2.0');
        });
    });

    describe('getObjectSize', () => {
        it('should return Content-Length from response for objectGet', () => {
            const request = { apiMethod: 'objectGet' };
            const response = {
                getHeader: name => name === 'Content-Length' ? '12345' : null,
            };
            const result = getObjectSize(request, response);
            assert.strictEqual(result, 12345);
        });

        it('should return Content-Length from request for objectPut', () => {
            const request = {
                apiMethod: 'objectPut',
                headers: { 'content-length': '54321' },
            };
            const response = {
                getHeader: () => null,
            };
            const result = getObjectSize(request, response);
            assert.strictEqual(result, 54321);
        });

        it('should return Content-Length from request for objectPutPart', () => {
            const request = {
                apiMethod: 'objectPutPart',
                headers: { 'content-length': '67890' },
            };
            const response = {
                getHeader: () => null,
            };
            const result = getObjectSize(request, response);
            assert.strictEqual(result, 67890);
        });

        it('should handle Content-Length of 0 for objectGet', () => {
            const request = { apiMethod: 'objectGet' };
            const response = {
                getHeader: name => name === 'Content-Length' ? '0' : null,
            };
            const result = getObjectSize(request, response);
            assert.strictEqual(result, 0);
        });

        it('should handle Content-Length of number 0 for objectGet', () => {
            const request = { apiMethod: 'objectGet' };
            const response = {
                getHeader: name => name === 'Content-Length' ? 0 : null,
            };
            const result = getObjectSize(request, response);
            assert.strictEqual(result, 0);
        });

        it('should handle Content-Length of 0 for objectPut', () => {
            const request = {
                apiMethod: 'objectPut',
                headers: { 'content-length': '0' },
            };
            const response = {
                getHeader: () => null,
            };
            const result = getObjectSize(request, response);
            assert.strictEqual(result, 0);
        });

        it('should handle Content-Length of number 0 for objectPut', () => {
            const request = {
                apiMethod: 'objectPut',
                headers: { 'content-length': 0 },
            };
            const response = {
                getHeader: () => null,
            };
            const result = getObjectSize(request, response);
            assert.strictEqual(result, 0);
        });

        it('should return null for objectGet with no Content-Length', () => {
            const request = { apiMethod: 'objectGet' };
            const response = {
                getHeader: () => null,
            };
            const result = getObjectSize(request, response);
            assert.strictEqual(result, null);
        });

        it('should return null for objectPut with no Content-Length', () => {
            const request = {
                apiMethod: 'objectPut',
                headers: {},
            };
            const response = {
                getHeader: () => null,
            };
            const result = getObjectSize(request, response);
            assert.strictEqual(result, null);
        });

        it('should return null for non-object operations', () => {
            const request = { apiMethod: 'bucketGet' };
            const response = {
                getHeader: () => '1000',
            };
            const result = getObjectSize(request, response);
            assert.strictEqual(result, null);
        });

        it('should return null for null request', () => {
            const response = {
                getHeader: () => '1000',
            };
            const result = getObjectSize(null, response);
            assert.strictEqual(result, null);
        });

        it('should return null for null response', () => {
            const request = { apiMethod: 'objectGet' };
            const result = getObjectSize(request, null);
            assert.strictEqual(result, null);
        });
    });

    describe('getBytesSent', () => {
        it('should return bytesSent when provided', () => {
            const res = null;
            const bytesSent = 12345;
            const result = getBytesSent(res, bytesSent);
            assert.strictEqual(result, 12345);
        });

        it('should return Content-Length from response when bytesSent is not provided', () => {
            const res = {
                getHeader: name => name === 'Content-Length' ? '67890' : null,
            };
            const result = getBytesSent(res, null);
            assert.strictEqual(result, '67890');
        });

        it('should return null when bytesSent is not provided and response is null', () => {
            const result = getBytesSent(null, null);
            assert.strictEqual(result, null);
        });

        it('should return null when bytesSent is not provided and Content-Length is missing', () => {
            const res = {
                getHeader: () => null,
            };
            const result = getBytesSent(res, null);
            assert.strictEqual(result, null);
        });

        it('should prefer bytesSent over Content-Length', () => {
            const res = {
                getHeader: name => name === 'Content-Length' ? '99999' : null,
            };
            const bytesSent = 11111;
            const result = getBytesSent(res, bytesSent);
            assert.strictEqual(result, 11111);
        });

        it('should handle bytesSent as 0', () => {
            const res = {
                getHeader: name => name === 'Content-Length' ? '99999' : null,
            };
            const bytesSent = 0;
            const result = getBytesSent(res, bytesSent);
            assert.strictEqual(result, 0);
        });

        it('should handle Content-Length as number 0', () => {
            const res = {
                getHeader: name => name === 'Content-Length' ? 0 : null,
            };
            const result = getBytesSent(res, null);
            assert.strictEqual(result, 0);
        });

        it('should handle Content-Length as string "0"', () => {
            const res = {
                getHeader: name => name === 'Content-Length' ? '0' : null,
            };
            const result = getBytesSent(res, null);
            assert.strictEqual(result, '0');
        });
    });

    describe('calculateTotalTime', () => {
        it('should calculate time difference in milliseconds', () => {
            const startTime = 1000000000n;
            const endTime = 1005000000n;
            const result = calculateTotalTime(startTime, endTime);
            assert.strictEqual(result, '5');
        });

        it('should handle large time differences', () => {
            const startTime = 0n;
            const endTime = 10000000000n;
            const result = calculateTotalTime(startTime, endTime);
            assert.strictEqual(result, '10000');
        });

        it('should handle small time differences', () => {
            const startTime = 1000000n;
            const endTime = 1000100n;
            const result = calculateTotalTime(startTime, endTime);
            assert.strictEqual(result, '0');
        });

        it('should return null when startTime is null', () => {
            const endTime = 1000000000n;
            const result = calculateTotalTime(null, endTime);
            assert.strictEqual(result, null);
        });

        it('should return null when endTime is null', () => {
            const startTime = 1000000000n;
            const result = calculateTotalTime(startTime, null);
            assert.strictEqual(result, null);
        });

        it('should return null when both times are null', () => {
            const result = calculateTotalTime(null, null);
            assert.strictEqual(result, null);
        });

        it('should return null when startTime is undefined', () => {
            const endTime = 1000000000n;
            const result = calculateTotalTime(undefined, endTime);
            assert.strictEqual(result, null);
        });

        it('should handle exactly 1 second', () => {
            const startTime = 0n;
            const endTime = 1000000n;
            const result = calculateTotalTime(startTime, endTime);
            assert.strictEqual(result, '1');
        });
    });

    describe('calculateTurnAroundTime', () => {
        it('should calculate time difference in milliseconds', () => {
            const startTurnAroundTime = 2000000000n;
            const endTurnAroundTime = 2007500000n;
            const result = calculateTurnAroundTime(startTurnAroundTime, endTurnAroundTime);
            assert.strictEqual(result, '7');
        });

        it('should handle large time differences', () => {
            const startTurnAroundTime = 0n;
            const endTurnAroundTime = 100000000000n;
            const result = calculateTurnAroundTime(startTurnAroundTime, endTurnAroundTime);
            assert.strictEqual(result, '100000');
        });

        it('should handle small time differences', () => {
            const startTurnAroundTime = 5000000n;
            const endTurnAroundTime = 5000500n;
            const result = calculateTurnAroundTime(startTurnAroundTime, endTurnAroundTime);
            assert.strictEqual(result, '0');
        });

        it('should return null when startTurnAroundTime is null', () => {
            const endTurnAroundTime = 1000000000n;
            const result = calculateTurnAroundTime(null, endTurnAroundTime);
            assert.strictEqual(result, null);
        });

        it('should return null when endTurnAroundTime is null', () => {
            const startTurnAroundTime = 1000000000n;
            const result = calculateTurnAroundTime(startTurnAroundTime, null);
            assert.strictEqual(result, null);
        });

        it('should return null when both times are null', () => {
            const result = calculateTurnAroundTime(null, null);
            assert.strictEqual(result, null);
        });

        it('should return null when startTurnAroundTime is undefined', () => {
            const endTurnAroundTime = 1000000000n;
            const result = calculateTurnAroundTime(undefined, endTurnAroundTime);
            assert.strictEqual(result, null);
        });

        it('should handle exactly 1 millisecond', () => {
            const startTurnAroundTime = 0n;
            const endTurnAroundTime = 1000000n;
            const result = calculateTurnAroundTime(startTurnAroundTime, endTurnAroundTime);
            assert.strictEqual(result, '1');
        });

        it('should handle fractional milliseconds (truncated)', () => {
            const startTurnAroundTime = 0n;
            const endTurnAroundTime = 1500000n;
            const result = calculateTurnAroundTime(startTurnAroundTime, endTurnAroundTime);
            assert.strictEqual(result, '1');
        });
    });

    describe('timestampToDateTime643', () => {
        it('should convert milliseconds to seconds with 3 decimal places', () => {
            const startTimeUnixMS = 1234567890000;
            const result = timestampToDateTime643(startTimeUnixMS);
            assert.strictEqual(result, '1234567890.000');
        });

        it('should handle timestamp with milliseconds', () => {
            const startTimeUnixMS = 1234567890123;
            const result = timestampToDateTime643(startTimeUnixMS);
            assert.strictEqual(result, '1234567890.123');
        });

        it('should handle 0', () => {
            const startTimeUnixMS = 0;
            const result = timestampToDateTime643(startTimeUnixMS);
            assert.strictEqual(result, '0.000');
        });

        it('should return null when startTimeUnixMS is null', () => {
            const result = timestampToDateTime643(null);
            assert.strictEqual(result, null);
        });

        it('should return null when startTimeUnixMS is undefined', () => {
            const result = timestampToDateTime643(undefined);
            assert.strictEqual(result, null);
        });

        it('should handle small timestamps', () => {
            const startTimeUnixMS = 1000;
            const result = timestampToDateTime643(startTimeUnixMS);
            assert.strictEqual(result, '1.000');
        });

        it('should handle timestamps with partial milliseconds', () => {
            const startTimeUnixMS = 1500;
            const result = timestampToDateTime643(startTimeUnixMS);
            assert.strictEqual(result, '1.500');
        });
    });

    describe('logServerAccess', () => {
        let mockLogger;
        let sandbox;

        beforeEach(() => {
            sandbox = sinon.createSandbox();
            mockLogger = {
                write: sandbox.stub(),
            };
        });

        afterEach(() => {
            sandbox.restore();
            setServerAccessLogger(undefined);
        });

        it('should not log if req.serverAccessLog is missing', () => {
            setServerAccessLogger(mockLogger);
            const req = {};
            const res = { serverAccessLog: {} };
            logServerAccess(req, res);
            assert.strictEqual(mockLogger.write.called, false);
        });

        it('should not log if res.serverAccessLog is missing', () => {
            setServerAccessLogger(mockLogger);
            const req = { serverAccessLog: {} };
            const res = {};
            logServerAccess(req, res);
            assert.strictEqual(mockLogger.write.called, false);
        });

        it('should not log if serverAccessLogger is not set', () => {
            setServerAccessLogger(undefined);
            const req = { serverAccessLog: {} };
            const res = { serverAccessLog: {} };
            logServerAccess(req, res);
            // No assertion needed - just ensuring no error is thrown
        });

        it('should log with minimal valid data', () => {
            setServerAccessLogger(mockLogger);
            const req = {
                serverAccessLog: {},
                headers: {},
                socket: {},
            };
            const res = {
                serverAccessLog: {},
                getHeader: () => null,
            };

            logServerAccess(req, res);

            assert.strictEqual(mockLogger.write.callCount, 1);
            const loggedData = JSON.parse(mockLogger.write.firstCall.args[0].trim());
            assert.strictEqual(typeof loggedData.time, 'number');
            assert.strictEqual(typeof loggedData.hostname, 'string');
            assert.strictEqual(typeof loggedData.pid, 'number');
        });

        it('should log complete request with all fields', () => {
            setServerAccessLogger(mockLogger);
            const authInfo = {
                getAccountDisplayName: () => 'testAccount',
                getCanonicalID: () => 'canonical123',
                isRequesterPublicUser: () => false,
                isRequesterAnIAMUser: () => false,
                getAuthVersion: () => 'AWS4-HMAC-SHA256',
                getAuthType: () => 'REST-HEADER',
                getAccessKey: () => 'AKIAIOSFODNN7EXAMPLE',
            };

            const req = {
                serverAccessLog: {
                    authInfo,
                    analyticsAction: 'ObjectGet',
                    analyticsAccountName: 'testAccount',
                    analyticsUserName: 'testUser',
                    analyticsBytesDeleted: 0,
                    startTime:           1000000000n,
                    onFinishEndTime:     1009000000n,
                    startTurnAroundTime: 1003000000n,
                    onCloseEndTime:      1020500000n,
                    startTimeUnixMS: 1234567890000,
                    bucketOwner: 'bucketOwner123',
                    bucketName: 'test-bucket',
                    objectKey: 'test-key.txt',
                    enabled: true,
                    loggingEnabled: {
                        TargetBucket: 'log-bucket',
                        TargetPrefix: 'logs/',
                    },
                    raftSessionID: 'raft-session-123',
                },
                method: 'GET',
                url: '/test-bucket/test-key.txt',
                httpVersion: '1.1',
                apiMethod: 'objectGet',
                headers: {
                    'content-length': '1024',
                    'user-agent': 'aws-cli/2.0.0',
                    'referer': 'https://example.com',
                    'host': 's3.amazonaws.com',
                    'x-forwarded-for': '192.168.1.100',
                },
                parsedContentLength: 1024,
                query: { versionId: 'version123' },
                socket: {
                    remotePort: 54321,
                    remoteAddress: '10.0.0.1',
                    encrypted: true,
                    getCipher: () => ({ standardName: 'TLS_AES_128_GCM_SHA256', version: 'TLSv1.3' }),
                },
            };

            const res = {
                serverAccessLog: {
                    errorCode: null,
                    endTurnAroundTime: 1004000000n,
                    requestID: 'req-id-123',
                    bytesSent: 2048,
                },
                statusCode: 200,
                getHeader: name => {
                    if (name === 'Content-Length') {return '2048';}
                    return null;
                },
            };

            logServerAccess(req, res);

            assert.strictEqual(mockLogger.write.callCount, 1);
            const loggedData = JSON.parse(mockLogger.write.firstCall.args[0].trim());

            // Verify werelog fields
            assert.strictEqual(typeof loggedData.time, 'number');
            assert.strictEqual(typeof loggedData.hostname, 'string');
            assert.strictEqual(typeof loggedData.pid, 'number');

            // Verify analytics fields
            assert.strictEqual(loggedData.action, 'ObjectGet');
            assert.strictEqual(loggedData.accountName, 'testAccount');
            assert.strictEqual(loggedData.userName, 'testUser');
            assert.strictEqual(loggedData.httpMethod, 'GET');
            assert.strictEqual(loggedData.bytesDeleted, 0);
            assert.strictEqual(loggedData.bytesReceived, 1024);
            assert.strictEqual(loggedData.bodyLength, 1024);
            assert.strictEqual(loggedData.contentLength, 2048);
            assert.strictEqual(loggedData.elapsed_ms, 20.5);

            // Verify AWS access server log fields
            assert.strictEqual(loggedData.startTime, '1234567890.000');
            assert.strictEqual(loggedData.requester, 'canonical123');
            assert.strictEqual(loggedData.operation, 'REST.GET.OBJECT');
            assert.strictEqual(loggedData.requestURI, 'GET /test-bucket/test-key.txt HTTP/1.1');
            assert.strictEqual('errorCode' in loggedData, false);
            assert.strictEqual(loggedData.objectSize, 2048);
            assert.strictEqual(loggedData.totalTime, '9');
            assert.strictEqual(loggedData.turnAroundTime, '1');
            assert.strictEqual(loggedData.referer, 'https://example.com');
            assert.strictEqual(loggedData.userAgent, 'aws-cli/2.0.0');
            assert.strictEqual(loggedData.versionId, 'version123');
            assert.strictEqual(loggedData.signatureVersion, 'AWS4-HMAC-SHA256');
            assert.strictEqual(loggedData.cipherSuite, 'TLS_AES_128_GCM_SHA256');
            assert.strictEqual(loggedData.authenticationType, 'REST-HEADER');
            assert.strictEqual(loggedData.hostHeader, 's3.amazonaws.com');
            assert.strictEqual(loggedData.tlsVersion, 'TLSv1.3');
            assert.strictEqual('aclRequired' in loggedData, false);

            // Verify shared fields
            assert.strictEqual(loggedData.bucketOwner, 'bucketOwner123');
            assert.strictEqual(loggedData.bucketName, 'test-bucket');
            assert.strictEqual(loggedData.req_id, 'req-id-123');
            assert.strictEqual(loggedData.bytesSent, 2048);
            assert.strictEqual(loggedData.clientIP, '192.168.1.100');
            assert.strictEqual(loggedData.httpCode, 200);
            assert.strictEqual(loggedData.objectKey, 'test-key.txt');

            // Verify Scality extra fields
            assert.strictEqual(loggedData.logFormatVersion, '0');
            assert.strictEqual(loggedData.loggingEnabled, true);
            assert.strictEqual(loggedData.loggingTargetBucket, 'log-bucket');
            assert.strictEqual(loggedData.loggingTargetPrefix, 'logs/');
            assert.strictEqual(loggedData.awsAccessKeyID, 'AKIAIOSFODNN7EXAMPLE');
            assert.strictEqual(loggedData.raftSessionID, 'raft-session-123');
        });

        it('should log error requests with error code', () => {
            setServerAccessLogger(mockLogger);
            const req = {
                serverAccessLog: {},
                headers: {},
                socket: {},
            };
            const res = {
                serverAccessLog: {
                    errorCode: 'NoSuchKey',
                },
                statusCode: 404,
                getHeader: () => null,
            };

            logServerAccess(req, res);

            assert.strictEqual(mockLogger.write.callCount, 1);
            const loggedData = JSON.parse(mockLogger.write.firstCall.args[0].trim());
            assert.strictEqual(loggedData.errorCode, 'NoSuchKey');
            assert.strictEqual(loggedData.httpCode, 404);
        });

        it('should handle missing authInfo', () => {
            setServerAccessLogger(mockLogger);
            const req = {
                serverAccessLog: {
                    authInfo: null,
                },
                headers: {},
                socket: {},
            };
            const res = {
                serverAccessLog: {},
                getHeader: () => null,
            };

            logServerAccess(req, res);

            assert.strictEqual(mockLogger.write.callCount, 1);
            const loggedData = JSON.parse(mockLogger.write.firstCall.args[0].trim());
            assert.strictEqual('requester' in loggedData, false);
            assert.strictEqual('signatureVersion' in loggedData, false);
            assert.strictEqual('authenticationType' in loggedData, false);
            assert.strictEqual('awsAccessKeyID' in loggedData, false);
        });

        it('should handle non-encrypted connection', () => {
            setServerAccessLogger(mockLogger);
            const req = {
                serverAccessLog: {},
                headers: {},
                socket: {
                    encrypted: false,
                },
            };
            const res = {
                serverAccessLog: {},
                getHeader: () => null,
            };

            logServerAccess(req, res);

            assert.strictEqual(mockLogger.write.callCount, 1);
            const loggedData = JSON.parse(mockLogger.write.firstCall.args[0].trim());
            assert.strictEqual('cipherSuite' in loggedData, false);
            assert.strictEqual('tlsVersion' in loggedData, false);
        });

        it('should read TLS info from proxy headers when socket is not encrypted', () => {
            setServerAccessLogger(mockLogger);
            const req = {
                serverAccessLog: {},
                headers: {
                    'x-ssl-cipher': 'ECDHE-RSA-AES256-GCM-SHA384',
                    'x-ssl-protocol': 'TLSv1.3',
                },
                socket: {
                    encrypted: false,
                },
            };
            const res = {
                serverAccessLog: {},
                getHeader: () => null,
            };

            logServerAccess(req, res);

            assert.strictEqual(mockLogger.write.callCount, 1);
            const loggedData = JSON.parse(mockLogger.write.firstCall.args[0].trim());
            assert.strictEqual(loggedData.cipherSuite, 'ECDHE-RSA-AES256-GCM-SHA384');
            assert.strictEqual(loggedData.tlsVersion, 'TLSv1.3');
        });

        it('should prefer socket TLS info over proxy headers when encrypted', () => {
            setServerAccessLogger(mockLogger);
            const req = {
                serverAccessLog: {},
                headers: {
                    'x-ssl-cipher': 'PROXY-CIPHER',
                    'x-ssl-protocol': 'TLSv1.2',
                },
                socket: {
                    encrypted: true,
                    getCipher: () => ({ standardName: 'TLS_AES_128_GCM_SHA256', version: 'TLSv1.3' }),
                },
            };
            const res = {
                serverAccessLog: {},
                getHeader: () => null,
            };

            logServerAccess(req, res);

            assert.strictEqual(mockLogger.write.callCount, 1);
            const loggedData = JSON.parse(mockLogger.write.firstCall.args[0].trim());
            assert.strictEqual(loggedData.cipherSuite, 'TLS_AES_128_GCM_SHA256');
            assert.strictEqual(loggedData.tlsVersion, 'TLSv1.3');
        });

        it('should handle missing query parameters', () => {
            setServerAccessLogger(mockLogger);
            const req = {
                serverAccessLog: {},
                headers: {},
                socket: {},
                query: null,
            };
            const res = {
                serverAccessLog: {},
                getHeader: () => null,
            };

            logServerAccess(req, res);

            assert.strictEqual(mockLogger.write.callCount, 1);
            const loggedData = JSON.parse(mockLogger.write.firstCall.args[0].trim());
            assert.strictEqual('versionId' in loggedData, false);
        });

        it('should handle loggingEnabled without TargetBucket/TargetPrefix', () => {
            setServerAccessLogger(mockLogger);
            const req = {
                serverAccessLog: {
                    loggingEnabled: null,
                },
                headers: {},
                socket: {},
            };
            const res = {
                serverAccessLog: {},
                getHeader: () => null,
            };

            logServerAccess(req, res);

            assert.strictEqual(mockLogger.write.callCount, 1);
            const loggedData = JSON.parse(mockLogger.write.firstCall.args[0].trim());
            assert.strictEqual('loggingTargetBucket' in loggedData, false);
            assert.strictEqual('loggingTargetPrefix' in loggedData, false);
        });

        it('should handle PUT request with objectPut', () => {
            setServerAccessLogger(mockLogger);
            const req = {
                serverAccessLog: {
                    startTime: 1000000000n,
                    onFinishEndTime: 1002000000n,
                },
                method: 'PUT',
                url: '/bucket/key',
                httpVersion: '1.1',
                apiMethod: 'objectPut',
                headers: {
                    'content-length': '5000',
                },
                parsedContentLength: 5000,
                socket: {},
            };
            const res = {
                serverAccessLog: {},
                statusCode: 200,
                getHeader: () => null,
            };

            logServerAccess(req, res);

            assert.strictEqual(mockLogger.write.callCount, 1);
            const loggedData = JSON.parse(mockLogger.write.firstCall.args[0].trim());
            assert.strictEqual(loggedData.operation, 'REST.PUT.OBJECT');
            assert.strictEqual(loggedData.objectSize, 5000);
            assert.strictEqual(loggedData.bytesReceived, 5000);
            assert.strictEqual(loggedData.totalTime, '2');
        });

        it('should output valid JSON with newline', () => {
            setServerAccessLogger(mockLogger);
            const req = {
                serverAccessLog: {},
                headers: {},
                socket: {},
            };
            const res = {
                serverAccessLog: {},
                getHeader: () => null,
            };

            logServerAccess(req, res);

            assert.strictEqual(mockLogger.write.callCount, 1);
            const writtenData = mockLogger.write.firstCall.args[0];
            assert.strictEqual(writtenData.endsWith('\n'), true);
            
            // Should be valid JSON without the newline
            const jsonData = writtenData.trim();
            assert.doesNotThrow(() => JSON.parse(jsonData));
        });

        it('should omit null fields from log output', () => {
            setServerAccessLogger(mockLogger);
            const req = {
                serverAccessLog: {},
                headers: {},
                socket: {},
            };
            const res = {
                serverAccessLog: {},
                getHeader: () => null,
            };

            logServerAccess(req, res);

            assert.strictEqual(mockLogger.write.callCount, 1);
            const loggedData = JSON.parse(mockLogger.write.firstCall.args[0].trim());

            // Verify null fields are omitted
            assert.strictEqual('errorCode' in loggedData, false);
            assert.strictEqual('bytesDeleted' in loggedData, false);
            assert.strictEqual('turnAroundTime' in loggedData, false);
            assert.strictEqual('referer' in loggedData, false);
            assert.strictEqual('versionId' in loggedData, false);
            assert.strictEqual('cipherSuite' in loggedData, false);
            assert.strictEqual('tlsVersion' in loggedData, false);
            assert.strictEqual('aclRequired' in loggedData, false);

            // Verify non-null fields are present
            assert.strictEqual('time' in loggedData, true);
            assert.strictEqual('hostname' in loggedData, true);
            assert.strictEqual('pid' in loggedData, true);
        });

        it('should preserve zero values in logs', () => {
            setServerAccessLogger(mockLogger);
            const req = {
                serverAccessLog: {
                    analyticsBytesDeleted: 0,
                },
                apiMethod: 'objectPut',
                headers: { 'content-length': '0' },
                parsedContentLength: 0,
                socket: {},
            };
            const res = {
                serverAccessLog: {},
                statusCode: 0,
                getHeader: () => null,
            };

            logServerAccess(req, res);

            assert.strictEqual(mockLogger.write.callCount, 1);
            const loggedData = JSON.parse(mockLogger.write.firstCall.args[0].trim());

            // Verify zero values are preserved
            assert.strictEqual(loggedData.bytesDeleted, 0);
            assert.strictEqual(loggedData.contentLength, 0);
            assert.strictEqual(loggedData.httpCode, 0);
        });

        it('should preserve false values in logs', () => {
            setServerAccessLogger(mockLogger);
            const req = {
                serverAccessLog: {
                    enabled: false,
                },
                headers: {},
                socket: {},
            };
            const res = {
                serverAccessLog: {},
                getHeader: () => null,
            };

            logServerAccess(req, res);

            assert.strictEqual(mockLogger.write.callCount, 1);
            const loggedData = JSON.parse(mockLogger.write.firstCall.args[0].trim());

            // Verify false is preserved
            assert.strictEqual(loggedData.loggingEnabled, false);
            assert.strictEqual('loggingEnabled' in loggedData, true);
        });

        it('should include error code when present', () => {
            setServerAccessLogger(mockLogger);
            const req = {
                serverAccessLog: {},
                headers: {},
                socket: {},
            };
            const res = {
                serverAccessLog: {
                    errorCode: 'NoSuchKey',
                },
                getHeader: () => null,
            };

            logServerAccess(req, res);

            assert.strictEqual(mockLogger.write.callCount, 1);
            const loggedData = JSON.parse(mockLogger.write.firstCall.args[0].trim());

            // Verify error code is included
            assert.strictEqual(loggedData.errorCode, 'NoSuchKey');
            assert.strictEqual('errorCode' in loggedData, true);
        });

        it('should omit NaN fields from log output', () => {
            setServerAccessLogger(mockLogger);
            const req = {
                serverAccessLog: {},
                headers: {},
                parsedContentLength: NaN, // Simulates Number.parseInt('', 10)
                socket: {},
            };
            const res = {
                serverAccessLog: {},
                getHeader: () => null,
            };

            logServerAccess(req, res);

            assert.strictEqual(mockLogger.write.callCount, 1);
            const loggedData = JSON.parse(mockLogger.write.firstCall.args[0].trim());

            // NaN values should be omitted from the log output
            assert.strictEqual('bytesReceived' in loggedData, false);
            assert.strictEqual('contentLength' in loggedData, false);
        });

        it('should log with loggingEnabled false when backbeat is enabled', () => {
            setServerAccessLogger(mockLogger);
            const req = {
                serverAccessLog: {
                    backbeat: true,
                    enabled: true,
                },
                method: 'GET',
                apiMethod: 'objectGet',
                headers: {},
                socket: {},
            };
            const res = {
                serverAccessLog: {},
                getHeader: () => null,
            };

            logServerAccess(req, res);

            assert.strictEqual(mockLogger.write.callCount, 1);
            const loggedData = JSON.parse(mockLogger.write.firstCall.args[0].trim());

            // Verify loggingEnabled is false in the logged data
            assert.strictEqual(loggedData.loggingEnabled, false);
        });

        it('should log REST.GET.BACKBEAT operation when backbeat is enabled', () => {
            setServerAccessLogger(mockLogger);
            const req = {
                serverAccessLog: {
                    backbeat: true,
                },
                method: 'GET',
                apiMethod: 'objectGet',
                headers: {},
                socket: {},
            };
            const res = {
                serverAccessLog: {},
                getHeader: () => null,
            };

            logServerAccess(req, res);

            assert.strictEqual(mockLogger.write.callCount, 1);
            const loggedData = JSON.parse(mockLogger.write.firstCall.args[0].trim());
            
            // Verify operation is REST.GET.BACKBEAT
            assert.strictEqual(loggedData.operation, 'REST.GET.BACKBEAT');
        });

        it('should override loggingEnabled when backbeat is enabled with logging config', () => {
            setServerAccessLogger(mockLogger);
            const req = {
                serverAccessLog: {
                    backbeat: true,
                    enabled: true,
                    loggingEnabled: {
                        TargetBucket: 'log-bucket',
                        TargetPrefix: 'logs/',
                    },
                },
                method: 'PUT',
                apiMethod: 'objectPut',
                headers: {},
                socket: {},
            };
            const res = {
                serverAccessLog: {},
                getHeader: () => null,
            };

            logServerAccess(req, res);

            assert.strictEqual(mockLogger.write.callCount, 1);
            const loggedData = JSON.parse(mockLogger.write.firstCall.args[0].trim());
            
            // Verify loggingEnabled is false (overridden by backbeat)
            assert.strictEqual(loggedData.loggingEnabled, false);
            // But TargetBucket and TargetPrefix should still be logged
            assert.strictEqual(loggedData.loggingTargetBucket, 'log-bucket');
            assert.strictEqual(loggedData.loggingTargetPrefix, 'logs/');
        });
    });
});

