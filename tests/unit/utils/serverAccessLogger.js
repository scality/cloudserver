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

        it('should return REST.PUT.RATELIMIT for bucketPutRateLimit', () => {
            const req = { method: 'PUT', apiMethod: 'bucketPutRateLimit' };
            const result = getOperation(req);
            assert.strictEqual(result, 'REST.PUT.RATELIMIT');
        });

        it('should return REST.GET.RATELIMIT for bucketGetRateLimit', () => {
            const req = { method: 'GET', apiMethod: 'bucketGetRateLimit' };
            const result = getOperation(req);
            assert.strictEqual(result, 'REST.GET.RATELIMIT');
        });

        it('should return REST.DELETE.RATELIMIT for bucketDeleteRateLimit', () => {
            const req = { method: 'DELETE', apiMethod: 'bucketDeleteRateLimit' };
            const result = getOperation(req);
            assert.strictEqual(result, 'REST.DELETE.RATELIMIT');
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

        it('should return S3.EXPIRE.OBJECT for lifecycle expiration requests', () => {
            const req = {
                method: 'DELETE',
                apiMethod: 'objectDelete',
                serverAccessLog: { backbeat: true, expiration: true },
            };
            const result = getOperation(req);
            assert.strictEqual(result, 'S3.EXPIRE.OBJECT');
        });

        it('should return REST.PUT.OBJECT for replication requests', () => {
            const req = {
                method: 'PUT',
                apiMethod: 'routeBackbeat',
                serverAccessLog: { backbeat: true, replication: true },
            };
            const result = getOperation(req);
            assert.strictEqual(result, 'REST.PUT.OBJECT');
        });

        it('should return REST.DELETE.OBJECT for delete-marker replication', () => {
            const req = {
                method: 'PUT',
                apiMethod: 'routeBackbeat',
                serverAccessLog: {
                    backbeat: true,
                    replication: true,
                    deleteMarker: true,
                },
            };
            const result = getOperation(req);
            assert.strictEqual(result, 'REST.DELETE.OBJECT');
        });

        it('should return REST.PUT.OBJECT_TAGGING for tag-only replication', () => {
            const req = {
                method: 'PUT',
                apiMethod: 'routeBackbeat',
                serverAccessLog: {
                    backbeat: true,
                    replication: true,
                    tagging: true,
                },
            };
            const result = getOperation(req);
            assert.strictEqual(result, 'REST.PUT.OBJECT_TAGGING');
        });

        it('should return REST.PUT.ACL for ACL-only replication', () => {
            const req = {
                method: 'PUT',
                apiMethod: 'routeBackbeat',
                serverAccessLog: {
                    backbeat: true,
                    replication: true,
                    acl: true,
                },
            };
            const result = getOperation(req);
            assert.strictEqual(result, 'REST.PUT.ACL');
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

        it('should return IAM ARN for IAM user', () => {
            const arn = 'arn:aws:iam::123456789012:user/myuser';
            const authInfo = {
                isRequesterPublicUser: () => false,
                isRequesterAnIAMUser: () => true,
                getArn: () => arn,
                getCanonicalID: () => 'canonicalID123',
            };
            const result = getRequester(authInfo);
            assert.strictEqual(result, arn);
        });

        it('should fall back to canonical ID for IAM user when ARN is missing', () => {
            const authInfo = {
                isRequesterPublicUser: () => false,
                isRequesterAnIAMUser: () => true,
                getArn: () => undefined,
                getCanonicalID: () => 'canonicalID123',
            };
            const result = getRequester(authInfo);
            assert.strictEqual(result, 'canonicalID123');
        });

        it('should return ARN for assumed-role session user', () => {
            const arn = 'arn:aws:sts::123456789012:assumed-role/lifecycle-role/backbeat-lifecycle';
            const authInfo = {
                isRequesterPublicUser: () => false,
                isRequesterAnIAMUser: () => false,
                getArn: () => arn,
                getCanonicalID: () => 'canonicalID789',
            };
            const result = getRequester(authInfo);
            assert.strictEqual(result, arn);
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

        it('should log aclRequired as Yes when set on request', () => {
            setServerAccessLogger(mockLogger);
            const req = {
                serverAccessLog: {
                    aclRequired: 'Yes',
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
            assert.strictEqual(loggedData.aclRequired, 'Yes');
        });

        it('should omit aclRequired when not set on request', () => {
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
            assert.strictEqual('aclRequired' in loggedData, false);
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

        it('should pass through loggingEnabled for lifecycle expiration', () => {
            setServerAccessLogger(mockLogger);
            const req = {
                serverAccessLog: {
                    backbeat: true,
                    expiration: true,
                    enabled: true,
                    loggingEnabled: {
                        TargetBucket: 'log-bucket',
                        TargetPrefix: 'logs/',
                    },
                },
                method: 'DELETE',
                apiMethod: 'objectDelete',
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
            assert.strictEqual(loggedData.loggingEnabled, true);
            assert.strictEqual(loggedData.loggingTargetBucket, 'log-bucket');
            assert.strictEqual(loggedData.loggingTargetPrefix, 'logs/');
            assert.strictEqual(loggedData.operation, 'S3.EXPIRE.OBJECT');
        });

        it('should force loggingEnabled false for non-expiration backbeat', () => {
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
            assert.strictEqual(loggedData.loggingEnabled, false);
        });

        it('should not deliver lifecycle expiration log when bucket has no logging config', () => {
            setServerAccessLogger(mockLogger);
            const req = {
                serverAccessLog: {
                    backbeat: true,
                    expiration: true,
                    enabled: false,
                },
                method: 'DELETE',
                apiMethod: 'objectDelete',
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
            assert.strictEqual(loggedData.loggingEnabled, false);
        });

        it('should produce a complete log entry for lifecycle expiration', () => {
            setServerAccessLogger(mockLogger);
            const authInfo = {
                getAccountDisplayName: () => 'lifecycleAccount',
                getCanonicalID: () => 'lifecycle-canonical-id',
                isRequesterPublicUser: () => false,
                isRequesterAnIAMUser: () => false,
                getAuthVersion: () => 'AWS4-HMAC-SHA256',
                getAuthType: () => 'REST-HEADER',
                getAccessKey: () => 'lifecycle-access-key',
            };
            const startTime = process.hrtime.bigint();
            const startTurnAroundTime = startTime + 1_000_000n;
            const endTurnAroundTime = startTurnAroundTime + 50_000_000n;
            const onFinishEndTime = startTime + 100_000_000n;
            const onCloseEndTime = startTime + 110_000_000n;

            const req = {
                serverAccessLog: {
                    backbeat: true,
                    expiration: true,
                    enabled: true,
                    loggingEnabled: {
                        TargetBucket: 'log-bucket',
                        TargetPrefix: 'access-logs/',
                    },
                    bucketOwner: 'bucket-owner-id',
                    bucketName: 'source-bucket',
                    objectKey: 'expired-object.txt',
                    authInfo,
                    analyticsAction: 'deleteObjectFromExpiration',
                    analyticsAccountName: 'lifecycleAccount',
                    analyticsUserName: '',
                    startTime,
                    startTimeUnixMS: Date.now(),
                    startTurnAroundTime,
                    onFinishEndTime,
                    onCloseEndTime,
                    objectSize: 1024,
                    analyticsBytesDeleted: 1024,
                },
                method: 'DELETE',
                apiMethod: 'objectDelete',
                headers: { host: 'source-bucket.s3.amazonaws.com' },
                socket: {},
            };
            const res = {
                serverAccessLog: {
                    endTurnAroundTime,
                },
                statusCode: 200,
                getHeader: () => null,
            };

            logServerAccess(req, res);

            assert.strictEqual(mockLogger.write.callCount, 1);
            const loggedData = JSON.parse(mockLogger.write.firstCall.args[0].trim());

            // Core expiration fields
            assert.strictEqual(loggedData.operation, 'S3.EXPIRE.OBJECT');
            assert.strictEqual(loggedData.loggingEnabled, true);
            assert.strictEqual(loggedData.loggingTargetBucket, 'log-bucket');
            assert.strictEqual(loggedData.loggingTargetPrefix, 'access-logs/');

            // Bucket and object info
            assert.strictEqual(loggedData.bucketOwner, 'bucket-owner-id');
            assert.strictEqual(loggedData.bucketName, 'source-bucket');
            assert.strictEqual(loggedData.objectKey, 'expired-object.txt');

            // Requester is the lifecycle service, not the auth identity
            assert.strictEqual(loggedData.requester, 'ScalityS3LifecycleService');

            // Object size
            assert.strictEqual(loggedData.objectSize, 1024);
            assert.strictEqual(loggedData.bytesDeleted, 1024);

            // HTTP-layer fields are null (internal operation, not an HTTP request)
            assert.strictEqual(loggedData.clientIP, undefined);
            assert.strictEqual(loggedData.requestURI, undefined);
            assert.strictEqual(loggedData.httpCode, undefined);
            assert.strictEqual(loggedData.bytesSent, undefined);
            assert.strictEqual(loggedData.totalTime, undefined);
            assert.strictEqual(loggedData.turnAroundTime, undefined);
            assert.strictEqual(loggedData.signatureVersion, undefined);
            assert.strictEqual(loggedData.authenticationType, undefined);
            assert.strictEqual(loggedData.hostHeader, undefined);
            assert.strictEqual(loggedData.awsAccessKeyID, undefined);
        });

        it('should log lifecycle expiration with error code on failure', () => {
            setServerAccessLogger(mockLogger);
            const req = {
                serverAccessLog: {
                    backbeat: true,
                    expiration: true,
                    enabled: true,
                    loggingEnabled: {
                        TargetBucket: 'log-bucket',
                        TargetPrefix: 'logs/',
                    },
                    bucketOwner: 'bucket-owner-id',
                    bucketName: 'source-bucket',
                    objectKey: 'expired-object.txt',
                    startTime: process.hrtime.bigint(),
                    startTimeUnixMS: Date.now(),
                },
                method: 'DELETE',
                apiMethod: 'objectDelete',
                headers: {},
                socket: {},
            };
            const res = {
                serverAccessLog: {
                    errorCode: 'NoSuchKey',
                    endTurnAroundTime: process.hrtime.bigint(),
                },
                statusCode: 404,
                getHeader: () => null,
            };

            logServerAccess(req, res);

            assert.strictEqual(mockLogger.write.callCount, 1);
            const loggedData = JSON.parse(mockLogger.write.firstCall.args[0].trim());
            assert.strictEqual(loggedData.operation, 'S3.EXPIRE.OBJECT');
            assert.strictEqual(loggedData.loggingEnabled, true);
            assert.strictEqual(loggedData.requester, 'ScalityS3LifecycleService');
            assert.strictEqual(loggedData.errorCode, 'NoSuchKey');
            assert.strictEqual(loggedData.httpCode, undefined);
        });

        it('should pass through loggingEnabled for replication', () => {
            setServerAccessLogger(mockLogger);
            const req = {
                serverAccessLog: {
                    backbeat: true,
                    replication: true,
                    enabled: true,
                    loggingEnabled: {
                        TargetBucket: 'log-bucket',
                        TargetPrefix: 'logs/',
                    },
                    bucketName: 'dest-bucket',
                    objectKey: 'replicated.txt',
                },
                method: 'PUT',
                apiMethod: 'routeBackbeat',
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
            assert.strictEqual(loggedData.loggingEnabled, true);
            assert.strictEqual(loggedData.loggingTargetBucket, 'log-bucket');
            assert.strictEqual(loggedData.loggingTargetPrefix, 'logs/');
            assert.strictEqual(loggedData.operation, 'REST.PUT.OBJECT');
        });

        it('should not deliver replication log when bucket has no logging config', () => {
            setServerAccessLogger(mockLogger);
            const req = {
                serverAccessLog: {
                    backbeat: true,
                    replication: true,
                    enabled: false,
                    bucketName: 'dest-bucket',
                    objectKey: 'replicated.txt',
                },
                method: 'PUT',
                apiMethod: 'routeBackbeat',
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
            assert.strictEqual(loggedData.loggingEnabled, false);
        });

        it('should produce a complete log entry for replication', () => {
            setServerAccessLogger(mockLogger);
            const authInfo = {
                getAccountDisplayName: () => 'replicationAccount',
                getCanonicalID: () => 'replication-canonical-id',
                isRequesterPublicUser: () => false,
                isRequesterAnIAMUser: () => false,
                getArn: () =>
                    'arn:aws:sts::123456789012:assumed-role/replication-role/backbeat-replication',
                getAuthVersion: () => 'AWS4-HMAC-SHA256',
                getAuthType: () => 'REST-HEADER',
                getAccessKey: () => 'replication-access-key',
            };
            const startTime = process.hrtime.bigint();
            const startTurnAroundTime = startTime + 1_000_000n;
            const endTurnAroundTime = startTurnAroundTime + 13_000_000n;
            const onFinishEndTime = startTime + 19_000_000n;
            const onCloseEndTime = startTime + 20_000_000n;

            const req = {
                serverAccessLog: {
                    backbeat: true,
                    replication: true,
                    enabled: true,
                    loggingEnabled: {
                        TargetBucket: 'log-bucket',
                        TargetPrefix: 'access-logs/',
                    },
                    bucketOwner: 'bucket-owner-id',
                    bucketName: 'dest-bucket',
                    objectKey: 'replicated.txt',
                    authInfo,
                    analyticsAction: 'putData',
                    analyticsAccountName: 'replicationAccount',
                    analyticsUserName: '',
                    startTime,
                    startTimeUnixMS: Date.now(),
                    startTurnAroundTime,
                    onFinishEndTime,
                    onCloseEndTime,
                    objectSize: 43,
                },
                method: 'PUT',
                apiMethod: 'routeBackbeat',
                url: '/_/backbeat/data/dest-bucket/replicated.txt?v2',
                httpVersion: '1.1',
                headers: {
                    host: '127.0.0.1:8000',
                    referer: 'http://example.com',
                    'user-agent': 'aws-sdk-nodejs/2.1692.0',
                },
                socket: {},
            };
            const res = {
                serverAccessLog: {
                    endTurnAroundTime,
                    bytesSent: 75,
                },
                statusCode: 200,
                getHeader: () => null,
            };

            logServerAccess(req, res);

            assert.strictEqual(mockLogger.write.callCount, 1);
            const loggedData = JSON.parse(mockLogger.write.firstCall.args[0].trim());

            // Core replication fields
            assert.strictEqual(loggedData.operation, 'REST.PUT.OBJECT');
            assert.strictEqual(loggedData.loggingEnabled, true);
            assert.strictEqual(loggedData.loggingTargetBucket, 'log-bucket');
            assert.strictEqual(loggedData.loggingTargetPrefix, 'access-logs/');

            // Bucket and object info
            assert.strictEqual(loggedData.bucketOwner, 'bucket-owner-id');
            assert.strictEqual(loggedData.bucketName, 'dest-bucket');
            assert.strictEqual(loggedData.objectKey, 'replicated.txt');

            // Requester is the assumed-role ARN from auth
            assert.strictEqual(loggedData.requester,
                'arn:aws:sts::123456789012:assumed-role/replication-role/backbeat-replication');

            // requestURI is synthesized to look like a normal S3 PUT,
            // not the internal /_/backbeat/data path
            assert.strictEqual(loggedData.requestURI,
                'PUT /dest-bucket/replicated.txt HTTP/1.1');

            // HTTP-layer fields that AWS blanks for replication
            assert.strictEqual(loggedData.clientIP, undefined);
            assert.strictEqual(loggedData.userAgent, undefined);
            assert.strictEqual(loggedData.referer, undefined);

            // Real HTTP timing/status preserved
            assert.strictEqual(loggedData.httpCode, 200);
            assert.strictEqual(loggedData.bytesSent, 75);
            assert.strictEqual(loggedData.objectSize, 43);
            assert.strictEqual(loggedData.hostHeader, '127.0.0.1:8000');
            assert.strictEqual(loggedData.signatureVersion, 'AWS4-HMAC-SHA256');
            assert.strictEqual(loggedData.authenticationType, 'REST-HEADER');
        });

        it('should produce a REST.DELETE.OBJECT entry for delete-marker replication', () => {
            setServerAccessLogger(mockLogger);
            const authInfo = {
                getAccountDisplayName: () => 'replicationAccount',
                getCanonicalID: () => 'replication-canonical-id',
                isRequesterPublicUser: () => false,
                isRequesterAnIAMUser: () => false,
                getArn: () =>
                    'arn:aws:sts::123456789012:assumed-role/replication-role/backbeat-replication',
                getAuthVersion: () => 'AWS4-HMAC-SHA256',
                getAuthType: () => 'REST-HEADER',
                getAccessKey: () => 'replication-access-key',
            };
            const req = {
                serverAccessLog: {
                    backbeat: true,
                    replication: true,
                    deleteMarker: true,
                    enabled: true,
                    loggingEnabled: {
                        TargetBucket: 'log-bucket',
                        TargetPrefix: 'logs/',
                    },
                    bucketOwner: 'bucket-owner-id',
                    bucketName: 'dest-bucket',
                    objectKey: 'replicated.txt',
                    authInfo,
                    startTime: process.hrtime.bigint(),
                    startTimeUnixMS: Date.now(),
                },
                method: 'PUT',
                apiMethod: 'routeBackbeat',
                url: '/_/backbeat/metadata/dest-bucket/replicated.txt',
                httpVersion: '1.1',
                headers: {
                    host: '127.0.0.1:8000',
                    referer: 'http://example.com',
                    'user-agent': 'aws-sdk-nodejs/2.1692.0',
                },
                socket: {},
            };
            const res = {
                serverAccessLog: {
                    endTurnAroundTime: process.hrtime.bigint(),
                },
                statusCode: 200,
                getHeader: () => null,
            };

            logServerAccess(req, res);

            assert.strictEqual(mockLogger.write.callCount, 1);
            const loggedData = JSON.parse(mockLogger.write.firstCall.args[0].trim());

            assert.strictEqual(loggedData.operation, 'REST.DELETE.OBJECT');
            assert.strictEqual(loggedData.loggingEnabled, true);
            assert.strictEqual(loggedData.requestURI,
                'DELETE /dest-bucket/replicated.txt HTTP/1.1');

            // HTTP-layer fields that AWS blanks for replication
            assert.strictEqual(loggedData.clientIP, undefined);
            assert.strictEqual(loggedData.userAgent, undefined);
            assert.strictEqual(loggedData.referer, undefined);

            // Replication identity preserved
            assert.strictEqual(loggedData.requester,
                'arn:aws:sts::123456789012:assumed-role/replication-role/backbeat-replication');
        });

        it('should produce a REST.PUT.OBJECT_TAGGING entry for tag-only replication', () => {
            setServerAccessLogger(mockLogger);
            const authInfo = {
                getAccountDisplayName: () => 'replicationAccount',
                getCanonicalID: () => 'replication-canonical-id',
                isRequesterPublicUser: () => false,
                isRequesterAnIAMUser: () => false,
                getArn: () =>
                    'arn:aws:sts::123456789012:assumed-role/replication-role/backbeat-replication',
                getAuthVersion: () => 'AWS4-HMAC-SHA256',
                getAuthType: () => 'REST-HEADER',
                getAccessKey: () => 'replication-access-key',
            };
            const versionId = 'aIXVkw5Tw2Pd00000000001I4j3QKsvf';
            const req = {
                serverAccessLog: {
                    backbeat: true,
                    replication: true,
                    tagging: true,
                    enabled: true,
                    loggingEnabled: {
                        TargetBucket: 'log-bucket',
                        TargetPrefix: 'logs/',
                    },
                    bucketOwner: 'bucket-owner-id',
                    bucketName: 'dest-bucket',
                    objectKey: 'replicated.txt',
                    authInfo,
                    startTime: process.hrtime.bigint(),
                    startTimeUnixMS: Date.now(),
                },
                method: 'PUT',
                apiMethod: 'routeBackbeat',
                url: `/_/backbeat/metadata/dest-bucket/replicated.txt?versionId=${versionId}`,
                query: { versionId },
                httpVersion: '1.1',
                headers: {
                    host: '127.0.0.1:8000',
                    referer: 'http://example.com',
                    'user-agent': 'aws-sdk-nodejs/2.1692.0',
                },
                socket: {},
            };
            const res = {
                serverAccessLog: {
                    endTurnAroundTime: process.hrtime.bigint(),
                },
                statusCode: 200,
                getHeader: () => null,
            };

            logServerAccess(req, res);

            assert.strictEqual(mockLogger.write.callCount, 1);
            const loggedData = JSON.parse(mockLogger.write.firstCall.args[0].trim());

            assert.strictEqual(loggedData.operation, 'REST.PUT.OBJECT_TAGGING');
            assert.strictEqual(loggedData.loggingEnabled, true);
            assert.strictEqual(loggedData.requestURI,
                `PUT /dest-bucket/replicated.txt?tagging&versionId=${versionId} HTTP/1.1`);
            assert.strictEqual(loggedData.versionId, versionId);

            // HTTP-layer fields that AWS blanks for replication
            assert.strictEqual(loggedData.clientIP, undefined);
            assert.strictEqual(loggedData.userAgent, undefined);
            assert.strictEqual(loggedData.referer, undefined);

            // Replication identity preserved
            assert.strictEqual(loggedData.requester,
                'arn:aws:sts::123456789012:assumed-role/replication-role/backbeat-replication');
        });

        it('should produce a REST.PUT.ACL entry for ACL-only replication', () => {
            setServerAccessLogger(mockLogger);
            const authInfo = {
                getAccountDisplayName: () => 'replicationAccount',
                getCanonicalID: () => 'replication-canonical-id',
                isRequesterPublicUser: () => false,
                isRequesterAnIAMUser: () => false,
                getArn: () =>
                    'arn:aws:sts::123456789012:assumed-role/replication-role/backbeat-replication',
                getAuthVersion: () => 'AWS4-HMAC-SHA256',
                getAuthType: () => 'REST-HEADER',
                getAccessKey: () => 'replication-access-key',
            };
            const versionId = 'aIXVkw5Tw2Pd00000000001I4j3QKsvf';
            const req = {
                serverAccessLog: {
                    backbeat: true,
                    replication: true,
                    acl: true,
                    aclRequired: 'Yes',
                    enabled: true,
                    loggingEnabled: {
                        TargetBucket: 'log-bucket',
                        TargetPrefix: 'logs/',
                    },
                    bucketOwner: 'bucket-owner-id',
                    bucketName: 'dest-bucket',
                    objectKey: 'replicated.txt',
                    authInfo,
                    startTime: process.hrtime.bigint(),
                    startTimeUnixMS: Date.now(),
                },
                method: 'PUT',
                apiMethod: 'routeBackbeat',
                url: `/_/backbeat/metadata/dest-bucket/replicated.txt?versionId=${versionId}`,
                query: { versionId },
                httpVersion: '1.1',
                headers: {
                    host: '127.0.0.1:8000',
                    referer: 'http://example.com',
                    'user-agent': 'aws-sdk-nodejs/2.1692.0',
                },
                socket: {},
            };
            const res = {
                serverAccessLog: {
                    endTurnAroundTime: process.hrtime.bigint(),
                },
                statusCode: 200,
                getHeader: () => null,
            };

            logServerAccess(req, res);

            assert.strictEqual(mockLogger.write.callCount, 1);
            const loggedData = JSON.parse(mockLogger.write.firstCall.args[0].trim());

            assert.strictEqual(loggedData.operation, 'REST.PUT.ACL');
            assert.strictEqual(loggedData.loggingEnabled, true);
            assert.strictEqual(loggedData.requestURI,
                `PUT /dest-bucket/replicated.txt?acl&versionId=${versionId} HTTP/1.1`);
            assert.strictEqual(loggedData.versionId, versionId);
            assert.strictEqual(loggedData.aclRequired, 'Yes');

            // HTTP-layer fields that AWS blanks for replication
            assert.strictEqual(loggedData.clientIP, undefined);
            assert.strictEqual(loggedData.userAgent, undefined);
            assert.strictEqual(loggedData.referer, undefined);

            // Replication identity preserved
            assert.strictEqual(loggedData.requester,
                'arn:aws:sts::123456789012:assumed-role/replication-role/backbeat-replication');
        });

        it('should log replication with error code on failure', () => {
            setServerAccessLogger(mockLogger);
            const req = {
                serverAccessLog: {
                    backbeat: true,
                    replication: true,
                    enabled: true,
                    loggingEnabled: {
                        TargetBucket: 'log-bucket',
                        TargetPrefix: 'logs/',
                    },
                    bucketOwner: 'bucket-owner-id',
                    bucketName: 'dest-bucket',
                    objectKey: 'replicated.txt',
                    startTime: process.hrtime.bigint(),
                    startTimeUnixMS: Date.now(),
                },
                method: 'PUT',
                apiMethod: 'routeBackbeat',
                headers: {},
                socket: {},
            };
            const res = {
                serverAccessLog: {
                    errorCode: 'InternalError',
                    endTurnAroundTime: process.hrtime.bigint(),
                },
                statusCode: 500,
                getHeader: () => null,
            };

            logServerAccess(req, res);

            assert.strictEqual(mockLogger.write.callCount, 1);
            const loggedData = JSON.parse(mockLogger.write.firstCall.args[0].trim());
            assert.strictEqual(loggedData.operation, 'REST.PUT.OBJECT');
            assert.strictEqual(loggedData.loggingEnabled, true);
            assert.strictEqual(loggedData.errorCode, 'InternalError');
            assert.strictEqual(loggedData.httpCode, 500);
        });

    });
});

