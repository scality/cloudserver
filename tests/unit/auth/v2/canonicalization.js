import assert from 'assert';

import getCanonicalizedAmzHeaders from
    '../../../../lib/auth/v2/getCanonicalizedAmzHeaders';
import getCanonicalizedResource from
    '../../../../lib/auth/v2/getCanonicalizedResource';

describe('canonicalization', () => {
    it('should construct a canonicalized header', () => {
        const headers = {
            date: 'Mon, 21 Sep 2015 22:29:27 GMT',
            'x-amz-request-payer': 'requester',
            authorization: 'AWS accessKey1:V8g5UJUFmMzruMqUHVT6ZwvUw+M=',
            host: 's3.amazonaws.com:80',
            connection: 'Keep-Alive',
            'user-agent': 'Cyberduck/4.7.2.18004 (Mac OS X/10.10.5) (x86_64)',
        };
        const canonicalizedHeader = getCanonicalizedAmzHeaders(headers);
        assert.strictEqual(canonicalizedHeader,
                           'x-amz-request-payer:requester\n');
    });

    it('should return an empty string as the canonicalized ' +
       'header if no amz headers', () => {
        const headers = {
            date: 'Mon, 21 Sep 2015 22:29:27 GMT',
            authorization: 'AWS accessKey1:V8g5UJUFmMzruMqUHVT6ZwvUw+M=',
            host: 's3.amazonaws.com:80',
            connection: 'Keep-Alive',
            'user-agent': 'Cyberduck/4.7.2.18004 (Mac OS X/10.10.5) (x86_64)',
        };
        const canonicalizedHeader = getCanonicalizedAmzHeaders(headers);
        assert.strictEqual(canonicalizedHeader, '');
    });

    it('should construct a canonicalized resource', () => {
        const request = {
            headers: {
                host: 'bucket.s3.amazonaws.com:80',
            },
            lowerCaseHeaders: {
                host: 'bucket.s3.amazonaws.com:80',
            },
            url: '/obj',
            query: {
                requestPayment: 'yes,please',
                ignore: 'me',
            }
        };
        const canonicalizedResource = getCanonicalizedResource(request);
        assert.strictEqual(canonicalizedResource,
                           '/bucket/obj?requestPayment=yes,please');
    });

    it('should return the path as the canonicalized resource ' +
       'if no bucket name, overriding headers or delete query', () => {
        const request = {
            headers: {
                host: 's3.amazonaws.com:80',
            },
            lowerCaseHeaders: {
                host: 's3.amazonaws.com:80',
            },
            url: '/',
            query: {
                ignore: 'me',
            }
        };
        const canonicalizedResource = getCanonicalizedResource(request);
        assert.strictEqual(canonicalizedResource, '/');
    });
});
