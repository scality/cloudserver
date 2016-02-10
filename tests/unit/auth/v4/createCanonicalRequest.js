import assert from 'assert';

import createCanonicalRequest from
    '../../../../lib/auth/v4/createCanonicalRequest';

describe('createCanonicalRequest function', () => {
    // Example taken from: http://docs.aws.amazon.com/AmazonS3/
    // latest/API/sig-v4-header-based-auth.html
    it('should construct a canonical request in accordance ' +
        'with AWS rules for a get object request (header auth)', () => {
        const params = {
            pHttpVerb: 'GET',
            pResource: '/test.txt',
            pQuery: {},
            pHeaders: {
                'host': 'examplebucket.s3.amazonaws.com',
                'x-amz-date': '20130524T000000Z',
                'authorization': 'AWS4-HMAC-SHA256 Credential' +
                    '=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/' +
                    's3/aws4_request,SignedHeaders=host;range;' +
                    'x-amz-content-sha256;x-amz-date,Signature=' +
                    'f0e8bdb87c964420e857bd35b5d6ed310bd44f' +
                    '0170aba48dd91039c6036bdb41',
                'range': 'bytes=0-9',
                'x-amz-content-sha256': 'e3b0c44298fc1c149afbf4c' +
                    '8996fb92427ae41e4649b934ca495991b7852b855',
            },
            pSignedHeaders: 'host;range;x-amz-content-sha256;x-amz-date',
            payloadChecksum: 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4' +
                '649b934ca495991b7852b855',
        };
        const expectedOutput = 'GET\n' +
            '/test.txt\n\n' +
            'host:examplebucket.s3.amazonaws.com\n' +
            'range:bytes=0-9\n' +
            'x-amz-content-sha256:e3b0c44298fc1c149afbf4c' +
            '8996fb92427ae41e4649b934ca495991b7852b855\n' +
            'x-amz-date:20130524T000000Z\n\n' +
            'host;range;x-amz-content-sha256;x-amz-date\n' +
            'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b' +
            '934ca495991b7852b855';
        const actualOutput = createCanonicalRequest(params);
        assert.strictEqual(actualOutput, expectedOutput);
    });

    // Example taken from: http://docs.aws.amazon.com/AmazonS3/
    // latest/API/sig-v4-header-based-auth.html
    it('should construct a canonical request in accordance ' +
        'with AWS rules for a put object request (header auth)', () => {
        const params = {
            pHttpVerb: 'PUT',
            pResource: '/test$file.text',
            pQuery: {},
            pHeaders: {
                'date': 'Fri, 24 May 2013 00:00:00 GMT',
                'host': 'examplebucket.s3.amazonaws.com',
                'x-amz-date': '20130524T000000Z',
                'authorization': 'AWS4-HMAC-SHA256 Credential' +
                    '=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1' +
                    '/s3/aws4_request,SignedHeaders=date;host;' +
                    'x-amz-content-sha256;x-amz-date;x-amz-storage' +
                    '-class,Signature=98ad721746da40c64f1a55b78f14c2' +
                    '38d841ea1380cd77a1b5971af0ece108bd',
                'x-amz-storage-class': 'REDUCED_REDUNDANCY',
                'x-amz-content-sha256': '44ce7dd67c959e0d3524ffac1' +
                    '771dfbba87d2b6b4b4e99e42034a8b803f8b072',
            },
            pSignedHeaders: 'date;host;x-amz-content-sha256;' +
                'x-amz-date;x-amz-storage-class',
            payloadChecksum: '44ce7dd67c959e0d3524ffac1771dfbba8' +
                '7d2b6b4b4e99e42034a8b803f8b072',
        };
        const expectedOutput = 'PUT\n' +
            '/test%24file.text\n\n' +
            'date:Fri, 24 May 2013 00:00:00 GMT\n' +
            'host:examplebucket.s3.amazonaws.com\n' +
            'x-amz-content-sha256:44ce7dd67c959e0' +
            'd3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072\n' +
            'x-amz-date:20130524T000000Z\n' +
            'x-amz-storage-class:REDUCED_REDUNDANCY\n\n' +
            'date;host;x-amz-content-sha256;x-amz-date;x-amz' +
            '-storage-class\n' +
            '44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072';
        const actualOutput = createCanonicalRequest(params);
        assert.strictEqual(actualOutput, expectedOutput);
    });

    // Example taken from: http://docs.aws.amazon.com/AmazonS3/latest/API/
    // sigv4-query-string-auth.html
    it('should construct a canonical request in accordance ' +
        'with AWS rules for a pre-signed get url request (query auth)', () => {
        const params = {
            pHttpVerb: 'GET',
            pResource: '/test.txt',
            pQuery: {
                'X-Amz-Algorithm': 'AWS4-HMAC-SHA256',
                'X-Amz-Credential': 'AKIAIOSFODNN7EXAMPLE/20130524/' +
                    'us-east-1/s3/aws4_request',
                'X-Amz-Date': '20130524T000000Z',
                'X-Amz-Expires': '86400',
                'X-Amz-SignedHeaders': 'host',
            },
            pHeaders: {
                host: 'examplebucket.s3.amazonaws.com',
            },
            pSignedHeaders: 'host',
            payloadChecksum: 'UNSIGNED-PAYLOAD',
        };
        const expectedOutput = 'GET\n' +
            '/test.txt\n' +
            'X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential' +
            '=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%' +
            '2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-' +
            'Expires=86400&X-Amz-SignedHeaders=host\n' +
            'host:examplebucket.s3.amazonaws.com\n\n' +
            'host\n' +
            'UNSIGNED-PAYLOAD';
        const actualOutput = createCanonicalRequest(params);
        assert.strictEqual(actualOutput, expectedOutput);
    });
});
