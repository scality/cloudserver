import assert from 'assert';

import constructStringToSign from
    '../../../../lib/auth/v4/constructStringToSign';
import { DummyRequestLogger } from '../../helpers';

const log = new DummyRequestLogger();


describe('constructStringToSign function', () => {
    // Example taken from: http://docs.aws.amazon.com/AmazonS3/
    // latest/API/sig-v4-header-based-auth.html
    it('should construct a stringToSign in accordance ' +
        'with AWS rules for a get object request (header auth)', () => {
        const params = {
            request: {
                method: 'GET',
                url: '/test.txt',
                headers: {
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
            },
            query: {},
            signedHeaders: 'host;range;x-amz-content-sha256;x-amz-date',
            payloadChecksum: 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4' +
                '649b934ca495991b7852b855',
            credentialScope: '20130524/us-east-1/s3/aws4_request',
            timestamp: '20130524T000000Z',
            log,
        };
        const expectedOutput = 'AWS4-HMAC-SHA256\n' +
            '20130524T000000Z\n' +
            '20130524/us-east-1/s3/aws4_request\n' +
            '7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972';
        const actualOutput = constructStringToSign(params);
        assert.strictEqual(actualOutput, expectedOutput);
    });

    // Example taken from: http://docs.aws.amazon.com/AmazonS3/
    // latest/API/sig-v4-header-based-auth.html
    it('should construct a stringToSign in accordance ' +
        'with AWS rules for a put object request (header auth)', () => {
        const params = {
            request: {
                method: 'PUT',
                url: '/test$file.text',
                headers: {
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
            },
            query: {},
            signedHeaders: 'date;host;x-amz-content-sha256;' +
                'x-amz-date;x-amz-storage-class',
            payloadChecksum: '44ce7dd67c959e0d3524ffac1771dfbba8' +
                '7d2b6b4b4e99e42034a8b803f8b072',
            credentialScope: '20130524/us-east-1/s3/aws4_request',
            timestamp: '20130524T000000Z',
            log,
        };
        const expectedOutput = 'AWS4-HMAC-SHA256\n' +
            '20130524T000000Z\n' +
            '20130524/us-east-1/s3/aws4_request\n' +
            '9e0e90d9c76de8fa5b200d8c849cd5b8dc7a3' +
            'be3951ddb7f6a76b4158342019d';
        const actualOutput = constructStringToSign(params);
        assert.strictEqual(actualOutput, expectedOutput);
    });

    // Example taken from: http://docs.aws.amazon.com/AmazonS3/
    // latest/API/sig-v4-header-based-auth.html
    it('should construct a stringToSign in accordance ' +
        'with AWS rules for a pre-signed get url request (query auth)', () => {
        const params = {
            request: {
                method: 'GET',
                url: '/test.txt',
                headers: {
                    host: 'examplebucket.s3.amazonaws.com',
                },
            },
            query: {
                'X-Amz-Algorithm': 'AWS4-HMAC-SHA256',
                'X-Amz-Credential': 'AKIAIOSFODNN7EXAMPLE/20130524/' +
                    'us-east-1/s3/aws4_request',
                'X-Amz-Date': '20130524T000000Z',
                'X-Amz-Expires': '86400',
                'X-Amz-SignedHeaders': 'host',
            },
            signedHeaders: 'host',
            payloadChecksum: 'UNSIGNED-PAYLOAD',
            credentialScope: '20130524/us-east-1/s3/aws4_request',
            timestamp: '20130524T000000Z',
            log,
        };
        const expectedOutput = 'AWS4-HMAC-SHA256\n' +
            '20130524T000000Z\n' +
            '20130524/us-east-1/s3/aws4_request\n' +
            '3bfa292879f6447bbcda7001decf97f4a54d' +
            'c650c8942174ae0a9121cf58ad04';
        const actualOutput = constructStringToSign(params);
        assert.strictEqual(actualOutput, expectedOutput);
    });
});
