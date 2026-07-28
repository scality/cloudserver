const assert = require('assert');
const crypto = require('crypto');
const async = require('async');
const { Scuba: MockScuba } = require('../utilities/mock/Scuba');
const { CreateBucketCommand, DeleteBucketCommand } = require('@aws-sdk/client-s3');

const { makeRequest } = require('../functional/raw-node/utils/makeRequest');
const HttpRequestAuthV4 = require('../functional/raw-node/utils/HttpRequestAuthV4');
const BucketUtility = require('../functional/aws-node-sdk/lib/utility/bucket-util');
const { algorithms } = require('../../lib/api/apiUtils/integrity/validateChecksums');

const ipAddress = process.env.IP ? process.env.IP : '127.0.0.1';

// The Veeam route strips the internal '/_/veeam' routing prefix from the URL
// before verifying the V4 signature, so requests must be signed on the
// un-prefixed path while being sent to the prefixed one.
class VeeamHttpRequestAuthV4 extends HttpRequestAuthV4 {
    getCanonicalRequest(urlObj, signedHeaders, contentSha256) {
        const signUrlObj = new URL(urlObj.href.replace('/_/veeam', ''));
        return super.getCanonicalRequest(signUrlObj, signedHeaders, contentSha256);
    }
}

const veeamAuthCredentials = {
    accessKey: 'accessKey1',
    secretKey: 'verySecretKey1',
};

const badVeeamAuthCredentials = {
    accessKey: 'accesKey1',
    secretKey: 'veryecretKey1',
};

const TEST_BUCKET = 'veeambucket';
const testArn = 'aws::iam:123456789012:user/bart';

const testCapacity = `<?xmlversion="1.0"encoding="UTF-8"?>
<CapacityInfo>
    <Capacity>1099511627776</Capacity>
    <Available>1099511627776</Available>
    <Used>0</Used>
</CapacityInfo>\n`;

const testCapacityMd5 = crypto.createHash('md5').update(testCapacity, 'utf-8').digest('hex');

const invalidTestCapacity = `<?xmlversion="1.0"encoding="UTF-8"?>
<CapacityInfo>
    <Capacity>1099511627776</Capacity>
    <Available>-5</Available>
    <Used>0</Used>
</CapacityInfo>\n`;

const invalidTestCapacityMd5 = crypto.createHash('md5').update(invalidTestCapacity, 'utf-8').digest('hex');

const testSystem = `<?xmlversion="1.0"encoding="UTF-8"?>
    <SystemInfo>
       <ProtocolVersion>"1.0"</ProtocolVersion>
       <ModelName>"ARTESCA"</ModelName>
       <ProtocolCapabilities>
          <CapacityInfo>true</CapacityInfo>
          <UploadSessions>false</UploadSessions>
          <IAMSTS>true</IAMSTS>
       </ProtocolCapabilities>
       <APIEndpoints>
            <IAMEndpoint>a</IAMEndpoint>
            <STSEndpoint>a</STSEndpoint>
       </APIEndpoints>
       <SystemRecommendations>
           <S3ConcurrentTaskLimit>0</S3ConcurrentTaskLimit>
           <S3MultiObjectDeleteLimit>1</S3MultiObjectDeleteLimit>
           <StorageCurrentTasksLimit>0</StorageCurrentTasksLimit>
           <KbBlockSize>256</KbBlockSize>
       </SystemRecommendations>
    </SystemInfo>\n`;

const testSystemMd5 = crypto.createHash('md5').update(testSystem, 'utf-8').digest('hex');

const invalidTestSystem = `<?xmlversion="1.0"encoding="UTF-8"?>
    <SystemInfo>
       <ProtocolVersion>"1.0"</ProtocolVersion>
       <ModelName>"ARTESCA"</ModelName>
       <ProtocolCapabilities>
          <CapacityInfo>true</CapacityInfo>
          <UploadSessions>false</UploadSessions>
          <IAMSTS>true</IAMSTS>
       </ProtocolCapabilities>
       <APIEndpoints>
            <IAMEndpoint>a</IAMEndpoint>
            <STSEndpoint>a</STSEndpoint>
       </APIEndpoints>
       <SystemRecommendations>
           <S3ConcurrentTaskLimit>0</S3ConcurrentTaskLimit>
           <S3MultiObjectDeleteLimit>-1</S3MultiObjectDeleteLimit>
           <StorageCurrentTasksLimit>0</StorageCurrentTasksLimit>
           <KbBlockSize>256</KbBlockSize>
       </SystemRecommendations>
    </SystemInfo>\n`;

const invalidTestSystemMd5 = crypto.createHash('md5').update(testSystem, 'utf-8').digest('hex');

let bucketUtil;
let s3;

/** makeVeeamRequest - utility function to generate a request going
 * through veeam route
 * @param {object} params - params for making request
 * @param {string} params.method - request method
 * @param {string} params.bucket - bucket name
 * @param {string} params.objectKey - object key
 * @param {object} [params.headers] - headers and their string values
 * @param {object} [params.authCredentials] - authentication credentials
 * @param {object} params.authCredentials.accessKey - access key
 * @param {object} params.authCredentials.secretKey - secret key
 * @param {string} [params.requestBody] - request body contents
 * @param {object} [params.queryObj] - request query parameters
 * @param {function} callback - with error and response parameters
 * @return {undefined} - and call callback
 */
function makeVeeamRequest(params, callback) {
    const { method, headers, bucket, objectKey, authCredentials, requestBody, queryObj } = params;
    const options = {
        authCredentials,
        hostname: ipAddress,
        port: 8000,
        method,
        headers,
        path: `/_/veeam/${bucket}/${objectKey}`,
        urlForSignature: `/${bucket}/${objectKey}`,
        requestBody,
        jsonResponse: false,
        queryObj,
    };
    makeRequest(options, callback);
}

(process.env.S3METADATA === 'mongodb' ? describe : describe.skip)('Veeam routes:', () => {
    let scuba;

    beforeEach(done => {
        scuba = new MockScuba();
        scuba.start();
        setTimeout(done, 500);
    });

    afterEach(() => {
        scuba.stop();
    });

    describe('veeam invalid requests:', () => {
        it('should return MethodNotAllowed for invalid request', done => {
            const options = {
                authCredentials: veeamAuthCredentials,
                hostname: ipAddress,
                port: 8000,
                method: 'GET',
                path: '/_/veeam',
                urlForSignature: '',
                jsonResponse: false,
            };
            makeRequest(options, (err, response) => {
                assert.strictEqual(response.statusCode, 405);
                done();
            });
        });
    });

    describe('veeam PUT routes:', () => {
        before(done => {
            bucketUtil = new BucketUtility('default', { signatureVersion: 'v4' });
            s3 = bucketUtil.s3;
            s3.send(new CreateBucketCommand({ Bucket: TEST_BUCKET }))
                .then(() => done())
                .catch(err => {
                    process.stdout.write(`Error creating bucket: ${err}\n`);
                    done(err);
                });
        });
        after(done => {
            bucketUtil
                .empty(TEST_BUCKET)
                .then(() => s3.send(new DeleteBucketCommand({ Bucket: TEST_BUCKET })))
                .then(() => done())
                .catch(done);
        });

        [
            ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml', testSystem, testSystemMd5],
            ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml', testCapacity, testCapacityMd5],
        ].forEach(key => {
            it(`PUT ${key[0]}`, done =>
                makeVeeamRequest(
                    {
                        method: 'PUT',
                        bucket: TEST_BUCKET,
                        objectKey: key[0],
                        headers: {
                            'content-length': key[1].length,
                            'content-md5': key[2],
                            'x-scal-canonical-id': testArn,
                        },
                        authCredentials: veeamAuthCredentials,
                        requestBody: key[1],
                    },
                    (err, response) => {
                        if (err) {
                            // Return the error, if any
                            return done(err);
                        }
                        assert.strictEqual(response.statusCode, 200);
                        return done();
                    },
                ));
        });

        [
            ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml', invalidTestSystem, invalidTestSystemMd5],
            ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml', invalidTestCapacity, invalidTestCapacityMd5],
        ].forEach(key => {
            it(`PUT ${key[0]} should fail for invalid XML`, done =>
                makeVeeamRequest(
                    {
                        method: 'PUT',
                        bucket: TEST_BUCKET,
                        objectKey: key[0],
                        headers: {
                            'content-length': key[1].length + 3,
                            'content-md5': key[2],
                            'x-scal-canonical-id': testArn,
                        },
                        authCredentials: veeamAuthCredentials,
                        requestBody: `${key[1]}gff`,
                    },
                    err => {
                        assert.strictEqual(err.code, 'MalformedXML');
                        return done();
                    },
                ));
        });

        [
            ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml', testSystem, testSystemMd5],
            ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml', testCapacity, testCapacityMd5],
        ].forEach(key => {
            it(`PUT ${key[0]} should fail if invalid credentials are sent`, done =>
                makeVeeamRequest(
                    {
                        method: 'PUT',
                        bucket: TEST_BUCKET,
                        objectKey: key[0],
                        headers: {
                            'content-length': key[1].length + 3,
                            'content-md5': key[2],
                            'x-scal-canonical-id': testArn,
                        },
                        authCredentials: badVeeamAuthCredentials,
                        requestBody: `${key[1]}gff`,
                    },
                    err => {
                        assert.strictEqual(err.code, 'InvalidAccessKeyId');
                        return done();
                    },
                ));
        });

        describe('streaming uploads with trailing checksum:', () => {
            const capacityKey = '.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml';
            let trailerDigest;

            const buildChunkedBody = digest =>
                `${testCapacity.length.toString(16)}\r\n${testCapacity}\r\n` +
                `0\r\nx-amz-checksum-crc64nvme:${digest}\r\n\r\n`;

            const makeStreamingVeeamRequest = (digest, done, callback) => {
                const requestBody = buildChunkedBody(digest);
                const req = new VeeamHttpRequestAuthV4(
                    `http://${ipAddress}:8000/_/veeam/${TEST_BUCKET}/${capacityKey}`,
                    Object.assign(
                        {
                            method: 'PUT',
                            headers: {
                                'content-length': requestBody.length,
                                'x-amz-decoded-content-length': testCapacity.length,
                                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                                'x-amz-trailer': 'x-amz-checksum-crc64nvme',
                                'x-scal-canonical-id': testArn,
                            },
                        },
                        veeamAuthCredentials,
                    ),
                    res => callback(res),
                );
                req.on('error', done);
                req.end(requestBody);
            };

            before(async () => {
                trailerDigest = await algorithms.crc64nvme.digest(Buffer.from(testCapacity));
            });

            it('should PUT capacity.xml with unsigned trailing checksum', done => {
                makeStreamingVeeamRequest(trailerDigest, done, res => {
                    assert.strictEqual(res.statusCode, 200);
                    res.on('data', () => {});
                    res.on('end', done);
                });
            });

            it('should return BadDigest for PUT capacity.xml with a wrong trailing checksum', done => {
                makeStreamingVeeamRequest('AAAAAAAAAAA=', done, res => {
                    assert.strictEqual(res.statusCode, 400);
                    const chunks = [];
                    res.on('data', chunk => chunks.push(chunk));
                    res.on('end', () => {
                        assert.match(chunks.join(''), /BadDigest/);
                        done();
                    });
                });
            });

            // Without a pre-set x-amz-content-sha256 header, the request
            // helper switches to signed streaming
            // (STREAMING-AWS4-HMAC-SHA256-PAYLOAD) and signs each chunk.
            it('should PUT capacity.xml with signed streaming (aws-chunked)', done => {
                const req = new VeeamHttpRequestAuthV4(
                    `http://${ipAddress}:8000/_/veeam/${TEST_BUCKET}/${capacityKey}`,
                    Object.assign(
                        {
                            method: 'PUT',
                            headers: {
                                'content-length': testCapacity.length,
                                'x-scal-canonical-id': testArn,
                            },
                        },
                        veeamAuthCredentials,
                    ),
                    res => {
                        assert.strictEqual(res.statusCode, 200);
                        res.on('data', () => {});
                        res.on('end', done);
                    },
                );
                req.on('error', done);
                // end() (rather than write() alone) is needed to send the
                // terminating zero-length signed chunk once 'finish' fires.
                req.end(testCapacity);
            });
        });
    });

    describe('veeam GET routes:', () => {
        beforeEach(done => {
            bucketUtil = new BucketUtility('default', { signatureVersion: 'v4' });
            s3 = bucketUtil.s3;
            s3.send(new CreateBucketCommand({ Bucket: TEST_BUCKET }))
                .then(() => done())
                .catch(err => {
                    process.stdout.write(`Error creating bucket: ${err}\n`);
                    done(err);
                });
        });
        afterEach(done => {
            bucketUtil
                .empty(TEST_BUCKET)
                .then(() => s3.send(new DeleteBucketCommand({ Bucket: TEST_BUCKET })))
                .then(() => done())
                .catch(done);
        });

        [
            ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml', testSystem, testSystemMd5],
            ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml', testCapacity, testCapacityMd5],
        ].forEach(key => {
            it(`GET ${key[0]} should return the expected XML file`, done => {
                scuba.incrementBytesForBucket(TEST_BUCKET, 0);
                async.waterfall(
                    [
                        next =>
                            makeVeeamRequest(
                                {
                                    method: 'PUT',
                                    bucket: TEST_BUCKET,
                                    objectKey: key[0],
                                    headers: {
                                        'content-length': key[1].length,
                                        'content-md5': key[2],
                                        'x-scal-canonical-id': testArn,
                                    },
                                    authCredentials: veeamAuthCredentials,
                                    requestBody: key[1],
                                },
                                (err, response) => {
                                    if (err) {
                                        return done(err);
                                    }
                                    assert.strictEqual(response.statusCode, 200);
                                    return next();
                                },
                            ),
                        next =>
                            makeVeeamRequest(
                                {
                                    method: 'GET',
                                    bucket: TEST_BUCKET,
                                    objectKey: key[0],
                                    headers: {
                                        'x-scal-canonical-id': testArn,
                                    },
                                    authCredentials: veeamAuthCredentials,
                                },
                                (err, response) => {
                                    if (err) {
                                        return done(err);
                                    }
                                    assert.strictEqual(response.statusCode, 200);
                                    assert.strictEqual(response.body.replaceAll(' ', ''), key[1].replaceAll(' ', ''));
                                    return next();
                                },
                            ),
                    ],
                    err => {
                        assert.ifError(err);
                        return done();
                    },
                );
            });
        });

        [
            ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml', testSystem, testSystemMd5],
            ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml', testCapacity, testCapacityMd5],
        ].forEach(key => {
            it(`GET ${key[0]} should return the expected XML file for cors requests`, done => {
                async.waterfall(
                    [
                        next =>
                            makeVeeamRequest(
                                {
                                    method: 'PUT',
                                    bucket: TEST_BUCKET,
                                    objectKey: key[0],
                                    headers: {
                                        origin: 'http://localhost:8000',
                                        'content-length': key[1].length,
                                        'content-md5': key[2],
                                        'x-scal-canonical-id': testArn,
                                    },
                                    authCredentials: veeamAuthCredentials,
                                    requestBody: key[1],
                                },
                                (err, response) => {
                                    if (err) {
                                        return done(err);
                                    }
                                    assert.strictEqual(response.statusCode, 200);
                                    return next();
                                },
                            ),
                        next =>
                            makeVeeamRequest(
                                {
                                    method: 'GET',
                                    bucket: TEST_BUCKET,
                                    objectKey: key[0],
                                    headers: {
                                        origin: 'http://localhost:8000',
                                        'x-scal-canonical-id': testArn,
                                    },
                                    authCredentials: veeamAuthCredentials,
                                },
                                (err, response) => {
                                    if (err) {
                                        return done(err);
                                    }
                                    assert.strictEqual(response.statusCode, 200);
                                    assert.strictEqual(response.body.replaceAll(' ', ''), key[1].replaceAll(' ', ''));
                                    return next();
                                },
                            ),
                    ],
                    err => {
                        assert.ifError(err);
                        return done();
                    },
                );
            });
        });

        [
            ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml', testSystem, testSystemMd5],
            ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml', testCapacity, testCapacityMd5],
        ].forEach(key => {
            it(`GET ${key[0]} should fail if no data in bucket metadata`, done =>
                makeVeeamRequest(
                    {
                        method: 'GET',
                        bucket: TEST_BUCKET,
                        objectKey: key[0],
                        headers: {
                            'x-scal-canonical-id': testArn,
                        },
                        authCredentials: veeamAuthCredentials,
                    },
                    err => {
                        assert.strictEqual(err.code, 'NoSuchKey');
                        return done();
                    },
                ));
        });

        it('GET capacity.xml should return 200 when scubaclient returns 404 (post-install scenario)', done => {
            // This test simulates the post-install scenario where scubaclient returns 404
            // because no metrics are available yet. By not calling scuba.incrementBytesForBucket,
            // the mock scuba server will return 404 for this bucket.

            async.waterfall(
                [
                    next =>
                        makeVeeamRequest(
                            {
                                method: 'PUT',
                                bucket: TEST_BUCKET,
                                objectKey: '.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml',
                                headers: {
                                    'content-length': testCapacity.length,
                                    'content-md5': testCapacityMd5,
                                    'x-scal-canonical-id': testArn,
                                },
                                authCredentials: veeamAuthCredentials,
                                requestBody: testCapacity,
                            },
                            (err, response) => {
                                if (err) {
                                    return done(err);
                                }
                                assert.strictEqual(response.statusCode, 200);
                                return next();
                            },
                        ),
                    next =>
                        makeVeeamRequest(
                            {
                                method: 'GET',
                                bucket: TEST_BUCKET,
                                objectKey: '.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml',
                                headers: {
                                    'x-scal-canonical-id': testArn,
                                },
                                authCredentials: veeamAuthCredentials,
                            },
                            (err, response) => {
                                if (err) {
                                    return done(err);
                                }
                                // Critical assertion: for 404 from scubaclient (no metrics yet),
                                // should return 200 with static capacity data (Used=0)
                                assert.strictEqual(
                                    response.statusCode,
                                    200,
                                    'should return 200 when scubaclient returns 404 (no metrics available)',
                                );
                                // Should return capacity.xml with static data
                                assert(response.body.includes('<CapacityInfo>'), 'should return capacity.xml content');
                                assert(
                                    response.body.includes('<Used>0</Used>'),
                                    'Used should be 0 from static bucket metadata',
                                );
                                return next();
                            },
                        ),
                ],
                err => {
                    assert.ifError(err);
                    return done();
                },
            );
        });

        it('GET system.xml should return 200 even when scubaclient is down', done => {
            // system.xml doesn't use scubaclient, so it should always work
            // This test stops scuba to verify system.xml is independent of utilization metrics
            async.waterfall(
                [
                    next =>
                        makeVeeamRequest(
                            {
                                method: 'PUT',
                                bucket: TEST_BUCKET,
                                objectKey: '.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml',
                                headers: {
                                    'content-length': testSystem.length,
                                    'content-md5': testSystemMd5,
                                    'x-scal-canonical-id': testArn,
                                },
                                authCredentials: veeamAuthCredentials,
                                requestBody: testSystem,
                            },
                            (err, response) => {
                                if (err) {
                                    return done(err);
                                }
                                assert.strictEqual(response.statusCode, 200);
                                return next();
                            },
                        ),
                    next => {
                        // Stop scuba - system.xml should still work
                        scuba.stop();
                        return next();
                    },
                    next =>
                        makeVeeamRequest(
                            {
                                method: 'GET',
                                bucket: TEST_BUCKET,
                                objectKey: '.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml',
                                headers: {
                                    'x-scal-canonical-id': testArn,
                                },
                                authCredentials: veeamAuthCredentials,
                            },
                            (err, response) => {
                                if (err) {
                                    return done(err);
                                }
                                assert.strictEqual(
                                    response.statusCode,
                                    200,
                                    'system.xml should always return 200 even when scuba is down',
                                );
                                assert.strictEqual(response.body.replaceAll(' ', ''), testSystem.replaceAll(' ', ''));
                                return next();
                            },
                        ),
                ],
                err => {
                    // Restart scuba for subsequent tests
                    scuba.start();
                    assert.ifError(err);
                    return done();
                },
            );
        });
    });

    describe('veeam DELETE routes:', () => {
        beforeEach(done => {
            bucketUtil = new BucketUtility('default', { signatureVersion: 'v4' });
            s3 = bucketUtil.s3;
            s3.send(new CreateBucketCommand({ Bucket: TEST_BUCKET }))
                .then(() => done())
                .catch(err => {
                    process.stdout.write(`Error creating bucket: ${err}\n`);
                    done(err);
                });
        });
        afterEach(done => {
            bucketUtil
                .empty(TEST_BUCKET)
                .then(() => s3.send(new DeleteBucketCommand({ Bucket: TEST_BUCKET })))
                .then(() => done())
                .catch(done);
        });

        [
            ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml', testSystem, testSystemMd5],
            ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml', testCapacity, testCapacityMd5],
        ].forEach(key => {
            it(`DELETE ${key[0]} should delete the XML file`, done => {
                async.waterfall(
                    [
                        next =>
                            makeVeeamRequest(
                                {
                                    method: 'PUT',
                                    bucket: TEST_BUCKET,
                                    objectKey: key[0],
                                    headers: {
                                        'content-length': key[1].length,
                                        'content-md5': key[2],
                                        'x-scal-canonical-id': testArn,
                                    },
                                    authCredentials: veeamAuthCredentials,
                                    requestBody: key[1],
                                },
                                (err, response) => {
                                    if (err) {
                                        return done(err);
                                    }
                                    assert.strictEqual(response.statusCode, 200);
                                    return next();
                                },
                            ),
                        next =>
                            makeVeeamRequest(
                                {
                                    method: 'GET',
                                    bucket: TEST_BUCKET,
                                    objectKey: key[0],
                                    headers: {
                                        'x-scal-canonical-id': testArn,
                                    },
                                    authCredentials: veeamAuthCredentials,
                                },
                                (err, response) => {
                                    if (err) {
                                        return done(err);
                                    }
                                    assert.strictEqual(response.statusCode, 200);
                                    assert.strictEqual(response.body.replaceAll(' ', ''), key[1].replaceAll(' ', ''));
                                    return next();
                                },
                            ),
                        next =>
                            makeVeeamRequest(
                                {
                                    method: 'DELETE',
                                    bucket: TEST_BUCKET,
                                    objectKey: key[0],
                                    headers: {
                                        'x-scal-canonical-id': testArn,
                                    },
                                    authCredentials: veeamAuthCredentials,
                                },
                                (err, response) => {
                                    if (err) {
                                        return done(err);
                                    }
                                    assert.strictEqual(response.statusCode, 204);
                                    return next();
                                },
                            ),
                        next =>
                            makeVeeamRequest(
                                {
                                    method: 'GET',
                                    bucket: TEST_BUCKET,
                                    objectKey: key[0],
                                    headers: {
                                        'x-scal-canonical-id': testArn,
                                    },
                                    authCredentials: veeamAuthCredentials,
                                },
                                err => {
                                    assert.strictEqual(err.code, 'NoSuchKey');
                                    return next();
                                },
                            ),
                    ],
                    err => {
                        assert.ifError(err);
                        return done();
                    },
                );
            });
        });

        [
            ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml', testSystem, testSystemMd5],
            ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml', testCapacity, testCapacityMd5],
        ].forEach(key => {
            it(`DELETE ${key[0]} should fail if XML doesn't exist yet`, done =>
                makeVeeamRequest(
                    {
                        method: 'DELETE',
                        bucket: TEST_BUCKET,
                        objectKey: key[0],
                        headers: {
                            'x-scal-canonical-id': testArn,
                        },
                        authCredentials: veeamAuthCredentials,
                    },
                    err => {
                        assert.strictEqual(err.code, 'NoSuchKey');
                        return done();
                    },
                ));
        });
    });

    describe('veeam HEAD routes:', () => {
        beforeEach(done => {
            bucketUtil = new BucketUtility('default', { signatureVersion: 'v4' });
            s3 = bucketUtil.s3;
            s3.send(new CreateBucketCommand({ Bucket: TEST_BUCKET }))
                .then(() => done())
                .catch(err => {
                    process.stdout.write(`Error creating bucket: ${err}\n`);
                    done(err);
                });
        });
        afterEach(done => {
            bucketUtil
                .empty(TEST_BUCKET)
                .then(() => s3.send(new DeleteBucketCommand({ Bucket: TEST_BUCKET })))
                .then(() => done())
                .catch(done);
        });

        [
            ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml', testSystem, testSystemMd5],
            ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml', testCapacity, testCapacityMd5],
        ].forEach(key => {
            it(`HEAD ${key[0]} should return the existing XML file metadata`, done => {
                async.waterfall(
                    [
                        next =>
                            makeVeeamRequest(
                                {
                                    method: 'PUT',
                                    bucket: TEST_BUCKET,
                                    objectKey: key[0],
                                    headers: {
                                        'content-length': key[1].length,
                                        'content-md5': key[2],
                                        'x-scal-canonical-id': testArn,
                                    },
                                    authCredentials: veeamAuthCredentials,
                                    requestBody: key[1],
                                },
                                (err, response) => {
                                    if (err) {
                                        return done(err);
                                    }
                                    assert.strictEqual(response.statusCode, 200);
                                    return next();
                                },
                            ),
                        next =>
                            makeVeeamRequest(
                                {
                                    method: 'HEAD',
                                    bucket: TEST_BUCKET,
                                    objectKey: key[0],
                                    headers: {
                                        'x-scal-canonical-id': testArn,
                                    },
                                    authCredentials: veeamAuthCredentials,
                                },
                                (err, response) => {
                                    if (err) {
                                        return done(err);
                                    }
                                    assert.strictEqual(response.statusCode, 200);
                                    return next();
                                },
                            ),
                    ],
                    err => {
                        assert.ifError(err);
                        return done();
                    },
                );
            });
        });

        [
            ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml', testSystem, testSystemMd5],
            ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml', testCapacity, testCapacityMd5],
        ].forEach(key => {
            it(`HEAD ${key[0]} should fail if no data in bucket metadata`, done =>
                makeVeeamRequest(
                    {
                        method: 'HEAD',
                        bucket: TEST_BUCKET,
                        objectKey: key[0],
                        headers: {
                            'x-scal-canonical-id': testArn,
                        },
                        authCredentials: veeamAuthCredentials,
                    },
                    (err, res) => {
                        assert.strictEqual(res.statusCode, 404);
                        return done();
                    },
                ));
        });
    });
});

// TODO {test_debt} handle query params tests with signature (happy path)
describe.skip('veeam LIST routes:', () => {
    beforeEach(done => {
        bucketUtil = new BucketUtility('default', { signatureVersion: 'v4' });
        s3 = bucketUtil.s3;
        s3.send(new CreateBucketCommand({ Bucket: TEST_BUCKET }))
            .then(() => done())
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                done(err);
            });
    });
    afterEach(done => {
        bucketUtil
            .empty(TEST_BUCKET)
            .then(() => s3.send(new DeleteBucketCommand({ Bucket: TEST_BUCKET })))
            .then(() => done())
            .catch(done);
    });
});
