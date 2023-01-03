const assert = require('assert');
const crypto = require('crypto');
const async = require('async');

const { makeRequest } = require('../../functional/raw-node/utils/makeRequest');
const BucketUtility =
    require('../../functional/aws-node-sdk/lib/utility/bucket-util');

const ipAddress = process.env.IP ? process.env.IP : '127.0.0.1';

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

const testCapacity = `<?xml version="1.0" encoding="utf-8"?>
<CapacityInfo>
    <Capacity>1099511627776</Capacity>
    <Available>0</Available>
    <Used>0</Used>
</CapacityInfo>`;

const testCapacityMd5 = crypto.createHash('md5')
    .update(testCapacity, 'utf-8')
    .digest('hex');

const invalidTestCapacity = `<?xml version="1.0" encoding="utf-8"?>
<CapacityInfo>
    <Capacity>1099511627776</Capacity>
    <Available>-5</Available>
    <Used>0</Used>
</CapacityInfo>`;

const invalidTestCapacityMd5 = crypto.createHash('md5')
    .update(invalidTestCapacity, 'utf-8')
    .digest('hex');

const testSystem = `<?xml version="1.0" encoding="utf-8"?>
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
    </SystemInfo>`;

const testSystemMd5 = crypto.createHash('md5')
    .update(testSystem, 'utf-8')
    .digest('hex');

const invalidTestSystem = `<?xml version="1.0" encoding="utf-8"?>
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
    </SystemInfo>`;

const invalidTestSystemMd5 = crypto.createHash('md5')
    .update(testSystem, 'utf-8')
    .digest('hex');

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
    const { method, headers, bucket, objectKey,
        authCredentials, requestBody, queryObj } = params;
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

describe('veeam PUT routes', () => {
    before(done => {
        bucketUtil = new BucketUtility(
            'default', { signatureVersion: 'v4' });
        s3 = bucketUtil.s3;
        s3.createBucket({ Bucket: TEST_BUCKET }).promise()
            .then(() => done())
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
    });
    after(done => {
        bucketUtil.empty(TEST_BUCKET)
            .then(() => s3.deleteBucket({ Bucket: TEST_BUCKET }).promise())
            .then(() => done());
    });

    [
        ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml', testSystem, testSystemMd5],
        ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml', testCapacity, testCapacityMd5],
    ].forEach(key => {
        it(`PUT ${key[0]}`, done => makeVeeamRequest({
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
            }, (err, response) => {
                if (err) {
                    // Return the error, if any
                    return done(err);
                }
                assert.strictEqual(response.statusCode, 200);
                return done();
            }));
    });

    [
        ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml', invalidTestSystem, invalidTestSystemMd5],
        ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml', invalidTestCapacity, invalidTestCapacityMd5],
    ].forEach(key => {
        it(`PUT ${key[0]} should fail for invalid XML`, done => makeVeeamRequest({
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
            }, err => {
                assert.strictEqual(err.code, 'MalformedXML');
                return done();
            }));
    });

    [
        ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml', testSystem, testSystemMd5],
        ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml', testCapacity, testCapacityMd5],
    ].forEach(key => {
        it(`PUT ${key[0]} should fail if invalid credentials are sent`, done => makeVeeamRequest({
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
            }, err => {
                assert.strictEqual(err.code, 'InvalidAccessKeyId');
                return done();
            }));
    });
});

describe('veeam GET routes', () => {
    beforeEach(done => {
        bucketUtil = new BucketUtility(
            'default', { signatureVersion: 'v4' });
        s3 = bucketUtil.s3;
        s3.createBucket({ Bucket: TEST_BUCKET }).promise()
            .then(() => done())
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
    });
    afterEach(done => {
        bucketUtil.empty(TEST_BUCKET)
            .then(() => s3.deleteBucket({ Bucket: TEST_BUCKET }).promise())
            .then(() => done());
    });

    [
        ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml', testSystem, testSystemMd5],
        ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml', testCapacity, testCapacityMd5],
    ].forEach(key => {
        it(`GET ${key[0]} should return the expected XML file`, done => {
            async.waterfall([
                next => makeVeeamRequest({
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
                }, (err, response) => {
                    if (err) {
                        return done(err);
                    }
                    assert.strictEqual(response.statusCode, 200);
                    return next();
                }),
                next => makeVeeamRequest({
                    method: 'GET',
                    bucket: TEST_BUCKET,
                    objectKey: key[0],
                    headers: {
                        'x-scal-canonical-id': testArn,
                    },
                    authCredentials: veeamAuthCredentials,
                }, (err, response) => {
                    if (err) {
                        return done(err);
                    }
                    assert.strictEqual(response.statusCode, 200);
                    assert.strictEqual(response.body.replaceAll(' ', ''), key[1].replaceAll(' ', ''));
                    return next();
                }),
            ], err => {
                assert.ifError(err);
                return done();
            });
        });
    });

    [
        ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml', testSystem, testSystemMd5],
        ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml', testCapacity, testCapacityMd5],
    ].forEach(key => {
        it(`GET ${key[0]} should fail if no data in bucket metadata`, done => makeVeeamRequest({
                method: 'GET',
                bucket: TEST_BUCKET,
                objectKey: key[0],
                headers: {
                    'x-scal-canonical-id': testArn,
                },
                authCredentials: veeamAuthCredentials,
            }, err => {
                assert.strictEqual(err.code, 'NoSuchKey');
                return done();
            }));
    });
});

describe('veeam DELETE routes', () => {
    beforeEach(done => {
        bucketUtil = new BucketUtility(
            'default', { signatureVersion: 'v4' });
        s3 = bucketUtil.s3;
        s3.createBucket({ Bucket: TEST_BUCKET }).promise()
            .then(() => done())
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
    });
    afterEach(done => {
        bucketUtil.empty(TEST_BUCKET)
            .then(() => s3.deleteBucket({ Bucket: TEST_BUCKET }).promise())
            .then(() => done());
    });

    [
        ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml', testSystem, testSystemMd5],
        ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml', testCapacity, testCapacityMd5],
    ].forEach(key => {
        it(`DELETE ${key[0]} should delete the XML file`, done => {
            async.waterfall([
                next => makeVeeamRequest({
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
                }, (err, response) => {
                    if (err) {
                        return done(err);
                    }
                    assert.strictEqual(response.statusCode, 200);
                    return next();
                }),
                next => makeVeeamRequest({
                    method: 'GET',
                    bucket: TEST_BUCKET,
                    objectKey: key[0],
                    headers: {
                        'x-scal-canonical-id': testArn,
                    },
                    authCredentials: veeamAuthCredentials,
                }, (err, response) => {
                    if (err) {
                        return done(err);
                    }
                    assert.strictEqual(response.statusCode, 200);
                    assert.strictEqual(response.body.replaceAll(' ', ''), key[1].replaceAll(' ', ''));
                    return next();
                }),
                next => makeVeeamRequest({
                    method: 'DELETE',
                    bucket: TEST_BUCKET,
                    objectKey: key[0],
                    headers: {
                        'x-scal-canonical-id': testArn,
                    },
                    authCredentials: veeamAuthCredentials,
                }, (err, response) => {
                    if (err) {
                        return done(err);
                    }
                    assert.strictEqual(response.statusCode, 204);
                    return next();
                }),
                next => makeVeeamRequest({
                    method: 'GET',
                    bucket: TEST_BUCKET,
                    objectKey: key[0],
                    headers: {
                        'x-scal-canonical-id': testArn,
                    },
                    authCredentials: veeamAuthCredentials,
                }, err => {
                    assert.strictEqual(err.code, 'NoSuchKey');
                    return next();
                }),
            ], err => {
                assert.ifError(err);
                return done();
            });
        });
    });

    [
        ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml', testSystem, testSystemMd5],
        ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml', testCapacity, testCapacityMd5],
    ].forEach(key => {
        it(`DELETE ${key[0]} should fail if XML doesn't exist yet`, done => makeVeeamRequest({
                method: 'DELETE',
                bucket: TEST_BUCKET,
                objectKey: key[0],
                headers: {
                    'x-scal-canonical-id': testArn,
                },
                authCredentials: veeamAuthCredentials,
            }, err => {
                assert.strictEqual(err.code, 'NoSuchKey');
                return done();
            }));
    });
});

describe('veeam HEAD routes', () => {
    beforeEach(done => {
        bucketUtil = new BucketUtility(
            'default', { signatureVersion: 'v4' });
        s3 = bucketUtil.s3;
        s3.createBucket({ Bucket: TEST_BUCKET }).promise()
            .then(() => done())
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
    });
    afterEach(done => {
        bucketUtil.empty(TEST_BUCKET)
            .then(() => s3.deleteBucket({ Bucket: TEST_BUCKET }).promise())
            .then(() => done());
    });

    [
        ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml', testSystem, testSystemMd5],
        ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml', testCapacity, testCapacityMd5],
    ].forEach(key => {
        it(`HEAD ${key[0]} should return the existing XML file metadata`, done => {
            async.waterfall([
                next => makeVeeamRequest({
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
                }, (err, response) => {
                    if (err) {
                        return done(err);
                    }
                    assert.strictEqual(response.statusCode, 200);
                    return next();
                }),
                next => makeVeeamRequest({
                    method: 'HEAD',
                    bucket: TEST_BUCKET,
                    objectKey: key[0],
                    headers: {
                        'x-scal-canonical-id': testArn,
                    },
                    authCredentials: veeamAuthCredentials,
                }, (err, response) => {
                    if (err) {
                        return done(err);
                    }
                    assert.strictEqual(response.statusCode, 200);
                    return next();
                }),
            ], err => {
                assert.ifError(err);
                return done();
            });
        });
    });

    [
        ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml', testSystem, testSystemMd5],
        ['.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml', testCapacity, testCapacityMd5],
    ].forEach(key => {
        it(`HEAD ${key[0]} should fail if no data in bucket metadata`, done => makeVeeamRequest({
                method: 'HEAD',
                bucket: TEST_BUCKET,
                objectKey: key[0],
                headers: {
                    'x-scal-canonical-id': testArn,
                },
                authCredentials: veeamAuthCredentials,
            }, (err, res) => {
                assert.strictEqual(res.statusCode, 404);
                return done();
            }));
    });
});


// TODO {test_debt} handle query params tests with signature (happy path)
describe.skip('veeam LIST routes', () => {
    beforeEach(done => {
        bucketUtil = new BucketUtility(
            'default', { signatureVersion: 'v4' });
        s3 = bucketUtil.s3;
        s3.createBucket({ Bucket: TEST_BUCKET }).promise()
            .then(() => done())
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
    });
    afterEach(done => {
        bucketUtil.empty(TEST_BUCKET)
            .then(() => s3.deleteBucket({ Bucket: TEST_BUCKET }).promise())
            .then(() => done());
    });
});
