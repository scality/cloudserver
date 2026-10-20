const {
    S3Client,
    PutObjectCommand,
    CopyObjectCommand,
    CreateMultipartUploadCommand,
    PutBucketEncryptionCommand,
    GetObjectCommand,
    UploadPartCommand,
    UploadPartCopyCommand,
    CompleteMultipartUploadCommand,
} = require('@aws-sdk/client-s3');
const crypto = require('crypto');
const { v4: uuidv4 } = require('uuid');
const { promisify } = require('util');
const config = require('../config.json');
const { auth } = require('@scality/arsenal');
const {
    KmsType,
    KmsProtocol,
    makeScalityArnPrefix,
    getKeyIdFromArn,
} = require('@scality/arsenal/build/lib/network/KMSInterface');
const { config: serverConfig } = require('../../../lib/Config');
const http = require('http');
const https = require('https');
const assert = require('assert');
const logger = { info: msg => process.stdout.write(`${msg}\n`) };
const async = require('async');

function _createBucketWithHeaders(name, headers, done) {
    const { transport, ipAddress, accessKey, secretKey } = config;
    const verbose = false;
    const options = {
        host: ipAddress,
        port: 8000,
        method: 'PUT',
        path: `/${name}/`,
        rejectUnauthorized: false,
    };

    if (headers) {
        Object.assign(options, { headers });
    }

    logger.info(`Creating encrypted bucket ${name}`);
    const client = transport === 'https' ? https : http;
    const request = client.request(options, response => {
        if (verbose) {
            logger.info('response status code', {
                statusCode: response.statusCode,
            });
            logger.info('response headers', { headers: response.headers });
        }
        const body = [];
        response.setEncoding('utf8');
        response.on('data', chunk => body.push(chunk));
        response.on('end', () => {
            if (response.statusCode >= 200 && response.statusCode < 300) {
                logger.info('Success', {
                    statusCode: response.statusCode,
                    body: verbose ? body.join('') : undefined,
                });
                done(null);
            } else {
                done({
                    statusCode: response.statusCode,
                    body: body.join(''),
                });
            }
        });
    });
    auth.client.generateV4Headers(request, '', accessKey, secretKey, 's3');
    if (verbose) {
        logger.info('request headers', { headers: request.getHeaders() });
    }
    request.end();
}

function _createBucket(name, encrypt, done) {
    const headers = encrypt ? { 'x-amz-scal-server-side-encryption': 'AES256' } : undefined;
    return _createBucketWithHeaders(name, headers, done);
}

function _buildS3() {
    const { transport, ipAddress, accessKey, secretKey } = config;
    const agent = transport === 'https' ? new https.Agent({ keepAlive: false }) : new http.Agent({ keepAlive: false });

    return new S3Client({
        endpoint: `${transport}://${ipAddress}:8000`,
        region: 'us-east-1',
        credentials: {
            accessKeyId: accessKey,
            secretAccessKey: secretKey,
        },
        forcePathStyle: true,
        requestHandler: {
            httpAgent: transport === 'http' ? agent : undefined,
            httpsAgent: transport === 'https' ? agent : undefined,
        },
    });
}
const s3 = _buildS3();

function _putObject(bucketName, objectName, encrypt, cb) {
    const params = {
        Bucket: bucketName,
        Key: objectName,
        Body: 'I am the best content ever',
    };

    if (encrypt) {
        Object.assign(params, {
            ServerSideEncryption: 'AES256',
        });
    }

    s3.send(new PutObjectCommand(params))
        .then(() => cb(null))
        .catch(err => cb(err));
}

function _copyObject(sourceBucket, sourceObject, targetBucket, targetObject, encrypt, cb) {
    const params = {
        Bucket: targetBucket,
        CopySource: `/${sourceBucket}/${sourceObject}`,
        Key: targetObject,
    };

    if (encrypt) {
        Object.assign(params, {
            ServerSideEncryption: 'AES256',
        });
    }

    s3.send(new CopyObjectCommand(params))
        .then(() => cb(null))
        .catch(err => cb(err));
}

function _initiateMultipartUpload(bucketName, objectName, encrypt, cb) {
    const params = {
        Bucket: bucketName,
        Key: objectName,
    };

    if (encrypt) {
        Object.assign(params, {
            ServerSideEncryption: 'AES256',
        });
    }

    s3.send(new CreateMultipartUploadCommand(params))
        .then(() => cb(null))
        .catch(err => cb(err));
}

describe('KMIP backed server-side encryption', () => {
    let bucketName;
    let objectName;

    beforeEach(() => {
        bucketName = uuidv4();
        objectName = uuidv4();
    });

    it('should create an encrypted bucket', done => {
        _createBucket(bucketName, true, err => {
            assert.equal(err, null, `Expected success, got error ${JSON.stringify(err)}`);
            done();
        });
    });

    it('should create an encrypted bucket and upload an object', done => {
        async.waterfall(
            [
                next => _createBucket(bucketName, true, err => next(err)),
                next => _putObject(bucketName, objectName, false, err => next(err)),
            ],
            err => {
                assert.equal(err, null, `Expected success, got error ${JSON.stringify(err)}`);
                done();
            },
        );
    });

    it('should allow object PUT with SSE header in encrypted bucket', done => {
        async.waterfall(
            [
                next => _createBucket(bucketName, true, err => next(err)),
                next => _putObject(bucketName, objectName, true, err => next(err)),
            ],
            err => {
                assert.equal(err, null, `Expected success, got error ${JSON.stringify(err)}`);
                done();
            },
        );
    });

    it('should allow object copy with SSE header in encrypted bucket', done => {
        async.waterfall(
            [
                next => _createBucket(bucketName, false, err => next(err)),
                next => _putObject(bucketName, objectName, false, err => next(err)),
                next => _createBucket(`${bucketName}2`, true, err => next(err)),
                next => _copyObject(bucketName, objectName, `${bucketName}2`, `${objectName}2`, true, err => next(err)),
            ],
            err => {
                assert.equal(err, null, `Expected success, got error ${JSON.stringify(err)}`);
                done();
            },
        );
    });

    it('should allow creating mpu with SSE header in encrypted bucket', done => {
        async.waterfall(
            [
                next => _createBucket(bucketName, true, err => next(err)),
                next => _initiateMultipartUpload(bucketName, objectName, true, err => next(err)),
            ],
            err => {
                assert.equal(err, null, `Expected success, got error ${JSON.stringify(err)}`);
                done();
            },
        );
    });
});

describe('KMIP backed server-side encryption with other KMSs configured', () => {
    const createBucket = promisify(_createBucket);
    const createBucketWithHeaders = promisify(_createBucketWithHeaders);
    const nonDefaultEntries = serverConfig.kms.filter(entry => !entry.default);
    const body = 'I am the best content ever';
    let bucketName;
    let objectName;

    function assertInvalidArgument(err) {
        assert.strictEqual(err.name, 'InvalidArgument');
        assert.strictEqual(err.$metadata.httpStatusCode, 400);
        return true;
    }

    async function createKeyArn(entry) {
        if (entry.protocol === KmsProtocol.file) {
            // The file KMS uses the key ID itself as the master key
            return `${makeScalityArnPrefix(KmsType.internal, entry.protocol, entry.providerName)}${crypto
                .randomBytes(32)
                .toString('hex')}`;
        }
        throw new Error(`no key creation for the ${entry.protocol} KMS ${entry.providerName}`);
    }

    async function assertEncryptedWith(bucket, key, keyArn) {
        const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
        assert.strictEqual(await res.Body.transformToString(), body);
        const expectedKeyId = serverConfig.kmsHideScalityArn ? getKeyIdFromArn(keyArn) : keyArn;
        assert.strictEqual(res.SSEKMSKeyId, expectedKeyId);
    }

    function putObject(bucket, key, keyArn) {
        const params = { Bucket: bucket, Key: key, Body: body };
        if (keyArn) {
            Object.assign(params, { ServerSideEncryption: 'aws:kms', SSEKMSKeyId: keyArn });
        }
        return s3.send(new PutObjectCommand(params));
    }

    function putBucketEncryption(bucket, keyArn) {
        return s3.send(
            new PutBucketEncryptionCommand({
                Bucket: bucket,
                ServerSideEncryptionConfiguration: {
                    Rules: [
                        {
                            ApplyServerSideEncryptionByDefault: {
                                SSEAlgorithm: 'aws:kms',
                                KMSMasterKeyID: keyArn,
                            },
                        },
                    ],
                },
            }),
        );
    }

    before(() => {
        assert(nonDefaultEntries.length > 0, 'the server config must have KMSs next to the default one');
    });

    beforeEach(async () => {
        bucketName = uuidv4();
        objectName = uuidv4();
        await createBucket(bucketName, false);
    });

    nonDefaultEntries.forEach(entry => {
        describe(`with a key of the ${entry.protocol} KMS ${entry.providerName}`, () => {
            let keyArn;

            before(async () => {
                keyArn = await createKeyArn(entry);
            });

            it('should encrypt PutObject with the key and decrypt the object on GET', async () => {
                await putObject(bucketName, objectName, keyArn);
                await assertEncryptedWith(bucketName, objectName, keyArn);
            });

            it('should encrypt with the key set by PutBucketEncryption', async () => {
                await putBucketEncryption(bucketName, keyArn);
                await putObject(bucketName, objectName);
                await assertEncryptedWith(bucketName, objectName, keyArn);
            });

            it('should encrypt with the key set on CreateBucket', async () => {
                const encryptedBucketName = `${bucketName}2`;
                await createBucketWithHeaders(encryptedBucketName, {
                    'x-amz-scal-server-side-encryption': 'aws:kms',
                    'x-amz-scal-server-side-encryption-aws-kms-key-id': keyArn,
                });
                await putObject(encryptedBucketName, objectName);
                await assertEncryptedWith(encryptedBucketName, objectName, keyArn);
            });

            it('should encrypt a multipart upload with the key', async () => {
                const { UploadId } = await s3.send(
                    new CreateMultipartUploadCommand({
                        Bucket: bucketName,
                        Key: objectName,
                        ServerSideEncryption: 'aws:kms',
                        SSEKMSKeyId: keyArn,
                    }),
                );
                const { ETag } = await s3.send(
                    new UploadPartCommand({
                        Bucket: bucketName,
                        Key: objectName,
                        UploadId,
                        PartNumber: 1,
                        Body: body,
                    }),
                );
                await s3.send(
                    new CompleteMultipartUploadCommand({
                        Bucket: bucketName,
                        Key: objectName,
                        UploadId,
                        MultipartUpload: { Parts: [{ ETag, PartNumber: 1 }] },
                    }),
                );
                await assertEncryptedWith(bucketName, objectName, keyArn);
            });

            it('should copy an object of the default KMS to the key', async () => {
                const sourceBucketName = `${bucketName}2`;
                await createBucket(sourceBucketName, true);
                await putObject(sourceBucketName, objectName);
                await s3.send(
                    new CopyObjectCommand({
                        Bucket: bucketName,
                        Key: objectName,
                        CopySource: `/${sourceBucketName}/${objectName}`,
                        ServerSideEncryption: 'aws:kms',
                        SSEKMSKeyId: keyArn,
                    }),
                );
                await assertEncryptedWith(bucketName, objectName, keyArn);
            });

            it('should copy a part encrypted with the key into an upload of the default KMS', async () => {
                const targetObjectName = `${objectName}2`;
                await putObject(bucketName, objectName, keyArn);
                const { UploadId } = await s3.send(
                    new CreateMultipartUploadCommand({
                        Bucket: bucketName,
                        Key: targetObjectName,
                        ServerSideEncryption: 'AES256',
                    }),
                );
                const { CopyPartResult } = await s3.send(
                    new UploadPartCopyCommand({
                        Bucket: bucketName,
                        Key: targetObjectName,
                        UploadId,
                        PartNumber: 1,
                        CopySource: `/${bucketName}/${objectName}`,
                    }),
                );
                await s3.send(
                    new CompleteMultipartUploadCommand({
                        Bucket: bucketName,
                        Key: targetObjectName,
                        UploadId,
                        MultipartUpload: { Parts: [{ ETag: CopyPartResult.ETag, PartNumber: 1 }] },
                    }),
                );
                const res = await s3.send(new GetObjectCommand({ Bucket: bucketName, Key: targetObjectName }));
                assert.strictEqual(await res.Body.transformToString(), body);
            });
        });
    });

    describe('with a key of a KMS that is not configured', () => {
        const keyArn = `${makeScalityArnPrefix(KmsType.external, KmsProtocol.kmip, 'unconfigured')}1`;

        it('should refuse PutObject', () =>
            assert.rejects(putObject(bucketName, objectName, keyArn), assertInvalidArgument));

        it('should refuse PutBucketEncryption on a bucket without a bucket key', () =>
            assert.rejects(putBucketEncryption(bucketName, keyArn), assertInvalidArgument));
    });
});
