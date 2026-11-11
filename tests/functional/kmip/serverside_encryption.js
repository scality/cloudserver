const {
    S3Client,
    PutObjectCommand,
    CopyObjectCommand,
    CreateMultipartUploadCommand,
    PutBucketEncryptionCommand,
    GetBucketEncryptionCommand,
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
const { KmsAWSClient } = require('@scality/arsenal').network;
const { KmsType, KmsProtocol, makeScalityArnPrefix } = require('@scality/arsenal/build/lib/network/KMSInterface');
const { config: serverConfig } = require('../../../lib/Config');
const { DummyRequestLogger } = require('../../unit/helpers');
const { getExpectedKeyId } = require('../../utilities/kmsKeyId-util');
const { getMetrics, parseMetric } = require('../../utilities/metrics-util');
const kms = require('../../../lib/kms/wrapper');
const http = require('http');
const https = require('https');
const assert = require('assert');
const logger = { info: msg => process.stdout.write(`${msg}\n`) };

// A KMS of the server config that accepts connections and never answers
const silentKmsProviderName = 'silent';
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

/**
 * @param {object} [options] - S3 client options, such as `maxAttempts`
 * @returns {S3Client} a client of the server under test
 */
function _buildS3(options) {
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
        ...options,
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
    const nonDefaultEntries = serverConfig.kms.filter(
        entry => !entry.default && entry.providerName !== silentKmsProviderName,
    );
    const body = 'I am the best content ever';
    let bucketName;
    let objectName;

    function assertInvalidArgument(err) {
        assert.strictEqual(err.name, 'InvalidArgument');
        assert.strictEqual(err.$metadata.httpStatusCode, 400);
        return true;
    }

    function createBucketKeyArn(client) {
        return new Promise((resolve, reject) =>
            client.createBucketKey('multiKmsTestBucket', new DummyRequestLogger(), (err, keyId, keyArn) =>
                err ? reject(err) : resolve(keyArn),
            ),
        );
    }

    async function createKeyArn(entry) {
        if (entry.protocol === KmsProtocol.file) {
            // The file KMS uses the key ID itself as the master key
            return `${makeScalityArnPrefix(KmsType.internal, entry.protocol, entry.providerName)}${crypto
                .randomBytes(32)
                .toString('hex')}`;
        }
        if (entry.protocol === KmsProtocol.aws_kms) {
            return createBucketKeyArn(new KmsAWSClient({ kmsAWS: entry.settings }));
        }
        throw new Error(`no key creation for the ${entry.protocol} KMS ${entry.providerName}`);
    }

    async function assertEncryptedWith(bucket, key, keyArn) {
        const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
        assert.strictEqual(await res.Body.transformToString(), body);
        assert.strictEqual(res.SSEKMSKeyId, getExpectedKeyId(keyArn));
    }

    async function assertPutObjectEncryptsWith(keyArn) {
        const { SSEKMSKeyId } = await putObject(bucketName, objectName, keyArn);
        assert.strictEqual(SSEKMSKeyId, getExpectedKeyId(keyArn));
        await assertEncryptedWith(bucketName, objectName, keyArn);
    }

    async function assertBucketEncryptionEncryptsWith(keyArn) {
        await putBucketEncryption(bucketName, keyArn);
        assert.strictEqual(await getBucketEncryptionKeyId(bucketName), getExpectedKeyId(keyArn));
        await putObject(bucketName, objectName);
        await assertEncryptedWith(bucketName, objectName, keyArn);
    }

    async function assertReturnedKeyIdEncryptsWith(keyArn) {
        await putObject(bucketName, objectName, keyArn);
        const { SSEKMSKeyId } = await s3.send(new GetObjectCommand({ Bucket: bucketName, Key: objectName }));
        await putObject(bucketName, `${objectName}2`, SSEKMSKeyId);
        await assertEncryptedWith(bucketName, `${objectName}2`, keyArn);
    }

    async function getBucketEncryptionKeyId(bucket) {
        const res = await s3.send(new GetBucketEncryptionCommand({ Bucket: bucket }));
        return res.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.KMSMasterKeyID;
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

            it('should encrypt PutObject with the key and decrypt the object on GET', () =>
                assertPutObjectEncryptsWith(keyArn));

            it('should encrypt with the key ID returned on GET when it is sent back', () =>
                assertReturnedKeyIdEncryptsWith(keyArn));

            it('should encrypt with the key set by PutBucketEncryption', () =>
                assertBucketEncryptionEncryptsWith(keyArn));

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
                const { UploadId, SSEKMSKeyId } = await s3.send(
                    new CreateMultipartUploadCommand({
                        Bucket: bucketName,
                        Key: objectName,
                        ServerSideEncryption: 'aws:kms',
                        SSEKMSKeyId: keyArn,
                    }),
                );
                assert.strictEqual(SSEKMSKeyId, getExpectedKeyId(keyArn));
                const part = await s3.send(
                    new UploadPartCommand({
                        Bucket: bucketName,
                        Key: objectName,
                        UploadId,
                        PartNumber: 1,
                        Body: body,
                    }),
                );
                assert.strictEqual(part.SSEKMSKeyId, getExpectedKeyId(keyArn));
                const complete = await s3.send(
                    new CompleteMultipartUploadCommand({
                        Bucket: bucketName,
                        Key: objectName,
                        UploadId,
                        MultipartUpload: { Parts: [{ ETag: part.ETag, PartNumber: 1 }] },
                    }),
                );
                assert.strictEqual(complete.SSEKMSKeyId, getExpectedKeyId(keyArn));
                await assertEncryptedWith(bucketName, objectName, keyArn);
            });

            it('should copy an object of the default KMS to the key', async () => {
                const sourceBucketName = `${bucketName}2`;
                await createBucket(sourceBucketName, true);
                await putObject(sourceBucketName, objectName);
                const { SSEKMSKeyId } = await s3.send(
                    new CopyObjectCommand({
                        Bucket: bucketName,
                        Key: objectName,
                        CopySource: `/${sourceBucketName}/${objectName}`,
                        ServerSideEncryption: 'aws:kms',
                        SSEKMSKeyId: keyArn,
                    }),
                );
                assert.strictEqual(SSEKMSKeyId, getExpectedKeyId(keyArn));
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

    describe('with a key of the default KMS', () => {
        let keyArn;

        before(async () => {
            keyArn = await createBucketKeyArn(kms.client);
        });

        it('should encrypt PutObject with the key and decrypt the object on GET', () =>
            assertPutObjectEncryptsWith(keyArn));

        it('should encrypt with the key ID returned on GET when it is sent back', () =>
            assertReturnedKeyIdEncryptsWith(keyArn));

        it('should encrypt with the key set by PutBucketEncryption', () => assertBucketEncryptionEncryptsWith(keyArn));
    });

    describe('with a key of a KMS that is not configured', () => {
        const keyArn = `${makeScalityArnPrefix(KmsType.external, KmsProtocol.kmip, 'unconfigured')}1`;

        function assertRefusedNamingOnlyTheKey(err) {
            assertInvalidArgument(err);
            assert.strictEqual(err.message, `KMS Scality KeyArn "${keyArn}" doesn't match any configured providers`);
            return true;
        }

        it('should refuse PutObject', () =>
            assert.rejects(putObject(bucketName, objectName, keyArn), assertRefusedNamingOnlyTheKey));

        it('should refuse PutBucketEncryption on a bucket without a bucket key', () =>
            assert.rejects(putBucketEncryption(bucketName, keyArn), assertRefusedNamingOnlyTheKey));
    });

    describe('with a key of a KMS that does not answer', () => {
        const keyArn = `${makeScalityArnPrefix(KmsType.external, KmsProtocol.kmip, silentKmsProviderName)}1`;

        it('should fail PutObject with an internal error after the time limit of the KMS', () =>
            assert.rejects(putObject(bucketName, objectName, keyArn), err => {
                assert.strictEqual(err.name, 'InternalError');
                assert.strictEqual(err.$metadata.httpStatusCode, 500);
                assert.match(err.message, /ETIMEDOUT/);
                return true;
            }));
    });

    describe('KMS request metrics', () => {
        const defaultProviderName = serverConfig.kms.find(entry => entry.default).providerName;
        const awsKmsEntry = nonDefaultEntries.find(entry => entry.protocol === KmsProtocol.aws_kms);
        let kmipKeyArn;
        let awsKmsKeyArn;

        before(async () => {
            kmipKeyArn = await createBucketKeyArn(kms.client);
            awsKmsKeyArn = await createKeyArn(awsKmsEntry);
        });

        /**
         * @param {object} labels - `{ provider, operation, outcome }`
         * @returns {Promise<{ count: number, sum: number }>} the count of the KMS requests
         * with these labels and the sum of their durations in seconds, from one scrape
         */
        async function getKmsRequestMetrics({ provider, operation, outcome }) {
            const metrics = await getMetrics(config.ipAddress);
            // in the order the labels are printed
            const labels = { provider, operation, outcome };
            const read = suffix =>
                parseFloat(parseMetric(metrics, `s3_cloudserver_kms_request_duration_seconds_${suffix}`, labels) ?? 0);
            return { count: read('count'), sum: read('sum') };
        }

        /**
         * Assert that an action sends one request to a KMS, counted with these labels
         * @param {object} labels - `{ provider, operation, outcome }`
         * @param {function} action - returns a promise
         * @returns {Promise<number>} the duration of the request in seconds
         */
        async function assertCountsOneRequest(labels, action) {
            const before = await getKmsRequestMetrics(labels);
            await action();
            const after = await getKmsRequestMetrics(labels);
            assert.strictEqual(after.count, before.count + 1, JSON.stringify(labels));
            return after.sum - before.sum;
        }

        async function getObjectBody() {
            const res = await s3.send(new GetObjectCommand({ Bucket: bucketName, Key: objectName }));
            return res.Body.transformToString();
        }

        it('should count the requests a KMIP KMS answers as successes, by operation', async () => {
            const labels = { provider: defaultProviderName, outcome: 'success' };
            await assertCountsOneRequest({ ...labels, operation: 'Encrypt' }, () =>
                putObject(bucketName, objectName, kmipKeyArn),
            );
            await assertCountsOneRequest({ ...labels, operation: 'Decrypt' }, getObjectBody);
        });

        it('should count the requests an AWS KMS answers as successes, by operation', async () => {
            const labels = { provider: awsKmsEntry.providerName, outcome: 'success' };
            await assertCountsOneRequest({ ...labels, operation: 'GenerateDataKey' }, () =>
                putObject(bucketName, objectName, awsKmsKeyArn),
            );
            await assertCountsOneRequest({ ...labels, operation: 'Decrypt' }, getObjectBody);
        });

        it('should count a request the KMS fails as an error', () =>
            assertCountsOneRequest({ provider: defaultProviderName, operation: 'Encrypt', outcome: 'error' }, () =>
                assert.rejects(putObject(bucketName, objectName, `${kms.arnPrefix}${uuidv4()}`)),
            ));

        it('should count a request over the time limit of the KMS as a timeout, with its duration', async () => {
            // the S3 client would send the PutObject again on the internal error
            const s3SingleAttempt = _buildS3({ maxAttempts: 1 });
            const keyArn = `${makeScalityArnPrefix(KmsType.external, KmsProtocol.kmip, silentKmsProviderName)}1`;
            const { requestTimeoutMs } = serverConfig.kms.find(entry => entry.providerName === silentKmsProviderName)
                .settings.transport;

            const durationSeconds = await assertCountsOneRequest(
                { provider: silentKmsProviderName, operation: 'Encrypt', outcome: 'timeout' },
                () =>
                    assert.rejects(
                        s3SingleAttempt.send(
                            new PutObjectCommand({
                                Bucket: bucketName,
                                Key: objectName,
                                Body: body,
                                ServerSideEncryption: 'aws:kms',
                                SSEKMSKeyId: keyArn,
                            }),
                        ),
                    ),
            );

            assert.ok(
                // the timer can fire 1 ms early
                durationSeconds >= (requestTimeoutMs - 1) / 1000 && durationSeconds < (2 * requestTimeoutMs) / 1000,
                `The request should take about ${requestTimeoutMs} ms, took ${durationSeconds} s`,
            );
        });
    });
});

describe('KMS health in the deep health check', () => {
    function getDeepHealthcheck() {
        return new Promise((resolve, reject) => {
            const req = http.get({ host: config.ipAddress, port: serverConfig.metricsPort, path: '/ready' }, res => {
                let body = '';
                res.on('data', chunk => {
                    body += chunk;
                });
                res.on('end', () => resolve({ statusCode: res.statusCode, body: JSON.parse(body) }));
            });
            req.on('error', reject);
        });
    }

    it('should report every configured KMS that answers, keyed by kms: and the provider name', async () => {
        const { statusCode, body } = await getDeepHealthcheck();

        assert.strictEqual(statusCode, 200);
        serverConfig.kms
            .filter(({ providerName }) => providerName !== silentKmsProviderName)
            .forEach(({ protocol, providerName }) => {
                const { lastChecked, ...health } = body[`kms:${providerName}`];
                assert.deepStrictEqual(health, { code: 200, message: 'OK' }, `KMS ${providerName}`);
                // A KMS whose client has no health check reports OK without being checked; today only the KMIP client has one
                assert.strictEqual(lastChecked !== undefined, protocol === KmsProtocol.kmip, `KMS ${providerName}`);
            });
    });

    it('should report a KMS that does not answer as failed after its time limit, without blocking start-up', async () => {
        const { statusCode, body } = await getDeepHealthcheck();

        assert.strictEqual(statusCode, 200);
        const { code, message, description } = body[`kms:${silentKmsProviderName}`];
        assert.strictEqual(code, 500);
        assert.strictEqual(message, 'KMS health check failed');
        // Checked at start-up, while the KMS was already listening
        assert.match(description, /ETIMEDOUT/);
    });
});
