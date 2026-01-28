const assert = require('assert');
const fs = require('fs');
const path = require('path');
const tv4 = require('tv4');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { config, serverAccessLogsModes } = require('../../../../../lib/Config');

const TEST_CONFIG = {
    MAX_LOG_WAIT_RETRIES: 50,
    LOG_POLL_DELAY_MS: 100,
};

// Load the JSON schema
const schemaPath = path.join(__dirname, '../../../../../schema/server_access_log.schema.json');
const schema = JSON.parse(fs.readFileSync(schemaPath, 'utf8'));

function truncateLogFileIfExists(filePath) {
    if (fs.existsSync(filePath)) {
        fs.truncateSync(filePath, 0);
    }
}

async function waitForLogs(filePath, expectedLines, maxRetries, delayMs) {
    for (let attempt = 0; attempt < maxRetries; attempt++) {
        const logEntries = fs.readFileSync(filePath, 'utf8');
        const lines = logEntries.trim().split('\n').filter(line => line.length > 0);
        if (lines.length >= expectedLines) {
            try {
                return lines.map(line => JSON.parse(line));
            } catch (err) {
                // FIXME(CLDSRV-800): readFileSync may read partial lines making JSON.parse fail, so we need to retry.
                if (attempt == maxRetries) {
                    throw new Error(`Failed to read log entries from ${filePath} after ${maxRetries} attempts: ${err}`);
                }
            }
        }
        await sleep(delayMs);
    }
    throw new Error(`Failed to read log entries from ${filePath} after ${maxRetries} attempts`);
}

async function waitForAction(filePath, action, maxRetries, delayMs) {
    for (let attempt = 0; attempt < maxRetries; attempt++) {
        const logEntries = fs.readFileSync(filePath, 'utf8');
        const lines = logEntries.trim().split('\n').filter(line => line.length > 0);
        for (const line of lines) {
            try {
                const obj = JSON.parse(line);
                if (obj.action === action) {
                    return;
                }
            } catch (err) {
                // FIXME(CLDSRV-800): readFileSync may read partial lines making JSON.parse fail, so we need to retry.
                if (attempt == maxRetries) {
                    throw new Error(`Failed to read log entries from ${filePath} after ${maxRetries} attempts: ${err}`);
                }
            }
        }
        await sleep(delayMs);
    }
    throw new Error(`Failed to read log entries from ${filePath} after ${maxRetries} attempts`);
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function cleanupBuckets(s3) {
    let lastAction = 'ListBuckets';
    const bucketsResponse = await s3.listBuckets().promise();
    for (const bucket of bucketsResponse.Buckets) {
        const listMPUResponse = await s3.listMultipartUploads({ Bucket: bucket.Name }).promise();
        if (listMPUResponse.Uploads && listMPUResponse.Uploads.length > 0) {
            await Promise.all(listMPUResponse.Uploads.map(upload =>
                s3.abortMultipartUpload({
                    Bucket: bucket.Name,
                    Key: upload.Key,
                    UploadId: upload.UploadId,
                }).promise(),
            ));
        }

        await bu.empty(bucket.Name, true);
        await s3.deleteBucket({ Bucket: bucket.Name }).promise();
        lastAction = 'DeleteBucket';
    }
    return lastAction;
}

var bu;

// TODO:
// - [ ] We skip websiteGet and websiteHead because they need to be tested with HTTP requests to the website endpoint.
//   Cannot delete the locked objects in the bucket, so we cannot delete the bucket.
// - [ ] Cloudserver returns PutBucketNotification action for PutBucketNotificationConfiguration (same for the Get)
// - We skip QUOTA methods because they are not part of the AWS API.
// - We skip objectRestore because it is not supported in CloudServer.
describe('Server Access Logs - File Output', async () => {
    withV4(async sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        bu = bucketUtil;
        const s3 = bucketUtil.s3;
        const logFilePath = config.serverAccessLogs.outputFile;
        const bucketName = 'test-server-access-log-bucket';
        const objectKey = 'test-object-key';

        // List of all the log properties.
        // Commented properties are set by the test or not tested.
        // UNKNOWN: The property is not accessible by the test or is non determistic.
        // DYNAMIC: The propery is set depending on the operation.
        // STATIC: Shared among all tests.
        // TODO: The property need to be tested.
        const commonProperties = {
            // 'time': '', // UNKNOWN
            // 'hostname': '', // UNKNOWN
            // 'pid': '', // UNKNOWN
            'action': 'REQUIRED', // DYNAMIC
            'accountName': 'Bart', // STATIC
            'userName': null, // TODO: Add test with IAM user to get a non null userName.
            // 'clientPort': '', // UNKNOWN
            'httpMethod': 'REQUIRED', // DYNAMIC
            // 'bytesDeleted': '', // TODO
            // 'bytesReceived': '', // TODO
            // 'bodyLength': '', // TODO
            // 'contentLength': '', // TODO
            // 'elapsed_ms': '', // UNKNOWN
            // 'httpURL': '', // TODO
            // 'startTime': '', // UNKNOWN
            'requester': '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be', // STATIC
            'operation': 'REQUIRED', // DYNAMIC
            // 'requestURI': '', // TODO
            'errorCode': null,  // DYNAMIC
            // 'objectSize': '', // TODO
            // 'totalTime': '', // UNKNOWN
            // 'turnAroundTime': '', // UNKNOWN
            'referer': null, // TODO: Add test that sets the referer.
            // 'userAgent': // UNKNOWN
            // 'versionID': '', // UNKNOWN
            'signatureVersion': 'SigV4', // STATIC
            'cipherSuite': null, // TODO: Add https tests.
            'authenticationType': 'AuthHeader', // STATIC
            // 'hostHeader': '', // UNKNOWN
            'tlsVersion': null, // TODO: Add https tests.
            'aclRequired': null, // TODO: Add https tests.
            'bucketOwner': '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be', // DYNAMIC
            bucketName, // DYNAMIC
            // 'req_id': '', // UNKNOWN
            // 'bytesSent': '', // TODO
            // 'clientIP': '', // UNKNOWN
            'httpCode': 200, // DYNAMIC
            'objectKey': null, // DYNAMIC
            'logFormatVersion': '0', // STATIC
            'loggingEnabled': false, // DYNAMIC
            'loggingTargetBucket': null, // DYNAMIC
            'loggingTargetPrefix': null, // DYNAMIC
            'awsAccessKeyID': 'accessKey1', // STATIC
            'raftSessionID': null, // UNKNOWN but available with scality backend, null otherwise
        };

        const operations = [
            (() => {
                // This operation tests deleting a bucket and expects a server access log entry for the bucket deletion.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.deleteBucket({ Bucket: bucketName }).promise();
                };
                return {
                    method,
                    methodName: 'bucketDelete',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.DELETE.BUCKET',
                            action: 'DeleteBucket',
                            httpCode: 204,
                            httpMethod: 'DELETE',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests deleting a bucket's CORS configuration
                // and expects a log entry for that operation.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    // CORS must be set before it can be deleted
                    await s3.putBucketCors({
                        Bucket: bucketName,
                        CORSConfiguration: {
                            CORSRules: [{
                                AllowedHeaders: ['*'],
                                AllowedMethods: ['GET', 'PUT'],
                                AllowedOrigins: ['*'],
                            }]
                        }
                    }).promise();
                    await s3.deleteBucketCors({ Bucket: bucketName }).promise();
                };
                return {
                    method,
                    methodName: 'bucketDeleteCors',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.CORS',
                            action: 'PutBucketCors',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.DELETE.CORS',
                            action: 'DeleteBucketCors',
                            httpCode: 204,
                            httpMethod: 'DELETE',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests deleting a bucket's encryption configuration
                // and expects a log entry for that operation.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    // Bucket encryption must be configured before it can be deleted
                    await s3.putBucketEncryption({
                        Bucket: bucketName,
                        ServerSideEncryptionConfiguration: {
                            Rules: [
                                {
                                    ApplyServerSideEncryptionByDefault: {
                                        SSEAlgorithm: 'AES256',
                                    }
                                }
                            ]
                        }
                    }).promise();
                    await s3.deleteBucketEncryption({ Bucket: bucketName }).promise();
                };
                return {
                    method,
                    methodName: 'bucketDeleteEncryption',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.ENCRYPTION',
                            action: 'PutBucketEncryption',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.DELETE.ENCRYPTION',
                            action: 'DeleteBucketEncryption',
                            httpCode: 204,
                            httpMethod: 'DELETE',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests deleting a bucket's website configuration
                // and expects a log entry for that operation.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    // Website configuration must be set before it can be deleted
                    await s3.putBucketWebsite({
                        Bucket: bucketName,
                        WebsiteConfiguration: {
                            IndexDocument: {
                                Suffix: 'index.html',
                            },
                        },
                    }).promise();
                    await s3.deleteBucketWebsite({ Bucket: bucketName }).promise();
                };
                return {
                    method,
                    methodName: 'bucketDeleteWebsite',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.WEBSITE',
                            action: 'PutBucketWebsite',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.DELETE.WEBSITE',
                            action: 'DeleteBucketWebsite',
                            httpCode: 204,
                            httpMethod: 'DELETE',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests the ListBucketV2 API and expects a log entry for that operation.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    // Upload an object to ensure the bucket is not empty
                    await s3.putObject({ Bucket: bucketName, Key: objectKey, Body: 'Data' }).promise();
                    // Issue the ListBucketV2 request
                    await s3.listObjectsV2({ Bucket: bucketName }).promise();
                };
                return {
                    method,
                    methodName: 'listObjectsV2',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.OBJECT',
                            action: 'PutObject',
                            objectKey,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.GET.BUCKET',
                            action: 'ListObjectsV2',
                            httpMethod: 'GET',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests the ListObjects (v1) API and expects a log entry for that operation.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    // Upload an object to ensure the bucket is not empty
                    await s3.putObject({ Bucket: bucketName, Key: objectKey, Body: 'Data' }).promise();
                    // Issue the ListObjects request
                    await s3.listObjects({ Bucket: bucketName }).promise();
                };
                return {
                    method,
                    methodName: 'bucketGet',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.OBJECT',
                            action: 'PutObject',
                            objectKey,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.GET.BUCKET',
                            action: 'ListObjects',
                            httpMethod: 'GET',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests getting a bucket's ACL
                // and expects log entries for bucket creation and getting the ACL.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.getBucketAcl({ Bucket: bucketName }).promise();
                };
                return {
                    method,
                    methodName: 'bucketGetACL',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.GET.ACL',
                            action: 'GetBucketAcl',
                            httpMethod: 'GET',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests getting a bucket's CORS configuration
                // and expects log entries for bucket creation,
                // putting a CORS configuration, and getting the CORS configuration.
                const corsConfig = {
                    CORSRules: [
                        {
                            AllowedOrigins: ['*'],
                            AllowedMethods: ['GET', 'POST'],
                        }
                    ]
                };
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putBucketCors({ Bucket: bucketName, CORSConfiguration: corsConfig }).promise();
                    await s3.getBucketCors({ Bucket: bucketName }).promise();
                };
                return {
                    method,
                    methodName: 'bucketGetCors',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.CORS',
                            action: 'PutBucketCors',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.GET.CORS',
                            action: 'GetBucketCors',
                            httpMethod: 'GET',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests getting a bucket's Object Lock configuration.
                const method = async () => {
                    await s3.createBucket({
                        Bucket: bucketName,
                        ObjectLockEnabledForBucket: true,
                    }).promise();
                    await s3.getObjectLockConfiguration({ Bucket: bucketName }).promise();
                };
                return {
                    method,
                    methodName: 'bucketGetObjectLock',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.GET.OBJECT',
                            action: 'GetObjectLockConfiguration',
                            httpMethod: 'GET',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests getting a bucket's versioning configuration.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.getBucketVersioning({ Bucket: bucketName }).promise();
                };
                return {
                    method,
                    methodName: 'bucketGetVersioning',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.GET.VERSIONING',
                            action: 'GetBucketVersioning',
                            httpMethod: 'GET',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests getting a bucket's website configuration.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putBucketWebsite({
                        Bucket: bucketName,
                        WebsiteConfiguration: {
                            IndexDocument: { Suffix: 'index.html' },
                        },
                    }).promise();
                    await s3.getBucketWebsite({ Bucket: bucketName }).promise();
                };
                return {
                    method,
                    methodName: 'bucketGetWebsite',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.WEBSITE',
                            action: 'PutBucketWebsite',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.GET.WEBSITE',
                            action: 'GetBucketWebsite',
                            httpMethod: 'GET',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests getting a bucket's location.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.getBucketLocation({ Bucket: bucketName }).promise();
                };
                return {
                    method,
                    methodName: 'bucketGetLocation',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.GET.LOCATION',
                            action: 'GetBucketLocation',
                            httpMethod: 'GET',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests getting a bucket's encryption configuration.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putBucketEncryption({
                        Bucket: bucketName,
                        ServerSideEncryptionConfiguration: {
                            Rules: [
                                {
                                    ApplyServerSideEncryptionByDefault: {
                                        SSEAlgorithm: 'AES256',
                                    }
                                }
                            ]
                        }
                    }).promise();
                    await s3.getBucketEncryption({ Bucket: bucketName }).promise();
                };
                return {
                    method,
                    methodName: 'bucketGetEncryption',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.ENCRYPTION',
                            action: 'PutBucketEncryption',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.GET.ENCRYPTION',
                            action: 'GetBucketEncryption',
                            httpMethod: 'GET',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests heading a bucket.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.headBucket({ Bucket: bucketName }).promise();
                };
                return {
                    method,
                    methodName: 'bucketHead',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.HEAD.BUCKET',
                            action: 'HeadBucket',
                            httpMethod: 'HEAD',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests creating a bucket.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                };
                return {
                    method,
                    methodName: 'bucketPut',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests putting a bucket ACL.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putBucketAcl({ Bucket: bucketName, ACL: 'private' }).promise();
                };
                return {
                    method,
                    methodName: 'bucketPutACL',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.ACL',
                            action: 'PutBucketAcl',
                            httpMethod: 'PUT',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests putting a bucket CORS configuration.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putBucketCors({
                        Bucket: bucketName,
                        CORSConfiguration: {
                            CORSRules: [{
                                AllowedHeaders: ['*'],
                                AllowedMethods: ['GET', 'PUT'],
                                AllowedOrigins: ['*'],
                            }]
                        }
                    }).promise();
                };
                return {
                    method,
                    methodName: 'bucketPutCors',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.CORS',
                            action: 'PutBucketCors',
                            httpMethod: 'PUT',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests putting bucket versioning configuration.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putBucketVersioning({
                        Bucket: bucketName, VersioningConfiguration: { Status: 'Enabled' },
                    }).promise();
                };
                return {
                    method,
                    methodName: 'bucketPutVersioning',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.VERSIONING',
                            action: 'PutBucketVersioning',
                            httpMethod: 'PUT',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests putting bucket tagging.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putBucketTagging({
                        Bucket: bucketName,
                        Tagging: {
                            TagSet: [{ Key: 'testKey', Value: 'testValue' }]
                        }
                    }).promise();
                };
                return {
                    method,
                    methodName: 'bucketPutTagging',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.TAGGING',
                            action: 'PutBucketTagging',
                            httpMethod: 'PUT',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests deleting bucket tagging.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putBucketTagging({
                        Bucket: bucketName,
                        Tagging: {
                            TagSet: [{ Key: 'testKey', Value: 'testValue' }]
                        }
                    }).promise();
                    await s3.deleteBucketTagging({ Bucket: bucketName }).promise();
                };
                return {
                    method,
                    methodName: 'bucketDeleteTagging',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.TAGGING',
                            action: 'PutBucketTagging',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.DELETE.TAGGING',
                            action: 'DeleteBucketTagging',
                            httpCode: 204,
                            httpMethod: 'DELETE',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests getting bucket tagging.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putBucketTagging({
                        Bucket: bucketName,
                        Tagging: {
                            TagSet: [{ Key: 'testKey', Value: 'testValue' }]
                        }
                    }).promise();
                    await s3.getBucketTagging({ Bucket: bucketName }).promise();
                };
                return {
                    method,
                    methodName: 'bucketGetTagging',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.TAGGING',
                            action: 'PutBucketTagging',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.GET.TAGGING',
                            action: 'GetBucketTagging',
                            httpMethod: 'GET',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests putting bucket replication.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putBucketVersioning({
                        Bucket: bucketName, VersioningConfiguration: { Status: 'Enabled' },
                    }).promise();
                    await s3.putBucketReplication({
                        Bucket: bucketName,
                        ReplicationConfiguration: {
                            Role: 'arn:aws:iam::123456789012:role/src-role,arn:aws:iam::123456789012:role/dest-role',
                            Rules: [{
                                ID: 'rule1',
                                Status: 'Enabled',
                                Priority: 1,
                                Filter: { Prefix: '' },
                                Destination: {
                                    Bucket: 'arn:aws:s3:::destination-bucket'
                                }
                            }]
                        }
                    }).promise();
                };
                return {
                    method,
                    methodName: 'bucketPutReplication',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.VERSIONING',
                            action: 'PutBucketVersioning',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.REPLICATION',
                            action: 'PutBucketReplication',
                            httpMethod: 'PUT',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests getting bucket replication.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putBucketVersioning({
                        Bucket: bucketName, VersioningConfiguration: { Status: 'Enabled' },
                    }).promise();
                    await s3.putBucketReplication({
                        Bucket: bucketName,
                        ReplicationConfiguration: {
                            Role: 'arn:aws:iam::123456789012:role/src-role,arn:aws:iam::123456789012:role/dest-role',
                            Rules: [{
                                ID: 'rule1',
                                Status: 'Enabled',
                                Priority: 1,
                                Filter: { Prefix: '' },
                                Destination: {
                                    Bucket: 'arn:aws:s3:::destination-bucket'
                                }
                            }]
                        }
                    }).promise();
                    await s3.getBucketReplication({ Bucket: bucketName }).promise();
                };
                return {
                    method,
                    methodName: 'bucketGetReplication',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.VERSIONING',
                            action: 'PutBucketVersioning',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.REPLICATION',
                            action: 'PutBucketReplication',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.GET.REPLICATION',
                            action: 'GetBucketReplication',
                            httpMethod: 'GET',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests deleting bucket replication.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putBucketVersioning({
                        Bucket: bucketName, VersioningConfiguration: { Status: 'Enabled' },
                    }).promise();
                    await s3.putBucketReplication({
                        Bucket: bucketName,
                        ReplicationConfiguration: {
                            Role: 'arn:aws:iam::123456789012:role/src-role,arn:aws:iam::123456789012:role/dest-role',
                            Rules: [{
                                ID: 'rule1',
                                Status: 'Enabled',
                                Priority: 1,
                                Filter: { Prefix: '' },
                                Destination: {
                                    Bucket: 'arn:aws:s3:::destination-bucket'
                                }
                            }]
                        }
                    }).promise();
                    await s3.deleteBucketReplication({ Bucket: bucketName }).promise();
                };
                return {
                    method,
                    methodName: 'bucketDeleteReplication',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.VERSIONING',
                            action: 'PutBucketVersioning',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.REPLICATION',
                            action: 'PutBucketReplication',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.DELETE.REPLICATION',
                            action: 'DeleteBucketReplication',
                            httpCode: 204,
                            httpMethod: 'DELETE',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests putting bucket lifecycle.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putBucketLifecycleConfiguration({
                        Bucket: bucketName,
                        LifecycleConfiguration: {
                            Rules: [{
                                ID: 'rule1',
                                Status: 'Enabled',
                                Filter: { Prefix: 'documents/' },
                                Expiration: { Days: 365 }
                            }]
                        }
                    }).promise();
                };
                return {
                    method,
                    methodName: 'bucketPutLifecycle',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.LIFECYCLE',
                            action: 'PutBucketLifecycleConfiguration',
                            httpMethod: 'PUT',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests getting bucket lifecycle.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putBucketLifecycleConfiguration({
                        Bucket: bucketName,
                        LifecycleConfiguration: {
                            Rules: [{
                                ID: 'rule1',
                                Status: 'Enabled',
                                Filter: { Prefix: 'documents/' },
                                Expiration: { Days: 365 }
                            }]
                        }
                    }).promise();
                    await s3.getBucketLifecycleConfiguration({ Bucket: bucketName }).promise();
                };
                return {
                    method,
                    methodName: 'bucketGetLifecycle',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.LIFECYCLE',
                            action: 'PutBucketLifecycleConfiguration',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.GET.LIFECYCLE',
                            action: 'GetBucketLifecycleConfiguration',
                            httpMethod: 'GET',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests deleting bucket lifecycle.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putBucketLifecycleConfiguration({
                        Bucket: bucketName,
                        LifecycleConfiguration: {
                            Rules: [{
                                ID: 'rule1',
                                Status: 'Enabled',
                                Filter: { Prefix: 'documents/' },
                                Expiration: { Days: 365 }
                            }]
                        }
                    }).promise();
                    await s3.deleteBucketLifecycle({ Bucket: bucketName }).promise();
                };
                return {
                    method,
                    methodName: 'bucketDeleteLifecycle',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.LIFECYCLE',
                            action: 'PutBucketLifecycleConfiguration',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.DELETE.LIFECYCLE',
                            action: 'DeleteBucketLifecycle',
                            httpCode: 204,
                            httpMethod: 'DELETE',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests putting bucket policy.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putBucketPolicy({
                        Bucket: bucketName,
                        Policy: JSON.stringify({
                            Version: '2012-10-17',
                            Statement: [{
                                Effect: 'Allow',
                                Principal: '*',
                                Action: 's3:GetObject',
                                Resource: `arn:aws:s3:::${bucketName}/*`
                            }]
                        })
                    }).promise();
                };
                return {
                    method,
                    methodName: 'bucketPutPolicy',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKETPOLICY',
                            action: 'PutBucketPolicy',
                            httpMethod: 'PUT',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests getting bucket policy.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putBucketPolicy({
                        Bucket: bucketName,
                        Policy: JSON.stringify({
                            Version: '2012-10-17',
                            Statement: [{
                                Effect: 'Allow',
                                Principal: '*',
                                Action: 's3:GetObject',
                                Resource: `arn:aws:s3:::${bucketName}/*`
                            }]
                        })
                    }).promise();
                    await s3.getBucketPolicy({ Bucket: bucketName }).promise();
                };
                return {
                    method,
                    methodName: 'bucketGetPolicy',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKETPOLICY',
                            action: 'PutBucketPolicy',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.GET.BUCKETPOLICY',
                            action: 'GetBucketPolicy',
                            httpMethod: 'GET',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests deleting bucket policy.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putBucketPolicy({
                        Bucket: bucketName,
                        Policy: JSON.stringify({
                            Version: '2012-10-17',
                            Statement: [{
                                Effect: 'Allow',
                                Principal: '*',
                                Action: 's3:GetObject',
                                Resource: `arn:aws:s3:::${bucketName}/*`
                            }]
                        })
                    }).promise();
                    await s3.deleteBucketPolicy({ Bucket: bucketName }).promise();
                };
                return {
                    method,
                    methodName: 'bucketDeletePolicy',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKETPOLICY',
                            action: 'PutBucketPolicy',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.DELETE.BUCKETPOLICY',
                            action: 'DeleteBucketPolicy',
                            httpCode: 204,
                            httpMethod: 'DELETE',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests putting bucket object lock configuration.
                const method = async () => {
                    await s3.createBucket({
                        Bucket: bucketName,
                        ObjectLockEnabledForBucket: true,
                    }).promise();
                    await s3.putObjectLockConfiguration({
                        Bucket: bucketName,
                        ObjectLockConfiguration: {
                            ObjectLockEnabled: 'Enabled',
                            Rule: {
                                DefaultRetention: {
                                    Mode: 'GOVERNANCE',
                                    Days: 1
                                }
                            }
                        }
                    }).promise();
                };
                return {
                    method,
                    methodName: 'bucketPutObjectLock',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.OBJECT',
                            action: 'PutObjectLockConfiguration',
                            httpMethod: 'PUT',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests putting bucket notification configuration.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putBucketNotificationConfiguration({
                        Bucket: bucketName,
                        NotificationConfiguration: {}
                    }).promise();
                };
                return {
                    method,
                    methodName: 'bucketPutNotification',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.NOTIFICATION',
                            action: 'PutBucketNotification',
                            httpMethod: 'PUT',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests getting bucket notification configuration.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.getBucketNotificationConfiguration({ Bucket: bucketName }).promise();
                };
                return {
                    method,
                    methodName: 'bucketGetNotification',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.GET.NOTIFICATION',
                            action: 'GetBucketNotification',
                            httpMethod: 'GET',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests putting bucket encryption.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putBucketEncryption({
                        Bucket: bucketName,
                        ServerSideEncryptionConfiguration: {
                            Rules: [
                                {
                                    ApplyServerSideEncryptionByDefault: {
                                        SSEAlgorithm: 'AES256',
                                    }
                                }
                            ]
                        }
                    }).promise();
                };
                return {
                    method,
                    methodName: 'bucketPutEncryption',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.ENCRYPTION',
                            action: 'PutBucketEncryption',
                            httpMethod: 'PUT',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests putting bucket logging.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putBucketLogging({
                        Bucket: bucketName,
                        BucketLoggingStatus: {}
                    }).promise();
                };
                return {
                    method,
                    methodName: 'bucketPutLogging',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.LOGGING_STATUS',
                            action: 'PutBucketLogging',
                            httpMethod: 'PUT',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests getting bucket logging.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putBucketLogging({
                        Bucket: bucketName,
                        BucketLoggingStatus: {
                            LoggingEnabled: { TargetBucket: bucketName, TargetPrefix: 'prefix' },
                        },
                    }).promise();
                    await s3.getBucketLogging({ Bucket: bucketName }).promise();
                    await s3.putBucketLogging({
                        Bucket: bucketName,
                        BucketLoggingStatus: {}
                    }).promise();
                };
                return {
                    method,
                    methodName: 'bucketGetLogging',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.LOGGING_STATUS',
                            action: 'PutBucketLogging',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.GET.LOGGING_STATUS',
                            action: 'GetBucketLogging',
                            loggingEnabled: true,
                            loggingTargetBucket: bucketName,
                            loggingTargetPrefix: 'prefix',
                            httpMethod: 'GET',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.LOGGING_STATUS',
                            action: 'PutBucketLogging',
                            loggingEnabled: true,
                            loggingTargetBucket: bucketName,
                            loggingTargetPrefix: 'prefix',
                            httpMethod: 'PUT',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests completing a multipart upload.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    const uploadId =
                        (await s3.createMultipartUpload({ Bucket: bucketName, Key: objectKey }).promise()).UploadId;
                    const uploadPartResponse = await s3.uploadPart({
                        Bucket: bucketName,
                        Key: objectKey,
                        PartNumber: 1,
                        UploadId: uploadId,
                        Body: 'test data'
                    }).promise();
                    await s3.completeMultipartUpload({
                        Bucket: bucketName,
                        Key: objectKey,
                        UploadId: uploadId,
                        MultipartUpload: {
                            Parts: [{
                                ETag: uploadPartResponse.ETag,
                                PartNumber: 1
                            }]
                        }
                    }).promise();
                };
                return {
                    method,
                    methodName: 'completeMultipartUpload',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.POST.UPLOADS',
                            action: 'CreateMultipartUpload',
                            objectKey,
                            httpMethod: 'POST',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.PART',
                            action: 'UploadPart',
                            objectKey,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.POST.UPLOAD',
                            action: 'CompleteMultipartUpload',
                            objectKey,
                            httpMethod: 'POST',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests initiating a multipart upload.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.createMultipartUpload({ Bucket: bucketName, Key: objectKey }).promise();
                };
                return {
                    method,
                    methodName: 'initiateMultipartUpload',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.POST.UPLOADS',
                            action: 'CreateMultipartUpload',
                            objectKey,
                            httpMethod: 'POST',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests listing multipart uploads.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.createMultipartUpload({ Bucket: bucketName, Key: objectKey }).promise();
                    await s3.listMultipartUploads({ Bucket: bucketName }).promise();
                };
                return {
                    method,
                    methodName: 'listMultipartUploads',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.POST.UPLOADS',
                            action: 'CreateMultipartUpload',
                            objectKey,
                            httpMethod: 'POST',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.GET.UPLOADS',
                            action: 'ListMultipartUploads',
                            httpMethod: 'GET',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests listing parts of a multipart upload.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    const uploadId =
                        (await s3.createMultipartUpload({ Bucket: bucketName, Key: objectKey }).promise()).UploadId;
                    await s3.uploadPart({
                        Bucket: bucketName,
                        Key: objectKey,
                        PartNumber: 1,
                        UploadId: uploadId,
                        Body: 'test data'
                    }).promise();
                    await s3.listParts({ Bucket: bucketName, Key: objectKey, UploadId: uploadId }).promise();
                };
                return {
                    method,
                    methodName: 'listParts',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.POST.UPLOADS',
                            action: 'CreateMultipartUpload',
                            objectKey,
                            httpMethod: 'POST',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.PART',
                            action: 'UploadPart',
                            objectKey,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.GET.UPLOAD',
                            action: 'ListParts',
                            objectKey,
                            httpMethod: 'GET',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests deleting multiple objects including a non-existent one.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putObject({ Bucket: bucketName, Key: objectKey, Body: 'test data' }).promise();
                    await s3.putObject({ Bucket: bucketName, Key: `${objectKey}2`, Body: 'test data 2' }).promise();
                    await s3.deleteObjects({
                        Bucket: bucketName,
                        Delete: {
                            Objects: [
                                { Key: objectKey },
                                { Key: `${objectKey}2` },
                                { Key: `${objectKey}-non-existent` }
                            ]
                        }
                    }).promise();
                };
                return {
                    method,
                    methodName: 'multiObjectDelete',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.OBJECT',
                            action: 'PutObject',
                            objectKey,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.OBJECT',
                            action: 'PutObject',
                            objectKey: `${objectKey}2`,
                            httpMethod: 'PUT',
                        },
                        // Objects are deleted concurrently, they might get logged in any order
                        // for example errors or non-existent objects might get logged first
                        {
                            unordered: [
                                {
                                    ...commonProperties,
                                    operation: 'BATCH.DELETE.OBJECT',
                                    action: 'DeleteObjects',
                                    objectKey,
                                    httpCode: 204,
                                    httpMethod: 'POST',
                                    referer: null,
                                    userAgent: null,
                                },
                                {
                                    ...commonProperties,
                                    operation: 'BATCH.DELETE.OBJECT',
                                    action: 'DeleteObjects',
                                    objectKey: `${objectKey}2`,
                                    httpCode: 204,
                                    httpMethod: 'POST',
                                    referer: null,
                                    userAgent: null,
                                },
                                {
                                    ...commonProperties,
                                    operation: 'BATCH.DELETE.OBJECT',
                                    action: 'DeleteObjects',
                                    objectKey: `${objectKey}-non-existent`,
                                    httpCode: 204,
                                    httpMethod: 'POST',
                                    referer: null,
                                    userAgent: null,
                                },
                            ],
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.POST.MULTI_OBJECT_DELETE',
                            action: 'DeleteObjects',
                            httpMethod: 'POST',
                            objectKey: null,
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests aborting a multipart upload.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    const uploadId =
                        (await s3.createMultipartUpload({ Bucket: bucketName, Key: objectKey }).promise()).UploadId;
                    await s3.abortMultipartUpload({ Bucket: bucketName, Key: objectKey, UploadId: uploadId }).promise();
                };
                return {
                    method,
                    methodName: 'multipartDelete',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.POST.UPLOADS',
                            action: 'CreateMultipartUpload',
                            objectKey,
                            httpMethod: 'POST',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.DELETE.UPLOAD',
                            action: 'AbortMultipartUpload',
                            httpCode: 204,
                            objectKey,
                            httpMethod: 'DELETE',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests deleting an object.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putObject({ Bucket: bucketName, Key: objectKey, Body: 'test data' }).promise();
                    await s3.deleteObject({ Bucket: bucketName, Key: objectKey }).promise();
                };
                return {
                    method,
                    methodName: 'objectDelete',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            action: 'CreateBucket',
                            bucketOwner: null,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.OBJECT',
                            action: 'PutObject',
                            objectKey,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.DELETE.OBJECT',
                            action: 'DeleteObject',
                            httpCode: 204,
                            objectKey,
                            httpMethod: 'DELETE',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests deleting object tagging.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putObject({ Bucket: bucketName, Key: objectKey, Body: 'test data' }).promise();
                    await s3.putObjectTagging({
                        Bucket: bucketName,
                        Key: objectKey,
                        Tagging: {
                            TagSet: [{ Key: 'testKey', Value: 'testValue' }]
                        }
                    }).promise();
                    await s3.deleteObjectTagging({ Bucket: bucketName, Key: objectKey }).promise();
                };
                return {
                    method,
                    methodName: 'objectDeleteTagging',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            action: 'CreateBucket',
                            bucketOwner: null,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.OBJECT',
                            action: 'PutObject',
                            objectKey,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.TAGGING',
                            action: 'PutObjectTagging',
                            objectKey,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.DELETE.TAGGING',
                            action: 'DeleteObjectTagging',
                            httpCode: 204,
                            objectKey,
                            httpMethod: 'DELETE',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests getting an object.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putObject({ Bucket: bucketName, Key: objectKey, Body: 'test data' }).promise();
                    await s3.getObject({ Bucket: bucketName, Key: objectKey }).promise();
                };
                return {
                    method,
                    methodName: 'objectGet',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            action: 'CreateBucket',
                            bucketOwner: null,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.OBJECT',
                            action: 'PutObject',
                            objectKey,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.GET.OBJECT',
                            action: 'GetObject',
                            objectKey,
                            httpMethod: 'GET',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests getting object ACL.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putObject({ Bucket: bucketName, Key: objectKey, Body: 'test data' }).promise();
                    await s3.getObjectAcl({ Bucket: bucketName, Key: objectKey }).promise();
                };
                return {
                    method,
                    methodName: 'objectGetACL',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            action: 'CreateBucket',
                            bucketOwner: null,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.OBJECT',
                            action: 'PutObject',
                            objectKey,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.GET.ACL',
                            action: 'GetObjectAcl',
                            objectKey,
                            httpMethod: 'GET',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests getting object legal hold.
                const method = async () => {
                    await s3.createBucket({
                        Bucket: bucketName,
                        ObjectLockEnabledForBucket: true,
                    }).promise();
                    await s3.putObject({ Bucket: bucketName, Key: objectKey, Body: 'test data' }).promise();
                    await s3.putObjectLegalHold({
                        Bucket: bucketName,
                        Key: objectKey,
                        LegalHold: { Status: 'ON' }
                    }).promise();
                    await s3.getObjectLegalHold({ Bucket: bucketName, Key: objectKey }).promise();
                    await s3.putObjectLegalHold({
                        Bucket: bucketName,
                        Key: objectKey,
                        LegalHold: { Status: 'OFF' }
                    }).promise();
                };
                return {
                    method,
                    methodName: 'objectGetLegalHold',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            action: 'CreateBucket',
                            bucketOwner: null,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.OBJECT',
                            action: 'PutObject',
                            objectKey,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.LEGALHOLD',
                            action: 'PutObjectLegalHold',
                            objectKey,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.GET.LEGALHOLD',
                            action: 'GetObjectLegalHold',
                            objectKey,
                            httpMethod: 'GET',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.LEGALHOLD',
                            action: 'PutObjectLegalHold',
                            objectKey,
                            httpMethod: 'PUT',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests getting object retention.
                const method = async () => {
                    await s3.createBucket({
                        Bucket: bucketName,
                        ObjectLockEnabledForBucket: true,
                    }).promise();
                    await s3.putObject({ Bucket: bucketName, Key: objectKey, Body: 'test data' }).promise();
                    const retainUntilDate = new Date();
                    retainUntilDate.setDate(retainUntilDate.getDate() + 1);
                    await s3.putObjectRetention({
                        Bucket: bucketName,
                        Key: objectKey,
                        Retention: {
                            Mode: 'GOVERNANCE',
                            RetainUntilDate: retainUntilDate
                        }
                    }).promise();
                    await s3.getObjectRetention({ Bucket: bucketName, Key: objectKey }).promise();
                };
                return {
                    method,
                    methodName: 'objectGetRetention',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            action: 'CreateBucket',
                            bucketOwner: null,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.OBJECT',
                            action: 'PutObject',
                            objectKey,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.OBJECT_LOCK_RETENTION',
                            action: 'PutObjectRetention',
                            objectKey,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.GET.OBJECT_LOCK_RETENTION',
                            action: 'GetObjectRetention',
                            objectKey,
                            httpMethod: 'GET',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests getting object tagging.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putObject({ Bucket: bucketName, Key: objectKey, Body: 'test data' }).promise();
                    await s3.getObjectTagging({ Bucket: bucketName, Key: objectKey }).promise();
                };
                return {
                    method,
                    methodName: 'objectGetTagging',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            action: 'CreateBucket',
                            bucketOwner: null,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.OBJECT',
                            action: 'PutObject',
                            objectKey,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.GET.TAGGING',
                            action: 'GetObjectTagging',
                            objectKey,
                            httpMethod: 'GET',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests copying an object.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putObject({ Bucket: bucketName, Key: objectKey, Body: 'test data' }).promise();
                    await s3.copyObject({
                        Bucket: bucketName,
                        CopySource: `${bucketName}/${objectKey}`,
                        Key: `${objectKey}-copy`
                    }).promise();
                };
                return {
                    method,
                    methodName: 'objectCopy',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            action: 'CreateBucket',
                            bucketOwner: null,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.OBJECT',
                            action: 'PutObject',
                            objectKey,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.COPY.OBJECT_GET',
                            action: 'CopyObject',
                            objectKey,
                            httpMethod: 'PUT',
                            requestURI: null,
                            totalTime: null,
                            turnAroundTime: null,
                            referer: null,
                            userAgent: null,
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.COPY',
                            action: 'CopyObject',
                            objectKey: `${objectKey}-copy`,
                            httpMethod: 'PUT',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests putting an object ACL.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putObject({ Bucket: bucketName, Key: objectKey, Body: 'test data' }).promise();
                    await s3.putObjectAcl({ Bucket: bucketName, Key: objectKey, ACL: 'private' }).promise();
                };
                return {
                    method,
                    methodName: 'objectPutACL',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            action: 'CreateBucket',
                            bucketOwner: null,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.OBJECT',
                            action: 'PutObject',
                            objectKey,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.ACL',
                            action: 'PutObjectAcl',
                            objectKey,
                            httpMethod: 'PUT',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests putting object legal hold.
                const method = async () => {
                    await s3.createBucket({
                        Bucket: bucketName,
                        ObjectLockEnabledForBucket: true,
                    }).promise();
                    await s3.putObject({ Bucket: bucketName, Key: objectKey, Body: 'test data' }).promise();
                    await s3.putObjectLegalHold({
                        Bucket: bucketName,
                        Key: objectKey,
                        LegalHold: { Status: 'ON' }
                    }).promise();
                    await s3.putObjectLegalHold({
                        Bucket: bucketName,
                        Key: objectKey,
                        LegalHold: { Status: 'OFF' }
                    }).promise();
                };
                return {
                    method,
                    methodName: 'objectPutLegalHold',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            action: 'CreateBucket',
                            bucketOwner: null,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.OBJECT',
                            action: 'PutObject',
                            objectKey,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.LEGALHOLD',
                            action: 'PutObjectLegalHold',
                            objectKey,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.LEGALHOLD',
                            action: 'PutObjectLegalHold',
                            objectKey,
                            httpMethod: 'PUT',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests putting object tagging.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putObject({ Bucket: bucketName, Key: objectKey, Body: 'test data' }).promise();
                    await s3.putObjectTagging({
                        Bucket: bucketName,
                        Key: objectKey,
                        Tagging: {
                            TagSet: [{ Key: 'testKey', Value: 'testValue' }]
                        }
                    }).promise();
                };
                return {
                    method,
                    methodName: 'objectPutTagging',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            action: 'CreateBucket',
                            bucketOwner: null,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.OBJECT',
                            action: 'PutObject',
                            objectKey,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.TAGGING',
                            action: 'PutObjectTagging',
                            objectKey,
                            httpMethod: 'PUT',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests uploading a part in a multipart upload.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    const uploadId =
                        (await s3.createMultipartUpload({ Bucket: bucketName, Key: objectKey }).promise()).UploadId;
                    await s3.uploadPart({
                        Bucket: bucketName,
                        Key: objectKey,
                        PartNumber: 1,
                        UploadId: uploadId,
                        Body: 'test data'
                    }).promise();
                };
                return {
                    method,
                    methodName: 'objectPutPart',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            action: 'CreateBucket',
                            bucketOwner: null,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.POST.UPLOADS',
                            action: 'CreateMultipartUpload',
                            objectKey,
                            httpMethod: 'POST',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.PART',
                            action: 'UploadPart',
                            objectKey,
                            httpMethod: 'PUT',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests uploading a part copy in a multipart upload.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putObject({ Bucket: bucketName, Key: objectKey, Body: 'test data for copy' }).promise();
                    const uploadId =
                        (await s3.createMultipartUpload({ Bucket: bucketName, Key: `${objectKey}-mpu` }).promise())
                            .UploadId;
                    await s3.uploadPartCopy({
                        Bucket: bucketName,
                        Key: `${objectKey}-mpu`,
                        PartNumber: 1,
                        UploadId: uploadId,
                        CopySource: `${bucketName}/${objectKey}`
                    }).promise();
                };
                return {
                    method,
                    methodName: 'objectPutCopyPart',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            action: 'CreateBucket',
                            bucketOwner: null,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.OBJECT',
                            action: 'PutObject',
                            objectKey,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.POST.UPLOADS',
                            action: 'CreateMultipartUpload',
                            objectKey: `${objectKey}-mpu`,
                            httpMethod: 'POST',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.COPY.PART_GET',
                            action: 'UploadPartCopy',
                            objectKey,
                            httpMethod: 'PUT',
                            requestURI: null,
                            totalTime: null,
                            turnAroundTime: null,
                            referer: null,
                            userAgent: null,
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.COPY',
                            action: 'UploadPartCopy',
                            objectKey: `${objectKey}-mpu`,
                            httpMethod: 'PUT',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests putting object retention.
                const method = async () => {
                    await s3.createBucket({
                        Bucket: bucketName,
                        ObjectLockEnabledForBucket: true,
                    }).promise();
                    await s3.putObject({ Bucket: bucketName, Key: objectKey, Body: 'test data' }).promise();
                    const retainUntilDate = new Date();
                    retainUntilDate.setDate(retainUntilDate.getDate() + 1);
                    await s3.putObjectRetention({
                        Bucket: bucketName,
                        Key: objectKey,
                        Retention: {
                            Mode: 'GOVERNANCE',
                            RetainUntilDate: retainUntilDate
                        }
                    }).promise();
                };
                return {
                    method,
                    methodName: 'objectPutRetention',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.OBJECT',
                            action: 'PutObject',
                            objectKey,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.OBJECT_LOCK_RETENTION',
                            action: 'PutObjectRetention',
                            objectKey,
                            httpMethod: 'PUT',
                        }
                    ],
                };
            })(),
            // Note: objectRestore can only be called on objects in GLACIER, DEEP_ARCHIVE, or 
            // GLACIER_IR storage classes. Since CloudServer only supports STANDARD storage class
            // by default, this operation returns "InvalidObjectState" error and cannot be tested.
            // This test is commented out until archive storage class support is added.
            // (() => {
            //     // This operation tests the restore object API call.
            //     const method = async () => {
            //         await s3.createBucket({ Bucket: bucketName }).promise();
            //         await s3.putObject({ 
            //             Bucket: bucketName, 
            //             Key: objectKey, 
            //             Body: 'test data',
            //             StorageClass: 'GLACIER'  // Not supported in CloudServer
            //         }).promise();
            //         await s3.restoreObject({
            //             Bucket: bucketName,
            //             Key: objectKey,
            //             RestoreRequest: {
            //                 Days: 1
            //             }
            //         }).promise();
            //     };
            //     const expectedOperations = ['REST.PUT.BUCKET','REST.PUT.OBJECT', 'REST.POST.OBJECT'];
            //     return { method, methodName: 'objectRestore', expectedOperations };
            // })(),
            (() => {
                const testBody = 'Hello, Server Access Logs!';
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putObject({ Bucket: bucketName, Key: objectKey, Body: testBody }).promise();
                };
                return {
                    method,
                    methodName: 'putObject',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.OBJECT',
                            action: 'PutObject',
                            objectKey,
                            httpMethod: 'PUT',
                        }
                    ],
                };
            })(),
            (() => {
                const testBody = 'Hello, Server Access Logs!';
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.putObject({ Bucket: bucketName, Key: objectKey, Body: testBody }).promise();
                    await s3.headObject({ Bucket: bucketName, Key: objectKey }).promise();
                };
                return {
                    method,
                    methodName: 'headObject',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.OBJECT',
                            action: 'PutObject',
                            objectKey,
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.HEAD.OBJECT',
                            action: 'HeadObject',
                            objectKey,
                            httpMethod: 'HEAD',
                        }
                    ],
                };
            })(),
            (() => {
                // This operation tests listing all buckets.
                const method = async () => {
                    await s3.createBucket({ Bucket: bucketName }).promise();
                    await s3.listBuckets().promise();
                };
                return {
                    method,
                    methodName: 'listBuckets',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.BUCKET',
                            bucketOwner: null,
                            action: 'CreateBucket',
                            httpMethod: 'PUT',
                        },
                        {
                            ...commonProperties,
                            operation: 'REST.GET.SERVICE',
                            action: 'ListBuckets',
                            bucketOwner: null,
                            bucketName: null,
                            httpMethod: 'GET',
                        }
                    ],
                };
            })(),
            (() => {
                // Test errorCode is set.
                const method = async () => {
                    try {
                        await s3.deleteBucket({ Bucket: 'xxx'}).promise();
                    } catch {
                        return;
                    }
                };
                return {
                    method,
                    methodName: 'bucketDeleteError',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.DELETE.BUCKET',
                            action: 'DeleteBucket',
                            httpCode: 404,
                            errorCode: 'NoSuchBucket',
                            bucketOwner: null,
                            bucketName: 'xxx',
                            httpMethod: 'DELETE',
                        }
                    ],
                };
            })(),
            (() => {
                // Test errorCode is set for PutObject.
                const method = async () => {
                    try {
                        await s3.putObject({ Bucket: 'xxx', Key: 'key', Body: 'test' }).promise();
                    } catch {
                        return;
                    }
                };
                return {
                    method,
                    methodName: 'putObjectError',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.PUT.OBJECT',
                            action: 'PutObject',
                            httpCode: 404,
                            errorCode: 'NoSuchBucket',
                            bucketOwner: null,
                            bucketName: 'xxx',
                            httpMethod: 'PUT',
                            objectKey: 'key',
                        }
                    ],
                };
            })(),
            (() => {
                // Test errorCode is set for GetObject.
                const method = async () => {
                    try {
                        await s3.getObject({ Bucket: 'xxx', Key: 'key' }).promise();
                    } catch {
                        return;
                    }
                };
                return {
                    method,
                    methodName: 'getObjectError',
                    expected: [
                        {
                            ...commonProperties,
                            operation: 'REST.GET.OBJECT',
                            action: 'GetObject',
                            httpCode: 404,
                            errorCode: 'NoSuchBucket',
                            bucketOwner: null,
                            bucketName: 'xxx',
                            httpMethod: 'GET',
                            objectKey: 'key',
                        }
                    ],
                };
            })()
            // TODO: CLDSRV-799
            // (() => {
            //     // Test errorCode is set.
            //     const method = async () => {
            //         try {
            //             await s3.deleteBucket({ Bucket: 'UPPERCASE'}).promise();
            //         } catch {
            //             return;
            //         }
            //     };
            //     return {
            //         method,
            //         methodName: 'bucketDeleteError',
            //         expected: [
            //             {
            //                 ...commonProperties,
            //                 operation: 'REST.DELETE.BUCKET',
            //                 action: 'DeleteBucket',
            //                 httpCode: 404,
            //                 errorCode: 'NoSuchBucket',
            //                 bucketOwner: null,
            //                 bucketName: 'xxx',
            //                 httpMethod: 'DELETE',
            //             }
            //         ],
            //     };
            // })()
        ];

        before(async function () {
            if (config.serverAccessLogs.mode === serverAccessLogsModes.DISABLED) {
                this.skip();
            }
            if (process.env.AWS_ON_AIR) {
                this.skip();
            }
            if (!fs.existsSync(path.dirname(logFilePath))) {
                throw new Error('Logs directory does not exist');
            }
        });

        after(async () => {
            truncateLogFileIfExists(logFilePath);
        });

        beforeEach(async () => {
            truncateLogFileIfExists(logFilePath);
        });

        afterEach(async () => {
            const lastAction = await cleanupBuckets(s3, bucketName);
            await waitForAction(logFilePath, lastAction,
                TEST_CONFIG.MAX_LOG_WAIT_RETRIES, TEST_CONFIG.LOG_POLL_DELAY_MS);
            truncateLogFileIfExists(logFilePath);
        });

        // Helper function to validate a log entry against expected properties
        const validateLogEntry = (logEntry, properties) => {
            const result = tv4.validateResult(logEntry, schema);
            assert.strictEqual(result.valid, true,
                `Log entry should match schema: ${JSON.stringify(result.error)}`);

            for (const [key, val] of Object.entries(properties)) {
                if (val === null) {
                    assert.strictEqual(key in logEntry, false,
                        `Field ${key} should be omitted when null, action ${properties.action}`);
                } else {
                    assert.strictEqual(logEntry[key], val,
                        `Invalid value for ${key}, action ${properties.action}`);
                }
            }

            if (config.backends.metadata === 'scality') {
                assert.strictEqual('raftSessionID' in logEntry, true,
                    `raftSessionID should be present for action ${properties.action}`);
                assert.strictEqual(typeof logEntry.raftSessionID, 'string',
                    `raftSessionID should be a string for action ${properties.action}`);
                assert.strictEqual(logEntry.raftSessionID.length > 0, true,
                    `raftSessionID should not be empty for action ${properties.action}`);
            }
        };

        for (const operation of operations) {
            it(`should log correct ${operation.methodName} operation with all required fields`, async () => {
                await operation.method();
                // Count total expected logs, including unordered entries
                let totalExpected = 0;
                for (const exp of operation.expected) {
                    totalExpected += exp.unordered ? exp.unordered.length : 1;
                }
                const logEntries = await waitForLogs(logFilePath, totalExpected,
                    TEST_CONFIG.MAX_LOG_WAIT_RETRIES, TEST_CONFIG.LOG_POLL_DELAY_MS);
                assert.strictEqual(logEntries.length, totalExpected,
                    `Expected ${totalExpected} log entries, got ${logEntries.length}`);

                let logIdx = 0;

                // Validate entries (ordered or unordered)
                for (let i = 0; i < operation.expected.length; i++) {
                    const expected = operation.expected[i];

                    if (expected.unordered) {
                        // Handle unordered entries
                        const unorderedLogs = logEntries.slice(logIdx, logIdx + expected.unordered.length);
                        const remaining = [...expected.unordered];

                        for (const logEntry of unorderedLogs) {
                            const matchIdx = remaining.findIndex(exp => exp.objectKey === logEntry.objectKey);

                            assert.notStrictEqual(matchIdx, -1,
                                `Unexpected log entry with objectKey: ${logEntry.objectKey}`);

                            validateLogEntry(logEntry, remaining[matchIdx]);
                            remaining.splice(matchIdx, 1);
                        }

                        assert.strictEqual(remaining.length, 0,
                            `Missing expected entries: ${JSON.stringify(remaining)}`);

                        logIdx += expected.unordered.length;
                    } else {
                        // Handle ordered entry
                        validateLogEntry(logEntries[logIdx], expected);
                        logIdx++;
                    }
                }
            });
        }
    });
});
