const getConfig = require('../aws-node-sdk/test/support/config');
const { S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketEncryptionCommand,
    GetBucketEncryptionCommand,
    CopyObjectCommand,
    ListObjectVersionsCommand,
    HeadObjectCommand,
    ListBucketsCommand,
    ListMultipartUploadsCommand,
    ListPartsCommand,
    GetObjectCommand,
    PutObjectCommand,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    UploadPartCopyCommand,
    CompleteMultipartUploadCommand,
    PutBucketVersioningCommand,
    HeadBucketCommand,
} = require('@aws-sdk/client-s3');
const { NodeHttpHandler } = require('@smithy/node-http-handler');
const { StandardRetryStrategy } = require('@aws-sdk/middleware-retry');
const { Agent: HttpAgent } = require('http');
const { Agent: HttpsAgent } = require('https');
const kms = require('../../../lib/kms/wrapper');
const { promisify } = require('util');
const { DummyRequestLogger } = require('../../unit/helpers');
const BucketUtility = require('../aws-node-sdk/lib/utility/bucket-util');
const metadata = require('../../../lib/metadata/wrapper');
const log = new DummyRequestLogger();
const { config } = require('../../../lib/Config');
const { getKeyIdFromArn } = require('arsenal/build/lib/network/KMSInterface');
const { ArsenalError } = require('arsenal/build/lib/errors');

function getKey(key) {
    return config.kmsHideScalityArn ? getKeyIdFromArn(key) : key;
}

// For Integration use default profile, in cloudserver use vault profile
const credsProfile = process.env.S3_END_TO_END === 'true' ? 'default' : 'vault';

// Create custom agents with specific pooling settings
const httpAgent = new HttpAgent({
    keepAlive: true,
    keepAliveMsecs: 30000, // Keep connections alive for 30 seconds
    maxSockets: 50, // Maximum concurrent sockets
    maxFreeSockets: 10, // Maximum free sockets to keep
    timeout: 120000, // Connection timeout
});

const httpsAgent = new HttpsAgent({
    keepAlive: true,
    keepAliveMsecs: 30000, // Keep connections alive for 30 seconds
    maxSockets: 50, // Reduced to avoid overwhelming the server
    maxFreeSockets: 10, // Reduced to match maxSockets reduction
    timeout: 120000, // Increased from 60s to 120s
});

const s3config = {
    ...getConfig(credsProfile, {}),
    requestHandler: new NodeHttpHandler({
        connectionTimeout: 120000, // Increased from 60s to 120s
        socketTimeout: 120000, // Increased from 60s to 120s
        httpAgent,
        httpsAgent,
    }),
    maxAttempts: 8,
    retryStrategy: new StandardRetryStrategy({
        maxAttempts: 8,
        retryDecider: error => 
            // Retry on common network and AWS-specific errors
             (
                error.code === 'ECONNREFUSED' ||
                error.code === 'ECONNRESET' ||
                error.name === 'TimeoutError' ||
                error.message?.includes('socket hang up') ||
                error.code === 'ThrottlingException' ||
                error.code === 'RequestTimeout'
            )
        ,
        delayDecider: attempts => Math.min(1000 * Math.pow(2, attempts), 30000), // Exponential backoff
    }),
};

const s3Client = new S3Client(s3config);
// eslint-disable-next-line no-console
console.log('s3 client config', s3Client.config);

// Remove logger middleware if present
if (s3Client.middlewareStack.identify().includes('loggerMiddleware')) {
    s3Client.middlewareStack.remove('loggerMiddleware');
}

const bucketUtil = new BucketUtility(credsProfile);

// Helper function to convert response Body to string
async function getBodyAsString(response) {
    if (!response?.Body) {
        throw new Error('No Body found in response');
    }
    try {
        const chunks = [];
        for await (const chunk of response.Body) {
            chunks.push(chunk);
        }
        return Buffer.concat(chunks).toString();
    } catch (error) {
        throw new Error(`Failed to read response body: ${error.message}`);
    }
}

// Wrapper for SDK v3 commands to return promises directly
const wrap = exec => exec();
const s3 = {
    createBucket: params => wrap(() => s3Client.send(new CreateBucketCommand(params))),
    putBucketEncryption: params => wrap(() => s3Client.send(new PutBucketEncryptionCommand(params))),
    getBucketEncryption: params => wrap(() => s3Client.send(new GetBucketEncryptionCommand(params))),
    putObject: params => wrap(() => s3Client.send(new PutObjectCommand(params))),
    getObject: params => wrap(async () => {
        const response = await s3Client.send(new GetObjectCommand(params));
        const body = await getBodyAsString(response);
        return { ...response, Body: body };
    }),
    listBuckets: params => wrap(() => s3Client.send(new ListBucketsCommand(params || {}))),
    copyObject: params => wrap(() => s3Client.send(new CopyObjectCommand(params))),
    listObjectVersions: params => wrap(async () => {
        const response = await s3Client.send(new ListObjectVersionsCommand(params));
        // eslint-disable-next-line no-console
        console.log('here is the listObjectVersions response', response);
        return {
            ...response,
            Versions: response.Versions || [],
            DeleteMarkers: response.DeleteMarkers || [],
            CommonPrefixes: response.CommonPrefixes || []
        };
    }),
    headObject: params => wrap(() => s3Client.send(new HeadObjectCommand(params))),
    createMultipartUpload: params => wrap(() => s3Client.send(new CreateMultipartUploadCommand(params))),
    uploadPart: params => wrap(() => s3Client.send(new UploadPartCommand(params))),
    uploadPartCopy: params => wrap(() => s3Client.send(new UploadPartCopyCommand(params))),
    completeMultipartUpload: params => wrap(() => s3Client.send(new CompleteMultipartUploadCommand(params))),
    putBucketVersioning: params => wrap(() => s3Client.send(new PutBucketVersioningCommand(params))),
    headBucket: params => wrap(() => s3Client.send(new HeadBucketCommand(params))),
    listMultipartUploads: params => wrap(async () => {
        const response = await s3Client.send(new ListMultipartUploadsCommand(params));
        // eslint-disable-next-line no-console
        console.log('here is the  listMultipartUploads response', response);
        return {
            ...response,
            Uploads: response.Uploads || [], // Ensure Uploads is always an array
            CommonPrefixes: response.CommonPrefixes || []
        };
    }),
    listParts: params => wrap(() => s3Client.send(new ListPartsCommand(params))),
    _compat: bucketUtil.s3,
    config: {
        credentials: s3config.credentials || {
            accessKeyId: s3config.accessKeyId,
            secretAccessKey: s3config.secretAccessKey,
        },
        endpoint: {
            hostname: s3config.endpoint.hostname,
            port: s3config.port,
        },
    },
};

function hydrateSSEConfig({ algo: SSEAlgorithm, masterKeyId: KMSMasterKeyID }) {
    // Stringify and parse to strip undefined values
    return JSON.parse(JSON.stringify({
        Rules: [{
            ApplyServerSideEncryptionByDefault: {
                SSEAlgorithm,
                KMSMasterKeyID,
            },
        }],
    }));
}

function putObjParams(Bucket, Key, sseConfig, kmsKeyId) {
    return {
        Bucket,
        Key,
        ...(sseConfig.algo && {
            ServerSideEncryption: sseConfig.algo,
            ...(sseConfig.masterKeyId && {
                SSEKMSKeyId: kmsKeyId,
            }),
        }),
    };
}

const MD = {
    setup: promisify(metadata.setup.bind(metadata)),
    getBucket: promisify(metadata.getBucket.bind(metadata)),
    getObject: promisify(metadata.getObjectMD.bind(metadata)),
    updateBucket: promisify(metadata.updateBucket.bind(metadata)),
};

async function getBucketSSE(Bucket) {
    try {
        const sse = await s3Client.send(new GetBucketEncryptionCommand({ Bucket }));
        return sse.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault;
    } catch (error) {
        if (error.name === 'ServerSideEncryptionConfigurationNotFoundError') {
            return null;
        }
        throw error;
    }
}

async function putEncryptedObject(Bucket, Key, sseConfig, kmsKeyId, Body) {
    return s3Client.send(new PutObjectCommand({
        ...putObjParams(Bucket, Key, sseConfig, kmsKeyId),
        Body,
    }));
}

async function getObjectMDSSE(Bucket, Key) {
    const objMD = await MD.getObject(Bucket, Key, {}, log);
    return {
        ServerSideEncryption: objMD['x-amz-server-side-encryption'],
        SSEKMSKeyId: objMD['x-amz-server-side-encryption-aws-kms-key-id'],
    };
}

async function createKmsKey(log) {
    return new Promise((resolve, reject) => {
        kms.client.createBucketKey('testFakeBucketName', log, (err, masterKeyId, masterKeyArn) => {
            if (err) {
                if (err instanceof ArsenalError) {
                    // Help see error detail in logs
                    // eslint-disable-next-line no-param-reassign
                    err.message += ` ${err.description}`;
                }
                return reject(err);
            }
            return resolve({ masterKeyId, masterKeyArn });
        });
    });
}

const destroyKmsKey = promisify(kms.destroyBucketKey);

async function cleanup(Bucket) {
    try {
        await bucketUtil.empty(Bucket);
        await s3Client.send(new DeleteBucketCommand({ Bucket }));
    }
    catch (error) {
        // eslint-disable-next-line no-console
        console.error(`Cleanup failed for bucket ${Bucket}: ${error.message}`);
        throw error;
    }
}

module.exports = {
    config,
    getKey,
    credsProfile,
    s3,
    s3Client,
    bucketUtil,
    hydrateSSEConfig,
    putObjParams,
    MD,
    getBucketSSE,
    putEncryptedObject,
    getObjectMDSSE,
    createKmsKey,
    destroyKmsKey,
    cleanup,
};
