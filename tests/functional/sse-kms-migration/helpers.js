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

const httpAgent = new HttpAgent({
    keepAlive: true,
    keepAliveMsecs: 30000,
    maxSockets: 50,
    maxFreeSockets: 10,
    timeout: 120000,
});

const httpsAgent = new HttpsAgent({
    keepAlive: true,
    keepAliveMsecs: 30000,
    maxSockets: 50,
    maxFreeSockets: 10,
    timeout: 120000, 
});

const s3config = {
    ...getConfig(credsProfile, {}),
    requestHandler: new NodeHttpHandler({
        connectionTimeout: 120000,
        socketTimeout: 120000,
        httpAgent,
        httpsAgent,
    }),
    maxAttempts: 3,
};

const s3Client = new S3Client(s3config);

if (s3Client.middlewareStack.identify().includes('loggerMiddleware')) {
    s3Client.middlewareStack.remove('loggerMiddleware');
}

const bucketUtil = new BucketUtility(credsProfile);

const wrap = exec => exec();
const s3 = {
    createBucket: params => wrap(() => s3Client.send(new CreateBucketCommand(params))),
    deleteBucket: params => wrap(() => s3Client.send(new DeleteBucketCommand(params))),
    putBucketEncryption: params => wrap(() => s3Client.send(new PutBucketEncryptionCommand(params))),
    getBucketEncryption: params => wrap(() => s3Client.send(new GetBucketEncryptionCommand(params))),
    putObject: params => wrap(() => s3Client.send(new PutObjectCommand(params))),
    getObject: params => wrap(async () => {
        const response = await s3Client.send(new GetObjectCommand(params));
        const body = await response.Body.transformToString();
        return { ...response, Body: body };
    }),
    listBuckets: params => wrap(() => s3Client.send(new ListBucketsCommand(params || {}))),
    copyObject: params => wrap(() => s3Client.send(new CopyObjectCommand(params))),
    listObjectVersions: params => wrap(async () => {
        const response = await s3Client.send(new ListObjectVersionsCommand(params));
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
        return {
            ...response,
            Uploads: response.Uploads || [],
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
    return JSON.parse(JSON.stringify({ Rules: [{
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
        const sse = await s3.getBucketEncryption({ Bucket });
        return sse.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault;
    } catch (error) {
        if (error.name === 'ServerSideEncryptionConfigurationNotFoundError') {
            return null;
        }
        throw error;
    }
}

async function putEncryptedObject(Bucket, Key, sseConfig, kmsKeyId, Body) {
    return s3.putObject({
        ...putObjParams(Bucket, Key, sseConfig, kmsKeyId),
        Body,
    });
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
    await bucketUtil.empty(Bucket);
    await s3.deleteBucket({ Bucket });
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
