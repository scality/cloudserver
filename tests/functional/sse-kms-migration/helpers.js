/* eslint-disable no-unused-expressions */
const getConfig = require('../aws-node-sdk/test/support/config');
const { S3 } = require('aws-sdk');
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

// for Integration use default profile, in cloudserver use vault profile
const credsProfile = process.env.S3_END_TO_END === 'true' ? 'default' : 'vault';
const s3config = getConfig(credsProfile, { signatureVersion: 'v4' });
const s3 = new S3(s3config);
const bucketUtil = new BucketUtility(credsProfile);

function hydrateSSEConfig({ algo: SSEAlgorithm, masterKeyId: KMSMasterKeyID }) {
    // stringify and parse to strip undefined values
    return JSON.parse(JSON.stringify({ Rules: [{
        ApplyServerSideEncryptionByDefault: {
            SSEAlgorithm,
            KMSMasterKeyID,
        },
    }] }));
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
    const sse = await s3.getBucketEncryption({ Bucket }).promise();
    return sse
        .ServerSideEncryptionConfiguration
        .Rules[0]
        .ApplyServerSideEncryptionByDefault;
}

async function putEncryptedObject(Bucket, Key, sseConfig, kmsKeyId, Body) {
    return s3.putObject({
        ...putObjParams(Bucket, Key, sseConfig, kmsKeyId),
        Body,
    }).promise();
}

async function getObjectMDSSE(Bucket, Key) {
    const objMD = await MD.getObject(Bucket, Key, {}, log);

    const sse = objMD['x-amz-server-side-encryption'];
    const key = objMD['x-amz-server-side-encryption-aws-kms-key-id'];

    return {
        ServerSideEncryption: sse,
        SSEKMSKeyId: key,
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

async function cleanup(Bucket) {
    await bucketUtil.empty(Bucket);
    await s3.deleteBucket({ Bucket }).promise();
}

module.exports = {
    config,
    getKey,
    credsProfile,
    s3,
    bucketUtil,
    hydrateSSEConfig,
    putObjParams,
    MD,
    getBucketSSE,
    putEncryptedObject,
    getObjectMDSSE,
    createKmsKey,
    cleanup,
};
