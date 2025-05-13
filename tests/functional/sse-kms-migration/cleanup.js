/* eslint-disable */
const getConfig = require('../aws-node-sdk/test/support/config');
const { S3 } = require('aws-sdk');
const { promisify } = require('util');
const BucketUtility = require('../aws-node-sdk/lib/utility/bucket-util');

const metadata = require('../../../lib/metadata/wrapper');

// use file to defined key in arn prefix, if no prefix mem is used

// copy part of aws-node-sdk/test/object/encryptionHeaders.js and add more tests
// around SSE Key prefix and migration
// always getObject to ensure decryption

const testCases = [
    {
        name: 'algo-none',
        // as the init insert objects with each encryption
        // this bucket will have a non mandatory AES256
    },
    {
        name: 'algo-none-del-sse',
        /** flag to remove non mandatory AES256 SS3 from bucket MD beforeEach test */
        deleteSSE: true,
    },
    {
        name: 'algo-aes256',
        algo: 'AES256',
    },
    {
        name: 'algo-awskms',
        algo: 'aws:kms',
    },
    {
        name: 'algo-awskms-key',
        algo: 'aws:kms',
        masterKeyId: true,
    },
    {
        name: 'algo-awskms-key-arnprefix',
        algo: 'aws:kms',
        masterKeyId: true,
        arnPrefix: true,
    },
];

const config = getConfig('vault', { signatureVersion: 'v4' });
const s3 = new S3(config);
const bucketUtil = new BucketUtility('vault');

async function cleanup(Bucket) {
    try {
        void await bucketUtil.empty(Bucket);
        void await s3.deleteBucket({ Bucket }).promise();
    } catch (e) {
        console.log('Ignore error for', Bucket, e.toString());
    }
}

describe('SSE KMS Cleanup', () => {
    /** Bucket to test CopyObject from and to */
    const copyBkt = 'enc-bkt-copy';
    const mpuCopyBkt = 'enc-bkt-mpu-copy';

    it('Empty and delete buckets for SSE KMS Migration', async () => {
        console.log('cleanup');
        void await promisify(metadata.setup.bind(metadata))();

        try {
            // pre cleanup
            void await cleanup(copyBkt);
            void await cleanup(mpuCopyBkt);
            void await Promise.all(testCases.map(async bktConf => {
                void await cleanup(`enc-bkt-${bktConf.name}`);
                return await cleanup(`versioned-enc-bkt-${bktConf.name}`);
            }));
        } catch (e) { void e; }
    });
});
