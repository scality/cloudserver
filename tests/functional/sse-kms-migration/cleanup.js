/* eslint-disable */
const helpers = require('./helpers');
const scenarios = require('./scenarios');

// copy part of aws-node-sdk/test/object/encryptionHeaders.js and add more tests

async function cleanup(Bucket) {
    try {
        void await helpers.cleanup(Bucket);
    } catch (e) {
        console.log('Ignore error for', Bucket, e.toString());
    }
}

describe('SSE KMS Cleanup', () => {
    /** Bucket to test CopyObject from and to */
    const copyBkt = 'enc-bkt-copy';
    const mpuCopyBkt = 'enc-bkt-mpu-copy';

    it('Empty and delete buckets for SSE KMS Migration', async () => {
        console.log('Run cleanup',
            { profile: helpers.credsProfile, accessKeyId: helpers.s3.config.credentials.accessKeyId });
        const allBuckets = (await helpers.s3.listBuckets().promise()).Buckets.map(b => b.Name);
        console.log('List buckets:', allBuckets);

        void await helpers.MD.setup();

        try {
            // pre cleanup
            void await cleanup(copyBkt);
            void await cleanup(mpuCopyBkt);
            void await Promise.all(scenarios.testCases.map(async bktConf => {
                void await cleanup(`enc-bkt-${bktConf.name}`);
                return await cleanup(`versioned-enc-bkt-${bktConf.name}`);
            }));
        } catch (e) { void e; }
    });
});
