const assert = require('assert');
const async = require('async');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external.GCP;
const { genUniqID, genBucketName, gcpRetry, gcpUploadWithRetry, waitForBucketReady } =
    require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
    ListObjectsCommand,
} = require('@aws-sdk/client-s3');

const credentialOne = 'gcpbackend';
const bucketNames = {
    main: {
        Name: genBucketName('upload'),
    },
    mpu: {
        Name: genBucketName('mpu-upload'),
    },
};

const body = Buffer.from('I am a body', 'utf8');
const bigBody = Buffer.alloc(10485760);
const smallMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const bigMD5 = 'a7d414b9133d6483d9a1c4e04e856e3b-2';

describe('GCP: Upload Object', function testSuite() {
    this.timeout(600000);
    let config;
    let gcpClient;

    before(async () => {
        config = getRealAwsConfig(credentialOne);
        gcpClient = new GCP(config);
        const buckets = Object.values(bucketNames);
        await async.eachSeries(
            buckets,
            async bucket => {
                await gcpRetry(
                    gcpClient,
                    new CreateBucketCommand({ Bucket: bucket.Name }),
                );
                await waitForBucketReady(gcpClient, bucket.Name);
            },
        );
    });

    after(async () => {
        const buckets = Object.values(bucketNames);
        await async.eachSeries(
            buckets,
            async bucket => {
                const listCmd = new ListObjectsCommand({
                    Bucket: bucket.Name,
                });
                const listRes = await gcpClient.send(listCmd);
                await async.map(listRes.Contents || [], async object => {
                    await gcpClient.deleteObject({
                        Bucket: bucket.Name,
                        Key: object.Key,
                    });
                });
                await gcpRetry(
                    gcpClient,
                    new DeleteBucketCommand({ Bucket: bucket.Name }),
                );
            },
        );
    });

    it('should put an object to GCP', async () => {
        const key = `somekey-${genUniqID()}`;
        const res = await gcpUploadWithRetry(gcpClient, {
            Bucket: bucketNames.main.Name,
            MPU: bucketNames.mpu.Name,
            Key: key,
            Body: body,
        });
        assert.strictEqual(res.ETag, `"${smallMD5}"`);
    });

    it('should put a large object to GCP', async () => {
        const key = `somekey-${genUniqID()}`;
        const res = await gcpUploadWithRetry(gcpClient, {
            Bucket: bucketNames.main.Name,
            MPU: bucketNames.mpu.Name,
            Key: key,
            Body: bigBody,
        });
        assert.strictEqual(res.ETag, `"${bigMD5}"`);
    });
});
