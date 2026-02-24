const assert = require('assert');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external.GCP;
const { genUniqID, gcpRetry } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
    PutObjectCommand,
    HeadObjectCommand,
} = require('@aws-sdk/client-s3');

const credentialOne = 'gcpbackend';
const bucketName = `cldsrvci-head-${genUniqID()}`;

describe('GCP: HEAD Object', function testSuite() {
    this.timeout(30000);
    const config = getRealAwsConfig(credentialOne);
    const gcpClient = new GCP(config);

    before(async () => {
        await gcpRetry(
            gcpClient,
            new CreateBucketCommand({ Bucket: bucketName }),
        );
    });

    after(async () => {
        const cmd = new DeleteBucketCommand({ Bucket: bucketName });
        await gcpClient.send(cmd);
    });

    describe('with existing object in bucket', () => {
        beforeEach(async function beforeFn() {
            this.currentTest.key = `somekey-${genUniqID()}`;
            const cmd = new PutObjectCommand({
                Bucket: bucketName,
                Key: this.currentTest.key,
            });
            const res = await gcpClient.send(cmd);
            this.currentTest.uploadId = res.VersionId;
            this.currentTest.ETag = res.ETag;
        });

        afterEach(async function afterFn() {
            if (!this.currentTest.key) {
                return;
            }
            await new Promise((resolve, reject) => {
                gcpClient.deleteObject({
                    Bucket: bucketName,
                    Key: this.currentTest.key,
                }, err => {
                    if (err) {
                        process.stdout.write(`err in deleting object ${err}\n`);
                        reject(err);
                        return;
                    }
                    resolve();
                });
            });
        });

        it('should successfully retrieve object', async function testFn() {
            const cmd = new HeadObjectCommand({
                Bucket: bucketName,
                Key: this.test.key,
            });
            const res = await gcpClient.send(cmd);
            assert.strictEqual(res.ETag, this.test.ETag);
            assert.ok(res.$metadata && res.$metadata.httpStatusCode === 200);
        });
    });

    describe('without existing object in bucket', () => {
        it('should return 404', async () => {
            const badObjectkey = `nonexistingkey-${genUniqID()}`;
            await new Promise(resolve => {
                gcpClient.headObject({
                    Bucket: bucketName,
                    Key: badObjectkey,
                }, err => {
                    assert(err);
                    assert.strictEqual(err.$metadata.httpStatusCode, 404);
                    resolve();
                });
            });
        });
    });
});
