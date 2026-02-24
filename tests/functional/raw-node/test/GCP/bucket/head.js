const assert = require('assert');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external.GCP;
const { genUniqID, gcpRetry } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
} = require('@aws-sdk/client-s3');

const credentialOne = 'gcpbackend';

describe('GCP: HEAD Bucket', () => {
    const config = getRealAwsConfig(credentialOne);
    const gcpClient = new GCP(config);

    describe('without existing bucket', () => {
        beforeEach(async function beforeFn() {
            this.currentTest.bucketName = `cldsrvci-somebucket-head-${genUniqID()}`;
        });

        it('should return 404', async function testFn() {
            const badBucketName = this.test.bucketName;
            try {
                await new Promise((resolve, reject) => {
                    gcpClient.headBucket({ Bucket: badBucketName },
                        (err, res) => (err ? reject(err) : resolve(res)));
                });
                assert.fail('Expected 404 error, but got success');
            } catch (err) {
                assert(err);
                assert.strictEqual(err.$metadata?.httpStatusCode, 404);
            }
        });
    });

    describe('with existing bucket', () => {
        beforeEach(async function beforeFn() {
            this.currentTest.bucketName = `cldsrvci-somebucket-head-${genUniqID()}`;
            process.stdout
                .write(`Creating test bucket ${this.currentTest.bucketName}\n`);
            await gcpRetry(
                gcpClient,
                new CreateBucketCommand({
                    Bucket: this.currentTest.bucketName,
                }),
            );
        });

        afterEach(async function afterFn() {
            await gcpRetry(
                gcpClient,
                new DeleteBucketCommand({
                    Bucket: this.currentTest.bucketName,
                }),
            );
        });

        it('should get bucket information', async function testFn() {
            const res = await new Promise((resolve, reject) => {
                gcpClient.headBucket({
                    Bucket: this.test.bucketName,
                }, (err, data) => (err ? reject(err) : resolve(data)));
            });
            const { $metadata, ...data } = res;
            assert.strictEqual($metadata?.httpStatusCode, 200);
            // Ensure MetaVersionId is present and non-empty
            assert.ok(
                typeof data.MetaVersionId === 'string'
                && data.MetaVersionId.length > 0
            );
        });
    });
});
