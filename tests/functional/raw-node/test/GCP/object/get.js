const assert = require('assert');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external.GCP;
const { genUniqID, genBucketName, gcpRetry } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
    PutObjectCommand,
    DeleteObjectCommand,
    GetObjectCommand,
} = require('@aws-sdk/client-s3');

const credentialOne = 'gcpbackend';
const bucketName = genBucketName('get');

describe('GCP: GET Object', function testSuite() {
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
            const cmd = new DeleteObjectCommand({
                Bucket: bucketName,
                Key: this.currentTest.key,
            });
            await gcpClient.send(cmd);
        });

        it('should successfully retrieve object', async function testFn() {
            const cmd = new GetObjectCommand({
                Bucket: bucketName,
                Key: this.test.key,
            });
            const res = await gcpClient.send(cmd);
            assert.strictEqual(res.ETag, this.test.ETag);
            assert.strictEqual(res.VersionId, this.test.uploadId);
        });
    });

    describe('without existing object in bucket', () => {
        it('should return 404 and NoSuchKey', done => {
            const badObjectKey = `nonexistingkey-${genUniqID()}`;
            gcpClient.getObject({
                Bucket: bucketName,
                Key: badObjectKey,
            }, err => {
                assert(err);
                assert.strictEqual(err.$metadata?.httpStatusCode, 404);
                assert.strictEqual(err.name, 'NoSuchKey');
                return done();
            });
        });
    });
});
