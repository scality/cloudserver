const assert = require('assert');
const arsenal = require('arsenal');
const {
    PutBucketVersioningCommand,
    GetBucketVersioningCommand,
    CreateBucketCommand,
    DeleteBucketCommand,
} = require('@aws-sdk/client-s3');
const { GCP } = arsenal.storage.data.external.GCP;
const { genBucketName, gcpRetry } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';
const config = getRealAwsConfig(credentialOne);
const gcpClient = new GCP(config);
const bucketName = genBucketName('versioning');

describe('GCP: Bucket Versioning', function testSuite() {
    this.timeout(120000);

    before(async () => {
        await gcpRetry(gcpClient, new CreateBucketCommand({ Bucket: bucketName }));
    });

    after(async () => {
        await gcpRetry(gcpClient, new DeleteBucketCommand({ Bucket: bucketName }));
    });

    it('should enable bucket versioning', async () => {
        await gcpClient.send(new PutBucketVersioningCommand({
            Bucket: bucketName,
            VersioningConfiguration: { Status: 'Enabled' },
        }));

        const res = await gcpClient.send(
            new GetBucketVersioningCommand({ Bucket: bucketName }));
        assert.strictEqual(res.Status, 'Enabled');
    });

    it('should disable bucket versioning', async () => {
        await gcpClient.send(new PutBucketVersioningCommand({
            Bucket: bucketName,
            VersioningConfiguration: { Status: 'Suspended' },
        }));

        const res = await gcpClient.send(
            new GetBucketVersioningCommand({ Bucket: bucketName }));
        assert.strictEqual(res.Status, 'Suspended');
    });
});
