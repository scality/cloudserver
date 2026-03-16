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
const verEnabledObj = 'Enabled';
const verDisabledObj = 'Suspended';

describe('GCP: GET Bucket Versioning', function testSuite() {
    this.timeout(120000);
    const config = getRealAwsConfig(credentialOne);
    const gcpClient = new GCP(config);

    beforeEach(async function beforeFn() {
        this.currentTest.bucketName = genBucketName('getversioning');
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

    it('should verify bucket versioning is enabled', async function testFn() {
        await gcpClient.send(new PutBucketVersioningCommand({
            Bucket: this.test.bucketName,
            VersioningConfiguration: { Status: 'Enabled' },
        }));

        const res = await gcpClient.send(new GetBucketVersioningCommand({
            Bucket: this.test.bucketName,
        }));
        assert.deepStrictEqual(res.Status, verEnabledObj);
    });

    it('should verify bucket versioning is disabled', async function testFn() {
        await gcpClient.send(new PutBucketVersioningCommand({
            Bucket: this.test.bucketName,
            VersioningConfiguration: { Status: 'Suspended' },
        }));

        const res = await gcpClient.send(new GetBucketVersioningCommand({
            Bucket: this.test.bucketName,
        }));
        assert.deepStrictEqual(res.Status, verDisabledObj);
    });
});
