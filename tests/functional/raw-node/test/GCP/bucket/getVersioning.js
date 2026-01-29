const assert = require('assert');
const async = require('async');
const arsenal = require('arsenal');
const {
    PutBucketVersioningCommand,
    GetBucketVersioningCommand,
    CreateBucketCommand,
    DeleteBucketCommand,
} = require('@aws-sdk/client-s3');
const { GCP } = arsenal.storage.data.external.GCP;
const { genUniqID } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';
const verEnabledObj = 'Enabled';
const verDisabledObj = 'Suspended';

describe('GCP: GET Bucket Versioning', () => {
    const config = getRealAwsConfig(credentialOne);
    const gcpClient = new GCP(config);

    beforeEach(async function beforeFn() {
        this.currentTest.bucketName = `somebucket-${genUniqID()}`;
        const cmd = new CreateBucketCommand({
            Bucket: this.currentTest.bucketName,
        });
        await gcpClient.send(cmd);
    });

    afterEach(async function afterFn() {
        const cmd = new DeleteBucketCommand({
            Bucket: this.currentTest.bucketName,
        });
        await gcpClient.send(cmd);
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
