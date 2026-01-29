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
const verEnabledStatus = 'Enabled';
const verDisabledStatus = 'Suspended';
const bucketName = `somebucket-${genUniqID()}`;

describe('GCP: PUT Bucket Versioning', () => {
    const config = getRealAwsConfig(credentialOne);
    const gcpClient = new GCP(config);

    before(async () => {
        const cmd = new CreateBucketCommand({ Bucket: bucketName });
        await gcpClient.send(cmd);
    });

    after(async () => {
        const cmd = new DeleteBucketCommand({ Bucket: bucketName });
        await gcpClient.send(cmd);
    });

    it('should enable bucket versioning', async () => {
        await gcpClient.send(new PutBucketVersioningCommand({
            Bucket: bucketName,
            VersioningConfiguration: {
                Status: 'Enabled',
            },
        }));

        const res = await gcpClient.send(new GetBucketVersioningCommand({
            Bucket: bucketName,
        }));
        assert.strictEqual(res.Status, verEnabledStatus);
    });

    it('should disable bucket versioning', async () => {
        await gcpClient.send(new PutBucketVersioningCommand({
            Bucket: bucketName,
            VersioningConfiguration: {
                Status: 'Suspended',
            },
        }));

        const res = await gcpClient.send(new GetBucketVersioningCommand({
            Bucket: bucketName,
        }));
        assert.strictEqual(res.Status, verDisabledStatus);
    });
});
