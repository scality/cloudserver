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

    before(done => {
        const cmd = new CreateBucketCommand({ Bucket: bucketName });
        gcpClient.send(cmd)
            .then(() => done())
            .catch(err => {
                process.stdout.write(`err in creating bucket ${err}\n`);
                return done(err);
            });
    });

    after(done => {
        const cmd = new DeleteBucketCommand({ Bucket: bucketName });
        gcpClient.send(cmd)
            .then(() => done())
            .catch(err => {
                process.stdout.write(`err in deleting bucket ${err}\n`);
                return done(err);
            });
    });

    it('should enable bucket versioning', done => async.waterfall([
            next => {
                const cmd = new PutBucketVersioningCommand({
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    },
                });
                return gcpClient.send(cmd)
                    .then(() => next())
                    .catch(err => next(err));
            },
            next => {
                const cmd = new GetBucketVersioningCommand({
                    Bucket: bucketName,
                });
                return gcpClient.send(cmd)
                    .then(res => {
                        assert.strictEqual(res.Status, verEnabledStatus);
                        return next();
                    })
                    .catch(err => next(err));
            },
        ], err => done(err)));

    it('should disable bucket versioning', done => async.waterfall([
            next => {
                const cmd = new PutBucketVersioningCommand({
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Suspended',
                    },
                });
                return gcpClient.send(cmd)
                    .then(() => next())
                    .catch(err => next(err));
            },
            next => {
                const cmd = new GetBucketVersioningCommand({
                    Bucket: bucketName,
                });
                return gcpClient.send(cmd)
                    .then(res => {
                        assert.strictEqual(res.Status, verDisabledStatus);
                        return next();
                    })
                    .catch(err => next(err));
            },
        ], err => done(err)));
});
