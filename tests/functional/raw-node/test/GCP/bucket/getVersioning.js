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

    beforeEach(function beforeFn(done) {
        this.currentTest.bucketName = `somebucket-${genUniqID()}`;
        const cmd = new CreateBucketCommand({
            Bucket: this.currentTest.bucketName,
        });
        gcpClient.send(cmd)
            .then(() => done())
            .catch(err => {
                process.stdout
                    .write(`err in creating bucket ${err.code}\n`);
                return done(err);
            });
    });

    afterEach(function afterFn(done) {
        const cmd = new DeleteBucketCommand({
            Bucket: this.currentTest.bucketName,
        });
        gcpClient.send(cmd)
            .then(() => done())
            .catch(err => {
                if (err) {
                    process.stdout
                        .write(`err in deleting bucket ${err.code}\n`);
                }
                return done(err);
            });
    });

    it('should verify bucket versioning is enabled', function testFn(done) {
        return async.waterfall([
            // Enable versioning using the official SDK client
            next => {
                const command = new PutBucketVersioningCommand({
                    Bucket: this.test.bucketName,
                    VersioningConfiguration: { Status: 'Enabled' },
                });
                return gcpClient.send(command)
                    .then(() => next())
                    .catch(err => next(err));
            },
            // Verify using GetBucketVersioningCommand
            next => {
                const command = new GetBucketVersioningCommand({
                    Bucket: this.test.bucketName,
                });
                return gcpClient.send(command)
                    .then(res => {
                        assert.deepStrictEqual(res.Status, verEnabledObj);
                        return next();
                    })
                    .catch(err => next(err));
            },
        ], err => done(err));
    });

    it('should verify bucket versioning is disabled', function testFn(done) {
        return async.waterfall([
            // Disable versioning using the official SDK client
            next => {
                const command = new PutBucketVersioningCommand({
                    Bucket: this.test.bucketName,
                    VersioningConfiguration: { Status: 'Suspended' },
                });
                return gcpClient.send(command)
                    .then(() => next())
                    .catch(err => next(err));
            },
            // Verify using GetBucketVersioningCommand
            next => {
                const command = new GetBucketVersioningCommand({
                    Bucket: this.test.bucketName,
                });
                return gcpClient.send(command)
                    .then(res => {
                        assert.deepStrictEqual(res.Status, verDisabledObj);
                        return next();
                    })
                    .catch(err => next(err));
            },
        ], err => done(err));
    });
});
