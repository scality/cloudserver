const assert = require('assert');
const async = require('async');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketVersioningCommand,
    PutBucketReplicationCommand,
    DeleteBucketReplicationCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucketName = `versioning-bucket-${Date.now()}`;

function checkError(err, code) {
    assert.notEqual(err, null, 'Expected failure but got success');
    assert.strictEqual(err.name, code);
}

function checkNoError(err) {
    assert.ifError(err, `Expected success, got error ${JSON.stringify(err)}`);
}

function testVersioning(s3, versioningStatus, replicationStatus, removeReplication, cb) {
    const versioningParams = {
        Bucket: bucketName,
        VersioningConfiguration: { Status: versioningStatus },
    };
    const replicationParams = {
        Bucket: bucketName,
        ReplicationConfiguration: {
            Role: 'arn:aws:iam::123456789012:role/examplerole,' + 'arn:aws:iam::123456789012:role/examplerole',
            Rules: [
                {
                    Destination: {
                        Bucket: 'arn:aws:s3:::destinationbucket',
                        StorageClass: 'STANDARD',
                    },
                    Prefix: '',
                    Status: replicationStatus,
                },
            ],
        },
    };

    async.waterfall(
        [
            cb =>
                s3
                    .send(new PutBucketReplicationCommand(replicationParams))
                    .then(() => cb())
                    .catch(cb),
            cb => {
                if (removeReplication) {
                    return s3
                        .send(new DeleteBucketReplicationCommand({ Bucket: bucketName }))
                        .then(() => cb())
                        .catch(cb);
                }
                return process.nextTick(() => cb());
            },
            cb =>
                s3
                    .send(new PutBucketVersioningCommand(versioningParams))
                    .then(() => cb())
                    .catch(cb),
        ],
        cb,
    );
}

describe('Versioning on a replication source bucket', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        beforeEach(done => {
            async.waterfall(
                [
                    cb =>
                        s3
                            .send(new CreateBucketCommand({ Bucket: bucketName }))
                            .then(() => cb())
                            .catch(cb),
                    cb =>
                        s3
                            .send(
                                new PutBucketVersioningCommand({
                                    Bucket: bucketName,
                                    VersioningConfiguration: {
                                        Status: 'Enabled',
                                    },
                                }),
                            )
                            .then(() => cb())
                            .catch(cb),
                ],
                done,
            );
        });

        afterEach(async () => {
            await s3.send(new DeleteBucketCommand({ Bucket: bucketName }));
        });

        it('should not be able to disable versioning if replication enabled', done => {
            testVersioning(s3, 'Suspended', 'Enabled', false, err => {
                checkError(err, 'InvalidBucketState');
                done();
            });
        });

        it('should be able to suspend versioning if replication disabled', done => {
            testVersioning(s3, 'Suspended', 'Disabled', false, err => {
                checkNoError(err);
                done();
            });
        });

        it('should be able to suspend versioning after removed replication', done => {
            testVersioning(s3, 'Suspended', 'Disabled', true, err => {
                checkNoError(err);
                done();
            });
        });
    });
});
