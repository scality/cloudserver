const assert = require('assert');
const async = require('async');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucketName = `versioning-bucket-${Date.now()}`;


function checkError(err, code) {
    assert.notEqual(err, null, 'Expected failure but got success');
    assert.strictEqual(err.code, code);
}

function checkNoError(err) {
    assert.ifError(err, `Expected success, got error ${JSON.stringify(err)}`);
}

function testVersioning(s3, versioningStatus, replicationStatus, removeReplication, cb) {
    const versioningParams = { Bucket: bucketName,
        VersioningConfiguration: { Status: versioningStatus } };
    const replicationParams = {
        Bucket: bucketName,
        ReplicationConfiguration: {
            Role: 'arn:aws:iam::123456789012:role/examplerole,' +
            'arn:aws:iam::123456789012:role/examplerole',
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
    async.waterfall([
        cb => s3.putBucketReplication(replicationParams, e => cb(e)),
        cb => {
            if (removeReplication) {
                return s3.deleteBucketReplication({ Bucket: bucketName }, e => cb(e));
            }
            return process.nextTick(() => cb());
        },
        cb => s3.putBucketVersioning(versioningParams, e => cb(e)),
    ], cb);
}

describe('Versioning on a replication source bucket', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        beforeEach(done => {
            async.waterfall([
                cb => s3.createBucket({ Bucket: bucketName }, e => cb(e)),
                cb => s3.putBucketVersioning({
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    },
                }, err => cb(err)),
            ], done);
        });

        afterEach(done => s3.deleteBucket({ Bucket: bucketName }, done));

        it('should not be able to disable versioning if replication enabled',
        done => {
            testVersioning(s3, 'Suspended', 'Enabled', false, err => {
                checkError(err, 'InvalidBucketState');
                done();
            });
        });

        it('should not be able to disable versioning if replication disabled',
        done => {
            testVersioning(s3, 'Suspended', 'Disabled', false, err => {
                checkError(err, 'InvalidBucketState');
                done();
            });
        });

        it('should be able to disable versioning after removed replication', done => {
            testVersioning(s3, 'Suspended', 'Disabled', true, err => {
                checkNoError(err);
                done();
            });
        });
    });
});
