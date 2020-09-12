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

function generateReplicationConfig(status) {
    return {
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
                    Status: status,
                },
            ],
        },
    };
}

describe('Versioning on a replication source bucket', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        beforeEach(done => {
            async.waterfall([
                cb => s3.createBucket({ Bucket: bucketName }, e => cb(e)),
                cb => s3.putBucketVersioningPromise({
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    },
                }, err => cb(err)),
            ], done);
        });

        afterEach(done => s3.deleteBucket({ Bucket: bucketName }, done));

        it('should not be able to disable versioning if replication enabled ',
        done => {
            const versioningParams = { Bucket: bucketName,
                VersioningConfiguration: { Status: 'Suspended' } };
            const replicationParams = generateReplicationConfig('Enabled');
            async.waterfall([
                cb => s3.putBucketReplication(replicationParams, e => cb(e)),
                cb => s3.putBucketVersioning(versioningParams, e => cb(e)),
            ], err => {
                checkError(err, 'InvalidBucketState');
                done();
            });
        });

        it('should be able to disable versioning if replication disabled ',
        done => {
            const params = { Bucket: bucketName,
                VersioningConfiguration: { Status: 'Suspended' } };
            s3.putBucketVersioning(params, err => {
                checkNoError(err);
                return done();
            });
        });
    });
});
