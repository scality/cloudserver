const assert = require('assert');
const async = require('async');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { removeAllVersions, versioningEnabled } =
    require('../../lib/utility/versioning-util');

const sourceBucket = 'source-bucket';
const keyPrefix = 'test-prefix';

describe("Head object 'ReplicationStatus' value", () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        function checkHeadObj(key, expectedStatus, cb) {
            const params = { Bucket: sourceBucket, Key: key };
            return async.series([
                next => s3.putObject(params, next),
                next => s3.headObject(params, (err, res) => {
                    if (err) {
                        return next(err);
                    }
                    assert.strictEqual(res.ReplicationStatus, expectedStatus);
                    return next();
                }),
            ], cb);
        }

        beforeEach(done => async.series([
            next => s3.createBucket({ Bucket: sourceBucket }, next),
            next => s3.putBucketVersioning({
                Bucket: sourceBucket,
                VersioningConfiguration: versioningEnabled,
            }, next),
        ], done));

        afterEach(done => async.series([
            next => removeAllVersions({ Bucket: sourceBucket }, next),
            next => s3.deleteBucket({ Bucket: sourceBucket }, next),
        ], done));

        it('should be `undefined` when there is no bucket replication config',
            done => checkHeadObj(`${keyPrefix}-foobar`, undefined, done));

        describe('With bucket replication config', () => {
            beforeEach(done => s3.putBucketReplication({
                Bucket: sourceBucket,
                ReplicationConfiguration: {
                    Role: 'arn:aws:iam::123456789012:role/resource',
                    Rules: [
                        {
                            Destination: { Bucket: 'arn:aws:s3:::dest-bucket' },
                            Prefix: keyPrefix,
                            Status: 'Enabled',
                        },
                    ],
                },
            }, done));

            it("should be 'PENDING' when object key prefix applies",
                done => checkHeadObj(`${keyPrefix}-foobar`, 'PENDING', done));

            it('should be `undefined` when object key prefix does not apply',
                done => checkHeadObj(`foobar-${keyPrefix}`, undefined, done));
        });
    });
});
