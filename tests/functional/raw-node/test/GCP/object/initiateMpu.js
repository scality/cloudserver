const assert = require('assert');
const async = require('async');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external.GCP;
const { genUniqID, gcpRetry } =
    require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
    HeadBucketCommand,
} = require('@aws-sdk/client-s3');

const credentialOne = 'gcpbackend';
const bucketNames = {
    main: {
        Name: `somebucket-${genUniqID()}`,
    },
    mpu: {
        Name: `mpubucket-${genUniqID()}`,
    },
};

describe('GCP: Initiate MPU', function testSuite() {
    this.timeout(180000);
    let config;
    let gcpClient;

    function waitForBucketReady(bucketName) {
        const cmd = new HeadBucketCommand({ Bucket: bucketName });
        return gcpRetry(gcpClient, cmd, {
            maxAttempts: 6,
            shouldRetry: err => err && (
                err.name === 'NoSuchBucket'
                || err.name === 'NotFound'
                || err.$metadata?.httpStatusCode === 404
                || err.name === 'SlowDown'
                || err.$metadata?.httpStatusCode === 429
            ),
            getDelayMs: attempt => (attempt + 1) * 1000,
        });
    }

    before(async () => {
        config = getRealAwsConfig(credentialOne);
        gcpClient = new GCP(config);
        const buckets = Object.values(bucketNames);
        await async.eachSeries(
            buckets,
            async bucket => {
                await gcpRetry(
                    gcpClient,
                    new CreateBucketCommand({ Bucket: bucket.Name }),
                );
                await waitForBucketReady(bucket.Name);
            },
        );
    });

    after(async () => {
        const buckets = Object.values(bucketNames);
        await async.eachSeries(
            buckets,
            async bucket => {
                await gcpRetry(
                    gcpClient,
                    new DeleteBucketCommand({ Bucket: bucket.Name }),
                );
            },
        );
    });

    it('Should create a multipart upload object', done => {
        const keyName = `somekey-${genUniqID()}`;
        const specialKey = `special-${genUniqID()}`;
        async.waterfall([
            next => gcpClient.createMultipartUpload({
                Bucket: bucketNames.mpu.Name,
                Key: keyName,
                Metadata: {
                    special: specialKey,
                },
            }, (err, res) => {
                assert.equal(err, null,
                    `Expected success, but got err ${err}`);
                return next(null, res.UploadId);
            }),
            (uploadId, next) => {
                const mpuInitKey = `${keyName}-${uploadId}/init`;
                gcpClient.headObject({
                    Bucket: bucketNames.mpu.Name,
                    Key: mpuInitKey,
                }, (err, res) => {
                    if (err) {
                        process.stdout
                            .write(`err in retrieving object ${err}`);
                        return next(err);
                    }
                    assert.strictEqual(res.Metadata.special, specialKey);
                    return next(null, uploadId);
                });
            },
            (uploadId, next) => gcpClient.abortMultipartUpload({
                Bucket: bucketNames.main.Name,
                MPU: bucketNames.mpu.Name,
                UploadId: uploadId,
                Key: keyName,
            }, err => {
                assert.equal(err, null,
                    `Expected success, but got err ${err}`);
                return next();
            }),
        ], done);
    });
});
