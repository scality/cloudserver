const assert = require('assert');
const async = require('async');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external.GCP;
const { gcpMpuSetup, genUniqID, genBucketName, gcpRetry, waitForBucketReady } =
    require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
    ListObjectsCommand,
} = require('@aws-sdk/client-s3');

const credentialOne = 'gcpbackend';
const bucketNames = {
    main: {
        Name: genBucketName('deletempu'),
    },
    mpu: {
        Name: genBucketName('mpu-deletempu'),
    },
};
const numParts = 10;
const partSize = 10;

function gcpMpuSetupWrapper(params, callback) {
    gcpMpuSetup(params, (err, result) => {
        assert.equal(err, null,
            `Unable to setup MPU test, error ${err}`);
        const { uploadId, etagList } = result;
        this.currentTest.uploadId = uploadId;
        this.currentTest.etagList = etagList;
        return callback();
    });
}

describe('GCP: Abort MPU', function testSuite() {
    this.timeout(30000);
    let config;
    let gcpClient;

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
                await waitForBucketReady(gcpClient, bucket.Name);
            },
        );
    });

    after(async () => {
        const buckets = Object.values(bucketNames);
        await async.eachSeries(
            buckets,
            async bucket => {
                const listCmd = new ListObjectsCommand({
                    Bucket: bucket.Name,
                });
                const listRes = await gcpClient.send(listCmd);
                await async.map(listRes.Contents || [], async object => {
                    await gcpClient.deleteObject({
                        Bucket: bucket.Name,
                        Key: object.Key,
                    });
                });
                await gcpRetry(
                    gcpClient,
                    new DeleteBucketCommand({ Bucket: bucket.Name }),
                );
            },
        );
    });

    describe('when MPU has 0 parts', () => {
        beforeEach(function beforeFn(done) {
            this.currentTest.key = `somekey-${genUniqID()}`;
            gcpMpuSetupWrapper.call(this, {
                gcpClient,
                bucketNames,
                key: this.currentTest.key,
                partCount: 0, partSize,
            }, done);
        });

        it('should abort MPU with 0 parts', function testFn(done) {
            return async.waterfall([
                next => {
                    const params = {
                        Bucket: bucketNames.main.Name,
                        MPU: bucketNames.mpu.Name,
                        Key: this.test.key,
                        UploadId: this.test.uploadId,
                    };
                    gcpClient.abortMultipartUpload(params, err => {
                        assert.equal(err, null,
                            `Expected success, but got error ${err}`);
                        return next();
                    });
                },
                next => {
                    const keyName =
                        `${this.test.key}-${this.test.uploadId}/init`;
                    gcpClient.headObject({
                        Bucket: bucketNames.mpu.Name,
                        Key: keyName,
                    }, err => {
                        assert(err);
                        assert.strictEqual(err.$metadata.httpStatusCode, 404);
                        return next();
                    });
                },
            ], done);
        });
    });

    describe('when MPU is incomplete', () => {
        beforeEach(function beforeFn(done) {
            this.currentTest.key = `somekey-${genUniqID()}`;
            gcpMpuSetupWrapper.call(this, {
                gcpClient,
                bucketNames,
                key: this.currentTest.key,
                partCount: numParts, partSize,
            }, done);
        });

        it('should abort incomplete MPU', function testFn(done) {
            return async.waterfall([
                next => {
                    const params = {
                        Bucket: bucketNames.main.Name,
                        MPU: bucketNames.mpu.Name,
                        Key: this.test.key,
                        UploadId: this.test.uploadId,
                    };
                    gcpClient.abortMultipartUpload(params, err => {
                        assert.equal(err, null,
                            `Expected success, but got error ${err}`);
                        return next();
                    });
                },
                next => {
                    const keyName =
                        `${this.test.key}-${this.test.uploadId}/init`;
                    gcpClient.headObject({
                        Bucket: bucketNames.mpu.Name,
                        Key: keyName,
                    }, err => {
                        assert(err);
                        assert.strictEqual(err.$metadata.httpStatusCode, 404);
                        return next();
                    });
                },
            ], err => done(err));
        });
    });
});
