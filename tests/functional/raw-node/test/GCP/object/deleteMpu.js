const assert = require('assert');
const async = require('async');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external.GCP;
const { gcpMpuSetup, genUniqID, gcpRetry, listBucketObjects } =
    require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
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

    before(done => {
        config = getRealAwsConfig(credentialOne);
        gcpClient = new GCP(config);

        const buckets = Object.values(bucketNames);
        async.eachSeries(
            buckets,
            (bucket, next) => gcpRetry(
                gcpClient,
                () => new CreateBucketCommand({ Bucket: bucket.Name }),
                null,
                next,
            ),
            done,
        );
    });

    after(done => {
        async.eachSeries(
            Object.values(bucketNames),
            (bucket, next) => listBucketObjects(
                gcpClient,
                { Bucket: bucket.Name },
                (err, res) => {
                assert.equal(err, null,
                    `Expected success, but got error ${err}`);
                async.map(res.Contents, (object, moveOn) => {
                    const deleteParams = {
                        Bucket: bucket.Name,
                        Key: object.Key,
                    };
                    gcpClient.deleteObject(
                        deleteParams, err => moveOn(err));
                }, err => {
                    assert.equal(err, null,
                        `Expected success, but got error ${err}`);
                    gcpRetry(
                        gcpClient,
                        () => new DeleteBucketCommand({ Bucket: bucket.Name }),
                        null,
                        error => {
                            if (error) {
                                process.stdout.write(
                                    `err in deleting bucket ${error}\n`);
                            }
                            return next(error);
                        },
                    );
                });
            }),
            done,
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
