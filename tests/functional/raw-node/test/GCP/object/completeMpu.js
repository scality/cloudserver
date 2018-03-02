const assert = require('assert');
const async = require('async');
const { GCP, GcpUtils } = require('../../../../../../lib/data/external/GCP');
const { gcpRequestRetry, setBucketClass, gcpMpuSetup } =
    require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';
const bucketNames = {
    main: {
        Name: `somebucket-${Date.now()}`,
        Type: 'MULTI_REGIONAL',
    },
    mpu: {
        Name: `mpubucket-${Date.now()}`,
        Type: 'MULTI_REGIONAL',
    },
};
const numParts = 1024;
const partSize = 10;

const smallMD5 = '583c466f3f31d97b361adc60caea72f5-1';
const bigMD5 = '9c8a62e2c04a512ce348d8280497b49e-1024';

function gcpMpuSetupWrapper(params, callback) {
    gcpMpuSetup(params, (err, result) => {
        assert.ifError(err, `Unable to setup MPU test, error ${err}`);
        const { uploadId, etagList } = result;
        this.currentTest.uploadId = uploadId;
        this.currentTest.etagList = etagList;
        return callback();
    });
}

describe('GCP: Complete MPU', function testSuite() {
    this.timeout(600000);
    let config;
    let gcpClient;

    before(done => {
        config = getRealAwsConfig(credentialOne);
        gcpClient = new GCP(config);
        async.eachSeries(bucketNames,
            (bucket, next) => gcpRequestRetry({
                method: 'PUT',
                bucket: bucket.Name,
                authCredentials: config.credentials,
                requestBody: setBucketClass(bucket.Type),
            }, 0, err => {
                if (err) {
                    process.stdout.write(`err in creating bucket ${err}\n`);
                }
                return next(err);
            }),
        done);
    });

    after(done => {
        async.eachSeries(bucketNames,
            (bucket, next) => gcpClient.listObjects({
                Bucket: bucket.Name,
            }, (err, res) => {
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
                    gcpRequestRetry({
                        method: 'DELETE',
                        bucket: bucket.Name,
                        authCredentials: config.credentials,
                    }, 0, err => {
                        if (err) {
                            process.stdout.write(
                                `err in deleting bucket ${err}\n`);
                        }
                        return next(err);
                    });
                });
            }),
        done);
    });

    describe('when MPU has 0 parts', () => {
        beforeEach(function beforeFn(done) {
            this.currentTest.key = `somekey-${Date.now()}`;
            gcpMpuSetupWrapper.call(this, {
                gcpClient,
                bucketNames,
                key: this.currentTest.key,
                partCount: 0, partSize,
            }, done);
        });

        it('should return error if 0 parts are given in MPU complete',
        function testFn(done) {
            const params = {
                Bucket: bucketNames.main.Name,
                MPU: bucketNames.mpu.Name,
                Key: this.test.key,
                UploadId: this.test.uploadId,
                MultipartUpload: { Parts: [] },
            };
            gcpClient.completeMultipartUpload(params, err => {
                assert(err);
                assert.strictEqual(err.code, 400);
                return done();
            });
        });
    });

    describe('when MPU has 1 uploaded part', () => {
        beforeEach(function beforeFn(done) {
            this.currentTest.key = `somekey-${Date.now()}`;
            gcpMpuSetupWrapper.call(this, {
                gcpClient,
                bucketNames,
                key: this.currentTest.key,
                partCount: 1, partSize,
            }, done);
        });

        it('should successfully complete MPU',
        function testFn(done) {
            const parts = GcpUtils.createMpuList({
                Key: this.test.key,
                UploadId: this.test.uploadId,
            }, 'parts', 1).map(item => {
                Object.assign(item, {
                    ETag: this.test.etagList[item.PartNumber - 1],
                });
                return item;
            });
            const params = {
                Bucket: bucketNames.main.Name,
                MPU: bucketNames.mpu.Name,
                Key: this.test.key,
                UploadId: this.test.uploadId,
                MultipartUpload: { Parts: parts },
            };
            gcpClient.completeMultipartUpload(params, (err, res) => {
                assert.equal(err, null,
                    `Expected success, but got error ${err}`);
                assert.strictEqual(res.ETag, `"${smallMD5}"`);
                return done();
            });
        });
    });

    describe('when MPU has 1024 uploaded parts', () => {
        beforeEach(function beforeFn(done) {
            this.currentTest.key = `somekey-${Date.now()}`;
            gcpMpuSetupWrapper.call(this, {
                gcpClient,
                bucketNames,
                key: this.currentTest.key,
                partCount: numParts, partSize,
            }, done);
        });

        it('should successfully complete MPU',
        function testFn(done) {
            const parts = GcpUtils.createMpuList({
                Key: this.test.key,
                UploadId: this.test.uploadId,
            }, 'parts', numParts).map(item => {
                Object.assign(item, {
                    ETag: this.test.etagList[item.PartNumber - 1],
                });
                return item;
            });
            const params = {
                Bucket: bucketNames.main.Name,
                MPU: bucketNames.mpu.Name,
                Key: this.test.key,
                UploadId: this.test.uploadId,
                MultipartUpload: { Parts: parts },
            };
            gcpClient.completeMultipartUpload(params, (err, res) => {
                assert.equal(err, null,
                    `Expected success, but got error ${err}`);
                assert.strictEqual(res.ETag, `"${bigMD5}"`);
                return done();
            });
        });
    });
});
