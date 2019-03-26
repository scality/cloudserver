const assert = require('assert');
const async = require('async');
const arsenal = require('arsenal');
const { GCP, GcpUtils } = arsenal.storage.data.external;
const { gcpRequestRetry, setBucketClass, gcpMpuSetup, genUniqID } =
    require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';
const bucketNames = {
    main: {
        Name: `somebucket-${genUniqID()}`,
        Type: 'MULTI_REGIONAL',
    },
    mpu: {
        Name: `mpubucket-${genUniqID()}`,
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

describe('GCP: Complete MPU', () => {
    let testContext;

    beforeEach(() => {
        testContext = {};
    });

    this.timeout(600000);
    let config;
    let gcpClient;

    beforeAll(done => {
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

    afterAll(done => {
        async.eachSeries(bucketNames,
            (bucket, next) => gcpClient.listObjects({
                Bucket: bucket.Name,
            }, (err, res) => {
                expect(err).toEqual(null);
                async.map(res.Contents, (object, moveOn) => {
                    const deleteParams = {
                        Bucket: bucket.Name,
                        Key: object.Key,
                    };
                    gcpClient.deleteObject(
                        deleteParams, err => moveOn(err));
                }, err => {
                    expect(err).toEqual(null);
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
        beforeEach(done => {
            testContext.currentTest.key = `somekey-${genUniqID()}`;
            gcpMpuSetupWrapper.call(this, {
                gcpClient,
                bucketNames,
                key: testContext.currentTest.key,
                partCount: 0, partSize,
            }, done);
        });

        test(
            'should return error if 0 parts are given in MPU complete',
            done => {
                const params = {
                    Bucket: bucketNames.main.Name,
                    MPU: bucketNames.mpu.Name,
                    Key: testContext.test.key,
                    UploadId: testContext.test.uploadId,
                    MultipartUpload: { Parts: [] },
                };
                gcpClient.completeMultipartUpload(params, err => {
                    expect(err).toBeTruthy();
                    expect(err.code).toBe(400);
                    return done();
                });
            }
        );
    });

    describe('when MPU has 1 uploaded part', () => {
        beforeEach(done => {
            testContext.currentTest.key = `somekey-${genUniqID()}`;
            gcpMpuSetupWrapper.call(this, {
                gcpClient,
                bucketNames,
                key: testContext.currentTest.key,
                partCount: 1, partSize,
            }, done);
        });

        test('should successfully complete MPU', done => {
            const parts = GcpUtils.createMpuList({
                Key: testContext.test.key,
                UploadId: testContext.test.uploadId,
            }, 'parts', 1).map(item => {
                Object.assign(item, {
                    ETag: testContext.test.etagList[item.PartNumber - 1],
                });
                return item;
            });
            const params = {
                Bucket: bucketNames.main.Name,
                MPU: bucketNames.mpu.Name,
                Key: testContext.test.key,
                UploadId: testContext.test.uploadId,
                MultipartUpload: { Parts: parts },
            };
            gcpClient.completeMultipartUpload(params, (err, res) => {
                expect(err).toEqual(null);
                expect(res.ETag).toBe(`"${smallMD5}"`);
                return done();
            });
        });
    });

    describe('when MPU has 1024 uploaded parts', () => {
        beforeEach(done => {
            testContext.currentTest.key = `somekey-${genUniqID()}`;
            gcpMpuSetupWrapper.call(this, {
                gcpClient,
                bucketNames,
                key: testContext.currentTest.key,
                partCount: numParts, partSize,
            }, done);
        });

        test('should successfully complete MPU', done => {
            const parts = GcpUtils.createMpuList({
                Key: testContext.test.key,
                UploadId: testContext.test.uploadId,
            }, 'parts', numParts).map(item => {
                Object.assign(item, {
                    ETag: testContext.test.etagList[item.PartNumber - 1],
                });
                return item;
            });
            const params = {
                Bucket: bucketNames.main.Name,
                MPU: bucketNames.mpu.Name,
                Key: testContext.test.key,
                UploadId: testContext.test.uploadId,
                MultipartUpload: { Parts: parts },
            };
            gcpClient.completeMultipartUpload(params, (err, res) => {
                expect(err).toEqual(null);
                expect(res.ETag).toBe(`"${bigMD5}"`);
                return done();
            });
        });
    });
});
