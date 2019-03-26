const assert = require('assert');
const async = require('async');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external;
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
const numParts = 10;
const partSize = 10;

function gcpMpuSetupWrapper(params, callback) {
    gcpMpuSetup(params, (err, result) => {
        expect(err).toEqual(null);
        const { uploadId, etagList } = result;
        this.currentTest.uploadId = uploadId;
        this.currentTest.etagList = etagList;
        return callback();
    });
}

describe('GCP: Abort MPU', () => {
    let testContext;

    beforeEach(() => {
        testContext = {};
    });

    this.timeout(30000);
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

        test('should abort MPU with 0 parts', done => {
            return async.waterfall([
                next => {
                    const params = {
                        Bucket: bucketNames.main.Name,
                        MPU: bucketNames.mpu.Name,
                        Key: testContext.test.key,
                        UploadId: testContext.test.uploadId,
                    };
                    gcpClient.abortMultipartUpload(params, err => {
                        expect(err).toEqual(null);
                        return next();
                    });
                },
                next => {
                    const keyName =
                        `${testContext.test.key}-${testContext.test.uploadId}/init`;
                    gcpClient.headObject({
                        Bucket: bucketNames.mpu.Name,
                        Key: keyName,
                    }, err => {
                        expect(err).toBeTruthy();
                        expect(err.code).toBe(404);
                        return next();
                    });
                },
            ], done);
        });
    });

    describe('when MPU is incomplete', () => {
        beforeEach(done => {
            testContext.currentTest.key = `somekey-${genUniqID()}`;
            gcpMpuSetupWrapper.call(this, {
                gcpClient,
                bucketNames,
                key: testContext.currentTest.key,
                partCount: numParts, partSize,
            }, done);
        });

        test('should abort incomplete MPU', done => {
            return async.waterfall([
                next => {
                    const params = {
                        Bucket: bucketNames.main.Name,
                        MPU: bucketNames.mpu.Name,
                        Key: testContext.test.key,
                        UploadId: testContext.test.uploadId,
                    };
                    gcpClient.abortMultipartUpload(params, err => {
                        expect(err).toEqual(null);
                        return next();
                    });
                },
                next => {
                    const keyName =
                        `${testContext.test.key}-${testContext.test.uploadId}/init`;
                    gcpClient.headObject({
                        Bucket: bucketNames.mpu.Name,
                        Key: keyName,
                    }, err => {
                        expect(err).toBeTruthy();
                        expect(err.code).toBe(404);
                        return next();
                    });
                },
            ], err => done(err));
        });
    });
});
