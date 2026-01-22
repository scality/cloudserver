const assert = require('assert');
const async = require('async');
const arsenal = require('arsenal');
const { ListObjectsCommand } = require('@aws-sdk/client-s3');
const { GCP } = arsenal.storage.data.external.GCP;
const { gcpMpuSetup, genUniqID } =
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
        gcpClient.listObjects = (params, callback) => {
            const command = new ListObjectsCommand(params);
            return gcpClient.send(command)
                .then(data => callback(null, data))
                .catch(err => {
                    if (err && err.$metadata && err.$metadata.httpStatusCode &&
                        err.statusCode === undefined) {
                        // eslint-disable-next-line no-param-reassign
                        err.statusCode = err.$metadata.httpStatusCode;
                    }
                    return callback(err);
                });
        };

        const maxAttempts = 3;
        const buckets = Object.values(bucketNames);
        const createBuckets = attempt => {
            async.eachSeries(buckets,
                (bucket, next) => {
                    const cmd = new CreateBucketCommand({ Bucket: bucket.Name });
                    gcpClient.send(cmd)
                        .then(() => next())
                        .catch(err => {
                            process.stdout.write(
                                `err in creating bucket (attempt ${attempt + 1}) ${err}\n`);
                            return next(err);
                        });
                },
            err => {
                if (err && (err.name === 'SlowDown'
                    || err.$metadata?.httpStatusCode === 429)
                    && attempt < maxAttempts - 1) {
                    const delay = Math.pow(2, attempt) * 1000;
                    return setTimeout(() => createBuckets(attempt + 1), delay);
                }
                return done(err);
            });
        };

        createBuckets(0);
    });

    after(done => {
        async.eachSeries(Object.values(bucketNames),
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
                    const cmd = new DeleteBucketCommand({ Bucket: bucket.Name });
                    gcpClient.send(cmd)
                        .then(() => next())
                        .catch(error => {
                            if (error) {
                                process.stdout.write(
                                    `err in deleting bucket ${error}\n`);
                            }
                            return next(error);
                        });
                });
            }),
        done);
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
