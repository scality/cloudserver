const assert = require('assert');
const async = require('async');
const arsenal = require('arsenal');
const { GCP, GcpUtils } = arsenal.storage.data.external.GCP;
const {
    gcpRequestRetry,
    gcpClientRetry,
    setBucketClass,
    gcpMpuSetup,
    genUniqID,
} = require('../../../utils/gcpUtils');
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

function listObjectsPaginated(gcpClient, bucketName, cb) {
    const objects = [];
    let marker;

    function _list() {
        const params = { Bucket: bucketName };
        if (marker) {
            params.Marker = marker;
        }

        return gcpClientRetry(gcpClient.listObjects.bind(gcpClient), params, (err, res) => {
            if (err) {
                return cb(err);
            }

            const contents = (res && res.Contents) || [];
            objects.push(...contents);

            const isTruncated = Boolean(res && res.IsTruncated);
            if (!isTruncated) {
                return cb(null, objects);
            }

            // AWS listObjects(V1) pagination: prefer NextMarker, fallback to last key.
            marker = (res && res.NextMarker) ||
                (contents.length ? contents[contents.length - 1].Key : undefined);

            if (!marker) {
                return cb(null, objects);
            }

            return _list();
        });
    }

    return _list();
}

function emptyBucket(gcpClient, bucketName, cb) {
    return listObjectsPaginated(gcpClient, bucketName, (err, objects) => {
        if (err) {
            return cb(err);
        }
        return async.eachLimit(objects, 20, (object, next) => {
            const deleteParams = {
                Bucket: bucketName,
                Key: object.Key,
            };
            return gcpClientRetry(gcpClient.deleteObject.bind(gcpClient), deleteParams, next);
        }, cb);
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
            (bucket, next) => emptyBucket(gcpClient, bucket.Name, err => {
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
            this.currentTest.key = `somekey-${genUniqID()}`;
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
            this.currentTest.key = `somekey-${genUniqID()}`;
            gcpMpuSetupWrapper.call(this, {
                gcpClient,
                bucketNames,
                key: this.currentTest.key,
                partCount: numParts, partSize,
            }, done);
        });

        it('should successfully complete MPU',
        function testFn(done) {
            this.timeout(1200000);
            this.retries(1);

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
