const assert = require('assert');
const async = require('async');
const { GCP } = require('../../../../../../lib/data/external/GCP');
const { makeGcpRequest } = require('../../../utils/makeRequest');
const { gcpRequestRetry } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');
const constants = require('../../../../../../constants');

const credentialOne = 'gcpbackend';

describe('GCP: GET Bucket', function testSuite() {
    this.timeout(180000);
    const config = getRealAwsConfig(credentialOne);
    const gcpClient = new GCP(config);

    describe('without existing bucket', () => {
        beforeEach(function beforeFn(done) {
            this.currentTest.bucketName = `somebucket-${Date.now()}`;
            return done();
        });

        it('should return 404 and NoSuchBucket', function testFn(done) {
            gcpClient.getBucket({
                Bucket: this.test.bucketName,
            }, err => {
                assert(err);
                assert.strictEqual(err.statusCode, 404);
                assert.strictEqual(err.code, 'NoSuchBucket');
                return done();
            });
        });
    });

    describe('with existing bucket', () => {
        let numberObjects;
        beforeEach(function beforeFn(done) {
            this.currentTest.bucketName = `somebucket-${Date.now()}`;
            this.currentTest.createdObjects = Array.from(
                Array(numberObjects).keys()).map(i => `someObject-${i}`);
            process.stdout
                .write(`Creating test bucket\n`);
            return async.waterfall([
                next => gcpRequestRetry({
                    method: 'PUT',
                    bucket: this.currentTest.bucketName,
                    authCredentials: config.credentials,
                }, 0, err => {
                    if (err) {
                        process.stdout.write(`err creating bucket ${err.code}`);
                    }
                    return next(err);
                }),
                next => {
                    process.stdout.write(
                        `Putting ${numberObjects} objects into bucket\n`);
                    async.mapLimit(this.currentTest.createdObjects, 10,
                    (object, moveOn) => {
                        makeGcpRequest({
                            method: 'PUT',
                            bucket: this.currentTest.bucketName,
                            objectKey: object,
                            authCredentials: config.credentials,
                        }, err => moveOn(err));
                    }, err => {
                        if (err) {
                            process.stdout
                                .write(`err putting objects ${err.code}`);
                        }
                        return next(err);
                    });
                },
            ], err => done(err));
        });

        afterEach(function afterFn(done) {
            return async.waterfall([
                next => {
                    process.stdout.write(
                        `Deleting ${numberObjects} objects from bucket\n`);
                    async.mapLimit(this.currentTest.createdObjects, 10,
                    (object, moveOn) => {
                        makeGcpRequest({
                            method: 'DELETE',
                            bucket: this.currentTest.bucketName,
                            objectKey: object,
                            authCredentials: config.credentials,
                        }, err => moveOn(err));
                    }, err => {
                        if (err) {
                            process.stdout
                                .write(`err deleting objects ${err.code}`);
                        }
                        return next(err);
                    });
                },
                next => gcpRequestRetry({
                    method: 'DELETE',
                    bucket: this.currentTest.bucketName,
                    authCredentials: config.credentials,
                }, 0, err => {
                    if (err) {
                        process.stdout.write(`err deleting bucket ${err.code}`);
                    }
                    return next(err);
                }),
            ], err => done(err));
        });

        describe('with less than listingHardLimit number of objects', () => {
            before('Number of objects: 20', () => {
                numberObjects = 20;
            });

            it('should list all 20 created objects',
            function testFn(done) {
                return gcpClient.listObjects({
                    Bucket: this.test.bucketName,
                }, (err, res) => {
                    assert.equal(err, null, `Expected success, but got ${err}`);
                    assert.strictEqual(res.Contents.length, numberObjects);
                    return done();
                });
            });

            describe('with MaxKeys at 10', () => {
                it('should list MaxKeys number of objects',
                function testFn(done) {
                    return gcpClient.listObjects({
                        Bucket: this.test.bucketName,
                        MaxKeys: 10,
                    }, (err, res) => {
                        assert.equal(err, null,
                            `Expected success, but got ${err}`);
                        assert.strictEqual(res.Contents.length, 10);
                        return done();
                    });
                });
            });
        });

        describe('with more than listingHardLimit number of objects', () => {
            before('Number of objects: 1001', () => {
                numberObjects = constants.listingHardLimit + 1;
            });

            it('should list at max 1000 of objects created',
            function testFn(done) {
                return gcpClient.listObjects({
                    Bucket: this.test.bucketName,
                }, (err, res) => {
                    assert.equal(err, null, `Expected success, but got ${err}`);
                    assert.strictEqual(res.Contents.length,
                        constants.listingHardLimit);
                    return done();
                });
            });

            describe('with MaxKeys at 1001', () => {
                it('should list at max 1000, ignoring MaxKeys',
                function testFn(done) {
                    return gcpClient.listObjects({
                        Bucket: this.test.bucketName,
                        MaxKeys: 1001,
                    }, (err, res) => {
                        assert.equal(err, null,
                            `Expected success, but got ${err}`);
                        assert.strictEqual(res.Contents.length,
                            constants.listingHardLimit);
                        return done();
                    });
                });
            });
        });
    });
});
