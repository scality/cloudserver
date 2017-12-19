const assert = require('assert');
const async = require('async');
const { GCP } = require('../../../../../../lib/data/external/GCP');
const { makeGcpRequest } = require('../../../utils/makeRequest');
const { gcpRequestRetry } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';

describe('GCP: DELETE Bucket', () => {
    let config;
    let gcpClient;

    before(done => {
        config = getRealAwsConfig(credentialOne);
        gcpClient = new GCP(config);
        return done();
    });

    describe('without existing bucket', () => {
        it('should return 404 and NoSuchBucket', done => {
            const bucketName = `nonexistingbucket-${Date.now()}`;
            return gcpClient.deleteBucket({
                Bucket: bucketName,
            }, err => {
                assert(err);
                assert.strictEqual(err.statusCode, 404);
                assert.strictEqual(err.code, 'NoSuchBucket');
                return done();
            });
        });
    });

    describe('with existing bucket', () => {
        beforeEach(function beforeFn(done) {
            this.currentTest.bucketName = `existingbucket-${Date.now()}`;
            return gcpRequestRetry({
                method: 'PUT',
                bucket: this.currentTest.bucketName,
                authCredentials: config.credentials,
            }, 0, err => {
                if (err) {
                    process.stdout.write(`err in creating bucket ${err}\n`);
                } else {
                    process.stdout.write('Created bucket\n');
                }
                return done(err);
            });
        });

        describe('when bucket is empty', () => {
            it('should delete bucket successfully', function testFn(done) {
                return setTimeout(() => gcpClient.deleteBucket({
                    Bucket: this.test.bucketName,
                }, err => {
                    assert.equal(err, null,
                        `Expected success, but got err ${err}`);
                    return done();
                }), 500);
            });
        });

        describe('when bucket is not empty', () => {
            beforeEach('populating bucket', function beforeFn(done) {
                this.currentTest.key = `somekey-${Date.now()}`;
                return makeGcpRequest({
                    method: 'PUT',
                    bucket: this.currentTest.bucketName,
                    objectKey: this.currentTest.key,
                    authCredentials: config.credentials,
                }, err => {
                    if (err) {
                        process.stdout.write(`err in creating object ${err}\n`);
                    } else {
                        process.stdout.write('Created object\n');
                    }
                    return done(err);
                });
            });

            afterEach(function afterFn(done) {
                return async.waterfall([
                    next => makeGcpRequest({
                        method: 'DELETE',
                        bucket: this.currentTest.bucketName,
                        objectKey: this.currentTest.key,
                        authCredentials: config.credentials,
                    }, err => {
                        if (err) {
                            process.stdout
                                .write(`err in deleting object ${err}\n`);
                        } else {
                            process.stdout.write('Deleted object\n');
                        }
                        return next(err);
                    }),
                    next => gcpRequestRetry({
                        method: 'DELETE',
                        bucket: this.currentTest.bucketName,
                        authCredentials: config.credentials,
                    }, 0, err => {
                        if (err) {
                            process.stdout
                                .write(`err in deleting bucket ${err}\n`);
                        } else {
                            process.stdout.write('Deleted bucket\n');
                        }
                        return next(err);
                    }),
                ], err => done(err));
            });

            it('should return 409 and BucketNotEmpty', function testFn(done) {
                return setTimeout(() => gcpClient.deleteBucket({
                    Bucket: this.test.bucketName,
                }, err => {
                    assert(err);
                    assert.strictEqual(err.statusCode, 409);
                    assert.strictEqual(err.code, 'BucketNotEmpty');
                    return done();
                }), 500);
            });
        });
    });
});
