const assert = require('assert');
const async = require('async');
const { GCP } = require('../../../../../../lib/data/external/GCP');
const { gcpRequestRetry } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';

describe('GCP: PUT Bucket', () => {
    let gcpClient;

    describe('when user does not have permissions', () => {
        let bucketName;
        let config;

        before(done => {
            config = getRealAwsConfig(credentialOne);
            gcpClient = new GCP(config);
            bucketName = `somebucket-${Date.now()}`;
            return done();
        });

        it('should return 400 and InvalidArgument',
            done => gcpClient.createBucket({
                Bucket: bucketName,
                ProjectId: `SomeRandomProjectName-${Date.now()}`,
            }, err => {
                assert(err);
                assert.strictEqual(err.statusCode, 400);
                assert.strictEqual(err.code, 'InvalidArgument');
                return done();
            })
        );
    });

    describe('when user has permissions', () => {
        describe('without existing bucket', () => {
            let bucketName;
            let config;

            before(done => {
                config = getRealAwsConfig(credentialOne);
                gcpClient = new GCP(config);
                bucketName = `somebucket-${Date.now()}`;
                return done();
            });

            after(done => {
                gcpRequestRetry({
                    method: 'DELETE',
                    bucket: bucketName,
                    authCredentials: config.credentials,
                }, 0, err => {
                    if (err) {
                        process.stdout.write('err in deleting bucket\n');
                    } else {
                        process.stdout.write('Deleted bucket\n');
                    }
                    return done(err);
                });
            });

            it('should create bucket succesfully',
                done => async.waterfall([
                    next => gcpClient.createBucket({
                        Bucket: bucketName,
                    }, (err, res) => {
                        assert.equal(err, null,
                            `Expected success, but got error ${err}`);
                        return next(null, res);
                    }),
                    (resObj, next) => gcpRequestRetry({
                        method: 'HEAD',
                        bucket: bucketName,
                        authCredentials: config.credentials,
                    }, 0, (err, res) => {
                        assert.equal(err, null,
                            `Expected success, but got error ${err}`);
                        const headResObj = {
                            MetaVersionId: res.headers['x-goog-metageneration'],
                        };
                        assert.deepStrictEqual(resObj, headResObj);
                        return next();
                    }),
                ], err => done(err))
            );
        });

        describe('with existing bucket', () => {
            let bucketName;
            let config;

            before(done => {
                config = getRealAwsConfig(credentialOne);
                gcpClient = new GCP(config);
                bucketName = `somebucket-${Date.now()}`;
                return gcpRequestRetry({
                    method: 'PUT',
                    bucket: bucketName,
                    authCredentials: config.credentials,
                }, 0, err => {
                    if (err) {
                        process.stdout.write('err in creating bucket\n');
                    } else {
                        process.stdout.write('Created bucket\n');
                    }
                    return done(err);
                });
            });

            after(done => {
                gcpRequestRetry({
                    method: 'DELETE',
                    bucket: bucketName,
                    authCredentials: config.credentials,
                }, 0, err => {
                    if (err) {
                        process.stdout.write('err in deleting bucket\n');
                    } else {
                        process.stdout.write('Deleted bucket\n');
                    }
                    return done(err);
                });
            });

            it('should return 409 and BucketAlreadyOwnedByYou',
                done => gcpClient.createBucket({
                    Bucket: bucketName,
                }, err => {
                    assert(err);
                    assert.strictEqual(err.statusCode, 409);
                    assert.strictEqual(err.code, 'BucketAlreadyOwnedByYou');
                    return done();
                })
            );
        });
    });
});
