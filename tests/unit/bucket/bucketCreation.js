const { errors } = require('arsenal');

const assert = require('assert');

const { cleanup, DummyRequestLogger } = require('../helpers');
const { createBucket } =
    require('../../../lib/api/apiUtils/bucket/bucketCreation');
const { makeAuthInfo } = require('../helpers');

const bucketName = 'creationbucket';
const log = new DummyRequestLogger();
const headers = {};
const authInfo = makeAuthInfo('accessKey1');

const normalBehaviorLocationConstraint = 'file';
const specialBehaviorLocationConstraint = 'us-east-1';

describe('bucket creation', () => {
    test('should create a bucket', done => {
        createBucket(authInfo, bucketName, headers,
            normalBehaviorLocationConstraint, log, err => {
                assert.ifError(err);
                done();
            });
    });

    describe('when you already created the bucket in us-east-1', () => {
        beforeEach(done => {
            cleanup();
            createBucket(authInfo, bucketName, headers,
                specialBehaviorLocationConstraint, log, err => {
                    assert.ifError(err);
                    done();
                });
        });

        test('should return 200 if try to recreate in us-east-1', done => {
            createBucket(authInfo, bucketName, headers,
            specialBehaviorLocationConstraint, log, err => {
                assert.ifError(err);
                done();
            });
        });

        test('should return 409 if try to recreate in non-us-east-1', done => {
            createBucket(authInfo, bucketName, headers,
            normalBehaviorLocationConstraint, log, err => {
                expect(err).toBe(errors.BucketAlreadyOwnedByYou);
                done();
            });
        });
    });

    describe('when you already created the bucket in non-us-east-1', () => {
        beforeEach(done => {
            cleanup();
            createBucket(authInfo, bucketName, headers,
                normalBehaviorLocationConstraint, log, err => {
                    assert.ifError(err);
                    done();
                });
        });

        test('should return 409 if try to recreate in us-east-1', done => {
            createBucket(authInfo, bucketName, headers,
            specialBehaviorLocationConstraint, log, err => {
                expect(err).toBe(errors.BucketAlreadyOwnedByYou);
                done();
            });
        });
    });
});
