import assert from 'assert';

import { errors } from 'arsenal';

import { cleanup, DummyRequestLogger } from '../helpers';
import { createBucket } from '../../../lib/api/apiUtils/bucket/bucketCreation';
import { makeAuthInfo } from '../helpers';

const bucketName = 'creationbucket';
const log = new DummyRequestLogger();
const normalBehaviorLocationConstraint = 'us-west-1';
const specialBehaviorLocationConstraint = 'us-east-1';
const usEastBehavior = true;
const headers = {};
const authInfo = makeAuthInfo('accessKey1');

describe('bucket creation', () => {
    it('should create a bucket', done => {
        createBucket(authInfo, bucketName, headers,
            normalBehaviorLocationConstraint, !usEastBehavior, log, err => {
                assert.ifError(err);
                done();
            });
    });

    describe('when you already created the bucket in us-east', () => {
        beforeEach(done => {
            cleanup();
            createBucket(authInfo, bucketName, headers,
                specialBehaviorLocationConstraint, usEastBehavior, log, err => {
                    assert.ifError(err);
                    done();
                });
        });

        it('should return 200 if try to recreate in us-east and ' +
            'usEastBehavior config set', done => {
            createBucket(authInfo, bucketName, headers,
            specialBehaviorLocationConstraint, usEastBehavior, log, err => {
                assert.ifError(err);
                done();
            });
        });

        it('should return 409 if try to recreate in non-us-east-1 even if ' +
            'usEastBehavior config set', done => {
            createBucket(authInfo, bucketName, headers,
            normalBehaviorLocationConstraint, usEastBehavior, log, err => {
                assert.strictEqual(err, errors.BucketAlreadyOwnedByYou);
                done();
            });
        });

        it('should return 409 if try to recreate in us-east-1 but without ' +
            'usEastBehavior config set', done => {
            createBucket(authInfo, bucketName, headers,
            specialBehaviorLocationConstraint, !usEastBehavior, log, err => {
                assert.strictEqual(err, errors.BucketAlreadyOwnedByYou);
                done();
            });
        });
    });

    describe('when you already created the bucket in non-us-east-1', () => {
        beforeEach(done => {
            cleanup();
            createBucket(authInfo, bucketName, headers,
                normalBehaviorLocationConstraint, usEastBehavior, log, err => {
                    assert.ifError(err);
                    done();
                });
        });

        it('should return 409 even if try to recreate in us-east and ' +
            'usEastBehavior config set', done => {
            createBucket(authInfo, bucketName, headers,
            specialBehaviorLocationConstraint, usEastBehavior, log, err => {
                assert.strictEqual(err, errors.BucketAlreadyOwnedByYou);
                done();
            });
        });

        it('should return 409 if try to recreate in non-us-east-1 even if ' +
            'usEastBehavior config set', done => {
            createBucket(authInfo, bucketName, headers,
            normalBehaviorLocationConstraint, usEastBehavior, log, err => {
                assert.strictEqual(err, errors.BucketAlreadyOwnedByYou);
                done();
            });
        });

        it('should return 409 if try to recreate in us-east-1 but without ' +
            'usEastBehavior config set', done => {
            createBucket(authInfo, bucketName, headers,
            specialBehaviorLocationConstraint, !usEastBehavior, log, err => {
                assert.strictEqual(err, errors.BucketAlreadyOwnedByYou);
                done();
            });
        });
    });
});
