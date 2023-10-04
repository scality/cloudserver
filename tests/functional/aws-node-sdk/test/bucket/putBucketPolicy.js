const assert = require('assert');
const { errors } = require('arsenal');
const { S3 } = require('aws-sdk');

const getConfig = require('../support/config');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = 'policyputtestbucket';
const basicStatement = {
    Sid: 'statementid',
    Effect: 'Allow',
    Principal: '*',
    Action: ['s3:putBucketPolicy'],
    Resource: `arn:aws:s3:::${bucket}`,
};

function getPolicyParams(paramToChange) {
    const newParam = {};
    const bucketPolicy = {
        Version: '2012-10-17',
        Statement: [basicStatement],
    };
    if (paramToChange) {
        newParam[paramToChange.key] = paramToChange.value;
        bucketPolicy.Statement[0] = Object.assign({}, basicStatement, newParam);
    }
    return {
        Bucket: bucket,
        Policy: JSON.stringify(bucketPolicy),
    };
}

function getPolicyParamsWithId(paramToChange, policyId) {
    const newParam = {};
    const bucketPolicy = {
        Version: '2012-10-17',
        Id: policyId,
        Statement: [basicStatement],
    };
    if (paramToChange) {
        newParam[paramToChange.key] = paramToChange.value;
        bucketPolicy.Statement[0] = Object.assign({}, basicStatement, newParam);
    }
    return {
        Bucket: bucket,
        Policy: JSON.stringify(bucketPolicy),
    };
}

function generateRandomString(length) {
    // All allowed characters matching the regex in arsenal
    const allowedCharacters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+=,.@ -/';
    const allowedCharactersLength = allowedCharacters.length;

    return [...Array(length)]
      .map(() => allowedCharacters[~~(Math.random() * allowedCharactersLength)])
      .join('');
}

// Check for the expected error response code and status code.
function assertError(err, expectedErr, cb) {
    if (expectedErr === null) {
        assert.strictEqual(err, null, `expected no error but got '${err}'`);
    } else {
        assert.strictEqual(err.code, expectedErr, 'incorrect error response ' +
            `code: should be '${expectedErr}' but got '${err.code}'`);
        assert.strictEqual(err.statusCode, errors[expectedErr].code,
            'incorrect error status code: should be  ' +
            `${errors[expectedErr].code}, but got '${err.statusCode}'`);
    }
    cb();
}


describe('aws-sdk test put bucket policy', () => {
    let s3;
    let otherAccountS3;

    before(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        otherAccountS3 = new BucketUtility('lisa', {}).s3;
        return done();
    });

    it('should return NoSuchBucket error if bucket does not exist', done => {
        const params = getPolicyParams();
        s3.putBucketPolicy(params, err =>
            assertError(err, 'NoSuchBucket', done));
    });

    describe('config rules', () => {
        beforeEach(done => s3.createBucket({ Bucket: bucket }, done));

        afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

        it('should return MethodNotAllowed if user is not bucket owner', done => {
            const params = getPolicyParams();
            otherAccountS3.putBucketPolicy(params,
                err => assertError(err, 'MethodNotAllowed', done));
        });

        it('should put a bucket policy on bucket', done => {
            const params = getPolicyParams();
            s3.putBucketPolicy(params, err =>
                assertError(err, null, done));
        });

        it('should not allow bucket policy with no Action', done => {
            const params = getPolicyParams({ key: 'Action', value: '' });
            s3.putBucketPolicy(params, err =>
                assertError(err, 'MalformedPolicy', done));
        });

        it('should not allow bucket policy with no Effect', done => {
            const params = getPolicyParams({ key: 'Effect', value: '' });
            s3.putBucketPolicy(params, err =>
                assertError(err, 'MalformedPolicy', done));
        });

        it('should not allow bucket policy with no Resource', done => {
            const params = getPolicyParams({ key: 'Resource', value: '' });
            s3.putBucketPolicy(params, err =>
                assertError(err, 'MalformedPolicy', done));
        });

        it('should not allow bucket policy with no Principal',
        done => {
            const params = getPolicyParams({ key: 'Principal', value: '' });
            s3.putBucketPolicy(params, err =>
                assertError(err, 'MalformedPolicy', done));
        });

        it('should return MalformedPolicy because Id is not a string',
        done => {
            const params = getPolicyParamsWithId(null, 59);
            s3.putBucketPolicy(params, err =>
                assertError(err, 'MalformedPolicy', done));
        });

        it('should put a bucket policy on bucket since Id is a string',
        done => {
            const params = getPolicyParamsWithId(null, 'cd3ad3d9-2776-4ef1-a904-4c229d1642e');
            s3.putBucketPolicy(params, err =>
                assertError(err, null, done));
        });

        it('should allow bucket policy with pincipal arn less than 2048 characters', done => {
            const params = getPolicyParams({ key: 'Principal', value: { AWS: `arn:aws:iam::767707094035:user/${generateRandomString(150)}` } }); // eslint-disable-line max-len
            s3.putBucketPolicy(params, err =>
                assertError(err, null, done));
        });

        it('should not allow bucket policy with pincipal arn more than 2048 characters', done => {
            const params = getPolicyParams({ key: 'Principal', value: { AWS: `arn:aws:iam::767707094035:user/${generateRandomString(2020)}` } }); // eslint-disable-line max-len
            s3.putBucketPolicy(params, err =>
                assertError(err, 'MalformedPolicy', done));
        });
    });
});
