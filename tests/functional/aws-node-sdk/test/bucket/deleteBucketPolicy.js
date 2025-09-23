const assert = require('assert');
const { errors } = require('arsenal');
const { S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    DeleteBucketPolicyCommand,
    PutBucketPolicyCommand,
    GetBucketPolicyCommand } = require('@aws-sdk/client-s3');

const getConfig = require('../support/config');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = 'deletebucketpolicy-test-bucket';
const bucketPolicy = {
    Version: '2012-10-17',
    Statement: [{
        Sid: 'testid',
        Effect: 'Allow',
        Principal: '*',
        Action: 's3:putBucketPolicy',
        Resource: `arn:aws:s3:::${bucket}`,
    }],
};

// Check for the expected error response code and status code.
function assertError(err, expectedErr) {
    if (expectedErr === null) {
        assert.strictEqual(err, null, `expected no error but got '${err}'`);
    } else {
        assert.strictEqual(err.Code, expectedErr, 'incorrect error response ' +
            `code: should be '${expectedErr}' but got '${err.Code}'`);
        assert.strictEqual(err.$metadata.httpStatusCode, errors[expectedErr].code,
            'incorrect error status code: should be 400 but got ' +
            `'${err.$metadata.httpStatusCode}'`);
    }
}

describe('aws-sdk test delete bucket policy', () => {
    let s3;
    let otherAccountS3;

    before(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
        otherAccountS3 = new BucketUtility('lisa', {}).s3;
        return done();
    });

    it('should return NoSuchBucket error if bucket does not exist', async () => {
        try {
            await s3.send(new DeleteBucketPolicyCommand({ Bucket: bucket }));
            throw new Error('Expected NoSuchBucket error');
        } catch (err) {
            assertError(err, 'NoSuchBucket');
        }
    });

    describe('policy rules', () => {
        beforeEach(async () => {
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));
        });

        afterEach(async () => {
            await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
        });

        it('should return MethodNotAllowed if user is not bucket owner', async () => {
            try {
                await otherAccountS3.send(new DeleteBucketPolicyCommand({ Bucket: bucket }));
                throw new Error('Expected MethodNotAllowed error');
            } catch (err) {
                assertError(err, 'MethodNotAllowed');
            }
        });

        it('should return no error if no policy on bucket', async () => {
            await s3.send(new DeleteBucketPolicyCommand({ Bucket: bucket }));
            // Should not throw an error
        });

        it('should delete policy from bucket', async () => {
            const params = { Bucket: bucket, Policy: JSON.stringify(bucketPolicy) };
            
            // Put bucket policy
            await s3.send(new PutBucketPolicyCommand(params));
            
            // Delete bucket policy
            await s3.send(new DeleteBucketPolicyCommand({ Bucket: bucket }));
            
            // Try to get bucket policy (should fail)
            try {
                await s3.send(new GetBucketPolicyCommand({ Bucket: bucket }));
                throw new Error('Expected NoSuchBucketPolicy error');
            } catch (err) {
                assertError(err, 'NoSuchBucketPolicy');
            }
        });
    });
});
