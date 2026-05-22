const assert = require('assert');
const { errors } = require('arsenal');
const {
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    GetBucketPolicyCommand,
    PutBucketPolicyCommand,
} = require('@aws-sdk/client-s3');

const getConfig = require('../support/config');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = 'getbucketpolicy-testbucket';
const bucketPolicy = {
    Version: '2012-10-17',
    Statement: [
        {
            Sid: 'testid',
            Effect: 'Allow',
            Principal: '*',
            Action: 's3:putBucketPolicy',
            Resource: `arn:aws:s3:::${bucket}`,
        },
    ],
};
const expectedPolicy = {
    Sid: 'testid',
    Effect: 'Allow',
    Principal: '*',
    Action: 's3:putBucketPolicy',
    Resource: `arn:aws:s3:::${bucket}`,
};

function assertError(err, expectedErr) {
    if (expectedErr === null) {
        assert.strictEqual(err, null, `expected no error but got '${err}'`);
    } else {
        assert.strictEqual(
            err.name,
            expectedErr,
            'incorrect error response ' + `code: should be '${expectedErr}' but got '${err.name}'`,
        );
        assert.strictEqual(
            err.$metadata.httpStatusCode,
            errors[expectedErr].code,
            'incorrect error status code: should be 400 but got ' + `'${err.$metadata.httpStatusCode}'`,
        );
    }
}

describe('aws-sdk test get bucket policy', () => {
    const config = getConfig('default', { signatureVersion: 'v4' });
    const s3 = new S3Client(config);
    const otherAccountS3 = new BucketUtility('lisa', {}).s3;

    it('should return NoSuchBucket error if bucket does not exist', async () => {
        try {
            await s3.send(new GetBucketPolicyCommand({ Bucket: bucket }));
            throw new Error('Expected NoSuchBucket error');
        } catch (err) {
            assertError(err, 'NoSuchBucket');
        }
    });

    describe('policy rules', () => {
        beforeEach(() => s3.send(new CreateBucketCommand({ Bucket: bucket })));

        afterEach(() => s3.send(new DeleteBucketCommand({ Bucket: bucket })));

        it('should return MethodNotAllowed if user is not bucket owner', async () => {
            try {
                await otherAccountS3.send(new GetBucketPolicyCommand({ Bucket: bucket }));
                throw new Error('Expected MethodNotAllowed error');
            } catch (err) {
                assertError(err, 'MethodNotAllowed');
            }
        });

        it('should return NoSuchBucketPolicy error if no policy put to bucket', async () => {
            try {
                await s3.send(new GetBucketPolicyCommand({ Bucket: bucket }));
                throw new Error('Expected NoSuchBucketPolicy error');
            } catch (err) {
                assertError(err, 'NoSuchBucketPolicy');
            }
        });

        it('should get bucket policy', async () => {
            await s3.send(
                new PutBucketPolicyCommand({
                    Bucket: bucket,
                    Policy: JSON.stringify(bucketPolicy),
                }),
            );
            const res = await s3.send(new GetBucketPolicyCommand({ Bucket: bucket }));
            const parsedRes = JSON.parse(res.Policy);
            assert.deepStrictEqual(parsedRes.Statement[0], expectedPolicy);
        });
    });
});
