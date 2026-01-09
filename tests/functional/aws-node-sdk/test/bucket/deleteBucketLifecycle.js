const assert = require('assert');
const { errors } = require('arsenal');
const { S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    DeleteBucketLifecycleCommand,
    PutBucketLifecycleConfigurationCommand,
    GetBucketLifecycleConfigurationCommand } = require('@aws-sdk/client-s3');

const getConfig = require('../support/config');

const bucket = 'lifecycledeletetestbucket';
const basicRule = {
    ID: 'test-id',
    Status: 'Enabled',
    Prefix: '',
    Expiration: {
        Days: 1,
    },
};

// Check for the expected error response code and status code.
function assertError(err, expectedErr) {
    if (expectedErr === null) {
        assert.strictEqual(err, null, `expected no error but got '${err}'`);
    } else {
        assert.strictEqual(err.name, expectedErr, 'incorrect error response ' +
            `code: should be '${expectedErr}' but got '${err.Code}'`);
        assert.strictEqual(err.$metadata.httpStatusCode, errors[expectedErr].code,
            'incorrect error status code: should be 400 but got ' +
            `'${err.$metadata.httpStatusCode}'`);
    }
}

describe('aws-sdk test delete bucket lifecycle', () => {
    let s3;
    let otherAccountS3;

    before(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
        const otherAccountConfig = getConfig('lisa', {});
        otherAccountS3 = new S3Client(otherAccountConfig);
        return done();
    });

    it('should return NoSuchBucket error if bucket does not exist', async () => {
        try {
            await s3.send(new DeleteBucketLifecycleCommand({ Bucket: bucket }));
            throw new Error('Expected NoSuchBucket error');
        } catch (err) {
            assertError(err, 'NoSuchBucket');
        }
    });

    describe('config rules', () => {
        beforeEach(() => s3.send(new CreateBucketCommand({ Bucket: bucket })));

        afterEach(() => s3.send(new DeleteBucketCommand({ Bucket: bucket })));

        it('should return AccessDenied if user is not bucket owner', async () => {
            try {
                await otherAccountS3.send(new DeleteBucketLifecycleCommand({ Bucket: bucket }));
                throw new Error('Expected AccessDenied error');
            } catch (err) {
                assertError(err, 'AccessDenied');
            }
        });

        it('should return no error if no lifecycle config on bucket', () => s3.send(new
            DeleteBucketLifecycleCommand({ Bucket: bucket })));

        it('should delete lifecycle configuration from bucket', async () => {
            const params = { Bucket: bucket,
                LifecycleConfiguration: { Rules: [basicRule] } };
            await s3.send(new PutBucketLifecycleConfigurationCommand(params));
            await s3.send(new DeleteBucketLifecycleCommand({ Bucket: bucket }));
            try {
                await s3.send(new GetBucketLifecycleConfigurationCommand({ Bucket: bucket }));
                throw new Error('Expected NoSuchLifecycleConfiguration error');
            } catch (err) {
                assertError(err, 'NoSuchLifecycleConfiguration');
            }
        });
    });
});
