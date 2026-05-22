const assert = require('assert');
const { errors } = require('arsenal');
const {
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    GetBucketLifecycleConfigurationCommand,
    PutBucketLifecycleConfigurationCommand,
} = require('@aws-sdk/client-s3');

const getConfig = require('../support/config');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = 'lifecycletestbucket';

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

describe('aws-sdk test get bucket lifecycle', () => {
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
            await s3.send(new GetBucketLifecycleConfigurationCommand({ Bucket: bucket }));
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
                await otherAccountS3.send(new GetBucketLifecycleConfigurationCommand({ Bucket: bucket }));
                throw new Error('Expected AccessDenied error');
            } catch (err) {
                assertError(err, 'AccessDenied');
            }
        });

        it('should return NoSuchLifecycleConfiguration error if no lifecycle ' + 'put to bucket', async () => {
            try {
                await s3.send(new GetBucketLifecycleConfigurationCommand({ Bucket: bucket }));
                throw new Error('Expected NoSuchLifecycleConfiguration error');
            } catch (err) {
                assertError(err, 'NoSuchLifecycleConfiguration');
            }
        });

        it('should get bucket lifecycle config with top-level prefix', async () => {
            await s3.send(
                new PutBucketLifecycleConfigurationCommand({
                    Bucket: bucket,
                    LifecycleConfiguration: {
                        Rules: [
                            {
                                ID: 'test-id',
                                Status: 'Enabled',
                                Prefix: '',
                                Expiration: { Days: 1 },
                            },
                        ],
                    },
                }),
            );
            const res = await s3.send(new GetBucketLifecycleConfigurationCommand({ Bucket: bucket }));
            assert.strictEqual(res.Rules.length, 1);
            assert.deepStrictEqual(res.Rules[0], {
                Expiration: { Days: 1 },
                ID: 'test-id',
                Prefix: '',
                Status: 'Enabled',
            });
        });

        it('should get bucket lifecycle config with filter prefix', async () => {
            await s3.send(
                new PutBucketLifecycleConfigurationCommand({
                    Bucket: bucket,
                    LifecycleConfiguration: {
                        Rules: [
                            {
                                ID: 'test-id',
                                Status: 'Enabled',
                                Filter: { Prefix: '' },
                                Expiration: { Days: 1 },
                            },
                        ],
                    },
                }),
            );
            const res = await s3.send(new GetBucketLifecycleConfigurationCommand({ Bucket: bucket }));
            assert.strictEqual(res.Rules.length, 1);
            assert.deepStrictEqual(res.Rules[0], {
                Expiration: { Days: 1 },
                ID: 'test-id',
                Filter: { Prefix: '' },
                Status: 'Enabled',
            });
        });

        it('should get bucket lifecycle config with filter prefix and tags', async () => {
            await s3.send(
                new PutBucketLifecycleConfigurationCommand({
                    Bucket: bucket,
                    LifecycleConfiguration: {
                        Rules: [
                            {
                                ID: 'test-id',
                                Status: 'Enabled',
                                Filter: {
                                    And: {
                                        Prefix: '',
                                        Tags: [
                                            {
                                                Key: 'key',
                                                Value: 'value',
                                            },
                                        ],
                                    },
                                },
                                Expiration: { Days: 1 },
                            },
                        ],
                    },
                }),
            );
            const res = await s3.send(new GetBucketLifecycleConfigurationCommand({ Bucket: bucket }));
            assert.strictEqual(res.Rules.length, 1);
            assert.deepStrictEqual(res.Rules[0], {
                Expiration: { Days: 1 },
                ID: 'test-id',
                Filter: {
                    And: {
                        Prefix: '',
                        Tags: [
                            {
                                Key: 'key',
                                Value: 'value',
                            },
                        ],
                    },
                },
                Status: 'Enabled',
            });
        });
    });
});
