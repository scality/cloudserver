const assert = require('assert');
const { errors } = require('arsenal');
const { S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketPolicyCommand } = require('@aws-sdk/client-s3');

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
function assertError(err, expectedErr) {
    if (expectedErr === null) {
        assert.strictEqual(err, null, `expected no error but got '${err}'`);
    } else {
        assert.strictEqual(err.name, expectedErr, 'incorrect error response ' +
            `code: should be '${expectedErr}' but got '${err.name}'`);
        assert.strictEqual(err.$metadata.httpStatusCode, errors[expectedErr].code,
            'incorrect error status code: should be  ' +
            `${errors[expectedErr].code}, but got '${err.$metadata.httpStatusCode}'`);
    }
}


describe('aws-sdk test put bucket policy', () => {
    let s3;
    let otherAccountS3;

    before(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
        otherAccountS3 = new BucketUtility('lisa', {}).s3;
    });

    it('should return NoSuchBucket error if bucket does not exist', async () => {
        const params = getPolicyParams();
        try {
            await s3.send(new PutBucketPolicyCommand(params));
            throw new Error('Expected NoSuchBucket error');
        } catch (err) {
            assertError(err, 'NoSuchBucket');
        }
    });

    describe('config rules', () => {
        beforeEach(() => s3.send(new CreateBucketCommand({ Bucket: bucket })));

        afterEach(() => s3.send(new DeleteBucketCommand({ Bucket: bucket })));

        it('should return MethodNotAllowed if user is not bucket owner', async () => {
            const params = getPolicyParams();
            try {
                await otherAccountS3.send(new PutBucketPolicyCommand(params));
                throw new Error('Expected MethodNotAllowed error');
            } catch (err) {
                assertError(err, 'MethodNotAllowed');
            }
        });

        it('should put a bucket policy on bucket', async () => {
            const params = getPolicyParams();
            await s3.send(new PutBucketPolicyCommand(params));
        });

        it('should not allow bucket policy with no Action', async () => {
            const params = getPolicyParams({ key: 'Action', value: '' });
            try {
                await s3.send(new PutBucketPolicyCommand(params));
                throw new Error('Expected MalformedPolicy error');
            } catch (err) {
                assertError(err, 'MalformedPolicy');
            }
        });

        it('should not allow bucket policy with no Effect', async () => {
            const params = getPolicyParams({ key: 'Effect', value: '' });
            try {
                await s3.send(new PutBucketPolicyCommand(params));
                throw new Error('Expected MalformedPolicy error');
            } catch (err) {
                assertError(err, 'MalformedPolicy');
            }
        });

        it('should not allow bucket policy with no Resource', async () => {
            const params = getPolicyParams({ key: 'Resource', value: '' });
            try {
                await s3.send(new PutBucketPolicyCommand(params));
                throw new Error('Expected MalformedPolicy error');
            } catch (err) {
                assertError(err, 'MalformedPolicy');
            }
        });

        it('should not allow bucket policy with no Principal', async () => {
            const params = getPolicyParams({ key: 'Principal', value: '' });
            try {
                await s3.send(new PutBucketPolicyCommand(params));
                throw new Error('Expected MalformedPolicy error');
            } catch (err) {
                assertError(err, 'MalformedPolicy');
            }
        });

        it('should return MalformedPolicy because Id is not a string', async () => {
            const params = getPolicyParamsWithId(null, 59);
            try {
                await s3.send(new PutBucketPolicyCommand(params));
                throw new Error('Expected MalformedPolicy error');
            } catch (err) {
                assertError(err, 'MalformedPolicy');
            }
        });

        it('should put a bucket policy on bucket since Id is a string', async () => {
            const params = getPolicyParamsWithId(null, 'cd3ad3d9-2776-4ef1-a904-4c229d1642e');
            await s3.send(new PutBucketPolicyCommand(params));
        });

        it('should allow bucket policy with pincipal arn less than 2048 characters', async () => {
            const params = getPolicyParams({ key: 'Principal', value: { AWS: `arn:aws:iam::767707094035:user/${generateRandomString(150)}` } }); // eslint-disable-line max-len
            await s3.send(new PutBucketPolicyCommand(params));
        });

        it('should not allow bucket policy with pincipal arn more than 2048 characters', async () => {
            const params = getPolicyParams({ key: 'Principal', value: { AWS: `arn:aws:iam::767707094035:user/${generateRandomString(2020)}` } }); // eslint-disable-line max-len
            try {
                await s3.send(new PutBucketPolicyCommand(params));
                throw new Error('Expected MalformedPolicy error');
            } catch (err) {
                assertError(err, 'MalformedPolicy');
            }
        });

        it('should allow bucket policy with valid SourceIp condition', async () => {
            const params = getPolicyParams({
                key: 'Condition', value: {
                    IpAddress: {
                        'aws:SourceIp': '192.168.100.0/24',
                    },
                },
            });
            await s3.send(new PutBucketPolicyCommand(params));
        });

        it('should not allow bucket policy with invalid SourceIp format', async () => {
            const params = getPolicyParams({
                key: 'Condition', value: {
                    IpAddress: {
                        'aws:SourceIp': '192.168.100', // Invalid IP format
                    },
                },
            });
            try {
                await s3.send(new PutBucketPolicyCommand(params));
                throw new Error('Expected MalformedPolicy error');
            } catch (err) {
                assertError(err, 'MalformedPolicy');
            }
        });

        it('should allow bucket policy with valid s3:object-lock-remaining-retention-days condition', async () => {
            const params = getPolicyParams({
                key: 'Condition', value: {
                    NumericGreaterThanEquals: {
                        's3:object-lock-remaining-retention-days': '30',
                    },
                },
            });
            await s3.send(new PutBucketPolicyCommand(params));
        });

        // yep, this is the expected behaviour
        it('should not reject policy with invalid s3:object-lock-remaining-retention-days value', async () => {
            const params = getPolicyParams({
                key: 'Condition', value: {
                    NumericGreaterThanEquals: {
                        's3:object-lock-remaining-retention-days': '-1', // Invalid value
                    },
                },
            });
            await s3.send(new PutBucketPolicyCommand(params));
        });

        // this too ¯\_(ツ)_/¯
        it('should not reject policy with a key starting with aws:', async () => {
            const params = getPolicyParams({
                key: 'Condition', value: {
                    NumericGreaterThanEquals: {
                        'aws:have-a-nice-day': 'blabla', // Invalid value
                    },
                },
            });
            await s3.send(new PutBucketPolicyCommand(params));
        });

        it('should reject policy with a key that does not exist that does not start with aws:', async () => {
            const params = getPolicyParams({
                key: 'Condition', value: {
                    NumericGreaterThanEquals: {
                        'have-a-nice-day': 'blabla', // Invalid value
                    },
                },
            });
            try {
                await s3.send(new PutBucketPolicyCommand(params));
                throw new Error('Expected MalformedPolicy error');
            } catch (err) {
                assertError(err, 'MalformedPolicy');
            }
        });

        it('should enforce policies with both SourceIp and s3:object-lock conditions together', async () => {
            const params = getPolicyParams({
                key: 'Condition', value: {
                    IpAddress: {
                        'aws:SourceIp': '192.168.100.0/24',
                    },
                    NumericGreaterThanEquals: {
                        's3:object-lock-remaining-retention-days': '30',
                    },
                },
            });
            await s3.send(new PutBucketPolicyCommand(params));
        });

        it('should return error if a condition one of the condition values is invalid', async () => {
            const params = getPolicyParams({
                key: 'Condition', value: {
                    IpAddress: {
                        'aws:SourceIp': '192.168.100',
                    },
                    NumericGreaterThanEquals: {
                        's3:object-lock-remaining-retention-days': '30',
                    },
                },
            });
            try {
                await s3.send(new PutBucketPolicyCommand(params));
                throw new Error('Expected MalformedPolicy error');
            } catch (err) {
                assertError(err, 'MalformedPolicy');
            }
        });
    });
});
