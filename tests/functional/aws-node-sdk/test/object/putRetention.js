const assert = require('assert');
const moment = require('moment');
const { promisify } = require('util');
const {
    CreateBucketCommand,
    PutObjectCommand,
    DeleteObjectCommand,
    PutObjectRetentionCommand,
    PutBucketPolicyCommand,
} = require('@aws-sdk/client-s3');
const { errorInstances } = require('arsenal');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const checkError = require('../../lib/utility/checkError');
const changeObjectLock = require('../../../../utilities/objectLock-util');

const bucketName = 'lockenabledputbucket';
const unlockedBucket = 'locknotenabledputbucket';
const objectName = 'putobjectretentionobject';

const retentionConfig = {
    Mode: 'GOVERNANCE',
    RetainUntilDate: moment().add(1, 'd').add(123, 'ms').toDate(),
};

const changeObjectLockPromise = promisify(changeObjectLock);

describe('PUT object retention', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const otherAccountBucketUtility = new BucketUtility('lisa', {});
        const otherAccountS3 = otherAccountBucketUtility.s3;
        let versionId;

        beforeEach(async () => {
            await s3.send(
                new CreateBucketCommand({
                    Bucket: bucketName,
                    ObjectLockEnabledForBucket: true,
                }),
            );
            await s3.send(new CreateBucketCommand({ Bucket: unlockedBucket }));
            await s3.send(new PutObjectCommand({ Bucket: unlockedBucket, Key: objectName }));
            const putRes = await s3.send(new PutObjectCommand({ Bucket: bucketName, Key: objectName }));
            versionId = putRes.VersionId;
        });

        afterEach(async () => {
            await bucketUtil.empty(bucketName, true);
            await bucketUtil.empty(unlockedBucket, true);
            await bucketUtil.deleteMany([bucketName, unlockedBucket]);
        });

        it('should return AccessDenied putting retention with another account', async () => {
            try {
                await otherAccountS3.send(
                    new PutObjectRetentionCommand({
                        Bucket: bucketName,
                        Key: objectName,
                        Retention: retentionConfig,
                    }),
                );
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'AccessDenied', 403);
            }
        });

        it('should return NoSuchKey error if key does not exist', async () => {
            try {
                await s3.send(
                    new PutObjectRetentionCommand({
                        Bucket: bucketName,
                        Key: 'thiskeydoesnotexist',
                        Retention: retentionConfig,
                    }),
                );
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'NoSuchKey', 404);
            }
        });

        it('should return NoSuchVersion error if version does not exist', async () => {
            try {
                await s3.send(
                    new PutObjectRetentionCommand({
                        Bucket: bucketName,
                        Key: objectName,
                        VersionId: '012345678901234567890123456789012',
                        Retention: retentionConfig,
                    }),
                );
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'NoSuchVersion', 404);
            }
        });

        it(
            'should return InvalidRequest error putting retention to object in bucket with no object lock ' + 'enabled',
            async () => {
                try {
                    await s3.send(
                        new PutObjectRetentionCommand({
                            Bucket: unlockedBucket,
                            Key: objectName,
                            Retention: retentionConfig,
                        }),
                    );
                    assert.fail('Expected error');
                } catch (err) {
                    checkError(err, 'InvalidRequest', 400);
                }
            },
        );

        it('should return MethodNotAllowed if object version is delete marker', async () => {
            await s3.send(new DeleteObjectCommand({ Bucket: bucketName, Key: objectName }));
            try {
                await s3.send(
                    new PutObjectRetentionCommand({
                        Bucket: bucketName,
                        Key: objectName,
                        Retention: retentionConfig,
                    }),
                );
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'MethodNotAllowed', 405);
            }
        });

        it('should put object retention', async () => {
            await s3.send(
                new PutObjectRetentionCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    Retention: retentionConfig,
                }),
            );
            await changeObjectLockPromise([{ bucket: bucketName, key: objectName, versionId }], '');
        });

        it('should support request with versionId parameter', async () => {
            await s3.send(
                new PutObjectRetentionCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    Retention: retentionConfig,
                    VersionId: versionId,
                }),
            );
            await changeObjectLockPromise([{ bucket: bucketName, key: objectName, versionId }], '');
        });
    });
});

// Use bucket policy to test iam action for putObjectRetention with version id
// It used to need non standard s3:PutObjectVersionRetention action but was fixed
// by ARSN-297 ARTESCA-7107
describe('PUT object retention iam action and version id', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const testBucket = 'bucket-policy-retention-test';
        let versionId;

        function awsRequest(auth, operation, params, callback) {
            if (auth) {
                const CommandClass = eval(operation);
                s3.send(new CommandClass(params))
                    .then(data => callback(null, data))
                    .catch(err => callback(err));
            } else {
                const unauthBucketUtil = new BucketUtility('default', sigCfg, true);
                const unauthS3 = unauthBucketUtil.s3;
                const CommandClass = eval(operation);
                unauthS3
                    .send(new CommandClass(params))
                    .then(data => callback(null, data))
                    .catch(err => callback(err));
            }
        }

        function cbNoError(done) {
            return err => {
                assert.ifError(err);
                done();
            };
        }

        function cbWithError(done) {
            return err => {
                assert.strictEqual(err.$metadata.httpStatusCode, errorInstances.AccessDenied.code);
                done();
            };
        }

        beforeEach(async () => {
            await s3.send(
                new CreateBucketCommand({
                    Bucket: testBucket,
                    ObjectLockEnabledForBucket: true,
                }),
            );
            const res = await s3.send(new PutObjectCommand({ Bucket: testBucket, Key: objectName }));
            versionId = res.VersionId;
        });

        afterEach(async () => {
            await bucketUtil.empty(testBucket, true);
            await bucketUtil.deleteMany([testBucket]);
        });

        const policyTestCases = [
            {
                action: 's3:PutObjectRetention',
                expectedResult: 'allow',
                callback: cbNoError,
            },
            {
                action: 's3:PutObjectVersionRetention',
                expectedResult: 'deny',
                callback: cbWithError,
            },
        ];

        policyTestCases.forEach(testCase => {
            describe(`with ${testCase.action} policy`, () => {
                beforeEach(done => {
                    const statement = {
                        Sid: 'AllowRetention',
                        Effect: 'Allow',
                        Principal: '*',
                        Action: [testCase.action],
                        Resource: [`arn:aws:s3:::${testBucket}/*`],
                    };
                    const bucketPolicy = {
                        Version: '2012-10-17',
                        Statement: [statement],
                    };
                    s3.send(
                        new PutBucketPolicyCommand({
                            Bucket: testBucket,
                            Policy: JSON.stringify(bucketPolicy),
                        }),
                    )
                        .then(() => {
                            done();
                        })
                        .catch(err => {
                            assert.ifError(err);
                            done();
                        });
                });

                it(`should ${testCase.expectedResult} unauthenticated putObjectRetention without VersionId`, done => {
                    const params = {
                        Bucket: testBucket,
                        Key: objectName,
                        Retention: retentionConfig,
                    };
                    awsRequest(false, 'PutObjectRetentionCommand', params, testCase.callback(done));
                });

                it(`should ${testCase.expectedResult} unauthenticated putObjectRetention with VersionId`, done => {
                    const params = {
                        Bucket: testBucket,
                        Key: objectName,
                        Retention: retentionConfig,
                        VersionId: versionId,
                    };
                    awsRequest(false, 'PutObjectRetentionCommand', params, testCase.callback(done));
                });
            });
        });
    });
});
