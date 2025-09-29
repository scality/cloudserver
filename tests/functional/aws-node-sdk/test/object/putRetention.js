const assert = require('assert');
const moment = require('moment');
const AWS = require('aws-sdk');
const { errorInstances } = require('arsenal');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const checkError = require('../../lib/utility/checkError');
const changeObjectLock = require('../../../../utilities/objectLock-util');
const { VALIDATE_CREDENTIALS, SIGN } = AWS.EventListeners.Core;

const bucketName = 'lockenabledputbucket';
const unlockedBucket = 'locknotenabledputbucket';
const objectName = 'putobjectretentionobject';

const retentionConfig = {
    Mode: 'GOVERNANCE',
    RetainUntilDate: moment().add(1, 'd').add(123, 'ms').toISOString(),
};

const isCEPH = process.env.CI_CEPH !== undefined;
const describeSkipIfCeph = isCEPH ? describe.skip : describe;

describeSkipIfCeph('PUT object retention', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const otherAccountBucketUtility = new BucketUtility('lisa', {});
        const otherAccountS3 = otherAccountBucketUtility.s3;
        let versionId;

        beforeEach(() => {
            process.stdout.write('Putting buckets and objects\n');
            return s3.createBucket({
                Bucket: bucketName,
                ObjectLockEnabledForBucket: true,
            }).promise()
            .then(() => s3.createBucket({ Bucket: unlockedBucket }).promise())
            .then(() => s3.putObject({ Bucket: unlockedBucket, Key: objectName }).promise())
            .then(() => s3.putObject({ Bucket: bucketName, Key: objectName }).promise())
            .then(res => {
                versionId = res.VersionId;
            })
            .catch(err => {
                process.stdout.write('Error in beforeEach\n');
                throw err;
            });
        });

        afterEach(() => {
            process.stdout.write('Emptying and deleting buckets\n');
            return bucketUtil.empty(bucketName)
            .then(() => bucketUtil.empty(unlockedBucket))
            .then(() => bucketUtil.deleteMany([bucketName, unlockedBucket]))
            .catch(err => {
                process.stdout.write('Error in afterEach\n');
                throw err;
            });
        });

        it('should return AccessDenied putting retention with another account',
        done => {
            otherAccountS3.putObjectRetention({
                Bucket: bucketName,
                Key: objectName,
                Retention: retentionConfig,
            }, err => {
                checkError(err, 'AccessDenied', 403);
                done();
            });
        });

        it('should return NoSuchKey error if key does not exist', done => {
            s3.putObjectRetention({
                Bucket: bucketName,
                Key: 'thiskeydoesnotexist',
                Retention: retentionConfig,
            }, err => {
                checkError(err, 'NoSuchKey', 404);
                done();
            });
        });

        it('should return NoSuchVersion error if version does not exist', done => {
            s3.putObjectRetention({
                Bucket: bucketName,
                Key: objectName,
                VersionId: '012345678901234567890123456789012',
                Retention: retentionConfig,
            }, err => {
                checkError(err, 'NoSuchVersion', 404);
                done();
            });
        });

        it('should return InvalidRequest error putting retention to object ' +
        'in bucket with no object lock enabled', done => {
            s3.putObjectRetention({
                Bucket: unlockedBucket,
                Key: objectName,
                Retention: retentionConfig,
            }, err => {
                checkError(err, 'InvalidRequest', 400);
                done();
            });
        });

        it('should return MethodNotAllowed if object version is delete marker',
        done => {
            s3.deleteObject({ Bucket: bucketName, Key: objectName }, err => {
                assert.ifError(err);
                s3.putObjectRetention({
                    Bucket: bucketName,
                    Key: objectName,
                    Retention: retentionConfig,
                }, err => {
                    checkError(err, 'MethodNotAllowed', 405);
                    done();
                });
            });
        });

        it('should put object retention', done => {
            s3.putObjectRetention({
                Bucket: bucketName,
                Key: objectName,
                Retention: retentionConfig,
            }, err => {
                assert.ifError(err);
                changeObjectLock([
                    { bucket: bucketName, key: objectName, versionId }], '', done);
            });
        });

        it('should support request with versionId parameter', done => {
            s3.putObjectRetention({
                Bucket: bucketName,
                Key: objectName,
                Retention: retentionConfig,
                VersionId: versionId,
            }, err => {
                assert.ifError(err);
                changeObjectLock([
                    { bucket: bucketName, key: objectName, versionId },
                ], '', done);
            });
        });
    });
});

// Use bucket policy to test iam action for putObjectRetention with version id
// It used to need non standard s3:PutObjectVersionRetention action but was fixed
// by ARSN-297 ARTESCA-7107
describeSkipIfCeph('PUT object retention iam action and version id', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const testBucket = 'bucket-policy-retention-test';
        let versionId;

        function awsRequest(auth, operation, params, callback) {
            if (auth) {
                bucketUtil.s3[operation](params, callback);
            } else {
                const unauthBucketUtil = new BucketUtility('default', sigCfg);
                const request = unauthBucketUtil.s3[operation](params);
                request.removeListener('validate', VALIDATE_CREDENTIALS);
                request.removeListener('sign', SIGN);
                request.send(callback);
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
                assert.strictEqual(err.statusCode, errorInstances.AccessDenied.code);
                done();
            };
        }

        beforeEach(() => {
            process.stdout.write('Setting up bucket policy retention tests\n');
            return s3.createBucket({
                Bucket: testBucket,
                ObjectLockEnabledForBucket: true,
            }).promise()
            .then(() => s3.putObject({ Bucket: testBucket, Key: objectName }).promise())
            .then(res => {
                versionId = res.VersionId;
            })
            .catch(err => {
                process.stdout.write('Error in beforeEach\n');
                throw err;
            });
        });

        afterEach(() => {
            process.stdout.write('Cleaning up bucket policy retention tests\n');
            return bucketUtil.empty(testBucket, true)
            .then(() => bucketUtil.deleteMany([testBucket]))
            .catch(err => {
                process.stdout.write('Error in afterEach\n');
                throw err;
            });
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
                    s3.putBucketPolicy({
                        Bucket: testBucket,
                        Policy: JSON.stringify(bucketPolicy),
                    }, err => {
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
                    awsRequest(false, 'putObjectRetention', params, testCase.callback(done));
                });

                it(`should ${testCase.expectedResult} unauthenticated putObjectRetention with VersionId`, done => {
                    const params = {
                        Bucket: testBucket,
                        Key: objectName,
                        Retention: retentionConfig,
                        VersionId: versionId,
                    };
                    awsRequest(false, 'putObjectRetention', params, testCase.callback(done));
                });
            });
        });
    });
});
