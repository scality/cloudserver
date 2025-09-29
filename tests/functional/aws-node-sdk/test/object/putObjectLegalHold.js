const assert = require('assert');
const AWS = require('aws-sdk');
const { errorInstances } = require('arsenal');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const checkError = require('../../lib/utility/checkError');
const changeObjectLock = require('../../../../utilities/objectLock-util');
const { VALIDATE_CREDENTIALS, SIGN } = AWS.EventListeners.Core;

const bucket = 'mock-bucket-lock';
const unlockedBucket = 'mock-bucket-no-lock';
const key = 'mock-object';

const mockLegalHold = {
    empty: {},
    emptyStatus: { Status: '' },
    invalidType: { Status: false },
    invalidVal: { Status: 'active' },
    off: { Status: 'OFF' },
    on: { Status: 'ON' },
};

function createLegalHoldParams(bucket, key, status, versionId) {
    const params = { Bucket: bucket, Key: key };
    if (!status) {
        return params;
    }
    if (versionId) {
        Object.assign(params, {
            VersionId: versionId,
            LegalHold: {
                Status: status,
            },
        });
        return params;
    }
    Object.assign(params, {
        LegalHold: {
            Status: status,
        },
    });
    return params;
}

const isCEPH = process.env.CI_CEPH !== undefined;
const describeSkipIfCeph = isCEPH ? describe.skip : describe;

describeSkipIfCeph('PUT object legal hold', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const otherAccountBucketUtility = new BucketUtility('lisa', {});
        const otherAccountS3 = otherAccountBucketUtility.s3;
        let versionId;

        beforeEach(() => {
            process.stdout.write('Putting buckets and objects\n');
            return s3.createBucket({
                Bucket: bucket,
                ObjectLockEnabledForBucket: true,
            }).promise()
            .then(() => s3.createBucket({ Bucket: unlockedBucket }).promise())
            .then(() => s3.putObject({ Bucket: unlockedBucket, Key: key }).promise())
            .then(() => s3.putObject({ Bucket: bucket, Key: key }).promise())
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
            return bucketUtil.empty(bucket)
            .then(() => bucketUtil.empty(unlockedBucket))
            .then(() => bucketUtil.deleteMany([bucket, unlockedBucket]))
            .catch(err => {
                process.stdout.write('Error in afterEach\n');
                throw err;
            });
        });

        it('should return AccessDenied putting legal hold with another account',
        done => {
            const params = createLegalHoldParams(bucket, key, 'ON');
            otherAccountS3.putObjectLegalHold(params, err => {
                checkError(err, 'AccessDenied', 403);
                done();
            });
        });

        it('should return NoSuchKey error if key does not exist', done => {
            const params = createLegalHoldParams(bucket, 'keynotexist', 'ON');
            s3.putObjectLegalHold(params, err => {
                checkError(err, 'NoSuchKey', 404);
                done();
            });
        });

        it('should return NoSuchVersion error if version does not exist', done => {
            s3.putObjectLegalHold({
                Bucket: bucket,
                Key: key,
                VersionId: '012345678901234567890123456789012',
                LegalHold: mockLegalHold.on,
            }, err => {
                checkError(err, 'NoSuchVersion', 404);
                done();
            });
        });

        it('should return InvalidRequest error putting legal hold to object ' +
        'in bucket with no object lock enabled', done => {
            const params = createLegalHoldParams(unlockedBucket, key, 'ON');
            s3.putObjectLegalHold(params, err => {
                checkError(err, 'InvalidRequest', 400);
                done();
            });
        });

        it('should return MethodNotAllowed if object version is delete marker',
        done => {
            s3.deleteObject({ Bucket: bucket, Key: key }, err => {
                assert.ifError(err);
                const params = createLegalHoldParams(bucket, key, 'ON');
                s3.putObjectLegalHold(params, err => {
                    checkError(err, 'MethodNotAllowed', 405);
                    done();
                });
            });
        });

        it('should put object legal hold ON', done => {
            const params = createLegalHoldParams(bucket, key, 'ON');
            s3.putObjectLegalHold(params, err => {
                assert.ifError(err);
                changeObjectLock([{ bucket, key, versionId }], '', done);
            });
        });

        it('should put object legal hold OFF', done => {
            const params = createLegalHoldParams(bucket, key, 'OFF');
            s3.putObjectLegalHold(params, err => {
                assert.ifError(err);
                changeObjectLock([{ bucket, key, versionId }], '', done);
            });
        });

        it('should error if request has empty or undefined Status', done => {
            const params = createLegalHoldParams(bucket, key, '');
            s3.putObjectLegalHold(params, err => {
                checkError(err, 'MalformedXML', 400);
                changeObjectLock([{ bucket, key, versionId }], '', done);
            });
        });

        it('should return error if request does not contain Status', done => {
            s3.putObjectLegalHold({
                Bucket: bucket,
                Key: key,
                LegalHold: {},
            }, err => {
                checkError(err, 'MalformedXML', 400);
                changeObjectLock([{ bucket, key, versionId }], '', done);
            });
        });

        it('expects params.LegalHold.Status to be a string', done => {
            const params = createLegalHoldParams(bucket, key, true);
            s3.putObjectLegalHold(params, err => {
                checkError(err, 'InvalidParameterType');
                changeObjectLock([{ bucket, key, versionId }], '', done);
            });
        });

        it('expects Status request xml must be one of "ON", "OFF"', done => {
            const params = createLegalHoldParams(bucket, key, 'on');
            s3.putObjectLegalHold(params, err => {
                checkError(err, 'MalformedXML', 400);
                changeObjectLock([{ bucket, key, versionId }], '', done);
            });
        });

        it('should support request with versionId parameter', done => {
            const params = createLegalHoldParams(bucket, key, 'ON', versionId);
            s3.putObjectLegalHold(params, err => {
                assert.ifError(err);
                changeObjectLock([{ bucket, key, versionId }], '', done);
            });
        });
    });
});

// Use bucket policy to test iam action for putObjectLegalHold with version id
// It used to need non standard s3:PutObjectVersionLegalHold action but was fixed
// by ARSN-297 ARTESCA-7107
describeSkipIfCeph('PUT object legal hold iam action and version id', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const testBucket = 'bucket-policy-legalhold-test';
        let versionId;

        const legalHoldConfig = { Status: 'ON' };

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
            process.stdout.write('Setting up bucket policy legal hold tests\n');
            return s3.createBucket({
                Bucket: testBucket,
                ObjectLockEnabledForBucket: true,
            }).promise()
            .then(() => s3.putObject({ Bucket: testBucket, Key: key }).promise())
            .then(res => {
                versionId = res.VersionId;
            })
            .catch(err => {
                process.stdout.write('Error in beforeEach\n');
                throw err;
            });
        });

        afterEach(() => {
            process.stdout.write('Cleaning up bucket policy legal hold tests\n');
            return bucketUtil.empty(testBucket)
            .then(() => bucketUtil.deleteMany([testBucket]))
            .catch(err => {
                process.stdout.write('Error in afterEach\n');
                throw err;
            });
        });

        const policyTestCases = [
            {
                action: 's3:PutObjectLegalHold',
                expectedResult: 'allow',
                callback: cbNoError,
            },
            {
                action: 's3:PutObjectVersionLegalHold',
                expectedResult: 'deny',
                callback: cbWithError,
            },
        ];

        policyTestCases.forEach(testCase => {
            describe(`with ${testCase.action} policy`, () => {
                beforeEach(done => {
                    const statement = {
                        Sid: 'AllowLegalHold',
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

                if (testCase.expectedResult === 'allow') {
                    afterEach(() =>
                        s3.putObjectLegalHold({
                            Bucket: testBucket,
                            Key: key,
                            LegalHold: { Status: 'OFF' },
                        }).promise()
                        .then(() => s3.putObjectLegalHold({
                            Bucket: testBucket,
                            Key: key,
                            VersionId: versionId,
                            LegalHold: { Status: 'OFF' },
                        }).promise())
                    );
                }

                it(`should ${testCase.expectedResult} unauthenticated putObjectLegalHold without VersionId`, done => {
                    const params = {
                        Bucket: testBucket,
                        Key: key,
                        LegalHold: legalHoldConfig,
                    };
                    awsRequest(false, 'putObjectLegalHold', params, testCase.callback(done));
                });

                it(`should ${testCase.expectedResult} unauthenticated putObjectLegalHold with VersionId`, done => {
                    const params = {
                        Bucket: testBucket,
                        Key: key,
                        LegalHold: legalHoldConfig,
                        VersionId: versionId,
                    };
                    awsRequest(false, 'putObjectLegalHold', params, testCase.callback(done));
                });
            });
        });
    });
});
