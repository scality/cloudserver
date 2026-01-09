const assert = require('assert');
const {
    CreateBucketCommand,
    PutObjectCommand,
    DeleteObjectCommand,
    PutObjectLegalHoldCommand,
    PutBucketPolicyCommand,
} = require('@aws-sdk/client-s3');
const { errorInstances } = require('arsenal');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const checkError = require('../../lib/utility/checkError');
const changeObjectLock = require('../../../../utilities/objectLock-util');

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

        beforeEach(async () => {
            await s3.send(new CreateBucketCommand({
                Bucket: bucket,
                ObjectLockEnabledForBucket: true,
            }));
            await s3.send(new CreateBucketCommand({ Bucket: unlockedBucket }));
            await s3.send(new PutObjectCommand({ Bucket: unlockedBucket, Key: key }));
            await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key }));
            const res = await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key }));
            versionId = res.VersionId;
        });

        afterEach(() => {
            process.stdout.write('Emptying and deleting buckets\n');
            return new Promise(resolve => {
                changeObjectLock([{ bucket, key, versionId }], '', () => {
                    resolve();
                });
            })
            .then(() => bucketUtil.empty(bucket, true))
            .then(() => bucketUtil.empty(unlockedBucket, true))
            .then(() => bucketUtil.deleteMany([bucket, unlockedBucket]));
        });

        it('should return AccessDenied putting legal hold with another account',
        done => {
            const params = createLegalHoldParams(bucket, key, 'ON');
            otherAccountS3.send(new PutObjectLegalHoldCommand(params)).then(() => {
                throw new Error('Expected AccessDenied error');
            }).catch(err => {
                checkError(err, 'AccessDenied', 403);
                done();
            });
        });

        it('should return NoSuchKey error if key does not exist', done => {
            const params = createLegalHoldParams(bucket, 'keynotexist', 'ON');
            s3.send(new PutObjectLegalHoldCommand(params)).then(() => {
                throw new Error('Expected NoSuchKey error');
            }).catch(err => {
                checkError(err, 'NoSuchKey', 404);
                done();
            });
        });

        it('should return NoSuchVersion error if version does not exist', done => {
            s3.send(new PutObjectLegalHoldCommand({
                Bucket: bucket,
                Key: key,
                VersionId: '012345678901234567890123456789012',
                LegalHold: mockLegalHold.on,
            })).then(() => {
                throw new Error('Expected NoSuchVersion error');
            }).catch(err => {
                checkError(err, 'NoSuchVersion', 404);
                done();
            });
        });

        it('should return InvalidRequest error putting legal hold to object ' +
        'in bucket with no object lock enabled', done => {
            const params = createLegalHoldParams(unlockedBucket, key, 'ON');
            s3.send(new PutObjectLegalHoldCommand(params)).then(() => {
                throw new Error('Expected InvalidRequest error');
            }).catch(err => {
                checkError(err, 'InvalidRequest', 400);
                done();
            });
        });

        it('should return MethodNotAllowed if object version is delete marker',
        done => {
            s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }))
            .then(() => {
                const params = createLegalHoldParams(bucket, key, 'ON');
                return s3.send(new PutObjectLegalHoldCommand(params));
            })
            .then(() => {
                throw new Error('Expected MethodNotAllowed error');
            })
            .catch(err => {
                checkError(err, 'MethodNotAllowed', 405);
                done();
            });
        });

        it('should put object legal hold ON', done => {
            const params = createLegalHoldParams(bucket, key, 'ON');
            s3.send(new PutObjectLegalHoldCommand(params)).then(() => {
                changeObjectLock([{ bucket, key, versionId }], '', done);
            });
        });


        it('should put object legal hold OFF', done => {
            const params = createLegalHoldParams(bucket, key, 'OFF');
            s3.send(new PutObjectLegalHoldCommand(params)).then(() => {
                 changeObjectLock([{ bucket, key, versionId }], '', done);
            });
        });

        it('should return error if request has empty or undefined Status', done => {
            const params = createLegalHoldParams(bucket, key, '');
            s3.send(new PutObjectLegalHoldCommand(params)).then(() => {
                throw new Error('Expected MalformedXML error');
            }).catch(err => {
                checkError(err, 'MalformedXML', 400);
                changeObjectLock([{ bucket, key, versionId }], '', done);
            });
        });

        it('should return error if request does not contain Status', done => {
            s3.send(new PutObjectLegalHoldCommand({
                Bucket: bucket,
                Key: key,
                LegalHold: {},
            })).then(() => {
                throw new Error('Expected MalformedXML error');
            }).catch(err => {
                checkError(err, 'MalformedXML', 400);
                changeObjectLock([{ bucket, key, versionId }], '', done);
            });
        });

        it('expects params.LegalHold.Status to be a string', done => {
            const params = createLegalHoldParams(bucket, key, true);
            s3.send(new PutObjectLegalHoldCommand(params)).then(() => {
                throw new Error('Expected InvalidParameterType error');
            }).catch(err => {
                checkError(err, 'MalformedXML', 400);
                changeObjectLock([{ bucket, key, versionId }], '', done);
            });
        });

        it('expects Status request xml must be one of "ON", "OFF"', done => {
            const params = createLegalHoldParams(bucket, key, 'on');
            s3.send(new PutObjectLegalHoldCommand(params)).then(() => {
                throw new Error('Expected MalformedXML error');
            }).catch(err => {
                checkError(err, 'MalformedXML', 400);
                changeObjectLock([{ bucket, key, versionId }], '', done);
            });
        });

        it('should support request with versionId parameter', done => {
            const params = createLegalHoldParams(bucket, key, 'ON', versionId);
            s3.send(new PutObjectLegalHoldCommand(params)).then(() => {
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
                const CommandClass = eval(operation);
                s3.send(new CommandClass(params))
                    .then(data => callback(null, data))
                    .catch(err => callback(err));
            } else {
                const unauthBucketUtil = new BucketUtility('default', sigCfg, true);
                const unauthS3 = unauthBucketUtil.s3;
                const CommandClass = eval(operation);
                unauthS3.send(new CommandClass(params))
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

        beforeEach(() => {
            process.stdout.write('Setting up bucket policy legal hold tests\n');
            return s3.send(new CreateBucketCommand({
                Bucket: testBucket,
                ObjectLockEnabledForBucket: true,
            }))
            .then(() => s3.send(new PutObjectCommand({ Bucket: testBucket, Key: key })))
            .then(res => {
                versionId = res.VersionId;
            })
            .catch(err => {
                process.stdout.write('Error in beforeEach\n');
                throw err;
            });
        });

        afterEach(async () => {
            await bucketUtil.empty(testBucket, true);
            await bucketUtil.deleteMany([testBucket]);
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
                    s3.send(new PutBucketPolicyCommand({
                        Bucket: testBucket,
                        Policy: JSON.stringify(bucketPolicy),
                    })).then(() => {
                        done();
                    }).catch(err => {
                        assert.ifError(err);
                        done();
                    });
                });

                if (testCase.expectedResult === 'allow') {
                    afterEach(() =>
                        s3.send(new PutObjectLegalHoldCommand({
                            Bucket: testBucket,
                            Key: key,
                            LegalHold: { Status: 'OFF' },
                        }))
                        .then(() => s3.send(new PutObjectLegalHoldCommand({
                            Bucket: testBucket,
                            Key: key,
                            VersionId: versionId,
                            LegalHold: { Status: 'OFF' },
                        })))
                    );
                }

                it(`should ${testCase.expectedResult} unauthenticated putObjectLegalHold without VersionId`, done => {
                    const params = {
                        Bucket: testBucket,
                        Key: key,
                        LegalHold: legalHoldConfig,
                    };
                    awsRequest(false, 'PutObjectLegalHoldCommand', params, testCase.callback(done));
                });

                it(`should ${testCase.expectedResult} unauthenticated putObjectLegalHold with VersionId`, done => {
                    const params = {
                        Bucket: testBucket,
                        Key: key,
                        LegalHold: legalHoldConfig,
                        VersionId: versionId,
                    };
                    awsRequest(false, 'PutObjectLegalHoldCommand', params, testCase.callback(done));
                });
            });
        });
    });
});
