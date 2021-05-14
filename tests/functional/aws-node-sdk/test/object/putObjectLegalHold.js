const assert = require('assert');

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
                VersionId: '000000000000',
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
