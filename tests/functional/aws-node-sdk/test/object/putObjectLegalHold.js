const assert = require('assert');
const { errors } = require('arsenal');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const removeObjectLock = require('../../lib/utility/objectLock-util');

const bucket = 'mock-bucket-with-lock';
const key = 'mock-object';

// Check for the expected error response code and status code.
function assertError(err, expectedErr, cb) {
    if (expectedErr === null) {
        assert.strictEqual(err, null, `expected no error but got '${err}'`);
    } else {
        if (typeof err === 'string') {
            assert.strictEqual(err, expectedErr, 'incorrect error: ' +
            `should be '${expectedErr}' but got '${err}'`);
        } else {
            assert.strictEqual(err.code, expectedErr, 'incorrect error: ' +
                `should be '${expectedErr}' but got '${err.code}'`);
            assert.strictEqual(err.statusCode, errors[expectedErr].code,
                'incorrect error status code: should be 400 but got ' +
                `'${err.statusCode}'`);
        }
    }
    return cb();
}

function createLegalHoldParams(status) {
    if (!status) {
        return { Bucket: bucket };
    }
    return {
        Bucket: bucket,
        Key: key,
        LegalHold: {
            Status: status,
        },
    };
}

describe('aws-sdk put object legal hold', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const otherAccountBucketUtility = new BucketUtility('lisa', {});
        const otherAccountS3 = otherAccountBucketUtility.s3;
        let versionId;

        before(done => {
            s3.createBucket({
                Bucket: bucket,
                ObjectLockEnabledForBucket: true
            }, done);
        });

        beforeEach(done => {
            const params = {
                Bucket: bucket,
                Key: key,
            };
            s3.putObject(params, (err, res) => {
                s3.getObject({ Bucket: bucket, Key: key }, (err, res) => {
                    versionId = res.VersionId;
                });
                done();
            });
        });

        afterEach(done => {
            s3.listObjects({ Bucket: bucket }, (err, res) => {
                res.Contents.forEach(obj => {
                    s3.deleteObject({ Bucket: bucket, Key: obj.Key }, err => {
                        if (err) {
                            return done(err);
                        }
                        done();
                    });
                });
            });
        });

        after(() => s3.deleteBucket({ Bucket: bucket }));

        it('should return NoSuchBucket error if bucket does not exist',
            done => {
                const params = {
                    Bucket: 'non-existant-bucket',
                    Key: key,
                    LegalHold: {
                        Status: 'ON',
                    },
                };
                s3.putObjectLegalHold(params, err =>
                    assertError(err, 'NoSuchBucket', done));
            });

        it('should return AccessDenied if the user is not bucket owner',
            done => {
                const params = createLegalHoldParams('ON');
                otherAccountS3.putObjectLegalHold(params, err =>
                    assertError(err, 'AccessDenied', done));
            });

        const validStatuses = ['ON', 'OFF'];
        validStatuses.forEach(status => {
            it(`should successfully put legal hold status ${status}`, done => {
                const params = createLegalHoldParams(`${status}`);
                s3.putObjectLegalHold(params, err => {
                    if (err) {
                        done(err);
                    }
                    const { Bucket, Key } = params;
                    s3.getObjectLegalHold({ Bucket, Key }, (err, res) => {
                        assert.ifError(err);
                        assert.deepStrictEqual(res,
                            { LegalHold: { Status: `${status}` } });
                        const objectWithLock = [
                            {
                                bucket: bucket,
                                key: key,
                                versionId,
                            },
                        ];
                        removeObjectLock(objectWithLock, done);
                    });
                });
            });
        });

        it('should not allow object legal hold with empty Status', done => {
            const params = {
                Bucket: bucket,
                Key: key,
                LegalHold: {
                    Status: '',
                },
            };
            s3.putObjectLegalHold(params, err =>
                assertError(err, 'MalformedXML', done));
        });

        it('should not allow empty LegalHold', done => {
            const params = {
                Bucket: bucket,
                Key: key,
                LegalHold: {},
            };
            s3.putObjectLegalHold(params, err =>
                assertError(err, 'MalformedXML', done));
        });

        const invalidStatuses = ['On', 'On', 'off', 'active', true, false];
        invalidStatuses.forEach(status => {
            it('should not allow invalid legal hold status "On"', done => {
                const params = createLegalHoldParams(`${status}`);
                s3.putObjectLegalHold(params, err =>
                    assertError(err, 'MalformedXML', done));
            });
        })
    });
});
