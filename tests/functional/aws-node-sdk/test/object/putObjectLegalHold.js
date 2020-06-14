const assert = require('assert');
const { errors } = require('arsenal');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = 'object-lock-test-bucket';

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

function getParams(status) {
    if (!status) {
        return { Bucket: bucket };
    }
    return {
        Bucket: bucket,
        Key: 'key',
        LegalHold: {
            Status: status
        },
    };
}

describe('aws-sdk put object legal hold', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const otherAccountBucketUtility = new BucketUtility('lisa', {});
        const otherAccountS3 = otherAccountBucketUtility.s3;

        beforeEach(done => s3.createBucket({
            Bucket: bucket,
            ObjectLockEnabledForBucket: true
        }, err => {
            if (err) {
                return done(err);
            }
            return s3.putObject({ Bucket: bucket, Key: 'key' }, done);
        }));

        afterEach(() => {
            process.stdout.write('Emptying bucket');
            return bucketUtil.empty(bucket)
            .then(() => {
                process.stdout.write('Deleting bucket');
                return bucketUtil.deleteOne(bucket);
            })
            .catch(err => {
                process.stdout.write('Error in afterEach');
                throw err;
            });
        });

        it('should return NoSuchBucket error if bucket does not exist', done => {
            const params = {
                Bucket: 'non-existant-bucket',
                Key: 'key',
                LegalHold: {
                    Status: 'ON'
                },
            };
            s3.putObjectLegalHold(params, err =>
                assertError(err, 'NoSuchBucket', done));
        });

        it('should return AccessDenied if the user is not bucket owner', done => {
            const params = getParams('ON');
            otherAccountS3.putObjectLegalHold(params,
                err => assertError(err, 'AccessDenied', done));
        });

        it('should put legal hold on object if legal hold status ON', done => {
            const params = getParams('ON');
            s3.putObjectLegalHold(params, err => assertError(err, null, done));
        });

        it('should put legal hold on object if legal hold status ON', done => {
            const params = getParams('ON');
            s3.putObjectLegalHold(params, err => {
                if (err) {
                    return done(err);
                }
                const { Bucket, Key } = params;
                return s3.getObjectLegalHold({ Bucket: Bucket, Key: Key },
                    (err, res) => {
                        assert.deepStrictEqual(res, { LegalHold: { Status: 'ON' } });
                        assertError(err, null, done);
                    });
            });
        });

        it('should remove legal hold on object if legal hold status OFF', done => {
            const params = getParams('OFF');
            s3.putObjectLegalHold(params, err => assertError(err, null, done));
        });

        it('should put legal hold on object if legal hold status OFF', done => {
            const params = getParams('OFF');
            s3.putObjectLegalHold(params, err => {
                if (err) {
                    return done(err);
                }
                const { Bucket, Key } = params;
                return s3.getObjectLegalHold({ Bucket: Bucket, Key: Key },
                    (err, res) => {
                        assert.deepStrictEqual(res, { LegalHold: { Status: 'OFF' } });
                        assertError(err, null, done);
                    });
            });
        });

        it('should not allow object legal hold with empty Status', done => {
            const params = { Bucket: bucket, Key: 'key', LegalHold: { Status: '' } };
            s3.putObjectLegalHold(params, err =>
                assertError(err, 'MalformedXML', done));
        });

        it('should not allow object legal hold with empty LegalHold', done => {
            const params = { Bucket: bucket, Key: 'key', LegalHold: {} };
            s3.putObjectLegalHold(params, err =>
                assertError(err, 'MalformedXML', done));
        });

        it('should not allow invalid legal hold status true', done => {
            const params = {
                Bucket: bucket,
                Key: 'key',
                LegalHold: {
                    Status: true
                },
            };
            s3.putObjectLegalHold(params, err =>
                assertError(err.code, 'InvalidParameterType', done));
        });

        it('should not allow invalid legal hold status false', done => {
            const params = {
                Bucket: bucket,
                Key: 'key',
                LegalHold: {
                    Status: false
                },
            };
            s3.putObjectLegalHold(params, err =>
                assertError(err.code, 'InvalidParameterType', done));
        });

        it('should not allow invalid legal hold status "On"', done => {
            const params = getParams('On');
            s3.putObjectLegalHold(params, err =>
                assertError(err, 'MalformedXML', done));
        });

        it('should not allow invalid legal hold status "on"', done => {
            const params = getParams('on');
            s3.putObjectLegalHold(params, err =>
                assertError(err, 'MalformedXML', done));
        });

        it('should not allow invalid legal hold status "Off"', done => {
            const params = getParams('Off');
            s3.putObjectLegalHold(params, err =>
                assertError(err, 'MalformedXML', done));
        });

        it('should not allow invalid legal hold status "off"', done => {
            const params = getParams('off');
            s3.putObjectLegalHold(params, err =>
                assertError(err, 'MalformedXML', done));
        });
    });
});
