const { S3 } = require('aws-sdk');

const getConfig = require('../support/config');
const BucketUtility = require('../../lib/utility/bucket-util');
const assertError = require('../../lib/utility/assertError');

const bucket = 'objectlockputtestbucket';
const basicConfig = {
    Mode: 'GOVERNANCE',
    Days: 1,
};

function getObjectLockParams(paramToChange) {
    const newParam = {};
    const objectLockConfig = {
        ObjectLockEnabled: 'Enabled',
        Rule: {
            DefaultRetention: basicConfig,
        },
    };
    if (paramToChange) {
        if (paramToChange.key === 'DefaultRetention') {
            objectLockConfig.Rule.DefaultRetention = paramToChange.value;
        } else if (paramToChange.key === 'Rule') {
            objectLockConfig.Rule = paramToChange.value;
        } else {
            newParam[paramToChange.key] = paramToChange.value;
            objectLockConfig.Rule.DefaultRetention = Object.assign(
                {}, basicConfig, newParam);
        }
    }
    return {
        Bucket: bucket,
        ObjectLockConfiguration: objectLockConfig,
    };
}

describe('aws-sdk test put bucket object lock', () => {
    let s3;
    let otherAccountS3;

    before(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        otherAccountS3 = new BucketUtility('lisa', {}).s3;
        return done();
    });

    it('should return NoSuchBucket error if bucket does not exist', done => {
        const params = getObjectLockParams();
        s3.putObjectLockConfiguration(params, err =>
            assertError(err, 'NoSuchBucket', done));
    });

    describe('without object lock enabled', () => {
        beforeEach(done => s3.createBucket({ Bucket: bucket }, done));

        it('should return InvalidBucketState', done => {
            const params = getObjectLockParams();
            s3.putObjectLockConfiguration(params, err =>
                assertError(err, 'InvalidBucketState', done));
        });
    });

    describe('config rules', () => {
        beforeEach(done => s3.createBucket({
            Bucket: bucket,
            ObjectLockEnabledForBucket: true,
        }, done));

        afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

        it('should return AccessDenied if user is not bucket owner', done => {
            const params = getObjectLockParams();
            otherAccountS3.putObjectLockConfiguration(params,
                err => assertError(err, 'AccessDenied', done));
        });

        it('should put object lock configuration on bucket', done => {
            const params = getObjectLockParams();
            s3.putObjectLockConfiguration(params, err =>
                assertError(err, null, done));
        });

        it('should not allow object lock config with empty Rule', done => {
            const params = getObjectLockParams({ key: 'Rule', value: {} });
            s3.putObjectLockConfiguration(params, err =>
                assertError(err, 'MalformedXML', done));
        });

        it('should not allow object lock config with no DefaultRetention',
        done => {
            const params = getObjectLockParams(
                { key: 'DefaultRetention', value: {} });
            s3.putObjectLockConfiguration(params, err =>
                assertError(err, 'MalformedXML', done));
        });

        it('should not allow object lock config with empty Mode', done => {
            const params = getObjectLockParams({ key: 'Mode', value: '' });
            s3.putObjectLockConfiguration(params, err =>
                assertError(err, 'MalformedXML', done));
        });

        it('should not allow object lock config with invalid Mode',
        done => {
            const params =
                getObjectLockParams({ key: 'Mode', value: 'GOVERPLIANCE' });
            s3.putObjectLockConfiguration(params, err =>
                assertError(err, 'InvalidArgument', done));
        });

        it('should not allow object lock config with empty Days', done => {
            const params = getObjectLockParams({ key: 'Days', value: '' });
            s3.putObjectLockConfiguration(params, err =>
                assertError(err, 'MalformedXML', done));
        });

        it('should not allow object lock config with 0 Days', done => {
            const params = getObjectLockParams({ key: 'Days', value: 0 });
            s3.putObjectLockConfiguration(params, err =>
                assertError(err, 'MalformedXML', done));
        });

        it('should not allow object lock config with invalid Days', done => {
            const params = getObjectLockParams({ key: 'Days', value: 'one' });
            s3.putObjectLockConfiguration(params, err =>
                assertError(err, 'MalformedXML', done));
        });

        it('should not allow object lock config with both Days and Years',
        done => {
            const params = getObjectLockParams({
                key: 'DefaultRetention',
                value: { Mode: 'GOVERNANCE', Days: 1, Years: 1 },
            });
            s3.putObjectLockConfiguration(params, err =>
                assertError(err, 'MalformedXML', done));
        });
    });
});
