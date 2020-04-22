const assert = require('assert');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutObjectLock = require('../../../lib/api/bucketPutObjectLock');
const { cleanup,
    DummyRequestLogger,
    makeAuthInfo,
} = require('../helpers');
const metadata = require('../../../lib/metadata/wrapper');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketputobjectlockbucket';
const bucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
};

const objectLockXml = '<ObjectLockConfiguration ' +
    'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
    '<ObjectLockEnabled>Enabled</ObjectLockEnabled>' +
    '<Rule><DefaultRetention>' +
    '<Mode>GOVERNANCE</Mode>' +
    '<Days>1</Days>' +
    '</DefaultRetention></Rule>' +
    '</ObjectLockConfiguration>';

const putObjLockRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    post: objectLockXml,
};

const expectedObjectLockConfig = {
    rule: {
        mode: 'GOVERNANCE',
        days: 1,
    },
};

describe('putBucketObjectLock API', () => {
    before(() => cleanup());

    describe('without Object Lock enabled on bucket', () => {
        beforeEach(done => bucketPut(authInfo, bucketPutRequest, log, done));
        afterEach(() => cleanup());

        it('should return Invalid State error', done => {
            bucketPutObjectLock(authInfo, putObjLockRequest, log, err => {
                assert.strictEqual(err.InvalidState, true);
                done();
            });
        });
    });

    describe('with Object Lock enabled on bucket', () => {
        const bucketObjLockRequest = Object.assign({}, bucketPutRequest);
        bucketObjLockRequest.headers['object-lock-enabled'] = true;

        beforeEach(done => bucketPut(authInfo, bucketObjLockRequest, log, done));
        afterEach(() => cleanup());

        it('should update a bucket\'s metadata with object lock config', done => {
            bucketPutObjectLock(authInfo, putObjLockRequest, log, err => {
                if (err) {
                    process.stdout.write(`Err putting lifecycle config ${err}`);
                    return done(err);
                }
                return metadata.getBucket(bucketName, log, (err, bucket) => {
                    if (err) {
                        process.stdout.write(`Err retrieving bucket MD ${err}`);
                        return done(err);
                    }
                    const bucketObjectLockConfig = bucket.
                        getObjectLockConfiguration();
                    assert.deepStrictEqual(
                        bucketObjectLockConfig, expectedObjectLockConfig);
                    return done();
                });
            });
        });
    });
});
