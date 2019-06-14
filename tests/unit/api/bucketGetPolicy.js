const assert = require('assert');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketGetPolicy = require('../../../lib/api/bucketGetPolicy');
const bucketPutPolicy = require('../../../lib/api/bucketPutPolicy');
const { cleanup,
    DummyRequestLogger,
    makeAuthInfo }
    = require('../helpers');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'getbucketpolicy-testbucket';

const testBasicRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
};

const expectedBucketPolicy = {
    version: '2012-10-17',
    statements: [
        {
            sid: '',
            effect: '',
            resource: '',
            principal: '',
            actions: '',
        },
    ],
};

const testPutPolicyRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    post: JSON.stringify(expectedBucketPolicy),
};

const describeSkipUntilImpl =
    process.env.BUCKET_POLICY ? describe : describe.skip;

describeSkipUntilImpl('getBucketPolicy API', () => {
    before(() => cleanup());
    beforeEach(done => bucketPut(authInfo, testBasicRequest, log, done));
    afterEach(() => cleanup());

    it('should return NoSuchBucketPolicy error if ' +
    'bucket has no policy', done => {
        bucketGetPolicy(authInfo, testBasicRequest, log, err => {
            assert.strictEqual(err.NoSuchBucketPolicy, true);
            done();
        });
    });

    describe('after bucket policy has been put', () => {
        beforeEach(done => {
            bucketPutPolicy(authInfo, testPutPolicyRequest, log, err => {
                assert.equal(err, null);
                done();
            });
        });

        it('should return bucket policy', done => {
            bucketGetPolicy(authInfo, testBasicRequest, log, (err, res) => {
                assert.equal(err, null);
                assert.deepStrictEqual(expectedBucketPolicy, res);
                done();
            });
        });
    });
});
