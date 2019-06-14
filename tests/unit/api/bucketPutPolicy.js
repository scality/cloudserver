const assert = require('assert');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutPolicy = require('../../../lib/api/bucketPutPolicy');
const { cleanup,
    DummyRequestLogger,
    makeAuthInfo }
    = require('../helpers');
const metadata = require('../../../lib/metadata/wrapper');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketname';
const testBucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
};

const expectedBucketPolicy = {
    version: '2012-10-17',
    statements: [
        {
            sid: '',
            effect: 'Allow',
            resource: 'arn:aws:s3::bucketname',
            principal: '*',
            actions: ['s3:sampleAction'],
        },
    ],
};

const testPutPolicyRequest = {
    bucketName,
    headers: {
        host: `${bucketName}.s3.amazonaws.com`,
    },
    post: JSON.stringify(expectedBucketPolicy),
};

const describeSkipUntilImpl =
    process.env.BUCKET_POLICY ? describe : describe.skip;

describeSkipUntilImpl('putBucketPolicy API', () => {
    before(() => cleanup());
    beforeEach(done => bucketPut(authInfo, testBucketPutRequest, log, done));
    afterEach(() => cleanup());

    it('should update a bucket\'s metadata with bucket policy obj', done => {
        bucketPutPolicy(authInfo, testPutPolicyRequest, log, err => {
            if (err) {
                process.stdout.write(`Err putting bucket policy ${err}`);
                return done(err);
            }
            return metadata.getBucket(bucketName, log, (err, bucket) => {
                if (err) {
                    process.stdout.write(`Err retrieving bucket MD ${err}`);
                    return done(err);
                }
                const bucketPolicy = bucket.getBucketPolicy();
                assert.deepStrictEqual(bucketPolicy, expectedBucketPolicy);
                return done();
            });
        });
    });
});
