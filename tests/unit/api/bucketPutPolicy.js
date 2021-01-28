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

let expectedBucketPolicy = {};
function getPolicyRequest(policy) {
    return {
        bucketName,
        headers: {
            host: `${bucketName}.s3.amazonaws.com`,
        },
        post: JSON.stringify(policy),
    };
}

describe('putBucketPolicy API', () => {
    before(() => cleanup());
    beforeEach(done => {
        expectedBucketPolicy = {
            Version: '2012-10-17',
            Statement: [
                {
                    Effect: 'Allow',
                    Resource: `arn:aws:s3:::${bucketName}`,
                    Principal: '*',
                    Action: ['s3:GetBucketLocation'],
                },
            ],
        };
        bucketPut(authInfo, testBucketPutRequest, log, done);
    });
    afterEach(() => cleanup());

    it('should update a bucket\'s metadata with bucket policy obj', done => {
        bucketPutPolicy(authInfo, getPolicyRequest(expectedBucketPolicy),
        log, err => {
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

    it('should return error if policy resource does not include bucket name',
    done => {
        expectedBucketPolicy.Statement[0].Resource = 'arn:aws::s3:::badname';
        bucketPutPolicy(authInfo, getPolicyRequest(expectedBucketPolicy),
        log, err => {
            assert.strictEqual(err.MalformedPolicy, true);
            assert.strictEqual(err.description, 'Policy has invalid resource');
            return done();
        });
    });

    it('should return error if policy contains conditions', done => {
        expectedBucketPolicy.Statement[0].Condition =
            { StringEquals: { 's3:x-amz-acl': ['public-read'] } };
        bucketPutPolicy(authInfo, getPolicyRequest(expectedBucketPolicy), log,
        err => {
            assert.strictEqual(err.NotImplemented, true);
            done();
        });
    });

    it('should return error if policy contains service principal', done => {
        expectedBucketPolicy.Statement[0].Principal = { Service: ['test.com'] };
        bucketPutPolicy(authInfo, getPolicyRequest(expectedBucketPolicy), log,
        err => {
            assert.strictEqual(err.NotImplemented, true);
            done();
        });
    });

    it('should return error if policy contains federated principal', done => {
        expectedBucketPolicy.Statement[0].Principal =
            { Federated: 'www.test.com' };
        bucketPutPolicy(authInfo, getPolicyRequest(expectedBucketPolicy), log,
        err => {
            assert.strictEqual(err.NotImplemented, true);
            done();
        });
    });
});
