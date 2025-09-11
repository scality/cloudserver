const assert = require('assert');
const { errors } = require('arsenal');

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
    actionImplicitDenies: false,
};

let expectedBucketPolicy = {};
function getPolicyRequest(policy) {
    return {
        bucketName,
        headers: {
            host: `${bucketName}.s3.amazonaws.com`,
        },
        post: JSON.stringify(policy),
        actionImplicitDenies: false,
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
            assert.strictEqual(err.is.MalformedPolicy, true);
            assert.strictEqual(err.description, 'Policy has invalid resource');
            return done();
        });
    });

    it('should not return error if policy contains conditions', done => {
        expectedBucketPolicy.Statement[0].Condition =
        { IpAddress: { 'aws:SourceIp': '123.123.123.123' } };
        bucketPutPolicy(authInfo, getPolicyRequest(expectedBucketPolicy), log,
        err => {
            assert.ifError(err);
            done();
        });
    });

    it('should return error if policy contains service principal', done => {
        expectedBucketPolicy.Statement[0].Principal = { Service: ['test.com'] };
        bucketPutPolicy(authInfo, getPolicyRequest(expectedBucketPolicy), log,
        err => {
            assert.strictEqual(err.is.NotImplemented, true);
            done();
        });
    });

    it('should return error if policy contains federated principal', done => {
        expectedBucketPolicy.Statement[0].Principal =
            { Federated: 'www.test.com' };
        bucketPutPolicy(authInfo, getPolicyRequest(expectedBucketPolicy), log,
        err => {
            assert.strictEqual(err.is.NotImplemented, true);
            done();
        });
    });

    describe('checksum validation', () => {
        const testPolicy = {
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
        const policyJson = JSON.stringify(testPolicy);

        it('should not return an error when Content-MD5 header is missing', done => {
            const testPolicyRequest = {
                bucketName,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                post: policyJson,
                url: '/?policy',
                query: { policy: '' },
                actionImplicitDenies: false,
            };

            bucketPutPolicy(authInfo, testPolicyRequest, log, err => {
                assert.ifError(err);
                done();
            });
        });

        it('should return BadDigest error when Content-MD5 header mismatches', done => {
            const testPolicyRequest = {
                bucketName,
                headers: {
                    'host': `${bucketName}.s3.amazonaws.com`,
                    'content-md5': '+5yj3kZsXledyKr18eaUDg==', // incorrect MD5
                },
                post: policyJson,
                url: '/?policy',
                query: { policy: '' },
                actionImplicitDenies: false,
            };

            bucketPutPolicy(authInfo, testPolicyRequest, log, err => {
                assert.deepStrictEqual(err, errors.BadDigest);
                done();
            });
        });

        it('should not return an error when Content-MD5 header matches', done => {
            const testPolicyRequest = {
                bucketName,
                headers: {
                    'host': `${bucketName}.s3.amazonaws.com`,
                    'content-md5': 'Q5txAQ0vMnQbyv1+5xhpvA==', // correct MD5
                },
                post: policyJson,
                url: '/?policy',
                query: { policy: '' },
                actionImplicitDenies: false,
            };

            bucketPutPolicy(authInfo, testPolicyRequest, log, err => {
                assert.ifError(err);
                done();
            });
        });
    });
});
