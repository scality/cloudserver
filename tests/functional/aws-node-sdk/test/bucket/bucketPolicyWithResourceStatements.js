const assert = require('assert');
const AWS = require('aws-sdk');
const { errors } = require('arsenal');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { VALIDATE_CREDENTIALS, SIGN } = AWS.EventListeners.Core;

withV4(sigCfg => {
    const ownerAccountBucketUtil = new BucketUtility('default', sigCfg);
    const s3 = ownerAccountBucketUtil.s3;
    const testBuckets = ['bucket-with-resource-stmnt', 'no-policy-bucket'];

    function awsRequest(auth, operation, params, callback) {
        if (auth) {
            ownerAccountBucketUtil.s3[operation](params, callback);
        } else {
            const bucketUtil = new BucketUtility('default', sigCfg);
            const request = bucketUtil.s3[operation](params);
            request.removeListener('validate', VALIDATE_CREDENTIALS);
            request.removeListener('sign', SIGN);
            request.send(callback);
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
            assert.strictEqual(err.statusCode, errors.AccessDenied.code);
            done();
        };
    }

    describe('Bucket policies with resource statement', () => {
        beforeEach(() => ownerAccountBucketUtil.createMany(testBuckets));
        afterEach(() => ownerAccountBucketUtil.emptyMany(testBuckets)
            .then(() => ownerAccountBucketUtil.deleteMany(testBuckets)));

        it('should allow action on a bucket specified in the policy', done => {
            const statement = {
                Sid: 'myBucketPolicy',
                Effect: 'Allow',
                Principal: '*',
                Action: ['s3:ListBucket'],
                Resource: [`arn:aws:s3:::${testBuckets[0]}`],
            };
            const bucketPolicy = {
                Version: '2012-10-17',
                Statement: [statement],
            };
            s3.putBucketPolicy({
                Bucket: testBuckets[0],
                Policy: JSON.stringify(bucketPolicy),
            }, err => {
                assert.ifError(err);
                const param = { Bucket: testBuckets[0] };
                awsRequest(true, 'listObjects', param, cbNoError(done));
            });
        });

        it('should deny action on a bucket not specified in the policy', done => {
            const statement = {
                Sid: 'myBucketPolicy',
                Effect: 'Allow',
                Principal: '*',
                Action: ['s3:ListBucket'],
                Resource: [`arn:aws:s3:::${testBuckets[0]}`],
            };
            const bucketPolicy = {
                Version: '2012-10-17',
                Statement: [statement],
            };
            s3.putBucketPolicy({
                Bucket: testBuckets[0],
                Policy: JSON.stringify(bucketPolicy),
            }, err => {
                assert.ifError(err);
                const param = { Bucket: testBuckets[1] };
                awsRequest(false, 'listObjects', param, cbWithError(done));
            });
        });

        it('should deny action on a bucket specified in the policy', done => {
            const statement = {
                Sid: 'myBucketPolicy',
                Effect: 'Deny',
                Principal: '*',
                Action: ['s3:ListBucket'],
                Resource: [`arn:aws:s3:::${testBuckets[0]}`],
            };
            const bucketPolicy = {
                Version: '2012-10-17',
                Statement: [statement],
            };
            s3.putBucketPolicy({
                Bucket: testBuckets[0],
                Policy: JSON.stringify(bucketPolicy),
            }, err => {
                assert.ifError(err);
                const param = { Bucket: testBuckets[0] };
                awsRequest(false, 'listObjects', param, cbWithError(done));
            });
        });

        it('should allow action on an object specified in the policy', done => {
            const testKey = '0.txt';
            const testBody = '0';
            const statement = {
                Sid: 'myBucketPolicy',
                Effect: 'Allow',
                Principal: '*',
                Action: ['s3:GetObject'],
                Resource: [`arn:aws:s3:::${testBuckets[0]}/${testKey}`],
            };
            const bucketPolicy = {
                Version: '2012-10-17',
                Statement: [statement],
            };
            s3.putBucketPolicy({
                Bucket: testBuckets[0],
                Policy: JSON.stringify(bucketPolicy),
            }, err => {
                assert.ifError(err);
                s3.putObject({
                    Bucket: testBuckets[0],
                    Body: testBody,
                    Key: testKey,
                }, er => {
                    assert.ifError(er);
                    const param = {
                        Bucket: testBuckets[0],
                        Key: testKey,
                    };
                    awsRequest(false, 'getObject', param, cbNoError(done));
                });
            });
        });

        it('should allow action on an object satisfying the wildcard in the policy', done => {
            const testKey = '0.txt';
            const testBody = '0';
            const statement = {
                Sid: 'myBucketPolicy',
                Effect: 'Allow',
                Principal: '*',
                Action: ['s3:GetObject'],
                Resource: [`arn:aws:s3:::${testBuckets[0]}/*`],
            };
            const bucketPolicy = {
                Version: '2012-10-17',
                Statement: [statement],
            };
            s3.putBucketPolicy({
                Bucket: testBuckets[0],
                Policy: JSON.stringify(bucketPolicy),
            }, err => {
                assert.ifError(err);
                s3.putObject({
                    Bucket: testBuckets[0],
                    Body: testBody,
                    Key: testKey,
                }, er => {
                    assert.ifError(er);
                    const param = {
                        Bucket: testBuckets[0],
                        Key: testKey,
                    };
                    awsRequest(false, 'getObject', param, cbNoError(done));
                });
            });
        });

        it('should deny action on an object specified in the policy', done => {
            const testKey = '0.txt';
            const testBody = '0';
            const statement = {
                Sid: 'myBucketPolicy',
                Effect: 'Deny',
                Principal: '*',
                Action: ['s3:GetObject'],
                Resource: [`arn:aws:s3:::${testBuckets[0]}/${testKey}`],
            };
            const bucketPolicy = {
                Version: '2012-10-17',
                Statement: [statement],
            };
            s3.putBucketPolicy({
                Bucket: testBuckets[0],
                Policy: JSON.stringify(bucketPolicy),
            }, err => {
                assert.ifError(err);
                s3.putObject({
                    Bucket: testBuckets[0],
                    Body: testBody,
                    Key: testKey,
                }, er => {
                    assert.ifError(er);
                    const param = {
                        Bucket: testBuckets[0],
                        Key: testKey,
                    };
                    awsRequest(false, 'getObject', param, cbWithError(done));
                });
            });
        });

        it('should deny action on an object not specified in the policy', done => {
            const testKey = '0.txt';
            const statement = {
                Sid: 'myBucketPolicy',
                Effect: 'Deny',
                Principal: '*',
                Action: ['s3:GetObject'],
                Resource: [`arn:aws:s3:::${testBuckets[0]}/${testKey}`],
            };
            const bucketPolicy = {
                Version: '2012-10-17',
                Statement: [statement],
            };
            s3.putBucketPolicy({
                Bucket: testBuckets[0],
                Policy: JSON.stringify(bucketPolicy),
            }, err => {
                assert.ifError(err);
                const param = {
                    Bucket: testBuckets[0],
                    Key: 'invalidkey',
                };
                awsRequest(false, 'getObject', param, cbWithError(done));
            });
        });

        it('should deny action on a bucket and an object not specified in the policy', done => {
            const testKey = '0.txt';
            const statement = {
                Sid: 'myBucketPolicy',
                Effect: 'Deny',
                Principal: '*',
                Action: ['s3:GetObject'],
                Resource: [`arn:aws:s3:::${testBuckets[0]}/${testKey}`],
            };
            const bucketPolicy = {
                Version: '2012-10-17',
                Statement: [statement],
            };
            s3.putBucketPolicy({
                Bucket: testBuckets[0],
                Policy: JSON.stringify(bucketPolicy),
            }, err => {
                assert.ifError(err);
                const param = {
                    Bucket: testBuckets[1],
                    Key: 'invalidkey',
                };
                awsRequest(false, 'getObject', param, cbWithError(done));
            });
        });
    });
});
