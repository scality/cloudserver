const assert = require('assert');
const async = require('async');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
    GetObjectLockConfigurationCommand,
    GetBucketVersioningCommand,
    GetBucketLocationCommand,
} = require('@aws-sdk/client-s3');

const BucketUtility = require('../../lib/utility/bucket-util');
const withV4 = require('../support/withV4');
const configOfficial = require('../../../../../lib/Config').config;

const {
    LOCATION_NAME_DMF,
} = require('../../../../constants');

const bucketName = 'bucketlocation';

const describeSkipAWS = process.env.AWS_ON_AIR ? describe.skip : describe;
const itSkipIfE2E = process.env.S3_END_TO_END ? it.skip : it;

const locationConstraints = configOfficial.locationConstraints;

describe('PUT Bucket - AWS.S3.createBucket', () => {
    describe('When user is unauthorized', () => {

        it('should return 403 and AccessDenied', async () => {
            const params = { Bucket: 'mybucket' };

            try {
                // In AWS SDK v3, makeUnauthenticatedRequest doesn't exist
                // We simulate this by creating a client with invalid credentials
                const unauthenticatedS3 = new BucketUtility('default', {}, true).s3;
                await unauthenticatedS3.send(new CreateBucketCommand(params));
                assert.fail('Expected request to fail with AccessDenied');
            } catch (error) {
                assert.strictEqual(error.$metadata?.httpStatusCode, 403);
                assert.strictEqual(error.Code, 'AccessDenied');
            }
        });
    });

    withV4(sigCfg => {
        let bucketUtil;

        before(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
        });

                // Replace the entire "create bucket twice" describe block:
        
        describe('create bucket twice', () => {
            let testBucketName;
        
            beforeEach(() => {
                // Use unique bucket name for each test to avoid conflicts
                testBucketName = `${bucketName}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
                return bucketUtil.s3.send(new CreateBucketCommand({
                    Bucket: testBucketName,
                    CreateBucketConfiguration: {
                        LocationConstraint: 'us-east-1',
                    },
                }));
            });
        
            afterEach(() => bucketUtil.s3.send(new DeleteBucketCommand({ Bucket: testBucketName }))
                    .catch(error => {
                        // eslint-disable-next-line no-console
                        console.warn(`Failed to cleanup bucket ${testBucketName}:`, error.message);
                    }));
        
            itSkipIfE2E('should return a 200 if no locationConstraints provided.',
                async () => {
                    await bucketUtil.s3.send(new CreateBucketCommand({ Bucket: testBucketName }));
                });
                
            it('should return a 200 if us-east behavior', async () => {
                const res = await bucketUtil.s3.send(new CreateBucketCommand({
                    Bucket: testBucketName,
                    CreateBucketConfiguration: {
                        LocationConstraint: 'us-east-1',
                    },
                }));
                assert.strictEqual(res.$metadata.httpStatusCode, 200);
            });
            
            it('should return a 409 if us-west behavior', async () => {
                try {
                    await bucketUtil.s3.send(new CreateBucketCommand({
                        Bucket: testBucketName,
                        CreateBucketConfiguration: {
                            LocationConstraint: 'scality-us-west-1',
                        },
                    }));
                    assert.fail('Expected failure but got success');
                } catch (error) {
                    assert.strictEqual(error.name, 'BucketAlreadyOwnedByYou');
                    assert.strictEqual(error.$metadata.httpStatusCode, 409);
                }
            });
        });

        describe('bucket naming restriction', () => {
            let testFn;

            before(() => {
                testFn = (bucketName, done, errStatus, errCode) => {
                    const expectedStatus = errStatus || 400;
                    const expectedCode = errCode || 'InvalidBucketName';
                    bucketUtil
                        .createOne(bucketName)
                        .then(() => {
                            const e = new Error('Expect failure in creation, ' +
                                'but it succeeded');

                            return done(e);
                        })
                        .catch(error => {
                            assert.strictEqual(error.Code, expectedCode);
                            assert.strictEqual(error.$metadata.httpStatusCode,
                                expectedStatus);
                            done();
                        });
                };
            });

            // Found that AWS has fewer restriction in naming than
            // they described in their document.
            // Hence it skips some of test suites.
            const itSkipIfAWS = process.env.AWS_ON_AIR ? it.skip : it;

            // aws-sdk now (v2.363.0) returns 'UriParameterError' error
            it.skip('should return 405 if empty name', done => {
                const shortName = '';

                testFn(shortName, done, 405, 'MethodNotAllowed');
            });

            it('should return 400 if name is shorter than 3 chars', done => {
                const shortName = 'as';

                testFn(shortName, done);
            });

            it('should return 403 if name is reserved (e.g., METADATA)',
                done => {
                    const reservedName = 'METADATA';
                    testFn(reservedName, done, 403, 'AccessDenied');
                });

            itSkipIfAWS('should return 400 if name is longer than 63 chars',
                done => {
                    const longName = 'x'.repeat(64);
                    testFn(longName, done);
                }
            );

            itSkipIfAWS('should return 400 if name is formatted as IP address',
                done => {
                    const ipAddress = '192.168.5.4';
                    testFn(ipAddress, done);
                }
            );

            itSkipIfAWS('should return 400 if name starts with period',
                done => {
                    const invalidName = '.myawsbucket';
                    testFn(invalidName, done);
                }
            );

            it('should return 400 if name ends with period', done => {
                const invalidName = 'myawsbucket.';
                testFn(invalidName, done);
            });

            itSkipIfAWS(
                'should return 400 if name has two period between labels',
                done => {
                    const invalidName = 'my..examplebucket';
                    testFn(invalidName, done);
                }
            );

            it('should return 400 if name has special chars', done => {
                const invalidName = 'my.#s3bucket';
                testFn(invalidName, done);
            });
        });

        describe('bucket creation success', () => {
            function _test(name, done) {
                bucketUtil.s3.send(new CreateBucketCommand({ Bucket: name }))
                    .then(res => {
                        assert(res.Location, 'No Location in response');
                        assert.deepStrictEqual(res.Location, `/${name}`,
                            'Wrong Location header');
                        return bucketUtil.deleteOne(name);
                    })
                    .then(() => done())
                    .catch(done);
            }
            it('should create bucket if name is valid', done =>
                _test('scality-very-valid-bucket-name', done));

            it('should create bucket if name is some prefix and an IP address',
                done => _test('prefix-192.168.5.4', done));

            it('should create bucket if name is an IP address with some suffix',
                done => _test('192.168.5.4-suffix', done));
        });

        describe('bucket creation success with object lock', () => {
            function _testObjectLockEnabled(name, done) {
                bucketUtil.s3.send(new CreateBucketCommand({
                    Bucket: name,
                    ObjectLockEnabledForBucket: true,
                })).then(res => {
                    assert.strictEqual(res.Location, `/${name}`, 'Wrong Location header');
                    return bucketUtil.s3.send(new GetObjectLockConfigurationCommand({ Bucket: name }));
                }).then(res => {
                    assert.deepStrictEqual(res.ObjectLockConfiguration,
                        { ObjectLockEnabled: 'Enabled' });
                    return bucketUtil.deleteOne(name);
                }).then(() => done()).catch(done);
            }

            function _testObjectLockDisabled(name, done) {
                bucketUtil.s3.send(new CreateBucketCommand({
                    Bucket: name,
                    ObjectLockEnabledForBucket: false,
                })).then(res => {
                    assert(res.Location, 'No Location in response');
                    assert.strictEqual(res.Location, `/${name}`, 'Wrong Location header');
                    return bucketUtil.s3.send(new GetObjectLockConfigurationCommand({ Bucket: name }));
                }).catch(err => {
                    assert.strictEqual(err.name, 'ObjectLockConfigurationNotFoundError');
                    return bucketUtil.deleteOne(name);
                }).then(() => done()).catch(done);
            }

            function _testVersioning(name, done) {
                bucketUtil.s3.send(new CreateBucketCommand({
                    Bucket: name,
                    ObjectLockEnabledForBucket: true,
                })).then(res => {
                    assert(res.Location, 'No Location in response');
                    assert.strictEqual(res.Location, `/${name}`, 'Wrong Location header');
                    return bucketUtil.s3.send(new GetBucketVersioningCommand({ Bucket: name }));
                }).then(res => {
                    assert.strictEqual(res.Status, 'Enabled');
                    assert.strictEqual(res.MFADelete, 'Disabled');
                    return bucketUtil.deleteOne(name);
                }).then(() => done()).catch(done);
            }
            it('should create bucket without error', done =>
                _testObjectLockEnabled('bucket-with-object-lock', done));

            it('should create bucket with versioning enabled by default', done =>
                _testVersioning('bucket-with-object-lock', done));

            it('should create bucket without error', done =>
                _testObjectLockDisabled('bucket-without-object-lock', done));
        });

        Object.keys(locationConstraints).forEach(
        location => {
            describeSkipAWS(`bucket creation with location: ${location}`,
            () => {
                after(done => {
                    bucketUtil.deleteOne(bucketName).finally(done);
                });
                it(`should create bucket with location: ${location}`, done => {
                    bucketUtil.s3.send(new CreateBucketCommand({
                        Bucket: bucketName,
                        CreateBucketConfiguration: {
                            LocationConstraint: location,
                        },
                    })).then(() => {
                        done();
                    }).catch(err => {
                        if (location === LOCATION_NAME_DMF) {
                            assert.strictEqual(
                                err.name,
                                'InvalidLocationConstraint'
                            );
                            assert.strictEqual(err.$metadata.httpStatusCode, 400);
                        }
                        done();
                    });
                });
            });
        });

        describe('bucket creation with invalid location', () => {
            it('should return errors InvalidLocationConstraint', done => {
                bucketUtil.s3.send(new CreateBucketCommand({
                    Bucket: bucketName,
                    CreateBucketConfiguration: {
                        LocationConstraint: 'coco',
                    },
                })).catch(err => {
                    assert.strictEqual(
                        err.name,
                        'InvalidLocationConstraint'
                    );
                    assert.strictEqual(err.$metadata.httpStatusCode, 400);
                    done();
                });
            });

            it('should return error InvalidLocationConstraint for location constraint dmf', done => {
                bucketUtil.s3.send(new CreateBucketCommand({
                    Bucket: bucketName,
                    CreateBucketConfiguration: {
                        LocationConstraint: LOCATION_NAME_DMF,
                    },
                })).catch(err => {
                    assert.strictEqual(
                        err.name,
                        'InvalidLocationConstraint'
                    );
                    assert.strictEqual(err.$metadata.httpStatusCode, 400);
                    done();
                });
            });
        });

        describe('bucket creation with ingestion location', () => {
            after(async () => {
                try {
                    await bucketUtil.s3.send(new DeleteBucketCommand({ Bucket: bucketName }));
                } catch (error) {
                    // eslint-disable-next-line no-console
                    console.warn(`Failed to cleanup bucket ${bucketName}:`, error.message);
                }
            });
            it('should create bucket with location and ingestion', done => {
                async.waterfall([
                    next => bucketUtil.s3.send(new CreateBucketCommand({
                        Bucket: bucketName,
                        CreateBucketConfiguration: {
                            LocationConstraint: 'us-east-2:ingest',
                        },
                    })).then(res => {
                        assert.strictEqual(res.Location, `/${bucketName}`);
                        next();
                    }).catch(next),

                    next => bucketUtil.s3.send(new GetBucketLocationCommand({
                        Bucket: bucketName,
                    })).then(res => {
                        assert.strictEqual(res.LocationConstraint, 'us-east-2');
                        next();
                    }).catch(next),

                    next => bucketUtil.s3.send(new GetBucketVersioningCommand({
                        Bucket: bucketName,
                    })).then(res => {
                        assert.strictEqual(res.Status, 'Enabled');
                        next();
                    }).catch(next),
                ], done);
            });
        });
    });
});
