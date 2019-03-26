const assert = require('assert');
const async = require('async');
const { S3 } = require('aws-sdk');

const BucketUtility = require('../../lib/utility/bucket-util');
const getConfig = require('../support/config');
const withV4 = require('../support/withV4');
const configOfficial = require('../../../../../lib/Config').config;

const bucketName = 'bucketlocation';

const describeSkipAWS = process.env.AWS_ON_AIR ? describe.skip : describe;
const itSkipIfE2E = process.env.S3_END_TO_END ? it.skip : it;

const locationConstraints = configOfficial.locationConstraints;

describe('PUT Bucket - AWS.S3.createBucket', () => {
    describe('When user is unauthorized', () => {
        let s3;
        let config;

        beforeEach(() => {
            config = getConfig('default');
            s3 = new S3(config);
        });

        test('should return 403 and AccessDenied', done => {
            const params = { Bucket: 'mybucket' };

            s3.makeUnauthenticatedRequest('createBucket', params, error => {
                expect(error).toBeTruthy();

                expect(error.statusCode).toBe(403);
                expect(error.code).toBe('AccessDenied');

                done();
            });
        });
    });

    withV4(sigCfg => {
        let bucketUtil;

        beforeAll(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
        });

        describe('create bucket twice', () => {
            beforeEach(done => bucketUtil.s3.createBucket({ Bucket:
                bucketName,
                CreateBucketConfiguration: {
                    LocationConstraint: 'us-east-1',
                },
            }, done));
            afterEach(done => bucketUtil.s3.deleteBucket({ Bucket: bucketName },
              done));
            // AWS JS SDK sends a request with locationConstraint us-east-1 if
            // no locationConstraint provided.
            // Skip this test on E2E because it is making the asumption that the
            // default region is us-east-1 which is not the case for the E2E
            itSkipIfE2E('should return a 200 if no locationConstraints ' +
            'provided.', done => {
                bucketUtil.s3.createBucket({ Bucket: bucketName }, done);
            });
            test('should return a 200 if us-east behavior', done => {
                bucketUtil.s3.createBucket({
                    Bucket: bucketName,
                    CreateBucketConfiguration: {
                        LocationConstraint: 'us-east-1',
                    },
                }, done);
            });
            test('should return a 409 if us-west behavior', done => {
                bucketUtil.s3.createBucket({
                    Bucket: bucketName,
                    CreateBucketConfiguration: {
                        LocationConstraint: 'scality-us-west-1',
                    },
                }, error => {
                    expect(error).not.toEqual(null);
                    expect(error.code).toBe('BucketAlreadyOwnedByYou');
                    expect(error.statusCode).toBe(409);
                    done();
                });
            });
        });

        describe('bucket naming restriction', () => {
            let testFn;

            beforeAll(() => {
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
                            expect(error.code).toBe(expectedCode);
                            expect(error.statusCode).toBe(expectedStatus);
                            done();
                        });
                };
            });

            // Found that AWS has fewer restriction in naming than
            // they described in their document.
            // Hence it skips some of test suites.
            const itSkipIfAWS = process.env.AWS_ON_AIR ? it.skip : it;

            test('should return 405 if empty name', done => {
                const shortName = '';

                testFn(shortName, done, 405, 'MethodNotAllowed');
            });

            test('should return 400 if name is shorter than 3 chars', done => {
                const shortName = 'as';

                testFn(shortName, done);
            });

            test('should return 403 if name is reserved (e.g., METADATA)', done => {
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

            test('should return 400 if name ends with period', done => {
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

            test('should return 400 if name has special chars', done => {
                const invalidName = 'my.#s3bucket';
                testFn(invalidName, done);
            });
        });

        describe('bucket creation success', () => {
            function _test(name, done) {
                bucketUtil.s3.createBucket({ Bucket: name }, (err, res) => {
                    assert.ifError(err);
                    expect(res.Location).toBeTruthy();
                    assert.deepStrictEqual(res.Location, `/${name}`,
                      'Wrong Location header');
                    bucketUtil.deleteOne(name).then(() => done()).catch(done);
                });
            }
            test('should create bucket if name is valid', done =>
                _test('scality-very-valid-bucket-name', done));

            test(
                'should create bucket if name is some prefix and an IP address',
                done => _test('prefix-192.168.5.4', done)
            );

            test(
                'should create bucket if name is an IP address with some suffix',
                done => _test('192.168.5.4-suffix', done)
            );
        });
        Object.keys(locationConstraints).forEach(
        location => {
            describeSkipAWS(`bucket creation with location: ${location}`,
            () => {
                afterAll(() => bucketUtil.deleteOne(bucketName));
                test(`should create bucket with location: ${location}`, done => {
                    bucketUtil.s3.createBucketAsync(
                        {
                            Bucket: bucketName,
                            CreateBucketConfiguration: {
                                LocationConstraint: location,
                            },
                        }, done);
                });
            });
        });

        describe('bucket creation with invalid location', () => {
            test('should return errors InvalidLocationConstraint', done => {
                bucketUtil.s3.createBucketAsync(
                    {
                        Bucket: bucketName,
                        CreateBucketConfiguration: {
                            LocationConstraint: 'coco',
                        },
                    }, err => {
                    expect(err.code).toBe('InvalidLocationConstraint');
                    expect(err.statusCode).toBe(400);
                    done();
                });
            });
        });

        describe('bucket creation with ingestion location', () => {
            afterAll(done =>
                bucketUtil.s3.deleteBucket({ Bucket: bucketName }, done));
            test('should create bucket with location and ingestion', done => {
                async.waterfall([
                    next => bucketUtil.s3.createBucketAsync(
                        {
                            Bucket: bucketName,
                            CreateBucketConfiguration: {
                                LocationConstraint: 'us-east-2:ingest',
                            },
                        }, (err, res) => {
                        assert.ifError(err);
                        expect(res.Location).toBe(`/${bucketName}`);
                        return next();
                    }),
                    next => bucketUtil.s3.getBucketLocation(
                        {
                            Bucket: bucketName,
                        }, (err, res) => {
                        assert.ifError(err);
                        expect(res.LocationConstraint).toBe('us-east-2');
                        return next();
                    }),
                    next => bucketUtil.s3.getBucketVersioning(
                        { Bucket: bucketName }, (err, res) => {
                            assert.ifError(err);
                            expect(res.Status).toBe('Enabled');
                            return next();
                    }),
                ], done);
            });
        });
    });
});
