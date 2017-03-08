import assert from 'assert';
import { S3 } from 'aws-sdk';

import BucketUtility from '../../lib/utility/bucket-util';
import getConfig from '../support/config';
import withV4 from '../support/withV4';
import configOfficial from '../../../../../lib/Config';

const bucketName = 'bucketlocation';

const describeSkipAWS = process.env.AWS_ON_AIR ? describe.skip : describe;

const describeSkipIfOldConfig = configOfficial.regions ? describe.skip :
describe;
// test for old and new config
const locationConstraints = configOfficial.locationConstraints ||
{ foo: 'foo', toto: 'toto' };

describe('PUT Bucket - AWS.S3.createBucket', () => {
    describe('When user is unauthorized', () => {
        let s3;
        let config;

        beforeEach(() => {
            config = getConfig('default');
            s3 = new S3(config);
        });

        it('should return 403 and AccessDenied', done => {
            const params = { Bucket: 'mybucket' };

            s3.makeUnauthenticatedRequest('createBucket', params, error => {
                assert(error);

                assert.strictEqual(error.statusCode, 403);
                assert.strictEqual(error.code, 'AccessDenied');

                done();
            });
        });
    });

    withV4(sigCfg => {
        let bucketUtil;

        before(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
        });

        // Why describeSkipIfOldConfig?
        // AWS returns 404 - NoSuchUpload in us-east-1. This behavior
        // can be toggled to be compatible with AWS by enabling
        // usEastBehavior in the config.
        describeSkipIfOldConfig('create bucket twice', () => {
            beforeEach(done => bucketUtil.s3.createBucket({ Bucket:
              bucketName }, done));
            afterEach(done => bucketUtil.s3.deleteBucket({ Bucket: bucketName },
              done));
            // AWS JS SDK sends a request with locationConstraint us-east-1 if
            // no locationConstraint provided.
            it('should return a 200 if no locationConstraints provided.',
            done => {
                bucketUtil.s3.createBucket({ Bucket: bucketName }, done);
            });
            it('should return a 200 if us-east behavior', done => {
                bucketUtil.s3.createBucket({
                    Bucket: bucketName,
                    CreateBucketConfiguration: {
                        LocationConstraint: 'us-east-1',
                    },
                }, done);
            });
            it('should return a 409 if us-west behavior', done => {
                bucketUtil.s3.createBucket({
                    Bucket: bucketName,
                    CreateBucketConfiguration: {
                        LocationConstraint: 'scality-us-west-1',
                    },
                }, error => {
                    assert.notEqual(error, null,
                      'Expected failure but got success');
                    assert.strictEqual(error.code, 'BucketAlreadyOwnedByYou');
                    assert.strictEqual(error.statusCode, 409);
                    done();
                });
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
                            assert.strictEqual(error.code, expectedCode);
                            assert.strictEqual(error.statusCode,
                                expectedStatus);
                            done();
                        });
                };
            });

            // Found that AWS has fewer restriction in naming than
            // they described in their document.
            // Hence it skips some of test suites.
            const itSkipIfAWS = process.env.AWS_ON_AIR ? it.skip : it;

            it('should return 405 if empty name', done => {
                const shortName = '';

                testFn(shortName, done, 405, 'MethodNotAllowed');
            });

            it('should return 400 if name is shorter than 3 chars', done => {
                const shortName = 'as';

                testFn(shortName, done);
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
                bucketUtil.s3.createBucket({ Bucket: name }, (err, res) => {
                    assert.ifError(err);
                    assert(res.Location, 'No Location in response');
                    assert.deepStrictEqual(res.Location, `/${name}`,
                      'Wrong Location header');
                    bucketUtil.deleteOne(name).then(() => done()).catch(done);
                });
            }
            it('should create bucket if name is valid', done =>
                _test('scality-very-valid-bucket-name', done));

            it('should create bucket if name is some prefix and an IP address',
                done => _test('prefix-192.168.5.4', done));

            it('should create bucket if name is an IP address with some suffix',
                done => _test('192.168.5.4-suffix', done));
        });
        Object.keys(locationConstraints).forEach(
        location => {
            describeSkipAWS(`bucket creation with location: ${location}`,
            () => {
                after(() => bucketUtil.deleteOne(bucketName));
                it(`should create bucket with location: ${location}`, done => {
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

        describeSkipIfOldConfig('bucket creation with invalid location', () => {
            it('should return errors InvalidLocationConstraint', done => {
                bucketUtil.s3.createBucketAsync(
                    {
                        Bucket: bucketName,
                        CreateBucketConfiguration: {
                            LocationConstraint: 'coco',
                        },
                    }, err => {
                    assert.strictEqual(err.code,
                    'InvalidLocationConstraint');
                    assert.strictEqual(err.statusCode, 400);
                    done();
                });
            });
        });
    });
});
