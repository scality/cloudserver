import assert from 'assert';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';
import config from '../../../../../lib/Config';

const bucketName = 'testgetlocationbucket';

const describeSkipAWS = process.env.AWS_ON_AIR ? describe.skip : describe;

describeSkipAWS('GET bucket location ', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const otherAccountBucketUtility = new BucketUtility('lisa', {});
        const otherAccountS3 = otherAccountBucketUtility.s3;
        // test for old and new config
        const locationConstraints = config.locationConstraints ||
        { foo: 'foo', toto: 'toto' };
        Object.keys(locationConstraints).forEach(
        location => {
            if (location === 'us-east-1') {
                // if location is us-east-1 should return empty string
                // see next test.
                return;
            }
            describe(`with location: ${location}`, () => {
                before(done => s3.createBucketAsync(
                    {
                        Bucket: bucketName,
                        CreateBucketConfiguration: {
                            LocationConstraint: location,
                        },
                    }, done));
                after(() => bucketUtil.deleteOne(bucketName));

                it(`should return location configuration: ${location} ` +
                'successfully',
                done => {
                    s3.getBucketLocation({ Bucket: bucketName },
                    (err, data) => {
                        assert.strictEqual(err, null,
                            `Found unexpected err ${err}`);
                        assert.deepStrictEqual(data.LocationConstraint,
                            location);
                        return done();
                    });
                });
            });
        });

        describe('with location us-east-1', () => {
            before(done => s3.createBucketAsync(
                {
                    Bucket: bucketName,
                    CreateBucketConfiguration: {
                        LocationConstraint: 'us-east-1',
                    },
                }, done));
            afterEach(() => bucketUtil.deleteOne(bucketName));
            it('should return empty location',
            done => {
                s3.getBucketLocation({ Bucket: bucketName },
                (err, data) => {
                    assert.strictEqual(err, null,
                        `Found unexpected err ${err}`);
                    assert.deepStrictEqual(data.LocationConstraint, '');
                    return done();
                });
            });
        });

        describe('without location configuration', () => {
            afterEach(() => bucketUtil.deleteOne(bucketName));
            before(done => s3.createBucketAsync({ Bucket: bucketName }, done));
            it('should return empty location',
            done => {
                s3.getBucketLocation({ Bucket: bucketName },
                (err, data) => {
                    assert.strictEqual(err, null,
                        `Found unexpected err ${err}`);
                    assert.deepStrictEqual(data.LocationConstraint, '');
                    return done();
                });
            });
        });

        describe('with location configuration', () => {
            before(done => s3.createBucketAsync(
                {
                    Bucket: bucketName,
                    CreateBucketConfiguration: {
                        LocationConstraint: 'aws-us-east-1',
                    },
                }, done));
            after(() => bucketUtil.deleteOne(bucketName));

            it('should return AccessDenied if user is not bucket owner',
            done => {
                otherAccountS3.getBucketLocation({ Bucket: bucketName },
                err => {
                    assert(err);
                    assert.strictEqual(err.code, 'AccessDenied');
                    assert.strictEqual(err.statusCode, 403);
                    return done();
                });
            });
        });
    });
});
