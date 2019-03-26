const assert = require('assert');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { config } = require('../../../../../lib/Config');

const bucketName = 'testgetlocationbucket';

const describeSkipAWS = process.env.AWS_ON_AIR ? describe.skip : describe;

describeSkipAWS('GET bucket location ', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const otherAccountBucketUtility = new BucketUtility('lisa', {});
        const otherAccountS3 = otherAccountBucketUtility.s3;
        const locationConstraints = config.locationConstraints;
        Object.keys(locationConstraints).forEach(
        location => {
            if (location === 'us-east-1') {
                // if location is us-east-1 should return empty string
                // see next test.
                return;
            }
            describe(`with location: ${location}`, () => {
                beforeAll(done => s3.createBucketAsync(
                    {
                        Bucket: bucketName,
                        CreateBucketConfiguration: {
                            LocationConstraint: location,
                        },
                    }, done));
                afterAll(() => bucketUtil.deleteOne(bucketName));

                test(`should return location configuration: ${location} ` +
                'successfully', done => {
                    s3.getBucketLocation({ Bucket: bucketName },
                    (err, data) => {
                        expect(err).toBe(null);
                        assert.deepStrictEqual(data.LocationConstraint,
                            location);
                        return done();
                    });
                });
            });
        });

        describe('with location us-east-1', () => {
            beforeAll(done => s3.createBucketAsync(
                {
                    Bucket: bucketName,
                    CreateBucketConfiguration: {
                        LocationConstraint: 'us-east-1',
                    },
                }, done));
            afterEach(() => bucketUtil.deleteOne(bucketName));
            test('should return empty location', done => {
                s3.getBucketLocation({ Bucket: bucketName },
                (err, data) => {
                    expect(err).toBe(null);
                    assert.deepStrictEqual(data.LocationConstraint, '');
                    return done();
                });
            });
        });

        describe('without location configuration', () => {
            afterAll(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(bucketName)
                .catch(err => {
                    process.stdout.write(`Error in after: ${err}\n`);
                    throw err;
                });
            });

            test('should return request endpoint as location', done => {
                process.stdout.write('Creating bucket');
                const request = s3.createBucket({ Bucket: bucketName });
                request.on('build', () => {
                    request.httpRequest.body = '';
                });
                request.send(err => {
                    expect(err).toBe(null);
                    const host = request.service.endpoint.hostname;
                    let endpoint = config.restEndpoints[host];
                    // s3 actually returns '' for us-east-1
                    if (endpoint === 'us-east-1') {
                        endpoint = '';
                    }
                    s3.getBucketLocation({ Bucket: bucketName },
                    (err, data) => {
                        expect(err).toBe(null);
                        expect(data.LocationConstraint).toBe(endpoint);
                        done();
                    });
                });
            });
        });

        describe('with location configuration', () => {
            beforeAll(done => s3.createBucketAsync(
                {
                    Bucket: bucketName,
                    CreateBucketConfiguration: {
                        LocationConstraint: 'us-east-1',
                    },
                }, done));
            afterAll(() => bucketUtil.deleteOne(bucketName));

            test('should return AccessDenied if user is not bucket owner', done => {
                otherAccountS3.getBucketLocation({ Bucket: bucketName },
                err => {
                    expect(err).toBeTruthy();
                    expect(err.code).toBe('AccessDenied');
                    expect(err.statusCode).toBe(403);
                    return done();
                });
            });
        });
    });
});
