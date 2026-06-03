const assert = require('assert');
const { GetBucketLocationCommand, CreateBucketCommand } = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const getConfig = require('../support/config');
const { config } = require('../../../../../lib/Config');

const { LOCATION_NAME_DMF } = require('../../../../constants');

const bucketName = 'testgetlocationbucket';

const describeSkipAWS = process.env.AWS_ON_AIR ? describe.skip : describe;

describeSkipAWS('GET bucket location ', () => {
    withV4(sigCfg => {
        const clientConfig = getConfig('default', sigCfg);
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const otherAccountBucketUtility = new BucketUtility('lisa', {});
        const otherAccountS3 = otherAccountBucketUtility.s3;
        const locationConstraints = config.locationConstraints;
        Object.keys(locationConstraints).forEach(location => {
            if (location === 'us-east-1') {
                // if location is us-east-1 should return empty string
                // see next test.
                return;
            }
            if (location === LOCATION_NAME_DMF) {
                // if region location-dmf-v1 should return InvalidLocationConstraint error
                return;
            }
            if (locationConstraints[location].isCRR) {
                // CRR location cannot be used as bucket location
                return;
            }
            describe(`with location: ${location}`, () => {
                before(() =>
                    s3.send(
                        new CreateBucketCommand({
                            Bucket: bucketName,
                            CreateBucketConfiguration: {
                                LocationConstraint: location,
                            },
                        }),
                    ),
                );
                after(() => bucketUtil.deleteOne(bucketName));

                it(`should return location configuration: ${location} ` + 'successfully', async () => {
                    const data = await s3.send(new GetBucketLocationCommand({ Bucket: bucketName }));
                    assert.deepStrictEqual(data.LocationConstraint, location);
                });
            });
        });

        describe('with location us-east-1', () => {
            before(() =>
                s3.send(
                    new CreateBucketCommand({
                        Bucket: bucketName,
                        CreateBucketConfiguration: {
                            LocationConstraint: 'us-east-1',
                        },
                    }),
                ),
            );
            afterEach(() => bucketUtil.deleteOne(bucketName));

            it('should return empty location', async () => {
                const data = await s3.send(new GetBucketLocationCommand({ Bucket: bucketName }));
                // SDK v3 returns undefined for us-east-1, normalize to empty string for comparison
                const locationConstraint = data.LocationConstraint || '';
                assert.deepStrictEqual(locationConstraint, '');
            });
        });

        describe('without location configuration', () => {
            after(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(bucketName).catch(err => {
                    process.stdout.write(`Error in after: ${err}\n`);
                    throw err;
                });
            });

            it('should return request endpoint as location', async () => {
                process.stdout.write('Creating bucket');
                await s3.send(new CreateBucketCommand({ Bucket: bucketName }));

                // In SDK v3, we need to get the endpoint from the client config
                let host = '127.0.0.1';

                if (clientConfig.endpoint) {
                    try {
                        const url = new URL(clientConfig.endpoint);
                        host = url.hostname;
                    } catch {
                        // If endpoint is not a valid URL, use it as-is
                        host = clientConfig.endpoint;
                    }
                }

                let endpoint = config.restEndpoints[host];
                // s3 actually returns '' for us-east-1
                if (endpoint === 'us-east-1') {
                    endpoint = '';
                }

                const data = await s3.send(new GetBucketLocationCommand({ Bucket: bucketName }));

                // S3C backend has 'dc-1' as default location constraint
                // Other backends use endpoint-based location
                const isS3C = process.env.S3BACKEND === 's3c';
                const expectedLocation = isS3C ? 'dc-1' : endpoint;

                const actualLocation = data.LocationConstraint || '';
                const normalizedExpected = expectedLocation || '';
                assert.strictEqual(actualLocation, normalizedExpected);
            });
        });

        describe('with location configuration', () => {
            before(() =>
                s3.send(
                    new CreateBucketCommand({
                        Bucket: bucketName,
                        CreateBucketConfiguration: {
                            LocationConstraint: 'us-east-1',
                        },
                    }),
                ),
            );
            after(() => bucketUtil.deleteOne(bucketName));

            it('should return AccessDenied if user is not bucket owner', async () => {
                try {
                    await otherAccountS3.send(new GetBucketLocationCommand({ Bucket: bucketName }));
                    throw new Error('Expected AccessDenied error');
                } catch (err) {
                    assert.strictEqual(err.name, 'AccessDenied');
                    assert.strictEqual(err.$metadata.httpStatusCode, 403);
                }
            });
        });
    });
});
