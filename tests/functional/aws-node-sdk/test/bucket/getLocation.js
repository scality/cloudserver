const assert = require('assert');
const { S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    GetBucketLocationCommand } = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const getConfig = require('../support/config');
const { config } = require('../../../../../lib/Config');

const {
    LOCATION_NAME_DMF,
} = require('../../../../constants');

const bucketName = 'testgetlocationbucket';

const describeSkipAWS = process.env.AWS_ON_AIR ? describe.skip : describe;

async function deleteBucket(s3, bucket) {
    try {
        await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
    } catch (err) {
        // eslint-disable-next-line no-console
        console.log(err);
    }
}

describeSkipAWS('GET bucket location ', () => {
    withV4(sigCfg => {
        const clientConfig = getConfig('default', sigCfg);
        const s3 = new S3Client(clientConfig);
        const otherAccountConfig = getConfig('lisa', {});
        const otherAccountS3 = new S3Client(otherAccountConfig);
        const locationConstraints = config.locationConstraints;
        Object.keys(locationConstraints).forEach(
        location => {
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
                before(async () => {
                    await s3.send(new CreateBucketCommand({
                        Bucket: bucketName,
                        CreateBucketConfiguration: {
                            LocationConstraint: location,
                        },
                    }));
                });
                after(() => deleteBucket(s3, bucketName));

                it(`should return location configuration: ${location} ` +
                'successfully', async () => {
                    const data = await s3.send(new GetBucketLocationCommand({ Bucket: bucketName }));
                    assert.deepStrictEqual(data.LocationConstraint, location);
                });
            });
        });

        describe('with location us-east-1', () => {
            before(() => s3.send(new CreateBucketCommand({
                Bucket: bucketName,
                CreateBucketConfiguration: {
                    LocationConstraint: 'us-east-1',
                },
            })));

            afterEach(() =>  s3.send(new DeleteBucketCommand({ Bucket: bucketName })));

            it('should return empty location', async () => {
                const data = await s3.send(new GetBucketLocationCommand({ Bucket: bucketName }));
                const expectedLocation = data.LocationConstraint || '';
                assert.deepStrictEqual(expectedLocation, '');
            });
        });

        describe('without location configuration', () => {
            after(() => s3.send(new DeleteBucketCommand({ Bucket: bucketName })));

            it('should return request endpoint as location', async () => {
                await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
                const host = clientConfig.endpoint?.hostname || clientConfig.endpoint?.host || '127.0.0.1:8000';
                let endpoint = config.restEndpoints[host];
                if (endpoint === 'us-east-1') {
                    endpoint = '';
                }
                const data = await s3.send(new GetBucketLocationCommand({ Bucket: bucketName }));
                assert.strictEqual(data.LocationConstraint, endpoint);
            });
        });

        describe('with location configuration', () => {
            before(() => s3.send(new CreateBucketCommand({
                Bucket: bucketName,
                CreateBucketConfiguration: {
                    LocationConstraint: 'us-east-1',
                },
            })));

            after(() => s3.send(new DeleteBucketCommand({ Bucket: bucketName })));

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
