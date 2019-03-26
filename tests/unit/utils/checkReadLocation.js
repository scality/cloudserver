const assert = require('assert');

const { ConfigObject } = require('../../../lib/Config');
const checkReadLocation =
    require('../../../lib/api/apiUtils/object/checkReadLocation');

const locationConstraints = {  // eslint-disable-line quote-props
    bucketmatch: {
        type: 'aws_s3',
        legacyAwsBehavior: true,
        details: {
            bucketMatch: true,
        },
    },
    nobucketmatch: {
        type: 'aws_s3',
        legacyAwsBehavior: true,
        details: {
            bucketMatch: false,
        },
    },
    'us-east-1': {
        type: 'file',
        legacyAwsBehavior: true,
        details: {},
    },
};

const bucket = 'testBucket';
const key = 'objectKey';

describe('Testing checkReadLocation', () => {
    let config;

    beforeAll(() => {
        config = new ConfigObject();
        config.setLocationConstraints(locationConstraints);
    });

    test('should return null if location does not exist', () => {
        const testResult = checkReadLocation(
            config, 'nonexistloc', key, bucket);
        assert.deepStrictEqual(testResult, null);
    });

    test('should return correct results for bucketMatch true location', () => {
        const testResult = checkReadLocation(
            config, 'bucketmatch', key, bucket);
        const expectedResult = {
            location: 'bucketmatch',
            key,
            locationType: 'aws_s3',
        };
        assert.deepStrictEqual(testResult, expectedResult);
    });

    test('should return correct results for bucketMatch false location', () => {
        const testResult = checkReadLocation(
            config, 'nobucketmatch', key, bucket);
        const expectedResult = {
            location: 'nobucketmatch',
            key: `${bucket}/${key}`,
            locationType: 'aws_s3',
        };
        assert.deepStrictEqual(testResult, expectedResult);
    });
});
