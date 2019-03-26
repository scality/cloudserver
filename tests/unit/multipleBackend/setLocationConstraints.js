const assert = require('assert');
const { parseLC } = require('arsenal').storage.data;

const { config } = require('../../../lib/Config');

const newLC = {};
const newLCKey = `test_location_constraint_${Date.now()}`;
newLC[newLCKey] = {
    type: 'aws_s3',
    legacyAwsBehavior: true,
    details: {
        awsEndpoint: 's3.amazonaws.com',
        bucketName: `test-bucket-${Date.now()}`,
        bucketMatch: true,
        credentialsProfile: 'default',
    },
};
const originalLCs = Object.assign({}, config.locationConstraints);
const expectedLCs = Object.assign({}, config.locationConstraints, newLC);

describe('Config::setLocationConstraints', () => {
    afterEach(() => config.setLocationConstraints(originalLCs));

    test('should update location constraint config', () => {
        assert.notDeepStrictEqual(config.locationConstraints, expectedLCs);
        config.setLocationConstraints(expectedLCs);
        assert.deepStrictEqual(config.locationConstraints, expectedLCs);
    });

    test('should update multiple backend clients', () => {
        expect(parseLC(config)[newLCKey]).toBe(undefined);
        config.setLocationConstraints(expectedLCs);
        expect(parseLC(config)[newLCKey]).toBeTruthy();
        expect(parseLC(config)[newLCKey].clientType).toBe('aws_s3');
    });
});
