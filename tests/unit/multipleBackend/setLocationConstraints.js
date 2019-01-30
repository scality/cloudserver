const assert = require('assert');
const { config } = require('../../../lib/Config');
const parseLC = require('../../../lib/data/locationConstraintParser');

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

    it('should update location constraint config', () => {
        assert.notDeepStrictEqual(config.locationConstraints, expectedLCs);
        config.setLocationConstraints(expectedLCs);
        assert.deepStrictEqual(config.locationConstraints, expectedLCs);
    });

    it('should update multiple backend clients', () => {
        assert.strictEqual(parseLC(config)[newLCKey], undefined);
        config.setLocationConstraints(expectedLCs);
        assert(parseLC(config)[newLCKey]);
        assert.strictEqual(parseLC(config)[newLCKey].clientType, 'aws_s3');
    });
});
