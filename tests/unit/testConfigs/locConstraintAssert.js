import assert from 'assert';
import { locationConstraintAssert } from '../../../lib/Config';

class LocationConstraint {
    constructor(type, legacyAwsBehavior, details) {
        this.type = type || 'scality';
        this.legacyAwsBehavior = legacyAwsBehavior || false;
        this.details = details || {
            awsEndpoint: 's3.amazonaws.com',
            bucketName: 'tester',
            credentialsProfile: 'default',
        };
    }
}

describe('locationConstraintAssert', () => {
    it('should throw error if locationConstraints is not an object', () => {
        assert.throws(() => {
            locationConstraintAssert('');
        },
        /bad config: locationConstraints must be an object/);
    });
    it('should throw error if any location constraint is not an object', () => {
        assert.throws(() => {
            locationConstraintAssert({ notObject: '' });
        },
        err => {
            assert.strictEqual(err.message, 'bad config: ' +
                'locationConstraints[region] must be an object');
            return true;
        });
    });
    it('should throw error if type is not a string', () => {
        const locationConstraint = new LocationConstraint(42);
        assert.throws(() => {
            locationConstraintAssert({ 'scality-east': locationConstraint });
        },
        /bad config: locationConstraints[region].type is mandatory/ +
            /and must be a string/);
    });
    it('should throw error if type is not mem/file/scality', () => {
        const locationConstraint = new LocationConstraint('notSupportedType');
        assert.throws(() => {
            locationConstraintAssert({ 'scality-east': locationConstraint });
        },
        /bad config: locationConstraints[region].type must be/ +
            /one of mem,file,scality/);
    });
    it('should throw error if legacyAwsBehavior is not a boolean', () => {
        const locationConstraint = new LocationConstraint('scality', 42);
        assert.throws(() => {
            locationConstraintAssert({ 'scality-east': locationConstraint });
        },
        /bad config: locationConstraints[region].legacyAwsBehavior / +
            /is mandatory and must be a boolean/);
    });
    it('should throw error if details is not an object', () => {
        const locationConstraint = new LocationConstraint('scality', false, 42);
        assert.throws(() => {
            locationConstraintAssert({ 'scality-east': locationConstraint });
        },
        /bad config: locationConstraints[region].details is / +
            /mandatory and must be an object/);
    });
    it('should throw error if awsEndpoint is not a string', () => {
        const locationConstraint = new LocationConstraint('scality', false,
            {
                awsEndpoint: 42,
            });
        assert.throws(() => {
            locationConstraintAssert({ 'scality-east': locationConstraint });
        },
        /bad config: awsEndpoint must be a string/);
    });
    it('should throw error if bucketName is not a string', () => {
        const locationConstraint = new LocationConstraint('scality', false,
            {
                awsEndpoint: 's3.amazonaws.com',
                bucketName: 42,
            });
        assert.throws(() => {
            locationConstraintAssert({ 'scality-east': locationConstraint });
        },
        /bad config: bucketName must be a string/);
    });
    it('should throw error if credentialsProfile is not a string', () => {
        const locationConstraint = new LocationConstraint('scality', false,
            {
                awsEndpoint: 's3.amazonaws.com',
                bucketName: 'premadebucket',
                credentialsProfile: 42,
            });
        assert.throws(() => {
            locationConstraintAssert({ 'scality-east': locationConstraint });
        },
        /bad config: credentialsProfile must be a string/);
    });
});
