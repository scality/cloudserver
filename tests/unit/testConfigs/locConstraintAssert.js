const assert = require('assert');
const { locationConstraintAssert } = require('../../../lib/Config');

class LocationConstraint {
    constructor(type, objectId, legacyAwsBehavior, details, sizeLimit) {
        this.type = type || 'scality';
        this.objectId = objectId;
        this.legacyAwsBehavior = legacyAwsBehavior || false;
        this.sizeLimitGB = sizeLimit || undefined;
        this.details = Object.assign({}, {
            awsEndpoint: 's3.amazonaws.com',
            bucketName: 'tester',
            credentialsProfile: 'default',
            region: 'us-west-1',
        }, details || {});
    }
}

function getAzureDetails(replaceParams) {
    return Object.assign({
        azureStorageEndpoint: 'https://fakeaccountname.blob.core.fake.net/',
        azureStorageAccountName: 'fakeaccountname',
        azureStorageAccessKey: 'Fake00Key123',
        bucketMatch: false,
        azureContainerName: 'test',
    }, replaceParams);
}

// FIXME: most of tests using a line-wrapped regexp are broken,
// because such regexp is converted to a string which does not enforce
// the check of the message. A more durable solution would be use
// 'joi' for config parsing.

describe('locationConstraintAssert', () => {
    test('should throw error if locationConstraints is not an object', () => {
        expect(() => {
            locationConstraintAssert('');
        }).toThrow();
    });
    test('should throw error if any location constraint is not an object', () => {
        expect(() => {
            locationConstraintAssert({ notObject: '' });
        }).toThrow();
    });
    test('should throw error if type is not a string', () => {
        const locationConstraint = new LocationConstraint(42, 'locId');
        expect(() => {
            locationConstraintAssert({ 'scality-east': locationConstraint });
        }).toThrow();
    });
    test('should throw error if type is not mem/file/scality', () => {
        const locationConstraint = new LocationConstraint(
            'notSupportedType', 'locId');
        expect(() => {
            locationConstraintAssert({ 'scality-east': locationConstraint });
        }).toThrow();
    });
    test('should throw error if legacyAwsBehavior is not a boolean', () => {
        const locationConstraint = new LocationConstraint(
            'scality', 'locId', 42);
        expect(() => {
            locationConstraintAssert({ 'scality-east': locationConstraint });
        }).toThrow();
    });
    test('should throw error if details is not an object', () => {
        const locationConstraint =
              new LocationConstraint('scality', 'locId', false, 42);
        expect(() => {
            locationConstraintAssert({ 'scality-east': locationConstraint });
        }).toThrow();
    });
    test('should throw error if awsEndpoint is not a string', () => {
        const locationConstraint = new LocationConstraint(
            'scality', 'locId', false,
            {
                awsEndpoint: 42,
            });
        expect(() => {
            locationConstraintAssert({ 'scality-east': locationConstraint });
        }).toThrow();
    });
    test('should throw error if bucketName is not a string', () => {
        const locationConstraint = new LocationConstraint(
            'scality', 'locId', false,
            {
                awsEndpoint: 's3.amazonaws.com',
                bucketName: 42,
            });
        expect(() => {
            locationConstraintAssert({ 'scality-east': locationConstraint });
        }).toThrow();
    });
    test('should throw error if credentialsProfile is not a string', () => {
        const locationConstraint = new LocationConstraint(
            'scality', 'locId', false,
            {
                awsEndpoint: 's3.amazonaws.com',
                bucketName: 'premadebucket',
                credentialsProfile: 42,
            });
        expect(() => {
            locationConstraintAssert({ 'scality-east': locationConstraint });
        }).toThrow();
    });
    test('should throw error if region is not a string', () => {
        const locationConstraint = new LocationConstraint(
            'scality', 'locId', false,
            {
                awsEndpoint: 's3.amazonaws.com',
                bucketName: 'premadebucket',
                credentialsProfile: 'zenko',
                region: 42,
            });
        expect(() => {
            locationConstraintAssert({ 'scality-east': locationConstraint });
        }).toThrow();
    });
    test('should throw error if us-east-1 not specified', () => {
        const locationConstraint = new LocationConstraint();
        expect(() => {
            locationConstraintAssert({ 'not-us-east-1': locationConstraint });
        }).toThrow();
    });
    test('should not throw error for a valid azure location constraint', () => {
        const usEast1 = new LocationConstraint(undefined, 'locId1');
        const locationConstraint = new LocationConstraint(
            'azure', 'locId2', true,
            getAzureDetails());
        expect(() => {
            locationConstraintAssert({ 'azurefaketest': locationConstraint,
            'us-east-1': usEast1 });
        }).not.toThrow();
    });
    test('should throw error if type is azure and azureContainerName is ' +
    'not specified', () => {
        const usEast1 = new LocationConstraint(undefined, 'locId1');
        const locationConstraint = new LocationConstraint(
            'azure', 'locId2', true,
            getAzureDetails({ azureContainerName: undefined }));
        expect(() => {
            locationConstraintAssert({
                'us-east-1': usEast1,
                'azurefaketest': locationConstraint,
            });
        }).toThrow();
    });
    test('should throw error if type is azure and azureContainerName is ' +
    'invalid value', () => {
        const usEast1 = new LocationConstraint(undefined, 'locId1');
        const locationConstraint = new LocationConstraint(
            'azure', 'locId2', true,
            getAzureDetails({ azureContainerName: '.' }));
        expect(() => {
            locationConstraintAssert({
                'us-east-1': usEast1,
                'azurefaketest': locationConstraint,
            });
        }).toThrow();
    });
    test('should throw error if type is azure and azureStorageEndpoint ' +
    'is not specified', () => {
        const usEast1 = new LocationConstraint(undefined, 'locId1');
        const locationConstraint = new LocationConstraint(
            'azure', 'locId2', true,
            getAzureDetails({ azureStorageEndpoint: undefined }));
        expect(() => {
            locationConstraintAssert({
                'us-east-1': usEast1,
                'azurefaketest': locationConstraint,
            });
        }).toThrow();
    });
    test('should throw error if type is azure and azureStorageAccountName ' +
    'is not specified', () => {
        const usEast1 = new LocationConstraint(undefined, 'locId1');
        const locationConstraint = new LocationConstraint(
            'azure', 'locId2', true,
            getAzureDetails({ azureStorageAccountName: undefined }));
        expect(() => {
            locationConstraintAssert({
                'us-east-1': usEast1,
                'azurefaketest': locationConstraint,
            });
        }).toThrow();
    });
    test('should throw error if type is azure and azureStorageAccountName ' +
    'is invalid value', () => {
        const usEast1 = new LocationConstraint(undefined, 'locId1');
        const locationConstraint = new LocationConstraint(
            'azure', 'locId2', true,
            getAzureDetails({ azureStorageAccountName: 'invalid!!!' }));
        expect(() => {
            locationConstraintAssert({
                'us-east-1': usEast1,
                'azurefaketest': locationConstraint,
            });
        }).toThrow();
    });
    test('should throw error if type is azure and azureStorageAccessKey ' +
    'is not specified', () => {
        const usEast1 = new LocationConstraint(undefined, 'locId1');
        const locationConstraint = new LocationConstraint(
            'azure', 'locId2', true,
            getAzureDetails({ azureStorageAccessKey: undefined }));
        expect(() => {
            locationConstraintAssert({
                'us-east-1': usEast1,
                'azurefaketest': locationConstraint,
            });
        }).toThrow();
    });
    test('should throw error if type is azure and azureStorageAccessKey ' +
    'is not a valid base64 string', () => {
        const usEast1 = new LocationConstraint(undefined, 'locId1');
        const locationConstraint = new LocationConstraint(
            'azure', 'locId2', true,
            getAzureDetails({ azureStorageAccessKey: 'invalid!!!' }));
        expect(() => {
            locationConstraintAssert({
                'us-east-1': usEast1,
                'azurefaketest': locationConstraint,
            });
        }).toThrow();
    });

    test('should set https to true by default', () => {
        const usEast1 = new LocationConstraint(undefined, 'locId1');
        const locationConstraint = new LocationConstraint(
            'aws_s3', 'locId2', true);
        expect(() => {
            locationConstraintAssert({
                'us-east-1': usEast1,
                'awshttpsDefault': locationConstraint,
            });
        }).not.toThrow();
        expect(locationConstraint.details.https).toBe(true);
    });

    test('should override default if https is set to false', () => {
        const usEast1 = new LocationConstraint(undefined, 'locId1');
        const locationConstraint = new LocationConstraint(
            'aws_s3', 'locId2', true, {
                https: false,
            });
        expect(() => {
            locationConstraintAssert({
                'us-east-1': usEast1,
                'awshttpsFalse': locationConstraint,
            });
        }).not.toThrow();
        expect(locationConstraint.details.https).toBe(false);
    });

    test('should set pathStyle config option to false by default', () => {
        const usEast1 = new LocationConstraint(undefined, 'locId1');
        const locationConstraint = new LocationConstraint(
            'aws_s3', 'locId2', true);
        expect(() => {
            locationConstraintAssert({
                'us-east-1': usEast1,
                'awsdefaultstyle': locationConstraint,
            });
        }).not.toThrow();
        expect(locationConstraint.details.pathStyle).toBe(false);
    });

    test('should override default if pathStyle is set to true', () => {
        const usEast1 = new LocationConstraint(undefined, 'locId1');
        const locationConstraint = new LocationConstraint(
            'aws_s3', 'locId2', true,
        { pathStyle: true });
        expect(() => {
            locationConstraintAssert({
                'us-east-1': usEast1,
                'awspathstyle': locationConstraint,
            });
        }).not.toThrow();
        expect(locationConstraint.details.pathStyle).toBe(true);
    });

    test('should throw error if sizeLimitGB is not a number', () => {
        const usEast1 = new LocationConstraint(undefined, 'locId1');
        const locationConstraint = new LocationConstraint(
            'aws_s3', 'locId2', true,
            null, true);
        expect(() => {
            locationConstraintAssert({
                'us-east-1': usEast1,
                'awsstoragesizelimit': locationConstraint,
            });
        }).toThrow();
    });

    test('should throw error if objectId is not set', () => {
        const usEast1 = new LocationConstraint(undefined, 'locId1');
        const locationConstraint = new LocationConstraint(
            'azure', undefined, true,
            getAzureDetails());
        expect(() => {
            locationConstraintAssert({ 'azurefaketest': locationConstraint,
            'us-east-1': usEast1 });
        }).toThrow();
    });

    test('should throw error if objectId is duplicated', () => {
        const usEast1 = new LocationConstraint(undefined, 'locId1');
        const locationConstraint = new LocationConstraint(
            'azure', 'locId1', true,
            getAzureDetails());
        expect(() => {
            locationConstraintAssert({ 'azurefaketest': locationConstraint,
            'us-east-1': usEast1 });
        }).toThrow();
    });
});
