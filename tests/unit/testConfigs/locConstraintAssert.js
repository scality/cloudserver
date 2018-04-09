const assert = require('assert');
const { locationConstraintAssert } = require('../../../lib/Config');

class LocationConstraint {
    constructor(type, legacyAwsBehavior, details) {
        this.type = type || 'scality';
        this.legacyAwsBehavior = legacyAwsBehavior || false;
        this.details = Object.assign({}, {
            awsEndpoint: 's3.amazonaws.com',
            bucketName: 'tester',
            credentialsProfile: 'default',
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
    it('should throw error if us-east-1 not specified', () => {
        const locationConstraint = new LocationConstraint();
        assert.throws(() => {
            locationConstraintAssert({ 'not-us-east-1': locationConstraint });
        },
        '/bad locationConfig: must ' +
        'include us-east-1 as a locationConstraint/');
    });
    it('should not throw error for a valid azure location constraint', () => {
        const usEast1 = new LocationConstraint();
        const locationConstraint = new LocationConstraint('azure', true,
            getAzureDetails());
        assert.doesNotThrow(() => {
            locationConstraintAssert({ 'azurefaketest': locationConstraint,
            'us-east-1': usEast1 });
        },
        '/should not throw for a valid azure location constraint/');
    });
    it('should throw error if type is azure and azureContainerName is ' +
    'not specified', () => {
        const usEast1 = new LocationConstraint();
        const locationConstraint = new LocationConstraint('azure', true,
            getAzureDetails({ azureContainerName: undefined }));
        assert.throws(() => {
            locationConstraintAssert({
                'us-east-1': usEast1,
                'azurefaketest': locationConstraint,
            });
        },
        '/bad location constraint: ' +
        '"azurefaketest" azureContainerName must be defined/');
    });
    it('should throw error if type is azure and azureContainerName is ' +
    'invalid value', () => {
        const usEast1 = new LocationConstraint();
        const locationConstraint = new LocationConstraint('azure', true,
            getAzureDetails({ azureContainerName: '.' }));
        assert.throws(() => {
            locationConstraintAssert({
                'us-east-1': usEast1,
                'azurefaketest': locationConstraint,
            });
        },
        '/bad location constraint: "azurefaketest" ' +
        'azureContainerName is an invalid container name/');
    });
    it('should throw error if type is azure and azureStorageEndpoint ' +
    'is not specified', () => {
        const usEast1 = new LocationConstraint();
        const locationConstraint = new LocationConstraint('azure', true,
            getAzureDetails({ azureStorageEndpoint: undefined }));
        assert.throws(() => {
            locationConstraintAssert({
                'us-east-1': usEast1,
                'azurefaketest': locationConstraint,
            });
        },
        '/bad location constraint: "azurefaketest" ' +
        'azureStorageEndpoint must be set in locationConfig ' +
        'or environment variable/');
    });
    it('should throw error if type is azure and azureStorageAccountName ' +
    'is not specified', () => {
        const usEast1 = new LocationConstraint();
        const locationConstraint = new LocationConstraint('azure', true,
            getAzureDetails({ azureStorageAccountName: undefined }));
        assert.throws(() => {
            locationConstraintAssert({
                'us-east-1': usEast1,
                'azurefaketest': locationConstraint,
            });
        },
        '/bad location constraint: "azurefaketest" ' +
        'azureStorageAccountName must be set in locationConfig ' +
        'or environment variable/');
    });
    it('should throw error if type is azure and azureStorageAccountName ' +
    'is invalid value', () => {
        const usEast1 = new LocationConstraint();
        const locationConstraint = new LocationConstraint('azure', true,
            getAzureDetails({ azureStorageAccountName: 'invalid!!!' }));
        assert.throws(() => {
            locationConstraintAssert({
                'us-east-1': usEast1,
                'azurefaketest': locationConstraint,
            });
        },
        '/bad location constraint: "azurefaketest" ' +
        'azureStorageAccountName "invalid!!!" is an invalid value/');
    });
    it('should throw error if type is azure and azureStorageAccessKey ' +
    'is not specified', () => {
        const usEast1 = new LocationConstraint();
        const locationConstraint = new LocationConstraint('azure', true,
            getAzureDetails({ azureStorageAccessKey: undefined }));
        assert.throws(() => {
            locationConstraintAssert({
                'us-east-1': usEast1,
                'azurefaketest': locationConstraint,
            });
        },
        '/bad location constraint: "azurefaketest" ' +
        'azureStorageAccessKey must be set in locationConfig ' +
        'or environment variable/');
    });
    it('should throw error if type is azure and azureStorageAccessKey ' +
    'is not a valid base64 string', () => {
        const usEast1 = new LocationConstraint();
        const locationConstraint = new LocationConstraint('azure', true,
            getAzureDetails({ azureStorageAccessKey: 'invalid!!!' }));
        assert.throws(() => {
            locationConstraintAssert({
                'us-east-1': usEast1,
                'azurefaketest': locationConstraint,
            });
        },
        '/bad location constraint: "azurefaketest" ' +
        'azureStorageAccessKey is not a valid base64 string/');
    });

    it('should set https to true by default', () => {
        const usEast1 = new LocationConstraint();
        const locationConstraint = new LocationConstraint('aws_s3', true);
        assert.doesNotThrow(() => {
            locationConstraintAssert({
                'us-east-1': usEast1,
                'awshttpsDefault': locationConstraint,
            });
        }, '/bad location constraint awshttpsDefault,' +
        'incorrect default config for https');
        assert.strictEqual(locationConstraint.details.https, true,
            'https config should be true');
    });

    it('should override default if https is set to false', () => {
        const usEast1 = new LocationConstraint();
        const locationConstraint = new LocationConstraint('aws_s3', true, {
            https: false,
        });
        assert.doesNotThrow(() => {
            locationConstraintAssert({
                'us-east-1': usEast1,
                'awshttpsFalse': locationConstraint,
            });
        }, '/bad location constraint awshttpsFalse,' +
        'incorrect config for https');
        assert.strictEqual(locationConstraint.details.https, false,
            'https config should be false');
    });

    it('should set pathStyle config option to false by default', () => {
        const usEast1 = new LocationConstraint();
        const locationConstraint = new LocationConstraint('aws_s3', true);
        assert.doesNotThrow(() => {
            locationConstraintAssert({
                'us-east-1': usEast1,
                'awsdefaultstyle': locationConstraint,
            });
        }, '/bad location constraint, unable to set default config');
        assert.strictEqual(locationConstraint.details.pathStyle, false,
            'pathstyle config should be false');
    });

    it('should override default if pathStyle is set to true', () => {
        const usEast1 = new LocationConstraint();
        const locationConstraint = new LocationConstraint('aws_s3', true,
        { pathStyle: true });
        assert.doesNotThrow(() => {
            locationConstraintAssert({
                'us-east-1': usEast1,
                'awspathstyle': locationConstraint,
            });
        }, '/bad location constraint, unable to set pathSytle config');
        assert.strictEqual(locationConstraint.details.pathStyle, true,
            'pathstyle config should be true');
    });
});
