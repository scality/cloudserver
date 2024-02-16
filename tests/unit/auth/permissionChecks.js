const assert = require('assert');
const {
    checkBucketAcls,
    checkObjectAcls,
    validatePolicyConditions,
} = require('../../../lib/api/apiUtils/authorization/permissionChecks');
const constants = require('../../../constants');

const { bucketOwnerActions, logId } = constants;

describe('checkBucketAcls', () => {
    const mockBucket = {
        getOwner: () => 'ownerId',
        getAcl: () => ({
            Canned: '',
            FULL_CONTROL: [],
            READ: [],
            READ_ACP: [],
            WRITE: [],
            WRITE_ACP: [],
        }),
    };

    const testScenarios = [
        {
            description: 'should return true if bucket owner matches canonicalID',
            input: {
                bucketAcl: {}, requestType: 'anyType', canonicalID: 'ownerId', mainApiCall: 'anyApiCall',
            },
            expected: true,
        },
        {
            description: 'should return true for objectGetTagging when mainApiCall is objectGet',
            input: {
                bucketAcl: {}, requestType: 'objectGetTagging', canonicalID: 'anyId', mainApiCall: 'objectGet',
            },
            expected: true,
        },
        {
            description: 'should return true for objectPutTagging when mainApiCall is objectPut',
            input: {
                bucketAcl: {}, requestType: 'objectPutTagging', canonicalID: 'anyId', mainApiCall: 'objectPut',
            },
            expected: true,
        },
        {
            description: 'should return true for objectPutLegalHold when mainApiCall is objectPut',
            input: {
                bucketAcl: {}, requestType: 'objectPutLegalHold', canonicalID: 'anyId', mainApiCall: 'objectPut',
            },
            expected: true,
        },
        {
            description: 'should return true for objectPutRetention when mainApiCall is objectPut',
            input: {
                bucketAcl: {}, requestType: 'objectPutRetention', canonicalID: 'anyId', mainApiCall: 'objectPut',
            },
            expected: true,
        },
        {
            description: 'should return true for bucketGet if canned acl is public-read-write',
            input: {
                bucketAcl: { Canned: 'public-read-write' },
                requestType: 'bucketGet',
                canonicalID: 'anyId',
                mainApiCall: 'anyApiCall',
            },
            expected: true,
        },
        {
            description: 'should return true for bucketGet if canned acl is authenticated-read and id is not publicId',
            input: {
                bucketAcl: { Canned: 'authenticated-read' },
                requestType: 'bucketGet',
                canonicalID: 'anyIdNotPublic',
                mainApiCall: 'anyApiCall',
            },
            expected: true,
        },
        {
            description: 'should return true for bucketHead if canned acl is public-read',
            input: {
                bucketAcl: { Canned: 'public-read' },
                requestType: 'bucketHead',
                canonicalID: 'anyId',
                mainApiCall: 'anyApiCall',
            },
            expected: true,
        },
        {
            description: 'should return false for bucketPut even if canonicalID has FULL_CONTROL and write access ',
            input: {
                bucketAcl: {
                    FULL_CONTROL: ['anyId'],
                    WRITE: ['anyId'],
                },
                requestType: 'bucketPut',
                canonicalID: 'anyId',
                mainApiCall: 'anyApiCall',
            },
            expected: false,
        },
        {
            description: 'should return true for log-delivery-write ACL when canonicalID matches logId',
            input: {
                bucketAcl: { Canned: 'log-delivery-write' },
                requestType: 'bucketGetACL',
                canonicalID: logId,
                mainApiCall: 'anyApiCall',
            },
            expected: true,
        },
        {
            description: 'should return false when the canonicalID is not the owner and has no ACL permissions',
            input: {
                bucketAcl: {
                    FULL_CONTROL: ['someOtherId'],
                    WRITE: ['someOtherId'],
                },
                requestType: 'objectPut',
                canonicalID: 'anyId',
                mainApiCall: 'anyApiCall',
            },
            expected: false,
        },
        {
            description: 'should return false for bucketPutACL if canonicalID does not have ACL permissions',
            input: {
                bucketAcl: {
                    FULL_CONTROL: ['someOtherId'],
                    WRITE_ACP: ['someOtherId'],
                },
                requestType: 'bucketPutACL',
                canonicalID: 'anyId',
                mainApiCall: 'anyApiCall',
            },
            expected: false,
        },
        {
            description: 'should return true for bucketGet if canonicalID has FULL_CONTROL access',
            input: {
                bucketAcl: { FULL_CONTROL: ['anyId'], READ: [] },
                requestType: 'bucketGet',
                canonicalID: 'anyId',
                mainApiCall: 'anyApiCall',
            },
            expected: true,
        },
        {
            description: 'should return true for bucketGetACL if canonicalID has FULL_CONTROL',
            input: {
                bucketAcl: { FULL_CONTROL: ['anyId'], READ_ACP: [] },
                requestType: 'bucketGetACL',
                canonicalID: 'anyId',
                mainApiCall: 'anyApiCall',
            },
            expected: true,
        },
        {
            description: 'should return true for objectDelete if bucketAcl.Canned is public-read-write',
            input: {
                bucketAcl: { Canned: 'public-read-write' },
                requestType: 'objectDelete',
                canonicalID: 'anyId',
                mainApiCall: 'anyApiCall',
            },
            expected: true,
        },
        {
            description: 'should return true for requestType ending with "Version"',
            input: {
                bucketAcl: {},
                requestType: 'objectGetVersion',
                canonicalID: 'anyId',
                mainApiCall: 'objectGet',
            },
            expected: true,
        },
        {
            description: 'should return true for objectPutACL',
            input: {
                bucketAcl: {},
                requestType: 'objectPutACL',
                canonicalID: 'anyId',
                mainApiCall: 'anyApiCall',
            },
            expected: true,
        },
        {
            description: 'should return true for objectGetACL',
            input: {
                bucketAcl: {},
                requestType: 'objectGetACL',
                canonicalID: 'anyId',
                mainApiCall: 'anyApiCall',
            },
            expected: true,
        },
        {
            description: 'should return false for unmatched scenarios',
            input: {
                bucketAcl: {},
                requestType: 'unmatchedRequest',
                canonicalID: 'anyId',
                mainApiCall: 'anyApiCall',
            },
            expected: false,
        },
    ];

    testScenarios.forEach(scenario => {
        it(scenario.description, () => {
            // Mock the bucket based on the test scenario's input
            mockBucket.getAcl = () => scenario.input.bucketAcl;

            const result = checkBucketAcls(mockBucket,
                scenario.input.requestType, scenario.input.canonicalID, scenario.input.mainApiCall);
            assert.strictEqual(result, scenario.expected);
        });
    });
});

describe('checkObjectAcls', () => {
    const mockBucket = {
        getOwner: () => 'bucketOwnerId',
        getName: () => 'bucketName',
        getAcl: () => ({ Canned: '' }),
    };
    const mockObjectMD = {
        'owner-id': 'objectOwnerId',
        'acl': {
            Canned: '',
            FULL_CONTROL: [],
            READ: [],
            READ_ACP: [],
            WRITE: [],
            WRITE_ACP: [],
        },
    };

    it('should return true if request type is in bucketOwnerActions and bucket owner matches canonicalID', () => {
        assert.strictEqual(checkObjectAcls(mockBucket, mockObjectMD, bucketOwnerActions[0],
            'bucketOwnerId', false, false, 'anyApiCall'), true);
    });

    it('should return true if objectMD owner matches canonicalID', () => {
        assert.strictEqual(checkObjectAcls(mockBucket, mockObjectMD, 'anyType',
            'objectOwnerId', false, false, 'anyApiCall'), true);
    });

    it('should return true for objectGetTagging when mainApiCall is objectGet and conditions met', () => {
        assert.strictEqual(checkObjectAcls(mockBucket, mockObjectMD, 'objectGetTagging',
            'anyIdNotPublic', true, true, 'objectGet'), true);
    });

    it('should return false if no acl provided in objectMD', () => {
        const objMDWithoutAcl = Object.assign({}, mockObjectMD);
        delete objMDWithoutAcl.acl;
        assert.strictEqual(checkObjectAcls(mockBucket, objMDWithoutAcl, 'anyType',
            'anyId', false, false, 'anyApiCall'), false);
    });

    const tests = [
        {
            acl: 'public-read', reqType: 'objectGet', id: 'anyIdNotPublic', expected: true,
        },
        {
            acl: 'public-read-write', reqType: 'objectGet', id: 'anyIdNotPublic', expected: true,
        },
        {
            acl: 'authenticated-read', reqType: 'objectGet', id: 'anyIdNotPublic', expected: true,
        },
        {
            acl: 'bucket-owner-read', reqType: 'objectGet', id: 'bucketOwnerId', expected: true,
        },
        {
            acl: 'bucket-owner-full-control', reqType: 'objectGet', id: 'bucketOwnerId', expected: true,
        },
        {
            aclList: ['someId', 'anyIdNotPublic'],
            aclField: 'FULL_CONTROL',
            reqType: 'objectGet',
            id: 'anyIdNotPublic',
            expected: true,
        },
        {
            aclList: ['someId', 'anyIdNotPublic'],
            aclField: 'READ',
            reqType: 'objectGet',
            id: 'anyIdNotPublic',
            expected: true,
        },
        { reqType: 'objectPut', id: 'anyId', expected: true },
        { reqType: 'objectDelete', id: 'anyId', expected: true },
        {
            aclList: ['anyId'], aclField: 'FULL_CONTROL', reqType: 'objectPutACL', id: 'anyId', expected: true,
        },
        {
            aclList: ['anyId'], aclField: 'FULL_CONTROL', reqType: 'objectGetACL', id: 'anyId', expected: true,
        },
        {
            acl: '', reqType: 'objectGet', id: 'randomId', expected: false,
        },
    ];

    tests.forEach(test => {
        it(`should return ${test.expected} for ${test.reqType} with ACL as ${test.acl
            || (`${test.aclField}:${JSON.stringify(test.aclList)}`)}`, () => {
            if (test.acl) {
                mockObjectMD.acl.Canned = test.acl;
            } else if (test.aclList && test.aclField) {
                mockObjectMD.acl[test.aclField] = test.aclList;
            }

            assert.strictEqual(
                checkObjectAcls(mockBucket, mockObjectMD, test.reqType, test.id, false, false, 'anyApiCall'),
                test.expected,
            );
        });
    });
});

describe('validatePolicyConditions', () => {
    const tests = [
        {
            description: 'should return null if conditions are empty',
            inputPolicy: {},
            expected: null,
        },
        {
            description: 'Should return null if conditions have a valid IP address',
            inputPolicy: {
                Statement: [{
                    Condition: {
                        IpAddress: { 'aws:SourceIp': '192.168.1.1/24' },
                    },
                }],
            },
            expected: null,
        },
        {
            description: 'Should return "Invalid IP address in Conditions" ' +
            'if conditions have an invalid IP address',
            inputPolicy: {
                Statement: [{
                    Condition: {
                        IpAddress: { 'aws:SourceIp': '123' },
                    },
                }],
            },
            expected: 'Invalid IP address in Conditions',
        },
        {
            description: 'Should return "Policy has an invalid condition key" if a' +
            ' condition key does not start with \'aws:\' and is not recognized',
            inputPolicy: {
                Statement: [{
                    Condition: {
                        NotARealCondition: { 's3:prefix': 'something' },
                    },
                }],
            },
            expected: 'Policy has an invalid condition key',
        },
        {
            description: 'Should return null if a statement in the policy does not contain a \'Condition\' block',
            inputPolicy: {
                Statement: [{}],
            },
            expected: null,
        },
        {
            description: 'Should return a relevant error message ' +
            'if the condition value is an empty string',
            inputPolicy: {
                Statement: [{
                    Condition: {
                        IpAddress: { 'aws:SourceIp': '' },
                    },
                }],
            },
            expected: 'Invalid IP address in Conditions',
        },
        {
            description: 'Should accept arrays of IPs',
            inputPolicy: {
                Statement: [{
                    Condition: {
                        IpAddress: {
                            'aws:SourceIp': [
                                '10.0.11.0/24',
                                '10.0.1.0/24',
                            ],
                        },
                    },
                }],
            },
            expected: null,
        },
        {
            description: 'Should return relevant error if one of the IPs in the array is invalid',
            inputPolicy: {
                Statement: [{
                    Condition: {
                        IpAddress: {
                            'aws:SourceIp': [
                                '10.0.11.0/24',
                                '123',
                            ],
                        },
                    },
                }],
            },
            expected: 'Invalid IP address in Conditions',
        },
        {
            description: 'Should not return error if array value in IP condition is empty', // this is AWS behavior
            inputPolicy: {
                Statement: [{
                    Condition: {
                        IpAddress: {
                            'aws:SourceIp': [],
                        },
                    },
                }],
            },
            expected: null,
        },
        {
            description: 'Should return null or a relevant error message ' +
            'if multiple conditions are provided in a single statement',
            inputPolicy: {
                Statement: [{
                    Condition: {
                        IpAddress: { 'aws:SourceIp': '192.168.1.1' },
                        NotARealCondition: { 's3:prefix': 'something' },
                    },
                }],
            },
            expected: 'Policy has an invalid condition key',
        },
        {
            description: 'Should test the function with multiple statements, each having various conditions',
            inputPolicy: {
                Statement: [
                    {
                        Condition: {
                            IpAddress: { 'aws:SourceIp': '192.168.1.1' },
                        },
                    },
                    {
                        Condition: {
                            NotARealCondition: { 's3:prefix': 'something' },
                        },
                    },
                ],
            },
            expected: 'Policy has an invalid condition key',
        },
        {
            description: 'Should return null if conditions have a valid IPv6 address',
            inputPolicy: {
                Statement: [{
                    Condition: {
                        IpAddress: { 'aws:SourceIp': '2001:0db8:85a3:0000:0000:8a2e:0370:7334' },
                    },
                }],
            },
            expected: null,
        },
        {
            description: 'Should return "Invalid IP address in Conditions" if conditions have an invalid IPv6 address',
            inputPolicy: {
                Statement: [{
                    Condition: {
                        IpAddress: { 'aws:SourceIp': '2001:0db8:85a3:0000:XYZZ:8a2e:0370:7334' },
                    },
                }],
            },
            expected: 'Invalid IP address in Conditions',
        },
        {
            description: 'Should return "Invalid IP address in Conditions" if conditions'
            + ' have an IPv6 address with unusual and invalid notation',
            inputPolicy: {
                Statement: [{
                    Condition: {
                        IpAddress: { 'aws:SourceIp': '2001::85a3::8a2e' },
                    },
                }],
            },
            expected: 'Invalid IP address in Conditions',
        },
    ];

    tests.forEach(test => {
        it(test.description, () => {
            const result = validatePolicyConditions(test.inputPolicy);
            if (test.expected === null) {
                assert.strictEqual(result, test.expected);
                return;
            }
            assert.strictEqual(result.description, test.expected);
        });
    });
});
