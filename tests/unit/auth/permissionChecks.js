const assert = require('assert');
const { checkBucketAcls, checkObjectAcls } = require('../../../lib/api/apiUtils/authorization/permissionChecks');
const constants = require('../../../constants');

const { bucketOwnerActions } = constants;

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

