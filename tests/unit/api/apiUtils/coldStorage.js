const assert = require('assert');

const { errors } = require('arsenal');
const { validatePutVersionId } = require('../../../../lib/api/apiUtils/object/coldStorage');
const { DummyRequestLogger } = require('../../helpers');
const log = new DummyRequestLogger();
const oneDay = 24 * 60 * 60 * 1000;

describe('cold storage', () => {
    describe('validatePutVersionId', () => {
        [
            {
                description: 'should return NoSuchKey if object metadata is empty',
                expectedRes: errors.NoSuchKey,
            },
            {
                description: 'should return NoSuchVersion if object md is empty and version id is provided',
                expectedRes: errors.NoSuchVersion,
                versionId: '123',
            },
            {
                description: 'should return MethodNotAllowed if object is a delete marker',
                objMD: {
                    isDeleteMarker: true,
                },
                expectedRes: errors.MethodNotAllowed,
            },
            {
                description: 'should return InvalidObjectState if object data is not stored in cold location',
                objMD: {
                    dataStoreName: 'us-east-1',
                },
                expectedRes: errors.InvalidObjectState,
            },
            {
                description: 'should return InvalidObjectState if object is not archived',
                objMD: {
                    dataStoreName: 'location-dmf-v1',
                },
                expectedRes: errors.InvalidObjectState,
            },
            {
                description: 'should return InvalidObjectState if object is already restored',
                objMD: {
                    dataStoreName: 'location-dmf-v1',
                    archive: {
                        restoreRequestedAt: new Date(0),
                        restoreRequestedDays: 5,
                        restoreCompletedAt: new Date(1000),
                        restoreWillExpireAt: new Date(1000 + 5 * oneDay),
                    },
                },
                expectedRes: errors.InvalidObjectState,
            },
            {
                description: 'should pass if object archived',
                objMD: {
                    dataStoreName: 'location-dmf-v1',
                    archive: {
                        restoreRequestedAt: new Date(0),
                        restoreRequestedDays: 5,
                    },
                },
                expectedRes: undefined,
            },
        ].forEach(testCase => it(testCase.description, () => {
            const res = validatePutVersionId(testCase.objMD, testCase.versionId, log);
            assert.deepStrictEqual(res, testCase.expectedRes);
        }));
    });
});
