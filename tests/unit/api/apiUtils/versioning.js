const assert = require('assert');

const { errors, versioning } = require('arsenal');
const { config } = require('../../../../lib/Config');
const INF_VID = versioning.VersionID.getInfVid(config.replicationGroupId);

const { processVersioningState, getMasterState,
        preprocessingVersioningDelete } =
      require('../../../../lib/api/apiUtils/object/versioning');

describe('versioning helpers', () => {
    describe('getMasterState+processVersioningState', () => {
        [
            {
                // no prior version exists
                objMD: null,
                versioningEnabledExpectedRes: {
                    options: {
                        versioning: true,
                    },
                },
                versioningSuspendedExpectedRes: {
                    options: {
                        isNull: true,
                        versionId: '',
                    },
                },
            },
            {
                // prior non-null object version exists
                objMD: {
                    versionId: 'v1',
                },
                versioningEnabledExpectedRes: {
                    options: {
                        versioning: true,
                    },
                },
                versioningSuspendedExpectedRes: {
                    options: {
                        isNull: true,
                        versionId: '',
                    },
                },
            },
            {
                // prior null object version exists
                objMD: {
                    versionId: 'vnull',
                    isNull: true,
                },
                versioningEnabledExpectedRes: {
                    options: {
                        versioning: true,
                        nullVersionId: 'vnull',
                    },
                    // instruct to first copy the null version onto a
                    // newly created version key preserving the version ID
                    storeOptions: {
                        isNull: true,
                        versionId: 'vnull',
                    },
                },
                versioningSuspendedExpectedRes: {
                    options: {
                        isNull: true,
                        versionId: '',
                    },
                    delOptions: {
                        versionId: 'vnull',
                    },
                },
            },
            {
                // prior object exists, put before versioning was first enabled
                objMD: {},
                versioningEnabledExpectedRes: {
                    options: {
                        versioning: true,
                        nullVersionId: INF_VID,
                    },
                    // instruct to first copy the null version onto a
                    // newly created version key as the oldest version
                    storeOptions: {
                        isNull: true,
                        versionId: INF_VID,
                    },
                },
                versioningSuspendedExpectedRes: {
                    options: {
                        isNull: true,
                        versionId: '',
                    },
                },
            },
            {
                // prior non-null object version exists with ref to null version
                objMD: {
                    versionId: 'v1',
                    nullVersionId: 'vnull',
                },
                versioningEnabledExpectedRes: {
                    options: {
                        versioning: true,
                        nullVersionId: 'vnull',
                    },
                },
                versioningSuspendedExpectedRes: {
                    options: {
                        isNull: true,
                        versionId: '',
                    },
                    delOptions: {
                        versionId: 'vnull',
                    },
                },
            },
        ].forEach(testCase =>
            ['Enabled', 'Suspended'].forEach(versioningStatus => it(
            `with objMD ${JSON.stringify(testCase.objMD)} and versioning ` +
            `Status=${versioningStatus}`, () => {
                const mst = getMasterState(testCase.objMD);
                // stringify and parse to get rid of the "undefined"
                // properties, artifacts of how the function builds the
                // result
                const res = JSON.parse(
                    JSON.stringify(
                        processVersioningState(mst, versioningStatus)
                    )
                );
                const expectedRes =
                      testCase[`versioning${versioningStatus}ExpectedRes`];
                assert.deepStrictEqual(res, expectedRes);
            })));
    });

    describe('preprocessingVersioningDelete', () => {
        [
            {
                objMD: {
                    versionId: 'v1',
                },
                // no reqVersionId: no delete action
                expectedRes: {},
            },
            {
                // delete non-null object version
                objMD: {
                    versionId: 'v1',
                },
                reqVersionId: 'v1',
                expectedRes: {
                    deleteData: true,
                    versionId: 'v1',
                },
            },
            {
                // delete null object version
                objMD: {
                    versionId: 'vnull',
                    isNull: true,
                },
                reqVersionId: 'null',
                expectedRes: {
                    deleteData: true,
                    versionId: 'vnull',
                },
            },
            {
                // delete object put before versioning was first enabled
                objMD: {},
                reqVersionId: 'null',
                expectedRes: {
                    deleteData: true,
                },
            },
            {
                // delete non-null object version with ref to null version
                objMD: {
                    versionId: 'v1',
                    nullVersionId: 'vnull',
                },
                reqVersionId: 'v1',
                expectedRes: {
                    deleteData: true,
                    versionId: 'v1',
                },
            },
            {
                // delete null object version from ref to null version
                objMD: {
                    versionId: 'v1',
                    nullVersionId: 'vnull',
                },
                reqVersionId: 'null',
                expectedRes: {
                    deleteData: true,
                    versionId: 'vnull',
                },
            },
            {
                // delete null version that does not exist
                objMD: {
                    versionId: 'v1',
                },
                reqVersionId: 'null',
                expectedError: errors.NoSuchKey,
            },
        ].forEach(testCase => it(
        `with objMD ${JSON.stringify(testCase.objMD)} and ` +
        `reqVersionId="${testCase.reqVersionId}"`, done => {
            const mockBucketMD = {
                getVersioningConfiguration: () => ({ Status: 'Enabled' }),
            };
            preprocessingVersioningDelete(
                'foobucket', mockBucketMD, testCase.objMD,
                testCase.reqVersionId, null, (err, options) => {
                    if (testCase.expectedError) {
                        assert.strictEqual(err, testCase.expectedError);
                    } else {
                        assert.ifError(err);
                        assert.deepStrictEqual(options, testCase.expectedRes);
                    }
                    done();
                });
        }));
    });
});
