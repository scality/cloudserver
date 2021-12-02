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
                description: 'no prior version exists',
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
                description: 'prior non-null object version exists',
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
                description: 'prior MPU object non-null version exists',
                objMD: {
                    versionId: 'v1',
                    uploadId: 'fooUploadId',
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
                description: 'prior null object version exists',
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
                description: 'prior MPU object null version exists',
                objMD: {
                    versionId: 'vnull',
                    isNull: true,
                    uploadId: 'fooUploadId',
                },
                versioningEnabledExpectedRes: {
                    options: {
                        versioning: true,
                        nullVersionId: 'vnull',
                        nullUploadId: 'fooUploadId',
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
                        replayId: 'fooUploadId',
                    },
                },
            },
            {
                description:
                'prior object exists, put before versioning was first enabled',
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
                description: 'prior MPU object exists, put before versioning ' +
                    'was first enabled',
                objMD: {
                    uploadId: 'fooUploadId',
                },
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
                description:
                'prior non-null object version exists with ref to null version',
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
            {
                description: 'prior MPU object non-null version exists with ' +
                    'ref to null version',
                objMD: {
                    versionId: 'v1',
                    uploadId: 'fooUploadId',
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
            {
                description: 'prior object non-null version exists with ' +
                    'ref to MPU null version',
                objMD: {
                    versionId: 'v1',
                    nullVersionId: 'vnull',
                    nullUploadId: 'nullFooUploadId',
                },
                versioningEnabledExpectedRes: {
                    options: {
                        versioning: true,
                        nullVersionId: 'vnull',
                        nullUploadId: 'nullFooUploadId',
                    },
                },
                versioningSuspendedExpectedRes: {
                    options: {
                        isNull: true,
                        versionId: '',
                    },
                    delOptions: {
                        versionId: 'vnull',
                        replayId: 'nullFooUploadId',
                    },
                },
            },
        ].forEach(testCase =>
            ['Enabled', 'Suspended'].forEach(versioningStatus => it(
            `${testCase.description}, versioning Status=${versioningStatus}`,
            () => {
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
                description: 'no reqVersionId: no delete action',
                objMD: {
                    versionId: 'v1',
                },
                expectedRes: {},
            },
            {
                description: 'delete non-null object version',
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
                description: 'delete MPU object non-null version',
                objMD: {
                    versionId: 'v1',
                    uploadId: 'fooUploadId',
                },
                reqVersionId: 'v1',
                expectedRes: {
                    deleteData: true,
                    versionId: 'v1',
                    replayId: 'fooUploadId',
                },
            },
            {
                description: 'delete null object version',
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
                description: 'delete MPU object null version',
                objMD: {
                    versionId: 'vnull',
                    isNull: true,
                    uploadId: 'fooUploadId',
                },
                reqVersionId: 'null',
                expectedRes: {
                    deleteData: true,
                    versionId: 'vnull',
                    replayId: 'fooUploadId',
                },
            },
            {
                description:
                'delete object put before versioning was first enabled',
                objMD: {},
                reqVersionId: 'null',
                expectedRes: {
                    deleteData: true,
                },
            },
            {
                description:
                'delete MPU object put before versioning was first enabled',
                objMD: {
                    uploadId: 'fooUploadId',
                },
                reqVersionId: 'null',
                expectedRes: {
                    deleteData: true,
                },
            },
            {
                description:
                'delete non-null object version with ref to null version',
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
                description:
                'delete MPU object non-null version with ref to null version',
                objMD: {
                    versionId: 'v1',
                    uploadId: 'fooUploadId',
                    nullVersionId: 'vnull',
                },
                reqVersionId: 'v1',
                expectedRes: {
                    deleteData: true,
                    versionId: 'v1',
                    replayId: 'fooUploadId',
                },
            },
            {
                description:
                'delete non-null object version with ref to MPU null version',
                objMD: {
                    versionId: 'v1',
                    nullVersionId: 'vnull',
                    nullUploadId: 'nullFooUploadId',
                },
                reqVersionId: 'v1',
                expectedRes: {
                    deleteData: true,
                    versionId: 'v1',
                },
            },
            {
                description:
                'delete null object version from ref to null version',
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
                description:
                'delete MPU object null version from ref to null version',
                objMD: {
                    versionId: 'v1',
                    nullVersionId: 'vnull',
                    nullUploadId: 'nullFooUploadId',
                },
                reqVersionId: 'null',
                expectedRes: {
                    deleteData: true,
                    versionId: 'vnull',
                    replayId: 'nullFooUploadId',
                },
            },
            {
                description: 'delete null version that does not exist',
                objMD: {
                    versionId: 'v1',
                },
                reqVersionId: 'null',
                expectedError: errors.NoSuchKey,
            },
        ].forEach(testCase => it(testCase.description, done => {
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
