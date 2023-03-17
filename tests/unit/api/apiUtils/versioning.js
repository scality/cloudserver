const assert = require('assert');

const { versioning } = require('arsenal');
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
                    delOptions: {
                        deleteData: true,
                        versionId: 'null',
                    },
                },
                versioningEnabledCompatExpectedRes: {
                    options: {
                        versioning: true,
                    },
                },
                versioningSuspendedCompatExpectedRes: {
                    options: {
                        isNull: true,
                        versionId: '',
                    },
                    delOptions: {
                        deleteData: true,
                        versionId: 'null',
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
                    delOptions: {
                        deleteData: true,
                        versionId: 'null',
                    },
                },
                versioningEnabledCompatExpectedRes: {
                    options: {
                        versioning: true,
                    },
                },
                versioningSuspendedCompatExpectedRes: {
                    options: {
                        isNull: true,
                        versionId: '',
                    },
                    delOptions: {
                        deleteData: true,
                        versionId: 'null',
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
                    delOptions: {
                        deleteData: true,
                        versionId: 'null',
                    },
                },
                versioningEnabledCompatExpectedRes: {
                    options: {
                        versioning: true,
                    },
                },
                versioningSuspendedCompatExpectedRes: {
                    options: {
                        isNull: true,
                        versionId: '',
                    },
                    delOptions: {
                        deleteData: true,
                        versionId: 'null',
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
                    },
                    // instruct to first copy the null version onto a
                    // newly created null key with version ID in its metadata
                    nullVersionId: 'vnull',
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
                versioningEnabledCompatExpectedRes: {
                    options: {
                        versioning: true,
                        extraMD: {
                            nullVersionId: 'vnull',
                        },
                    },
                    // instruct to first copy the null version onto a
                    // newly created version key preserving the version ID
                    nullVersionId: 'vnull',
                },
                versioningSuspendedCompatExpectedRes: {
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
                description: 'prior MPU object legacy null version exists',
                objMD: {
                    versionId: 'vnull',
                    isNull: true,
                    uploadId: 'fooUploadId',
                },
                versioningEnabledExpectedRes: {
                    options: {
                        versioning: true,
                    },
                    // instruct to first copy the null version onto a
                    // newly created null key with version ID in its metadata
                    nullVersionId: 'vnull',
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
                versioningEnabledCompatExpectedRes: {
                    options: {
                        versioning: true,
                        extraMD: {
                            nullVersionId: 'vnull',
                            nullUploadId: 'fooUploadId',
                        },
                    },
                    // instruct to first copy the null version onto a
                    // newly created version key preserving the version ID
                    nullVersionId: 'vnull',
                },
                versioningSuspendedCompatExpectedRes: {
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
                description: 'prior MPU object non-legacy null version exists',
                objMD: {
                    versionId: 'vnull',
                    isNull: true,
                    isNull2: true, // flag marking that it's a non-legacy null version
                    uploadId: 'fooUploadId',
                },
                versioningEnabledExpectedRes: {
                    options: {
                        versioning: true,
                    },
                    // instruct to first copy the null version onto a
                    // newly created null key with version ID in its metadata
                    nullVersionId: 'vnull',
                },
                versioningSuspendedExpectedRes: {
                    options: {
                        isNull: true,
                        versionId: '',
                    },
                },
                versioningEnabledCompatExpectedRes: {
                    options: {
                        versioning: true,
                        extraMD: {
                            nullVersionId: 'vnull',
                            nullUploadId: 'fooUploadId',
                        },
                    },
                    // instruct to first copy the null version onto a
                    // newly created version key preserving the version ID
                    nullVersionId: 'vnull',
                },
                versioningSuspendedCompatExpectedRes: {
                    options: {
                        isNull: true,
                        versionId: '',
                    },
                },
            },
            {
                description: 'prior object exists, put before versioning was first enabled',
                objMD: {},
                versioningEnabledExpectedRes: {
                    options: {
                        versioning: true,
                    },
                    // instruct to first copy the null version onto a
                    // newly created null key as the oldest version
                    nullVersionId: INF_VID,
                },
                versioningSuspendedExpectedRes: {
                    options: {
                        isNull: true,
                        versionId: '',
                    },
                },
                versioningEnabledCompatExpectedRes: {
                    options: {
                        versioning: true,
                        extraMD: {
                            nullVersionId: INF_VID,
                        },
                    },
                    // instruct to first copy the null version onto a
                    // newly created version key as the oldest version
                    nullVersionId: INF_VID,
                },
                versioningSuspendedCompatExpectedRes: {
                    options: {
                        isNull: true,
                        versionId: '',
                    },
                },
            },
            {
                description: 'prior MPU object exists, put before versioning was first enabled',
                objMD: {
                    uploadId: 'fooUploadId',
                },
                versioningEnabledExpectedRes: {
                    options: {
                        versioning: true,
                    },
                    // instruct to first copy the null version onto a
                    // newly created null key as the oldest version
                    nullVersionId: INF_VID,
                },
                versioningSuspendedExpectedRes: {
                    options: {
                        isNull: true,
                        versionId: '',
                    },
                },
                versioningEnabledCompatExpectedRes: {
                    options: {
                        versioning: true,
                        extraMD: {
                            nullVersionId: INF_VID,
                            nullUploadId: 'fooUploadId',
                        },
                    },
                    // instruct to first copy the null version onto a
                    // newly created version key as the oldest version
                    nullVersionId: INF_VID,
                },
                versioningSuspendedCompatExpectedRes: {
                    options: {
                        isNull: true,
                        versionId: '',
                    },
                },
            },
            {
                description: 'prior non-null object version exists with ref to null version',
                objMD: {
                    versionId: 'v1',
                    nullVersionId: 'vnull',
                },
                versioningEnabledExpectedRes: {
                    options: {
                        versioning: true,
                    },
                    // backward-compat: delete old null version key
                    delOptions: {
                        versionId: 'vnull',
                    },
                    // backward-compat: create new null key
                    nullVersionId: 'vnull',
                },
                versioningSuspendedExpectedRes: {
                    options: {
                        isNull: true,
                        versionId: '',
                    },
                    // backward-compat: delete old null version key
                    delOptions: {
                        versionId: 'vnull',
                        deleteData: true,
                    },
                },
                versioningEnabledCompatExpectedRes: {
                    options: {
                        versioning: true,
                        extraMD: {
                            nullVersionId: 'vnull',
                        },
                    },
                },
                versioningSuspendedCompatExpectedRes: {
                    options: {
                        isNull: true,
                        versionId: '',
                    },
                    delOptions: {
                        versionId: 'vnull',
                        deleteData: true,
                    },
                },
            },
            {
                description: 'prior MPU object non-null version exists with ref to null version',
                objMD: {
                    versionId: 'v1',
                    uploadId: 'fooUploadId',
                    nullVersionId: 'vnull',
                },
                versioningEnabledExpectedRes: {
                    options: {
                        versioning: true,
                    },
                    // backward-compat: delete old null version key
                    delOptions: {
                        versionId: 'vnull',
                    },
                    // backward-compat: create new null key
                    nullVersionId: 'vnull',
                },
                versioningSuspendedExpectedRes: {
                    options: {
                        isNull: true,
                        versionId: '',
                    },
                    // backward-compat: delete old null version key
                    delOptions: {
                        versionId: 'vnull',
                        deleteData: true,
                    },
                },
                versioningEnabledCompatExpectedRes: {
                    options: {
                        versioning: true,
                        extraMD: {
                            nullVersionId: 'vnull',
                        },
                    },
                },
                versioningSuspendedCompatExpectedRes: {
                    options: {
                        isNull: true,
                        versionId: '',
                    },
                    delOptions: {
                        versionId: 'vnull',
                        deleteData: true,
                    },
                },
            },
            {
                description: 'prior object non-null version exists with ref to MPU null version',
                objMD: {
                    versionId: 'v1',
                    nullVersionId: 'vnull',
                    nullUploadId: 'nullFooUploadId',
                },
                versioningEnabledExpectedRes: {
                    options: {
                        versioning: true,
                    },
                    // backward-compat: delete old null version key
                    delOptions: {
                        versionId: 'vnull',
                    },
                    // backward-compat: create new null key
                    nullVersionId: 'vnull',
                },
                versioningSuspendedExpectedRes: {
                    options: {
                        isNull: true,
                        versionId: '',
                    },
                    // backward-compat: delete old null version key
                    delOptions: {
                        versionId: 'vnull',
                        replayId: 'nullFooUploadId',
                        deleteData: true,
                    },
                },
                versioningEnabledCompatExpectedRes: {
                    options: {
                        versioning: true,
                        extraMD: {
                            nullVersionId: 'vnull',
                            nullUploadId: 'nullFooUploadId',
                        },
                    },
                },
                versioningSuspendedCompatExpectedRes: {
                    options: {
                        isNull: true,
                        versionId: '',
                    },
                    delOptions: {
                        versionId: 'vnull',
                        replayId: 'nullFooUploadId',
                        deleteData: true,
                    },
                },
            },
        ].forEach(testCase =>
            [false, true].forEach(nullVersionCompatMode =>
                ['Enabled', 'Suspended'].forEach(versioningStatus => it(
                `${testCase.description}${nullVersionCompatMode ? ' (null compat)' : ''}` +
                `, versioning Status=${versioningStatus}`,
                () => {
                    const mst = getMasterState(testCase.objMD);
                    const res = processVersioningState(mst, versioningStatus, nullVersionCompatMode);
                    const resultName = `versioning${versioningStatus}` +
                          `${nullVersionCompatMode ? 'Compat' : ''}ExpectedRes`;
                    const expectedRes = testCase[resultName];
                    assert.deepStrictEqual(res, expectedRes);
                }))));
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
                description: 'delete object put before versioning was first enabled',
                objMD: {},
                reqVersionId: 'null',
                expectedRes: {
                    deleteData: true,
                },
            },
        ].forEach(testCase => it(testCase.description, () => {
            const mockBucketMD = {
                getVersioningConfiguration: () => ({ Status: 'Enabled' }),
            };
            const options = preprocessingVersioningDelete(
                'foobucket', mockBucketMD, testCase.objMD, testCase.reqVersionId);
            assert.deepStrictEqual(options, testCase.expectedRes);
        }));
    });
});
