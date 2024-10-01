const assert = require('assert');

const { versioning } = require('arsenal');
const { config } = require('../../../../lib/Config');
const INF_VID = versioning.VersionID.getInfVid(config.replicationGroupId);
const { scaledMsPerDay } = config.getTimeOptions();
const sinon = require('sinon');

const { processVersioningState, getMasterState,
        getVersionSpecificMetadataOptions,
        preprocessingVersioningDelete,
        overwritingVersioning } =
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
                description: 'prior legacy null object version exists',
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
                    // delete possibly existing null versioned key
                    // that is identical to the null master
                    delOptions: {
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
                description: 'prior non-legacy null object version exists',
                objMD: {
                    versionId: 'vnull',
                    isNull: true,
                    isNull2: true, // flag marking that it's a non-legacy null version
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
                    // delete possibly existing null versioned key
                    // that is identical to the null master
                    delOptions: {
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
                        extraMD: {
                            nullVersionId: 'vnull',
                        },
                    },
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
                    // backward-compat: delete old null version key
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
                        extraMD: {
                            nullVersionId: 'vnull',
                        },
                    },
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
                    // backward-compat: delete old null version key
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
                        extraMD: {
                            nullVersionId: 'vnull',
                            nullUploadId: 'nullFooUploadId',
                        },
                    },
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
                    // backward-compat: delete old null version key
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

    describe('getVersionSpecificMetadataOptions', () => {
        [
            {
                description: 'object put before versioning was first enabled',
                objMD: {},
                expectedRes: {},
                expectedResCompat: {},
            },
            {
                description: 'non-null object version',
                objMD: {
                    versionId: 'v1',
                },
                expectedRes: {
                    versionId: 'v1',
                    isNull: false,
                },
                expectedResCompat: {
                    versionId: 'v1',
                },
            },
            {
                description: 'legacy null object version',
                objMD: {
                    versionId: 'vnull',
                    isNull: true,
                },
                expectedRes: {
                    versionId: 'vnull',
                },
                expectedResCompat: {
                    versionId: 'vnull',
                },
            },
            {
                description: 'null object version in null key',
                objMD: {
                    versionId: 'vnull',
                    isNull: true,
                    isNull2: true,
                },
                expectedRes: {
                    versionId: 'vnull',
                    isNull: true,
                },
                expectedResCompat: {
                    versionId: 'vnull',
                    isNull: true,
                },
            },
        ].forEach(testCase =>
            [false, true].forEach(nullVersionCompatMode =>
                it(`${testCase.description}${nullVersionCompatMode ? ' (null compat)' : ''}`,
                () => {
                    const options = getVersionSpecificMetadataOptions(
                        testCase.objMD, nullVersionCompatMode);
                    const expectedResAttr = nullVersionCompatMode ?
                          'expectedResCompat' : 'expectedRes';
                    assert.deepStrictEqual(options, testCase[expectedResAttr]);
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
                expectedResCompat: {},
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
                    isNull: false,
                },
                expectedResCompat: {
                    deleteData: true,
                    versionId: 'v1',
                },
            },
            {
                description: 'delete legacy null object version',
                objMD: {
                    versionId: 'vnull',
                    isNull: true,
                },
                reqVersionId: 'null',
                expectedRes: {
                    deleteData: true,
                    versionId: 'vnull',
                },
                expectedResCompat: {
                    deleteData: true,
                    versionId: 'vnull',
                },
            },
            {
                description: 'delete null object version in null key',
                objMD: {
                    versionId: 'vnull',
                    isNull: true,
                    isNull2: true,
                },
                reqVersionId: 'null',
                expectedRes: {
                    deleteData: true,
                    versionId: 'vnull',
                    isNull: true,
                },
                expectedResCompat: {
                    deleteData: true,
                    versionId: 'vnull',
                    isNull: true,
                },
            },
            {
                description: 'delete object put before versioning was first enabled',
                objMD: {},
                reqVersionId: 'null',
                expectedRes: {
                    deleteData: true,
                    // no 'isNull' parameter, as there is no 'versionId', the code will
                    // not use the version-specific DELETE route but a regular DELETE
                },
                expectedResCompat: {
                    deleteData: true,
                },
            },
        ].forEach(testCase =>
            [false, true].forEach(nullVersionCompatMode =>
                it(`${testCase.description}${nullVersionCompatMode ? ' (null compat)' : ''}`,
                () => {
                    const mockBucketMD = {
                        getVersioningConfiguration: () => ({ Status: 'Enabled' }),
                    };
                    const options = preprocessingVersioningDelete(
                        'foobucket', mockBucketMD, testCase.objMD, testCase.reqVersionId,
                        nullVersionCompatMode);
                    const expectedResAttr = nullVersionCompatMode ?
                          'expectedResCompat' : 'expectedRes';
                    assert.deepStrictEqual(options, testCase[expectedResAttr]);
                })));
    });

    describe('overwritingVersioning', () => {
        const days = 3;
        const archiveInfo = {
            'archiveID': '126783123678',
        };
        const now = Date.now();
        let clock;

        beforeEach(() => {
            clock = sinon.useFakeTimers(now);
        });

        afterEach(() => {
            clock.restore();
        });

        [
            {
                description: 'Should update archive with restore infos',
                    objMD: {
                    'versionId': '2345678',
                    'creation-time': now,
                    'last-modified': now,
                    'originOp': 's3:PutObject',
                    'x-amz-storage-class': 'cold-location',
                    'archive': {
                        'restoreRequestedDays': days,
                        'restoreRequestedAt': now,
                        archiveInfo
                    }
                },
                expectedRes: {
                    'creationTime': now,
                    'lastModifiedDate': now,
                    'updateMicroVersionId': true,
                    'originOp': 's3:ObjectRestore:Completed',
                    'taggingCopy': undefined,
                    'amzStorageClass': 'cold-location',
                    'archive': {
                        archiveInfo,
                        'restoreRequestedDays': 3,
                        'restoreRequestedAt': now,
                        'restoreCompletedAt': new Date(now),
                        'restoreWillExpireAt': new Date(now + (days * scaledMsPerDay)),
                    }
                }
            },
            {
                description: 'Should keep user mds and tags',
                hasUserMD: true,
                objMD: {
                    'versionId': '2345678',
                    'creation-time': now,
                    'last-modified': now,
                    'originOp': 's3:PutObject',
                    'x-amz-meta-test': 'test',
                    'x-amz-meta-test2': 'test2',
                    'tags': { 'testtag': 'testtag', 'testtag2': 'testtag2' },
                    'x-amz-storage-class': 'cold-location',
                    'archive': {
                        'restoreRequestedDays': days,
                        'restoreRequestedAt': now,
                        archiveInfo
                    }
                },
                expectedRes: {
                    'creationTime': now,
                    'lastModifiedDate': now,
                    'updateMicroVersionId': true,
                    'originOp': 's3:ObjectRestore:Completed',
                    'metaHeaders': {
                        'x-amz-meta-test': 'test',
                        'x-amz-meta-test2': 'test2',
                    },
                    'taggingCopy': { 'testtag': 'testtag', 'testtag2': 'testtag2' },
                    'amzStorageClass': 'cold-location',
                    'archive': {
                        archiveInfo,
                        'restoreRequestedDays': days,
                        'restoreRequestedAt': now,
                        'restoreCompletedAt': new Date(now),
                        'restoreWillExpireAt': new Date(now + (days * scaledMsPerDay)),
                    }
                },
            },
            {
                description: 'Should not fail with a nullVersionId',
                objMD: {
                    'creation-time': now,
                    'last-modified': now,
                    'originOp': 's3:PutObject',
                    'nullVersionId': 'vnull',
                    'isNull': true,
                    'x-amz-storage-class': 'cold-location',
                    'archive': {
                        'restoreRequestedDays': days,
                        'restoreRequestedAt': now,
                        archiveInfo
                    }
                },
                expectedRes: {
                    'creationTime': now,
                    'lastModifiedDate': now,
                    'updateMicroVersionId': true,
                    'originOp': 's3:ObjectRestore:Completed',
                    'amzStorageClass': 'cold-location',
                    'taggingCopy': undefined,
                    'archive': {
                        archiveInfo,
                        'restoreRequestedDays': 3,
                        'restoreRequestedAt': now,
                        'restoreCompletedAt': new Date(now),
                        'restoreWillExpireAt': new Date(now + (days * scaledMsPerDay)),
                    }
                }
            },
            {
                description: 'Should not keep x-amz-meta-scal-s3-restore-attempt user MD',
                hasUserMD: true,
                objMD: {
                    'versionId': '2345678',
                    'creation-time': now,
                    'last-modified': now,
                    'originOp': 's3:PutObject',
                    'x-amz-meta-test': 'test',
                    'x-amz-meta-scal-s3-restore-attempt': 14,
                    'x-amz-storage-class': 'cold-location',
                    'archive': {
                        'restoreRequestedDays': days,
                        'restoreRequestedAt': now,
                        archiveInfo
                    }
                },
                expectedRes: {
                    'creationTime': now,
                    'lastModifiedDate': now,
                    'updateMicroVersionId': true,
                    'originOp': 's3:ObjectRestore:Completed',
                    'metaHeaders': {
                        'x-amz-meta-test': 'test',
                    },
                    'taggingCopy': undefined,
                    'amzStorageClass': 'cold-location',
                    'archive': {
                        archiveInfo,
                        'restoreRequestedDays': 3,
                        'restoreRequestedAt': now,
                        'restoreCompletedAt': new Date(now),
                        'restoreWillExpireAt': new Date(now + (days * scaledMsPerDay)),
                    }
                }
            },
            {
                description: 'Should keep replication infos',
                objMD: {
                'versionId': '2345678',
                'creation-time': now,
                'last-modified': now,
                'originOp': 's3:PutObject',
                'x-amz-storage-class': 'cold-location',
                'replicationInfo': {
                    'status': 'COMPLETED',
                    'backends': [
                        {
                            'site': 'azure-blob',
                            'status': 'COMPLETED',
                            'dataStoreVersionId': ''
                        }
                    ],
                    'content': [
                            'DATA',
                            'METADATA'
                    ],
                    'destination': 'arn:aws:s3:::replicate-cold',
                    'storageClass': 'azure-blob',
                    'role': 'arn:aws:iam::root:role/s3-replication-role',
                    'storageType': 'azure',
                    'dataStoreVersionId': '',
                },
                archive: {
                    'restoreRequestedDays': days,
                    'restoreRequestedAt': now,
                    archiveInfo
                    }
                },
                expectedRes: {
                    'creationTime': now,
                    'lastModifiedDate': now,
                    'updateMicroVersionId': true,
                    'originOp': 's3:ObjectRestore:Completed',
                    'amzStorageClass': 'cold-location',
                    'replicationInfo': {
                        'status': 'COMPLETED',
                        'backends': [
                            {
                                'site': 'azure-blob',
                                'status': 'COMPLETED',
                                'dataStoreVersionId': ''
                            }
                        ],
                        'content': [
                                'DATA',
                                'METADATA'
                        ],
                        'destination': 'arn:aws:s3:::replicate-cold',
                        'storageClass': 'azure-blob',
                        'role': 'arn:aws:iam::root:role/s3-replication-role',
                        'storageType': 'azure',
                        'dataStoreVersionId': '',
                    },
                    'taggingCopy': undefined,
                    archive: {
                        archiveInfo,
                        'restoreRequestedDays': 3,
                        'restoreRequestedAt': now,
                        'restoreCompletedAt': new Date(now),
                        'restoreWillExpireAt': new Date(now + (days * scaledMsPerDay)),
                    }
                }
            },
            {
                description: 'Should keep legalHold',
                objMD: {
                'versionId': '2345678',
                'creation-time': now,
                'last-modified': now,
                'originOp': 's3:PutObject',
                'legalHold': true,
                'x-amz-storage-class': 'cold-location',
                'archive': {
                    'restoreRequestedDays': days,
                    'restoreRequestedAt': now,
                    archiveInfo
                    }
                },
                expectedRes: {
                    'creationTime': now,
                    'lastModifiedDate': now,
                    'updateMicroVersionId': true,
                    'originOp': 's3:ObjectRestore:Completed',
                    'legalHold': true,
                    'amzStorageClass': 'cold-location',
                    'taggingCopy': undefined,
                    'archive': {
                        archiveInfo,
                        'restoreRequestedDays': 3,
                        'restoreRequestedAt': now,
                        'restoreCompletedAt': new Date(now),
                        'restoreWillExpireAt': new Date(now + (days * scaledMsPerDay)),
                    }
                }
            },
            {
                description: 'Should keep ACLs',
                objMD: {
                'versionId': '2345678',
                'creation-time': now,
                'last-modified': now,
                'originOp': 's3:PutObject',
                'x-amz-storage-class': 'cold-location',
                'acl': {
                    'Canned': '',
                    'FULL_CONTROL': [
                            '872c04772893deae2b48365752362cd92672eb80eb3deea50d89e834a10ce185'
                    ],
                    'WRITE_ACP': [],
                    'READ': [
                            'http://acs.amazonaws.com/groups/global/AllUsers'
                    ],
                    'READ_ACP': []
                },
                'archive': {
                    'restoreRequestedDays': days,
                    'restoreRequestedAt': now,
                    archiveInfo
                    }
                },
                expectedRes: {
                    'creationTime': now,
                    'lastModifiedDate': now,
                    'updateMicroVersionId': true,
                    'originOp': 's3:ObjectRestore:Completed',
                    'acl': {
                        'Canned': '',
                        'FULL_CONTROL': [
                                '872c04772893deae2b48365752362cd92672eb80eb3deea50d89e834a10ce185'
                        ],
                        'WRITE_ACP': [],
                        'READ': [
                                'http://acs.amazonaws.com/groups/global/AllUsers'
                        ],
                        'READ_ACP': []
                    },
                    'taggingCopy': undefined,
                    'amzStorageClass': 'cold-location',
                    'archive': {
                        archiveInfo,
                        'restoreRequestedDays': 3,
                        'restoreRequestedAt': now,
                        'restoreCompletedAt': new Date(now),
                        'restoreWillExpireAt': new Date(now + (days * scaledMsPerDay)),
                    }
                },
            },
                {
                    description: 'Should keep contentMD5 of the original object',
                    objMD: {
                    'versionId': '2345678',
                    'creation-time': now,
                    'last-modified': now,
                    'originOp': 's3:PutObject',
                    'x-amz-storage-class': 'cold-location',
                    'content-md5': '123456789-5',
                    'acl': {},
                    'archive': {
                        'restoreRequestedDays': days,
                        'restoreRequestedAt': now,
                        archiveInfo
                        }
                    },
                    metadataStoreParams: {
                        'contentMD5': '987654321-3',
                    },
                    expectedRes: {
                        'creationTime': now,
                        'lastModifiedDate': now,
                        'updateMicroVersionId': true,
                        'originOp': 's3:ObjectRestore:Completed',
                        'contentMD5': '123456789-5',
                        'restoredEtag': '987654321-3',
                        'acl': {},
                        'taggingCopy': undefined,
                        'amzStorageClass': 'cold-location',
                        'archive': {
                            archiveInfo,
                            'restoreRequestedDays': 3,
                            'restoreRequestedAt': now,
                            'restoreCompletedAt': new Date(now),
                            'restoreWillExpireAt': new Date(now + (days * scaledMsPerDay)),
                        }
                    }
            },
        ].forEach(testCase => {
            it(testCase.description, () => {
                const metadataStoreParams = {};
                if (testCase.hasUserMD) {
                    metadataStoreParams.metaHeaders = {};
                }
                if (testCase.metadataStoreParams) {
                    Object.assign(metadataStoreParams, testCase.metadataStoreParams);
                }
                const options = overwritingVersioning(testCase.objMD, metadataStoreParams);
                assert.deepStrictEqual(options.versionId, testCase.objMD.versionId);
                assert.deepStrictEqual(metadataStoreParams, testCase.expectedRes);

                if (testCase.objMD.isNull) {
                    assert.deepStrictEqual(options.extraMD.nullVersionId, 'vnull');
                    assert.deepStrictEqual(options.isNull, true);
                }
            });
        });
    });
});
