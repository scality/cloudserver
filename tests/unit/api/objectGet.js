const assert = require('assert');
const async = require('async');
const crypto = require('crypto');
const { parseString } = require('xml2js');
const { models } = require('arsenal');
const { ObjectMD, ObjectMDChecksum } = models;
const { algorithms } = require('../../../lib/api/apiUtils/integrity/validateChecksums');

const { bucketPut } = require('../../../lib/api/bucketPut');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const completeMultipartUpload
    = require('../../../lib/api/completeMultipartUpload');
const DummyRequest = require('../DummyRequest');
const initiateMultipartUpload
    = require('../../../lib/api/initiateMultipartUpload');
const objectPut = require('../../../lib/api/objectPut');
const objectGet = require('../../../lib/api/objectGet');
const objectPutPart = require('../../../lib/api/objectPutPart');
const changeObjectLock = require('../../utilities/objectLock-util');
const mdColdHelper = require('./utils/metadataMockColdStorage');

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const objectName = 'objectName';
const postBody = Buffer.from('I am a body', 'utf8');

describe('objectGet API', () => {
    let testPutObjectRequest;

    beforeEach(() => {
        cleanup();
        testPutObjectRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {
                'x-amz-meta-test': 'some metadata',
                'content-length': '12',
            },
            parsedContentLength: 12,
            url: `/${bucketName}/${objectName}`,
        }, postBody);
    });

    const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
    const testPutBucketRequest = {
        bucketName,
        namespace,
        headers: {},
        url: `/${bucketName}`,
        actionImplicitDenies: false,
    };
    const userMetadataKey = 'x-amz-meta-test';
    const userMetadataValue = 'some metadata';
    const testGetRequest = {
        bucketName,
        namespace,
        objectKey: objectName,
        headers: {},
        url: `/${bucketName}/${objectName}`,
        actionImplicitDenies: false,
    };

    it('should get the object metadata', done => {
        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined,
                log, (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectGet(authInfo, testGetRequest, false,
                        log, (err, result, responseMetaHeaders) => {
                            assert.strictEqual(
                                responseMetaHeaders[userMetadataKey],
                                userMetadataValue);
                            assert.strictEqual(responseMetaHeaders.ETag,
                                `"${correctMD5}"`);
                            done();
                        });
                });
        });
    });

    const testPutBucketRequestObjectLock = {
        bucketName,
        namespace,
        headers: {
            'host': `${bucketName}.s3.amazonaws.com`,
            'x-amz-bucket-object-lock-enabled': 'true',
        },
        url: `/${bucketName}`,
        actionImplicitDenies: false,
    };

    const createPutDummyRetention = (date, mode) => new DummyRequest({
        bucketName,
        namespace,
        objectKey: objectName,
        headers: {
            'x-amz-object-lock-retain-until-date': date,
            'x-amz-object-lock-mode': mode,
            'content-length': '12',
        },
        parsedContentLength: 12,
        url: `/${bucketName}/${objectName}`,
    }, postBody);

    const threeDaysMilliSecs = 3 * 24 * 60 * 60 * 1000;
    const testDate = new Date(Date.now() + threeDaysMilliSecs).toISOString();

    it('should get the object metadata with valid retention info', done => {
        bucketPut(authInfo, testPutBucketRequestObjectLock, log, () => {
            const request = createPutDummyRetention(testDate, 'GOVERNANCE');
            objectPut(authInfo, request, undefined,
                log, (err, headers) => {
                    assert.ifError(err);
                    assert.strictEqual(headers.ETag, `"${correctMD5}"`);
                    const req = testGetRequest;
                    objectGet(authInfo, req, false, log, (err, r, headers) => {
                        assert.ifError(err);
                        assert.strictEqual(
                            headers['x-amz-object-lock-retain-until-date'],
                            testDate);
                        assert.strictEqual(
                            headers['x-amz-object-lock-mode'],
                            'GOVERNANCE');
                        assert.strictEqual(headers.ETag,
                            `"${correctMD5}"`);
                        changeObjectLock([{
                            bucket: bucketName,
                            key: objectName,
                            versionId: headers['x-amz-version-id'],
                        }], '', done);
                    });
                });
        });
    });

    const createPutDummyLegalHold = legalHold => new DummyRequest({
        bucketName,
        namespace,
        objectKey: objectName,
        headers: {
            'x-amz-object-lock-legal-hold': legalHold,
            'content-length': '12',
        },
        parsedContentLength: 12,
        url: `/${bucketName}/${objectName}`,
    }, postBody);

    const testStatuses = ['ON', 'OFF'];
    testStatuses.forEach(status => {
        it(`should get object metadata with legal hold ${status}`, done => {
            bucketPut(authInfo, testPutBucketRequestObjectLock, log, () => {
                const request = createPutDummyLegalHold(status);
                objectPut(authInfo, request, undefined, log,
                    (err, resHeaders) => {
                        assert.ifError(err);
                        assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                        objectGet(authInfo, testGetRequest, false, log,
                            (err, res, headers) => {
                                assert.ifError(err);
                                assert.strictEqual(
                                    headers['x-amz-object-lock-legal-hold'],
                                    status);
                                assert.strictEqual(headers.ETag,
                                    `"${correctMD5}"`);
                                changeObjectLock([{
                                    bucket: bucketName,
                                    key: objectName,
                                    versionId: headers['x-amz-version-id'],
                                }], '', done);
                            });
                    });
            });
        });
    });

    const createPutDummyRetentionAndLegalHold = (date, mode, status) =>
        new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {
                'x-amz-object-lock-retain-until-date': date,
                'x-amz-object-lock-mode': mode,
                'x-amz-object-lock-legal-hold': status,
                'content-length': '12',
            },
            parsedContentLength: 12,
            url: `/${bucketName}/${objectName}`,
        }, postBody);

    it('should get the object metadata with both retention and legal hold',
        done => {
            bucketPut(authInfo, testPutBucketRequestObjectLock, log, () => {
                const request = createPutDummyRetentionAndLegalHold(
                    testDate, 'COMPLIANCE', 'ON');
                objectPut(authInfo, request, undefined, log,
                    (err, resHeaders) => {
                        assert.ifError(err);
                        assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                        const auth = authInfo;
                        const req = testGetRequest;
                        objectGet(auth, req, false, log, (err, r, headers) => {
                            assert.ifError(err);
                            assert.strictEqual(
                                headers['x-amz-object-lock-legal-hold'],
                                'ON');
                            assert.strictEqual(
                                headers['x-amz-object-lock-retain-until-date'],
                                testDate);
                            assert.strictEqual(
                                headers['x-amz-object-lock-mode'],
                                'COMPLIANCE');
                            assert.strictEqual(headers.ETag,
                                `"${correctMD5}"`);
                            done();
                        });
                    });
            });
        });

    it('should get the object data retrieval info', done => {
        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectGet(authInfo, testGetRequest, false, log,
                        (err, dataGetInfo) => {
                            assert.deepStrictEqual(dataGetInfo,
                                [{
                                    key: 1,
                                    start: 0,
                                    size: 12,
                                    dataStoreName: 'mem',
                                    dataStoreETag: `1:${correctMD5}`,
                                }]);
                            done();
                        });
                });
        });
    });

    it('should get the object data retrieval info for an object put by MPU',
        done => {
            const partBody = Buffer.from('I am a part\n', 'utf8');
            const initiateRequest = {
                bucketName,
                namespace,
                objectKey: objectName,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                url: `/${objectName}?uploads`,
                actionImplicitDenies: false,
            };
            async.waterfall([
                next => bucketPut(authInfo, testPutBucketRequest, log, next),
                (corsHeaders, next) => initiateMultipartUpload(authInfo,
                    initiateRequest, log, next),
                (result, corsHeaders, next) => parseString(result, next),
                (json, next) => {
                    const testUploadId =
                    json.InitiateMultipartUploadResult.UploadId[0];
                    const partHash = crypto.createHash('md5').update(partBody).digest('hex');
                    const partRequest = new DummyRequest({
                        bucketName,
                        namespace,
                        objectKey: objectName,
                        headers: {
                            'host': `${bucketName}.s3.amazonaws.com`,
                            // Part (other than last part) must be at least 5MB
                            'content-length': '5242880',
                        },
                        parsedContentLength: 5242880,
                        url: `/${objectName}?partNumber=1&uploadId` +
                            `=${testUploadId}`,
                        query: {
                            partNumber: '1',
                            uploadId: testUploadId,
                        },
                        partHash,
                    }, partBody);
                    objectPutPart(authInfo, partRequest, undefined, log, () => {
                        next(null, testUploadId, partHash);
                    });
                },
                (testUploadId, partHash, next) => {
                    const part2Request = new DummyRequest({
                        bucketName,
                        namespace,
                        objectKey: objectName,
                        headers: {
                            'host': `${bucketName}.s3.amazonaws.com`,
                            'content-length': '12',
                        },
                        parsedContentLength: 12,
                        url: `/${objectName}?partNumber=2&uploadId=` +
                            `${testUploadId}`,
                        query: {
                            partNumber: '2',
                            uploadId: testUploadId,
                        },
                        partHash,
                    }, partBody);
                    objectPutPart(authInfo, part2Request, undefined,
                        log, () => {
                            next(null, testUploadId, partHash);
                        });
                },
                (testUploadId, partHash, next) => {
                    const completeBody = '<CompleteMultipartUpload>' +
                        '<Part>' +
                        '<PartNumber>1</PartNumber>' +
                        `<ETag>"${partHash}"</ETag>` +
                        '</Part>' +
                        '<Part>' +
                        '<PartNumber>2</PartNumber>' +
                        `<ETag>"${partHash}"</ETag>` +
                        '</Part>' +
                        '</CompleteMultipartUpload>';
                    const completeRequest = {
                        bucketName,
                        namespace,
                        objectKey: objectName,
                        parsedHost: 's3.amazonaws.com',
                        url: `/${objectName}?uploadId=${testUploadId}`,
                        headers: { host: `${bucketName}.s3.amazonaws.com` },
                        query: { uploadId: testUploadId },
                        post: completeBody,
                        actionImplicitDenies: false,
                    };
                    completeMultipartUpload(authInfo, completeRequest,
                                            log, err => {
                                                next(err, partHash);
                                            });
                },
            ],
            (err, partHash) => {
                assert.ifError(err);
                objectGet(authInfo, testGetRequest, false, log,
                (err, dataGetInfo) => {
                    assert.ifError(err);
                    assert.deepStrictEqual(dataGetInfo,
                        [{
                            key: 1,
                            dataStoreName: 'mem',
                            dataStoreETag: `1:${partHash}`,
                            size: 5242880,
                            start: 0,
                        },
                        {
                            key: 2,
                            dataStoreName: 'mem',
                            dataStoreETag: `2:${partHash}`,
                            size: 12,
                            start: 5242880,
                        }]);
                    done();
                });
            });
        });

    it('should get a 0 bytes object', done => {
        const postBody = '';
        const correctMD5 = 'd41d8cd98f00b204e9800998ecf8427e';
        const testPutObjectRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {
                'content-length': '0',
                'x-amz-meta-test': 'some metadata',
            },
            parsedContentLength: 0,
            url: `/${bucketName}/${objectName}`,
            partHash: 'd41d8cd98f00b204e9800998ecf8427e',
        }, postBody);
        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectGet(authInfo, testGetRequest, false,
                    log, (err, result, responseMetaHeaders) => {
                        assert.strictEqual(result, null);
                        assert.strictEqual(
                            responseMetaHeaders[userMetadataKey],
                            userMetadataValue);
                        assert.strictEqual(responseMetaHeaders.ETag,
                            `"${correctMD5}"`);
                        done();
                    });
                });
        });
    });

    it('should return InvalidObjectState if trying to GET the object in cold storage', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
        };
        mdColdHelper.putBucketMock(bucketName, null, () => {
            mdColdHelper.putObjectMock(bucketName, objectName, mdColdHelper.getArchivedObjectMD(), () => {
                objectGet(authInfo, testGetRequest, false, log, err => {
                    assert.strictEqual(err.is.InvalidObjectState, true);
                    done();
                });
            });
        });
    });

    it('should return InvalidObjectState if trying to GET the object in cold storage being restored', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
        };
        mdColdHelper.putBucketMock(bucketName, null, () => {
            mdColdHelper.putObjectMock(bucketName, objectName, mdColdHelper.getRestoringObjectMD(), () => {
                objectGet(authInfo, testGetRequest, false, log, err => {
                    assert.strictEqual(err.is.InvalidObjectState, true);
                    done();
                });
            });
        });
    });

    it('should GET a transitioning object', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
        };
        mdColdHelper.putBucketMock(bucketName, null, () => {
            mdColdHelper.putObjectMock(bucketName, objectName, mdColdHelper.getTransitionInProgressObjectMD(), () => {
                objectGet(authInfo, testGetRequest, false, log, (err, res, headers) => {
                    assert.ifError(err);
                    assert.ok(res);
                    // Object is not yet cold, so storage class is still same as the bucket ("STANDARD")
                    // and thus not set in the response
                    assert.strictEqual(headers['x-amz-storage-class'], undefined);
                    done();
                });
            });
        });
    });

    it('should not reflect the storage location in storage class if the bucket location is not cold', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
        };
        mdColdHelper.putBucketMock(bucketName, 'scality-internal-file', () => {
            mdColdHelper.putObjectMock(bucketName, objectName, undefined, () => {
                objectGet(authInfo, testGetRequest, false, log, (err, res, headers) => {
                    assert.ifError(err);
                    assert.ok(res);
                    assert.strictEqual(headers['x-amz-storage-class'], undefined);
                    done();
                });
            });
        });
    });

    it('should reflect the restore header with ongoing-request=true if the object is restored', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
        };
        mdColdHelper.putBucketMock(bucketName, null, () => {
            const objectCustomMDFields = mdColdHelper.getRestoredObjectMD();
            const restoreInfo = objectCustomMDFields.getAmzRestore();
            mdColdHelper.putObjectMock(bucketName, objectName, objectCustomMDFields, () => {
                objectGet(authInfo, testGetRequest, false, log, (err, res, headers) => {
                    assert.ifError(err);
                    assert.ok(res);
                    assert.strictEqual(headers['x-amz-storage-class'], mdColdHelper.defaultLocation);
                    const utcDate = new Date(restoreInfo.getExpiryDate()).toUTCString();
                    assert.strictEqual(headers['x-amz-restore'], `ongoing-request="false", expiry-date="${utcDate}"`);
                    done();
                });
            });
        });
    });

    it('should reflect the restore header with ongoing-request=false and expiry-date set ' +
        'if the object is restored and not yet expired', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
        };
        mdColdHelper.putBucketMock(bucketName, null, () => {
            const objectCustomMDFields = mdColdHelper.getRestoredObjectMD();
            const restoreInfo = objectCustomMDFields.getAmzRestore();
            mdColdHelper.putObjectMock(bucketName, objectName, objectCustomMDFields, () => {
                objectGet(authInfo, testGetRequest, false, log, (err, res, headers) => {
                    assert.ifError(err);
                    assert.ok(res);
                    assert.strictEqual(headers['x-amz-storage-class'], mdColdHelper.defaultLocation);
                    const utcDate = new Date(restoreInfo.getExpiryDate()).toUTCString();
                    assert.strictEqual(headers['x-amz-restore'], `ongoing-request="false", expiry-date="${utcDate}"`);
                    done();
                });
            });
        });
    });
});

describe('objectGet API - x-amz-checksum-mode', () => {
    const checksumAlgorithms = [
        { name: 'sha256',    header: 'x-amz-checksum-sha256'    },
        { name: 'sha1',      header: 'x-amz-checksum-sha1'      },
        { name: 'crc32',     header: 'x-amz-checksum-crc32'     },
        { name: 'crc32c',    header: 'x-amz-checksum-crc32c'    },
        { name: 'crc64nvme', header: 'x-amz-checksum-crc64nvme' },
    ];

    const expectedDigests = {};

    before(done => {
        Promise.all(checksumAlgorithms.map(async ({ name }) => {
            expectedDigests[name] = await algorithms[name].digest(postBody);
        })).then(() => done(), done);
    });

    beforeEach(() => cleanup());

    checksumAlgorithms.forEach(({ name, header }) => {
        it(`should return ${header} and x-amz-checksum-type when mode is ENABLED`, done => {
            const md = new ObjectMD(mdColdHelper.baseMd)
                .setChecksum(new ObjectMDChecksum(name, expectedDigests[name], 'FULL_OBJECT'));
            mdColdHelper.putBucketMock(bucketName, null, () =>
                mdColdHelper.putObjectMock(bucketName, objectName, md, () => {
                    const req = {
                        bucketName,
                        namespace,
                        objectKey: objectName,
                        headers: { 'x-amz-checksum-mode': 'ENABLED' },
                        url: `/${bucketName}/${objectName}`,
                        query: {},
                    };
                    objectGet(authInfo, req, false, log, (err, _locator, headers) => {
                        assert.ifError(err);
                        assert.strictEqual(headers[header], expectedDigests[name]);
                        assert.strictEqual(headers['x-amz-checksum-type'], 'FULL_OBJECT');
                        done();
                    });
                }));
        });
    });

    it('should not return checksum headers when mode is ENABLED but object has no checksum', done => {
        mdColdHelper.putBucketMock(bucketName, null, () =>
            mdColdHelper.putObjectMock(bucketName, objectName, undefined, () => {
                const req = {
                    bucketName,
                    namespace,
                    objectKey: objectName,
                    headers: { 'x-amz-checksum-mode': 'ENABLED' },
                    url: `/${bucketName}/${objectName}`,
                    query: {},
                };
                objectGet(authInfo, req, false, log, (err, _locator, headers) => {
                    assert.ifError(err);
                    checksumAlgorithms.forEach(({ header }) =>
                        assert.strictEqual(headers[header], undefined));
                    assert.strictEqual(headers['x-amz-checksum-type'], undefined);
                    done();
                });
            }));
    });

    it('should not return checksum headers when x-amz-checksum-mode is not set', done => {
        const md = new ObjectMD(mdColdHelper.baseMd)
            .setChecksum(new ObjectMDChecksum('sha256', expectedDigests.sha256, 'FULL_OBJECT'));
        mdColdHelper.putBucketMock(bucketName, null, () =>
            mdColdHelper.putObjectMock(bucketName, objectName, md, () => {
                const req = {
                    bucketName,
                    namespace,
                    objectKey: objectName,
                    headers: {},
                    url: `/${bucketName}/${objectName}`,
                    query: {},
                };
                objectGet(authInfo, req, false, log, (err, _locator, headers) => {
                    assert.ifError(err);
                    checksumAlgorithms.forEach(({ header }) =>
                        assert.strictEqual(headers[header], undefined));
                    assert.strictEqual(headers['x-amz-checksum-type'], undefined);
                    done();
                });
            }));
    });

    it('should return InvalidArgument when x-amz-checksum-mode is not ENABLED', done => {
        const req = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { 'x-amz-checksum-mode': 'DISABLED' },
            url: `/${bucketName}/${objectName}`,
            query: {},
        };
        objectGet(authInfo, req, false, log, err => {
            assert.strictEqual(err.is.InvalidArgument, true);
            done();
        });
    });

    it('should not return checksum headers when Range header is set', done => {
        const md = new ObjectMD(mdColdHelper.baseMd)
            .setChecksum(new ObjectMDChecksum('sha256', expectedDigests.sha256, 'FULL_OBJECT'));
        mdColdHelper.putBucketMock(bucketName, null, () =>
            mdColdHelper.putObjectMock(bucketName, objectName, md, () => {
                const req = {
                    bucketName,
                    namespace,
                    objectKey: objectName,
                    headers: {
                        'x-amz-checksum-mode': 'ENABLED',
                        range: 'bytes=0-3',
                    },
                    url: `/${bucketName}/${objectName}`,
                    query: {},
                };
                objectGet(authInfo, req, false, log, (err, _locator, headers) => {
                    assert.ifError(err);
                    checksumAlgorithms.forEach(({ header }) =>
                        assert.strictEqual(headers[header], undefined));
                    assert.strictEqual(headers['x-amz-checksum-type'], undefined);
                    done();
                });
            }));
    });

    it('should not return checksum headers when partNumber is set', done => {
        const md = new ObjectMD(mdColdHelper.baseMd)
            .setChecksum(new ObjectMDChecksum('sha256', expectedDigests.sha256, 'FULL_OBJECT'));
        mdColdHelper.putBucketMock(bucketName, null, () =>
            mdColdHelper.putObjectMock(bucketName, objectName, md, () => {
                const req = {
                    bucketName,
                    namespace,
                    objectKey: objectName,
                    headers: { 'x-amz-checksum-mode': 'ENABLED' },
                    url: `/${bucketName}/${objectName}`,
                    query: { partNumber: '1' },
                };
                objectGet(authInfo, req, false, log, (err, _locator, headers) => {
                    assert.ifError(err);
                    checksumAlgorithms.forEach(({ header }) =>
                        assert.strictEqual(headers[header], undefined));
                    assert.strictEqual(headers['x-amz-checksum-type'], undefined);
                    done();
                });
            }));
    });
});
