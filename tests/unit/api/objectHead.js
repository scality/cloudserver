const assert = require('assert');
const { models } = require('arsenal');
const { ObjectMD, ObjectMDChecksum } = models;
const { algorithms } = require('../../../lib/api/apiUtils/integrity/validateChecksums');

const { bucketPut } = require('../../../lib/api/bucketPut');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const objectPut = require('../../../lib/api/objectPut');
const objectHead = require('../../../lib/api/objectHead');
const DummyRequest = require('../DummyRequest');
const changeObjectLock = require('../../utilities/objectLock-util');
const mdColdHelper = require('./utils/metadataMockColdStorage');

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = Buffer.from('I am a body', 'utf8');
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const incorrectMD5 = 'fkjwelfjlslfksdfsdfsdfsdfsdfsdj';
const objectName = 'objectName';
const laterDate = new Date();
laterDate.setMinutes(laterDate.getMinutes() + 30);
const earlierDate = new Date();
earlierDate.setMinutes(earlierDate.getMinutes() - 30);
const testPutBucketRequest = {
    bucketName,
    namespace,
    headers: {},
    url: `/${bucketName}`,
    actionImplicitDenies: false,
};
const userMetadataKey = 'x-amz-meta-test';
const userMetadataValue = 'some metadata';

let testPutObjectRequest;

describe('objectHead API', () => {
    beforeEach(() => {
        cleanup();
        testPutObjectRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { 'x-amz-meta-test': userMetadataValue },
            url: `/${bucketName}/${objectName}`,
            calculatedHash: correctMD5,
        }, postBody);
    });

    it('should return NotModified if request header ' +
       'includes "if-modified-since" and object ' +
       'not modified since specified time', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { 'if-modified-since': laterDate },
            url: `/${bucketName}/${objectName}`,
            actionImplicitDenies: false,
        };

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectHead(authInfo, testGetRequest, log, err => {
                        assert.strictEqual(err.is.NotModified, true);
                        done();
                    });
                });
        });
    });

    it('should return PreconditionFailed if request header ' +
       'includes "if-unmodified-since" and object has ' +
       'been modified since specified time', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { 'if-unmodified-since': earlierDate },
            url: `/${bucketName}/${objectName}`,
            actionImplicitDenies: false,
        };

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.ifError(err);
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectHead(authInfo, testGetRequest, log, err => {
                        assert.strictEqual(err.is.PreconditionFailed, true);
                        done();
                    });
                });
        });
    });

    it('should return PreconditionFailed if request header ' +
       'includes "if-match" and ETag of object ' +
       'does not match specified ETag', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { 'if-match': incorrectMD5 },
            url: `/${bucketName}/${objectName}`,
            actionImplicitDenies: false,
        };

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectHead(authInfo, testGetRequest, log, err => {
                        assert.strictEqual(err.is.PreconditionFailed, true);
                        done();
                    });
                });
        });
    });

    it('should return NotModified if request header ' +
       'includes "if-none-match" and ETag of object does ' +
       'match specified ETag', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { 'if-none-match': correctMD5 },
            url: `/${bucketName}/${objectName}`,
            actionImplicitDenies: false,
        };

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectHead(authInfo, testGetRequest, log, err => {
                        assert.strictEqual(err.is.NotModified, true);
                        done();
                    });
                });
        });
    });

    it('should return Accept-Ranges header if request includes "Range" ' +
       'header with specified range bytes of an object', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { range: 'bytes=1-9' },
            url: `/${bucketName}/${objectName}`,
            actionImplicitDenies: false,
        };

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
                assert.strictEqual(err, null, `Error copying: ${err}`);
                objectHead(authInfo, testGetRequest, log, (err, res) => {
                    assert.strictEqual(res['accept-ranges'], 'bytes');
                    done();
                });
            });
        });
    });

    it('should return InvalidRequest error when both the Range header and ' +
       'the partNumber query parameter specified', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { range: 'bytes=1-9' },
            url: `/${bucketName}/${objectName}`,
            query: {
                partNumber: '1',
            },
            actionImplicitDenies: false,
        };

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
                assert.strictEqual(err, null, `Error objectPut: ${err}`);
                objectHead(authInfo, testGetRequest, log, err => {
                    assert.strictEqual(err.is.InvalidRequest, true);
                    assert.strictEqual(err.description,
                        'Cannot specify both Range header and ' +
                        'partNumber query parameter.');
                    done();
                });
            });
        });
    });

    it('should return InvalidArgument error if partNumber is nan', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
            query: {
                partNumber: 'nan',
            },
            actionImplicitDenies: false,
        };

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
                assert.strictEqual(err, null, `Error objectPut: ${err}`);
                objectHead(authInfo, testGetRequest, log, err => {
                    assert.strictEqual(err.is.InvalidArgument, true);
                    assert.strictEqual(err.description, 'Part number must be a number.');
                    done();
                });
            });
        });
    });

    it('should not return Accept-Ranges header if request does not include ' +
       '"Range" header with specified range bytes of an object', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
            actionImplicitDenies: false,
        };

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
                assert.strictEqual(err, null, `Error objectPut: ${err}`);
                objectHead(authInfo, testGetRequest, log, (err, res) => {
                    assert.strictEqual(res['accept-ranges'], undefined);
                    done();
                });
            });
        });
    });

    it('should get the object metadata', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
            actionImplicitDenies: false,
        };

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectHead(authInfo, testGetRequest, log, (err, res) => {
                        assert.strictEqual(res[userMetadataKey],
                            userMetadataValue);
                        assert
                        .strictEqual(res.ETag, `"${correctMD5}"`);
                        done();
                    });
                });
        });
    });

    it('should get the object metadata with object lock', done => {
        const testPutBucketRequestLock = {
            bucketName,
            namespace,
            headers: { 'x-amz-bucket-object-lock-enabled': 'true' },
            url: `/${bucketName}`,
            actionImplicitDenies: false,
        };
        const testPutObjectRequestLock = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {
                'x-amz-object-lock-retain-until-date': '2050-10-10',
                'x-amz-object-lock-mode': 'GOVERNANCE',
                'x-amz-object-lock-legal-hold': 'ON',
            },
            url: `/${bucketName}/${objectName}`,
            calculatedHash: correctMD5,
        }, postBody);
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
            actionImplicitDenies: false,
        };

        bucketPut(authInfo, testPutBucketRequestLock, log, () => {
            objectPut(authInfo, testPutObjectRequestLock, undefined, log,
                (err, resHeaders) => {
                    assert.ifError(err);
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectHead(authInfo, testGetRequest, log, (err, res) => {
                        assert.ifError(err);
                        const expectedDate = testPutObjectRequestLock
                        .headers['x-amz-object-lock-retain-until-date'];
                        const expectedMode = testPutObjectRequestLock
                        .headers['x-amz-object-lock-mode'];
                        assert.ifError(err);
                        assert.strictEqual(
                            res['x-amz-object-lock-retain-until-date'],
                            expectedDate);
                        assert.strictEqual(res['x-amz-object-lock-mode'],
                            expectedMode);
                        assert.strictEqual(res['x-amz-object-lock-legal-hold'],
                            'ON');
                        changeObjectLock([{
                            bucket: bucketName,
                            key: objectName,
                            versionId: res['x-amz-version-id'],
                        }], '', done);
                    });
                });
        });
    });

    it('should reflect the storage location in storage class if the object is archived', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
        };
        mdColdHelper.putBucketMock(bucketName, null, () => {
            mdColdHelper.putObjectMock(bucketName, objectName, mdColdHelper.getArchivedObjectMD(), () => {
                objectHead(authInfo, testGetRequest, log, (err, res) => {
                    assert.strictEqual(res[userMetadataKey], userMetadataValue);
                    assert.strictEqual(res.ETag, `"${correctMD5}"`);
                    assert.strictEqual(res['x-amz-storage-class'], mdColdHelper.defaultLocation);
                    // Check we do not leak non-standard fields
                    assert.strictEqual(res['x-amz-scal-transition-in-progress'], undefined);
                    assert.strictEqual(res['x-amz-scal-archive-info'], undefined);
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
                objectHead(authInfo, testGetRequest, log, (err, res) => {
                    assert.strictEqual(res[userMetadataKey], userMetadataValue);
                    assert.strictEqual(res.ETag, `"${correctMD5}"`);
                    assert.strictEqual(res['x-amz-storage-class'], undefined);
                    done();
                });
            });
        });
    });

    it('should reflect the restore header with ongoing-request=true if the object is being restored', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
        };
        mdColdHelper.putBucketMock(bucketName, null, () => {
            mdColdHelper.putObjectMock(bucketName, objectName, mdColdHelper.getRestoringObjectMD(), () => {
                objectHead(authInfo, testGetRequest, log, (err, res) => {
                    assert.strictEqual(res[userMetadataKey], userMetadataValue);
                    assert.strictEqual(res.ETag, `"${correctMD5}"`);
                    assert.strictEqual(res['x-amz-storage-class'], mdColdHelper.defaultLocation);
                    assert.strictEqual(res['x-amz-restore'], 'ongoing-request="true"');
                    // Check we do not leak non-standard fields
                    assert.strictEqual(res['x-amz-scal-transition-in-progress'], undefined);
                    assert.strictEqual(res['x-amz-scal-archive-info'], undefined);
                    assert.strictEqual(res['x-amz-scal-restore-requested-at'], undefined);
                    assert.strictEqual(res['x-amz-scal-restore-requested-days'], undefined);
                    assert.strictEqual(res['x-amz-scal-owner-id'], undefined);
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
            mdColdHelper.putObjectMock(bucketName, objectName, objectCustomMDFields, () => {
                objectHead(authInfo, testGetRequest, log, (err, res) => {
                    const restoreInfo = objectCustomMDFields.getAmzRestore();
                    assert.strictEqual(res[userMetadataKey], userMetadataValue);
                    assert.strictEqual(res.ETag, `"${correctMD5}"`);
                    assert.strictEqual(res['x-amz-storage-class'], mdColdHelper.defaultLocation);
                    const utcDate = new Date(restoreInfo.getExpiryDate()).toUTCString();
                    assert.strictEqual(res['x-amz-restore'], `ongoing-request="false", expiry-date="${utcDate}"`);
                    // Check we do not leak non-standard fields
                    assert.strictEqual(res['x-amz-scal-transition-in-progress'], undefined);
                    assert.strictEqual(res['x-amz-scal-archive-info'], undefined);
                    assert.strictEqual(res['x-amz-scal-restore-requested-at'], undefined);
                    assert.strictEqual(res['x-amz-scal-restore-completed-at'], undefined);
                    assert.strictEqual(res['x-amz-scal-restore-will-expire-at'], undefined);
                    assert.strictEqual(res['x-amz-scal-owner-id'], undefined);
                    done();
                });
            });
        });
    });

    it('should report when transition in progress', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
        };
        mdColdHelper.putBucketMock(bucketName, null, () => {
            const objectCustomMDFields = mdColdHelper.getTransitionInProgressObjectMD();
            mdColdHelper.putObjectMock(bucketName, objectName, objectCustomMDFields, () => {
                objectHead(authInfo, testGetRequest, log, (err, res) => {
                    assert.strictEqual(res['x-amz-storage-class'], undefined);
                    assert.strictEqual(res['x-amz-meta-scal-s3-transition-in-progress'], true);
                    assert.strictEqual(res['x-amz-scal-transition-in-progress'], undefined);
                    assert.strictEqual(res['x-amz-scal-transition-time'], undefined);
                    assert.strictEqual(res['x-amz-scal-archive-info'], undefined);
                    assert.strictEqual(res['x-amz-scal-owner-id'], undefined);
                    done(err);
                });
            });
        });
    });

    it('should report details when transition in progress', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {
                'x-amz-scal-archive-info': true,
            },
            url: `/${bucketName}/${objectName}`,
        };
        mdColdHelper.putBucketMock(bucketName, null, () => {
            const objectCustomMDFields = mdColdHelper.getTransitionInProgressObjectMD();
            mdColdHelper.putObjectMock(bucketName, objectName, objectCustomMDFields, () => {
                objectHead(authInfo, testGetRequest, log, (err, res) => {
                    assert.strictEqual(res['x-amz-meta-scal-s3-transition-in-progress'], true);
                    assert.strictEqual(res['x-amz-scal-transition-in-progress'], true);
                    assert.strictEqual(res['x-amz-scal-transition-time'],
                        new Date(objectCustomMDFields.getTransitionTime()).toUTCString());
                    assert.strictEqual(res['x-amz-scal-archive-info'], undefined);
                    assert.strictEqual(res['x-amz-scal-owner-id'], mdColdHelper.defaultOwnerId);
                    done(err);
                });
            });
        });
    });

    it('should report details when object is archived', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {
                'x-amz-scal-archive-info': true,
            },
            url: `/${bucketName}/${objectName}`,
        };
        mdColdHelper.putBucketMock(bucketName, null, () => {
            const objectCustomMDFields = mdColdHelper.getArchivedObjectMD();
            mdColdHelper.putObjectMock(bucketName, objectName, objectCustomMDFields, () => {
                objectHead(authInfo, testGetRequest, log, (err, res) => {
                    assert.strictEqual(res['x-amz-meta-scal-s3-transition-in-progress'], undefined);
                    assert.strictEqual(res['x-amz-scal-transition-in-progress'], undefined);
                    assert.strictEqual(res['x-amz-scal-archive-info'], '{"foo":0,"bar":"stuff"}');
                    assert.strictEqual(res['x-amz-storage-class'], mdColdHelper.defaultLocation);
                    assert.strictEqual(res['x-amz-scal-owner-id'], mdColdHelper.defaultOwnerId);
                    done(err);
                });
            });
        });
    });

    it('should report details when restore has been requested', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {
                'x-amz-scal-archive-info': true,
            },
            url: `/${bucketName}/${objectName}`,
        };
        mdColdHelper.putBucketMock(bucketName, null, () => {
            const objectCustomMDFields = mdColdHelper.getRestoringObjectMD();
            const archive = objectCustomMDFields.getArchive();
            mdColdHelper.putObjectMock(bucketName, objectName, objectCustomMDFields, () => {
                objectHead(authInfo, testGetRequest, log, (err, res) => {
                    assert.strictEqual(res['x-amz-meta-scal-s3-transition-in-progress'], undefined);
                    assert.strictEqual(res['x-amz-scal-transition-in-progress'], undefined);
                    assert.strictEqual(res['x-amz-scal-archive-info'], '{"foo":0,"bar":"stuff"}');
                    assert.strictEqual(res['x-amz-scal-restore-requested-at'],
                        new Date(archive.restoreRequestedAt).toUTCString());
                    assert.strictEqual(res['x-amz-scal-restore-requested-days'],
                        archive.restoreRequestedDays);
                    assert.strictEqual(res['x-amz-storage-class'], mdColdHelper.defaultLocation);
                    assert.strictEqual(res['x-amz-scal-owner-id'], mdColdHelper.defaultOwnerId);
                    done(err);
                });
            });
        });
    });

    it('should report details when object has been restored', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {
                'x-amz-scal-archive-info': true,
            },
            url: `/${bucketName}/${objectName}`,
        };
        mdColdHelper.putBucketMock(bucketName, null, () => {
            const objectCustomMDFields = mdColdHelper.getRestoredObjectMD();
            const archive = objectCustomMDFields.getArchive();
            mdColdHelper.putObjectMock(bucketName, objectName, objectCustomMDFields, () => {
                objectHead(authInfo, testGetRequest, log, (err, res) => {
                    assert.strictEqual(res['x-amz-meta-scal-s3-transition-in-progress'], undefined);
                    assert.strictEqual(res['x-amz-scal-transition-in-progress'], undefined);
                    assert.strictEqual(res['x-amz-scal-archive-info'], '{"foo":0,"bar":"stuff"}');
                    assert.strictEqual(res['x-amz-scal-restore-requested-at'],
                        new Date(archive.restoreRequestedAt).toUTCString());
                    assert.strictEqual(res['x-amz-scal-restore-requested-days'],
                        archive.restoreRequestedDays);
                    assert.strictEqual(res['x-amz-scal-restore-completed-at'],
                        new Date(archive.restoreCompletedAt).toUTCString());
                    assert.strictEqual(res['x-amz-scal-restore-will-expire-at'],
                        new Date(archive.restoreWillExpireAt).toUTCString());
                    assert.strictEqual(res['x-amz-scal-restore-etag'], mdColdHelper.restoredEtag);
                    assert.strictEqual(res['x-amz-storage-class'], mdColdHelper.defaultLocation);
                    assert.strictEqual(res['x-amz-scal-owner-id'], mdColdHelper.defaultOwnerId);
                    done(err);
                });
            });
        });
    });

    describe('x-amz-checksum-mode', () => {
        const checksumAlgorithms = [
            { name: 'sha256',    header: 'x-amz-checksum-sha256'    },
            { name: 'sha1',      header: 'x-amz-checksum-sha1'      },
            { name: 'crc32',     header: 'x-amz-checksum-crc32'     },
            { name: 'crc32c',    header: 'x-amz-checksum-crc32c'    },
            { name: 'crc64nvme', header: 'x-amz-checksum-crc64nvme' },
        ];

        // Digests of postBody ("I am a body") for each algorithm, computed once.
        const expectedDigests = {};

        before(done => {
            Promise.all(checksumAlgorithms.map(async ({ name }) => {
                expectedDigests[name] = await algorithms[name].digest(postBody);
            })).then(() => done(), done);
        });

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
                        };
                        objectHead(authInfo, req, log, (err, res) => {
                            assert.ifError(err);
                            assert.strictEqual(res[header], expectedDigests[name]);
                            assert.strictEqual(res['x-amz-checksum-type'], 'FULL_OBJECT');
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
                    };
                    objectHead(authInfo, req, log, (err, res) => {
                        assert.ifError(err);
                        checksumAlgorithms.forEach(({ header }) =>
                            assert.strictEqual(res[header], undefined));
                        assert.strictEqual(res['x-amz-checksum-type'], undefined);
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
                    };
                    objectHead(authInfo, req, log, (err, res) => {
                        assert.ifError(err);
                        checksumAlgorithms.forEach(({ header }) =>
                            assert.strictEqual(res[header], undefined));
                        assert.strictEqual(res['x-amz-checksum-type'], undefined);
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
            };
            objectHead(authInfo, req, log, err => {
                assert.strictEqual(err.is.InvalidArgument, true);
                done();
            });
        });
    });

    [
        {
            name: 'should return content-length of 0 when requesting part 1 of empty object',
            partNumber: '1',
            expectedError: null,
            expectedContentLength: 0
        },
        {
            name: 'should return InvalidRange error when requesting part > 1 of empty object',
            partNumber: '2',
            expectedError: 'InvalidRange',
            expectedContentLength: undefined
        }
    ].forEach(testCase => {
        it(testCase.name, done => {
            const emptyBody = '';
            const emptyMD5 = 'd41d8cd98f00b204e9800998ecf8427e';
            const testPutEmptyObjectRequest = new DummyRequest({
                bucketName,
                namespace,
                objectKey: objectName,
                headers: {
                    'content-length': '0',
                    'x-amz-meta-test': userMetadataValue,
                },
                parsedContentLength: 0,
                url: `/${bucketName}/${objectName}`,
                calculatedHash: emptyMD5,
            }, emptyBody);

            const testGetRequest = {
                bucketName,
                namespace,
                objectKey: objectName,
                headers: {},
                url: `/${bucketName}/${objectName}`,
                query: {
                    partNumber: testCase.partNumber,
                },
                actionImplicitDenies: false,
            };

            bucketPut(authInfo, testPutBucketRequest, log, () => {
                objectPut(authInfo, testPutEmptyObjectRequest, undefined, log, (err, resHeaders) => {
                    assert.strictEqual(err, null, `Error putting object: ${err}`);
                    assert.strictEqual(resHeaders.ETag, `"${emptyMD5}"`);
                    objectHead(authInfo, testGetRequest, log, (err, res) => {
                        if (testCase.expectedError) {
                            assert.strictEqual(err.is[testCase.expectedError], true);
                        } else {
                            assert.strictEqual(err, null);
                            assert.strictEqual(res[userMetadataKey], userMetadataValue);
                            assert.strictEqual(res.ETag, `"${emptyMD5}"`);
                            assert.strictEqual(res['content-length'], testCase.expectedContentLength);
                        }
                        done();
                    });
                });
            });
        });
    });
});
