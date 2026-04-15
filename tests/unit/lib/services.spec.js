const assert = require('assert');
const sinon = require('sinon');
const { versioning } = require('arsenal');

const services = require('../../../lib/services');
const metadata = require('../../../lib/metadata/wrapper');
const acl = require('../../../lib/metadata/acl');
const { DummyRequestLogger } = require('../helpers');

const { VersionId } = versioning.VersioningConstants;

describe('services', () => {
    const log = new DummyRequestLogger();
    const bucketName = 'test-bucket';
    const objectKey = 'test-object';

    afterEach(() => {
        sinon.restore();
    });

    describe('findObjectVersionByUploadId', () => {
        let getObjectListingStub;

        beforeEach(() => {
            getObjectListingStub = sinon.stub(services, 'getObjectListing');
        });

        it('should call getObjectListing with an optimized prefix', done => {
            getObjectListingStub.yields(null, { Versions: [], IsTruncated: false });

            services.findObjectVersionByUploadId(bucketName, objectKey, 'any-upload-id', log, err => {
                assert.ifError(err);
                sinon.assert.calledOnce(getObjectListingStub);

                const listParams = getObjectListingStub.getCall(0).args[1];
                const expectedPrefix = `${objectKey}${VersionId.Separator}`;
                assert.strictEqual(listParams.prefix, expectedPrefix);
                assert.strictEqual(listParams.listingType, 'DelimiterVersions');
                done();
            });
        });

        it('should handle an error from getObjectListing', done => {
            const testError = new Error('listing failed');
            getObjectListingStub.yields(testError);

            services.findObjectVersionByUploadId(bucketName, objectKey, 'any-upload-id', log, err => {
                assert.deepStrictEqual(err, testError);
                sinon.assert.calledOnce(getObjectListingStub);
                done();
            });
        });

        it('should return null if no matching version is found', done => {
            getObjectListingStub.yields(null, { Versions: [], IsTruncated: false });

            services.findObjectVersionByUploadId(bucketName, objectKey, 'any-upload-id', log, (err, foundVersion) => {
                assert.ifError(err);
                sinon.assert.calledOnce(getObjectListingStub);
                assert.strictEqual(foundVersion, null);
                done();
            });
        });

        it('should find a version on the only page of results', done => {
            const uploadIdToFind = 'the-correct-upload-id';
            const correctVersionValue = { uploadId: uploadIdToFind, data: 'this is it' };
            const versions = [
                { key: objectKey, value: { uploadId: 'some-other-id' } },
                // Version with a different key but same uploadId to test the key check
                { key: 'another-object-key', value: { uploadId: uploadIdToFind } },
                { key: objectKey, value: correctVersionValue },
            ];
            getObjectListingStub.yields(null, { Versions: versions, IsTruncated: false });

            services.findObjectVersionByUploadId(bucketName, objectKey, uploadIdToFind, log, (err, foundVersion) => {
                assert.ifError(err);
                sinon.assert.calledOnce(getObjectListingStub);
                assert.deepStrictEqual(foundVersion, correctVersionValue);
                done();
            });
        });

        it('should read all pages if a matching version is not found', done => {
            getObjectListingStub.onFirstCall().yields(null, {
                Versions: [{ key: objectKey, value: { uploadId: 'id-page-1' } }],
                IsTruncated: true,
                NextKeyMarker: 'key-marker',
                NextVersionIdMarker: 'version-marker',
            });
            getObjectListingStub.onSecondCall().yields(null, {
                Versions: [{ key: objectKey, value: { uploadId: 'id-page-2' } }],
                IsTruncated: false,
            });

            services.findObjectVersionByUploadId(bucketName, objectKey, 'non-existent-upload-id',
                log, (err, foundVersion) => {
                    assert.ifError(err);
                    sinon.assert.calledTwice(getObjectListingStub);

                    const secondCallParams = getObjectListingStub.getCall(1).args[1];
                    assert.strictEqual(secondCallParams.keyMarker, 'key-marker');
                    assert.strictEqual(secondCallParams.versionIdMarker, 'version-marker');
                    assert.strictEqual(foundVersion, null);
                    done();
                });
        });

        it('should find a version on the first page of many and stop listing', done => {
            const uploadIdToFind = 'the-correct-upload-id';
            const correctVersionValue = { uploadId: uploadIdToFind, data: 'this is it' };
            const versions = [{ key: objectKey, value: correctVersionValue }];

            getObjectListingStub.onFirstCall().yields(null, {
                Versions: versions,
                IsTruncated: true,
                NextKeyMarker: 'key-marker',
                NextVersionIdMarker: 'version-marker',
            });
            getObjectListingStub.onSecondCall().yields(new Error('should not have been called'));

            services.findObjectVersionByUploadId(bucketName, objectKey, uploadIdToFind, log, (err, foundVersion) => {
                assert.ifError(err);
                sinon.assert.calledOnce(getObjectListingStub);
                assert.deepStrictEqual(foundVersion, correctVersionValue);
                done();
            });
        });

        it('should find a version on a subsequent page', done => {
            const uploadIdToFind = 'the-correct-upload-id';
            const correctVersionValue = { uploadId: uploadIdToFind, data: 'this is it' };
            const secondPageVersions = [{ key: objectKey, value: correctVersionValue }];

            getObjectListingStub.onFirstCall().yields(null, {
                Versions: [{ key: objectKey, value: { uploadId: 'some-other-id' } }],
                IsTruncated: true,
                NextKeyMarker: 'key-marker',
                NextVersionIdMarker: 'version-marker',
            });
            getObjectListingStub.onSecondCall().yields(null, {
                Versions: secondPageVersions,
                IsTruncated: false,
            });

            services.findObjectVersionByUploadId(bucketName, objectKey, uploadIdToFind, log, (err, foundVersion) => {
                assert.ifError(err);
                sinon.assert.calledTwice(getObjectListingStub);

                const secondCallParams = getObjectListingStub.getCall(1).args[1];
                assert.strictEqual(secondCallParams.keyMarker, 'key-marker');
                assert.strictEqual(secondCallParams.versionIdMarker, 'version-marker');

                assert.deepStrictEqual(foundVersion, correctVersionValue);
                done();
            });
        });
    });

    describe('metadataStoreMPObject checksum fields', () => {
        const baseParams = {
            objectKey,
            splitter: '|',
            uploadId: 'test-upload-id',
            eventualStorageBucket: bucketName,
            ownerDisplayName: 'owner',
            ownerID: 'ownerCanonicalId',
            initiatorDisplayName: 'initiator',
            initiatorID: 'initiatorId',
            headers: {},
            storageClass: 'STANDARD',
            metaHeaders: {},
        };

        let putObjectMDStub;

        beforeEach(() => {
            putObjectMDStub = sinon.stub(metadata, 'putObjectMD')
                .callsFake((bucket, key, md, opts, reqLog, cb) => cb(null));
            sinon.stub(acl, 'parseAclFromHeaders')
                .callsFake((params, cb) => cb(null, { Canned: 'private' }));
        });

        it('should store checksumAlgorithm, checksumType and checksumIsDefault when provided', done => {
            const params = {
                ...baseParams,
                checksumAlgorithm: 'crc32',
                checksumType: 'COMPOSITE',
                checksumIsDefault: false,
            };

            services.metadataStoreMPObject(bucketName, null, params, log, (err, mpuMD) => {
                assert.ifError(err);
                assert.strictEqual(mpuMD.checksumAlgorithm, 'crc32');
                assert.strictEqual(mpuMD.checksumType, 'COMPOSITE');
                assert.strictEqual(mpuMD.checksumIsDefault, false);
                done();
            });
        });

        it('should store default crc64nvme with checksumIsDefault true', done => {
            const params = {
                ...baseParams,
                checksumAlgorithm: 'crc64nvme',
                checksumType: 'FULL_OBJECT',
                checksumIsDefault: true,
            };

            services.metadataStoreMPObject(bucketName, null, params, log, (err, mpuMD) => {
                assert.ifError(err);
                assert.strictEqual(mpuMD.checksumAlgorithm, 'crc64nvme');
                assert.strictEqual(mpuMD.checksumType, 'FULL_OBJECT');
                assert.strictEqual(mpuMD.checksumIsDefault, true);
                done();
            });
        });

        it('should persist checksum fields to metadata backend', done => {
            const params = {
                ...baseParams,
                checksumAlgorithm: 'sha256',
                checksumType: 'COMPOSITE',
                checksumIsDefault: false,
            };

            services.metadataStoreMPObject(bucketName, null, params, log, err => {
                assert.ifError(err);
                sinon.assert.calledOnce(putObjectMDStub);
                const storedMD = putObjectMDStub.getCall(0).args[2];
                assert.strictEqual(storedMD.checksumAlgorithm, 'sha256');
                assert.strictEqual(storedMD.checksumType, 'COMPOSITE');
                assert.strictEqual(storedMD.checksumIsDefault, false);
                done();
            });
        });
    });
});
