const assert = require('assert');
const sinon = require('sinon');
const { versioning } = require('arsenal');

const services = require('../../../lib/services');
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
});
