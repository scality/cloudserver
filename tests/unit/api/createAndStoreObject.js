/**
 * Unit tests for createAndStoreObject function
 * Tests cold storage restoration, versioning, and corner cases
 */

const assert = require('assert');
const { promisify } = require('util');
const { storage } = require('arsenal');
const sinon = require('sinon');

const { bucketPut } = require('../../../lib/api/bucketPut');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const metadata = require('../metadataswitch');
const rawCreateAndStoreObject = require('../../../lib/api/apiUtils/object/createAndStoreObject');
const DummyRequest = require('../DummyRequest');

const createAndStoreObject = promisify(rawCreateAndStoreObject);

const { ds } = storage.data.inMemory.datastore;
const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const canonicalID = authInfo.getCanonicalID();
const bucketName = 'test-bucket';
const objectKey = 'test-object';

const getObjectMDAsync = (bucket, key, params = {}) => new Promise((resolve, reject) => {
    metadata.getObjectMD(bucket, key, params, log, (err, data) => {
        if (err) {
            reject(err);
        } else {
            resolve(data);
        }
    });
});

describe('createAndStoreObject', () => {
    let testBucket;
    const getStoredObjectData = () => metadata.putObjectMD.lastCall.args[2].getValue();
    const getStoredOptions = () => metadata.putObjectMD.lastCall.args[3];

    beforeEach(done => {
        cleanup();
        const bucketRequest = new DummyRequest({
            bucketName,
            namespace: 'default',
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            url: '/',
        });
        bucketPut(authInfo, bucketRequest, log, err => {
            if (err) {
                return done(err);
            }
            return metadata.getBucket(bucketName, log, (err, bucket) => {
                testBucket = bucket;
                done(err);
            });
        });
    });

    afterEach(() => {
        cleanup();
        sinon.restore();
    });

    describe('Regular object creation', () => {
        it('should create object successfully', async () => {
            const request = new DummyRequest({
                bucketName,
                namespace: 'default',
                objectKey,
                headers: { 'content-type': 'text/plain' },
                url: `/${bucketName}/${objectKey}`,
            }, Buffer.from('test data', 'utf8'));

            const result = await createAndStoreObject(bucketName, testBucket, objectKey, null,
                authInfo, canonicalID, null, request, false, null,
                ['overhead'], log, 's3:ObjectCreated:Put');

            assert(result.contentMD5);

            const objMD = await getObjectMDAsync(bucketName, objectKey, {});
            assert.strictEqual(objMD['content-md5'], result.contentMD5);
        });

        it('should handle zero-byte object', async () => {
            const request = new DummyRequest({
                bucketName,
                namespace: 'default',
                objectKey,
                headers: { 'content-type': 'text/plain' },
                parsedContentLength: 0,
                url: `/${bucketName}/${objectKey}`,
            }, '');

            const result = await createAndStoreObject(bucketName, testBucket, objectKey, null,
                authInfo, canonicalID, null, request, false, null,
                ['overhead'], log, 's3:ObjectCreated:Put');

            assert(result.contentMD5);
        });

        it('should set bucketOwnerId when requester is not bucket owner', async () => {
            const authInfo2 = makeAuthInfo('accessKey2');
            sinon.spy(metadata, 'putObjectMD');

            const request = new DummyRequest({
                bucketName,
                namespace: 'default',
                objectKey,
                headers: {},
                url: `/${bucketName}/${objectKey}`,
            }, Buffer.from('test', 'utf8'));

            await createAndStoreObject(bucketName, testBucket, objectKey, null,
                authInfo2, authInfo2.getCanonicalID(), null, request, false, null,
                ['overhead'], log, 's3:ObjectCreated:Put');

            const storedObjMD = getStoredObjectData();
            assert.strictEqual(storedObjMD.bucketOwnerId, canonicalID);
        });
    });

    describe('Delete marker creation', () => {
        it('should create delete marker without storing data', async () => {
            const request = new DummyRequest({
                bucketName,
                namespace: 'default',
                objectKey,
                headers: {},
                url: `/${bucketName}/${objectKey}`,
            });

            await createAndStoreObject(bucketName, testBucket, objectKey, null,
                authInfo, canonicalID, null, request, true, null,
                ['overhead'], log, 's3:ObjectRemoved:DeleteMarkerCreated');

            assert.deepStrictEqual(ds, []);

            const objMD = await getObjectMDAsync(bucketName, objectKey, {});
            assert(objMD.isDeleteMarker);
        });
    });

    describe('Archived object replacement', () => {
        it('should trigger oplog event when replacing archived object in non-versioned bucket', async () => {
            const archivedObjMD = {
                'content-md5': 'abc123',
                'content-length': 100,
                'archive': {
                    'archiveInfo': { 'archiveID': 'archive-123' },
                },
            };

            sinon.spy(metadata, 'putObjectMD');

            const request = new DummyRequest({
                bucketName,
                namespace: 'default',
                objectKey,
                headers: {},
                url: `/${bucketName}/${objectKey}`,
            }, Buffer.from('new data', 'utf8'));

            await createAndStoreObject(bucketName, testBucket, objectKey, archivedObjMD,
                authInfo, canonicalID, null, request, false, null,
                ['overhead'], log, 's3:ObjectCreated:Put');

            const options = getStoredOptions();
            assert.strictEqual(options.needOplogUpdate, true);
            assert.strictEqual(options.originOp, 's3:ReplaceArchivedObject');
        });

        it('should not trigger oplog event for archived object in versioned bucket', async () => {
            const archivedObjMD = {
                'content-md5': 'abc123',
                'content-length': 100,
                'versionId': 'v1',
                'archive': {
                    'archiveInfo': { 'archiveID': 'archive-123' },
                },
            };

            sinon.stub(testBucket, 'isVersioningEnabled').returns(true);
            sinon.spy(metadata, 'putObjectMD');

            const request = new DummyRequest({
                bucketName,
                namespace: 'default',
                objectKey,
                headers: {},
                url: `/${bucketName}/${objectKey}`,
            }, Buffer.from('new data', 'utf8'));

            await createAndStoreObject(bucketName, testBucket, objectKey, archivedObjMD,
                authInfo, canonicalID, null, request, false, null,
                ['overhead'], log, 's3:ObjectCreated:Put');

            const options = getStoredOptions();
            assert.strictEqual(options.needOplogUpdate, undefined);
            assert.strictEqual(options.originOp, undefined);
        });

        it('should trigger oplog event for archived object in version-suspended bucket', async () => {
            const archivedObjMD = {
                'content-md5': 'abc123',
                'content-length': 100,
                archive: {
                    archiveInfo: { archiveID: 'archive-123' },
                },
            };

            sinon.stub(testBucket, 'isVersioningEnabled').returns(false);
            sinon.spy(metadata, 'putObjectMD');

            const request = new DummyRequest({
                bucketName,
                namespace: 'default',
                objectKey,
                headers: {},
                url: `/${bucketName}/${objectKey}`,
            }, Buffer.from('new data', 'utf8'));

            await createAndStoreObject(bucketName, testBucket, objectKey, archivedObjMD,
                authInfo, canonicalID, null, request, false, null,
                ['overhead'], log, 's3:ObjectCreated:Put');

            const options = getStoredOptions();
            assert.strictEqual(options.needOplogUpdate, true);
            assert.strictEqual(options.originOp, 's3:ReplaceArchivedObject');
        });

        it('should not trigger oplog event when archiveInfo is absent', async () => {
            const archivedObjMD = {
                'content-md5': 'abc123',
                'content-length': 100,
                archive: {
                    restoreRequestedAt: new Date().toISOString(),
                },
            };
            sinon.spy(metadata, 'putObjectMD');

            const request = new DummyRequest({
                bucketName,
                namespace: 'default',
                objectKey,
                headers: {},
                url: `/${bucketName}/${objectKey}`,
            }, Buffer.from('new data', 'utf8'));

            await createAndStoreObject(bucketName, testBucket, objectKey, archivedObjMD,
                authInfo, canonicalID, null, request, false, null,
                ['overhead'], log, 's3:ObjectCreated:Put');

            const options = getStoredOptions();
            assert.strictEqual(options.needOplogUpdate, undefined);
            assert.strictEqual(options.originOp, undefined);
        });
    });

    describe('Cold storage restoration (putObjectVersion)', () => {
        it('should restore object with x-scal-s3-version-id header', async () => {
            const now = Date.now();
            const archivedObjMD = {
                'key': objectKey,
                'versionId': 'v123',
                'content-md5': 'original-hash',
                'content-length': 100,
                'x-amz-storage-class': 'cold-location',
                'dataStoreName': 'cold-location',
                'x-amz-meta-custom': 'preserved-value',
                'tags': { 'tagkey': 'tagvalue' },
                'archive': {
                    'archiveInfo': { 'archiveID': 'archive-123' },
                    'restoreRequestedAt': new Date(now).toISOString(),
                    'restoreRequestedDays': 7,
                },
            };

            sinon.spy(metadata, 'putObjectMD');

            const request = new DummyRequest({
                bucketName,
                namespace: 'default',
                objectKey,
                headers: { 'x-scal-s3-version-id': 'v123' },
                url: `/${bucketName}/${objectKey}`,
            }, Buffer.from('restored data', 'utf8'));

            await createAndStoreObject(bucketName, testBucket, objectKey, archivedObjMD,
                authInfo, canonicalID, null, request, false, null,
                ['overhead'], log, 's3:ObjectCreated:Put');

            const storedObjMD = getStoredObjectData();
            const options = getStoredOptions();
            assert(storedObjMD.archive.restoreCompletedAt, 'restoreCompletedAt should be set');
            assert(storedObjMD.archive.restoreWillExpireAt, 'restoreWillExpireAt should be set');
            assert.strictEqual(storedObjMD.archive.restoreRequestedDays, 7);
            assert.strictEqual(storedObjMD['x-amz-meta-custom'], 'preserved-value');
            assert.deepStrictEqual(storedObjMD.tags, { 'tagkey': 'tagvalue' });
            assert.strictEqual(storedObjMD.originOp, 's3:ObjectRestore:Completed');
            assert.strictEqual(options.needOplogUpdate, undefined);
            assert.strictEqual(options.originOp, undefined);
        });

        it('should preserve original etag for MPU restoration with different part count', async () => {
            const archivedObjMD = {
                'versionId': 'v123',
                'content-md5': 'original-abc123-5', // Original had 5 parts
                'archive': {
                    'archiveInfo': { 'archiveID': 'archive-123' },
                    'restoreRequestedAt': new Date().toISOString(),
                    'restoreRequestedDays': 7,
                },
            };

            sinon.spy(metadata, 'putObjectMD');

            const request = new DummyRequest({
                bucketName,
                namespace: 'default',
                objectKey,
                headers: { 'x-scal-s3-version-id': 'v123' },
                url: `/${bucketName}/${objectKey}`,
                calculatedHash: 'restored-def456-3', // Restored with 3 parts
            }, Buffer.from('restored data', 'utf8'));

            await createAndStoreObject(bucketName, testBucket, objectKey, archivedObjMD,
                authInfo, canonicalID, null, request, false, null,
                ['overhead'], log, 's3:ObjectCreated:Put');

            const storedObjMD = getStoredObjectData();
            assert.strictEqual(storedObjMD['content-md5'], 'original-abc123-5',
                'Original etag should be preserved');
            assert(storedObjMD['x-amz-restore']['content-md5']);
            assert.notStrictEqual(storedObjMD['x-amz-restore']['content-md5'],
                storedObjMD['content-md5']);
        });

        it('should preserve replication info during restoration', async () => {
            const replicationInfo = {
                status: 'COMPLETED',
                backends: [{ site: 'site1', status: 'COMPLETED' }],
            };

            const archivedObjMD = {
                'versionId': 'v123',
                replicationInfo,
                'archive': {
                    'archiveInfo': { 'archiveID': 'archive-123' },
                    'restoreRequestedAt': new Date().toISOString(),
                    'restoreRequestedDays': 7,
                },
            };

            sinon.spy(metadata, 'putObjectMD');

            const request = new DummyRequest({
                bucketName,
                namespace: 'default',
                objectKey,
                headers: { 'x-scal-s3-version-id': 'v123' },
                url: `/${bucketName}/${objectKey}`,
            }, Buffer.from('restored', 'utf8'));

            await createAndStoreObject(bucketName, testBucket, objectKey, archivedObjMD,
                authInfo, canonicalID, null, request, false, null,
                ['overhead'], log, 's3:ObjectCreated:Put');

            const storedObjMD = getStoredObjectData();
            assert.strictEqual(storedObjMD.replicationInfo.status, replicationInfo.status,
                'Replication status should be preserved');
            assert.deepStrictEqual(storedObjMD.replicationInfo.backends, replicationInfo.backends,
                'Replication backends should be preserved');
        });

        it('should preserve legal hold during restoration', async () => {
            const archivedObjMD = {
                'versionId': 'v123',
                'legalHold': true,
                'archive': {
                    'archiveInfo': { 'archiveID': 'archive-123' },
                    'restoreRequestedAt': new Date().toISOString(),
                    'restoreRequestedDays': 7,
                },
            };

            sinon.spy(metadata, 'putObjectMD');

            const request = new DummyRequest({
                bucketName,
                namespace: 'default',
                objectKey,
                headers: { 'x-scal-s3-version-id': 'v123' },
                url: `/${bucketName}/${objectKey}`,
            }, Buffer.from('restored', 'utf8'));

            await createAndStoreObject(bucketName, testBucket, objectKey, archivedObjMD,
                authInfo, canonicalID, null, request, false, null,
                ['overhead'], log, 's3:ObjectCreated:Put');

            const storedObjMD = getStoredObjectData();
            assert.strictEqual(storedObjMD.legalHold, true,
                'Legal hold should be preserved');
        });

        it('should preserve ACLs during restoration', async () => {
            const acl = {
                'Canned': '',
                'FULL_CONTROL': ['canonical-id-1'],
                'READ': ['canonical-id-2'],
            };

            const archivedObjMD = {
                'versionId': 'v123',
                acl,
                'archive': {
                    'archiveInfo': { 'archiveID': 'archive-123' },
                    'restoreRequestedAt': new Date().toISOString(),
                    'restoreRequestedDays': 7,
                },
            };

            sinon.spy(metadata, 'putObjectMD');

            const request = new DummyRequest({
                bucketName,
                namespace: 'default',
                objectKey,
                headers: { 'x-scal-s3-version-id': 'v123' },
                url: `/${bucketName}/${objectKey}`,
            }, Buffer.from('restored', 'utf8'));

            await createAndStoreObject(bucketName, testBucket, objectKey, archivedObjMD,
                authInfo, canonicalID, null, request, false, null,
                ['overhead'], log, 's3:ObjectCreated:Put');

            const storedObjMD = getStoredObjectData();
            assert.deepStrictEqual(storedObjMD.acl, acl,
                'ACLs should be preserved');
        });

        it('should not preserve x-amz-meta-scal-s3-restore-attempt metadata', async () => {
            const archivedObjMD = {
                'versionId': 'v123',
                'x-amz-meta-custom': 'keep-this',
                'x-amz-meta-scal-s3-restore-attempt': '3',
                'archive': {
                    'archiveInfo': { 'archiveID': 'archive-123' },
                    'restoreRequestedAt': new Date().toISOString(),
                    'restoreRequestedDays': 7,
                },
            };

            sinon.spy(metadata, 'putObjectMD');

            const request = new DummyRequest({
                bucketName,
                namespace: 'default',
                objectKey,
                headers: { 'x-scal-s3-version-id': 'v123' },
                url: `/${bucketName}/${objectKey}`,
            }, Buffer.from('restored', 'utf8'));

            await createAndStoreObject(bucketName, testBucket, objectKey, archivedObjMD,
                authInfo, canonicalID, null, request, false, null,
                ['overhead'], log, 's3:ObjectCreated:Put');

            const storedObjMD = getStoredObjectData();
            assert.strictEqual(storedObjMD['x-amz-meta-custom'], 'keep-this',
                'Custom metadata should be preserved');
            assert.strictEqual(storedObjMD['x-amz-meta-scal-s3-restore-attempt'], undefined,
                'Restore attempt metadata should NOT be preserved');
        });
    });

    describe('MPU scenarios', () => {
        it('should set oldReplayId when overwriting MPU object', async () => {
            const mpuObjMD = {
                'uploadId': 'mpu-upload-123',
                'content-md5': 'abc123',
            };

            sinon.spy(metadata, 'putObjectMD');

            const request = new DummyRequest({
                bucketName,
                namespace: 'default',
                objectKey,
                headers: {},
                url: `/${bucketName}/${objectKey}`,
            }, Buffer.from('new data', 'utf8'));

            await createAndStoreObject(bucketName, testBucket, objectKey, mpuObjMD,
                authInfo, canonicalID, null, request, false, null,
                ['overhead'], log, 's3:ObjectCreated:Put');

            const options = getStoredOptions();
            assert.strictEqual(options.oldReplayId, 'mpu-upload-123');
        });
    });

    describe('Azure compatibility', () => {
        it('should preserve creation-time from existing object', async () => {
            const existingObjMD = {
                'creation-time': '2024-01-01T00:00:00.000Z',
                'last-modified': '2024-02-01T00:00:00.000Z',
            };

            sinon.spy(metadata, 'putObjectMD');

            const request = new DummyRequest({
                bucketName,
                namespace: 'default',
                objectKey,
                headers: {},
                url: `/${bucketName}/${objectKey}`,
            }, Buffer.from('new data', 'utf8'));

            await createAndStoreObject(bucketName, testBucket, objectKey, existingObjMD,
                authInfo, canonicalID, null, request, false, null,
                ['overhead'], log, 's3:ObjectCreated:Put');

            const storedObjMD = getStoredObjectData();
            assert.strictEqual(storedObjMD['creation-time'], '2024-01-01T00:00:00.000Z');
        });

        it('should fall back to last-modified if creation-time missing', async () => {
            const existingObjMD = {
                'last-modified': '2024-02-01T00:00:00.000Z',
            };

            sinon.spy(metadata, 'putObjectMD');

            const request = new DummyRequest({
                bucketName,
                namespace: 'default',
                objectKey,
                headers: {},
                url: `/${bucketName}/${objectKey}`,
            }, Buffer.from('new data', 'utf8'));

            await createAndStoreObject(bucketName, testBucket, objectKey, existingObjMD,
                authInfo, canonicalID, null, request, false, null,
                ['overhead'], log, 's3:ObjectCreated:Put');

            const storedObjMD = getStoredObjectData();
            assert.strictEqual(storedObjMD['creation-time'], '2024-02-01T00:00:00.000Z');
        });
    });

    describe('Integration-sensitive restore behavior', () => {
        it('should keep x-amz-meta-scal-version-id when restoring to ingestion location', async () => {
            sinon.stub(testBucket, 'isIngestionBucket').returns(true);
            sinon.spy(metadata, 'putObjectMD');
            const putVersionId = 'restore-version-id';
            const archivedObjMD = {
                versionId: putVersionId,
                archive: {
                    archiveInfo: { archiveID: 'archive-123' },
                    restoreRequestedAt: new Date().toISOString(),
                    restoreRequestedDays: 7,
                },
            };

            const request = new DummyRequest({
                bucketName,
                namespace: 'default',
                objectKey,
                headers: { 'x-scal-s3-version-id': putVersionId },
                url: `/${bucketName}/${objectKey}`,
            }, Buffer.from('restored', 'utf8'));

            await createAndStoreObject(bucketName, testBucket, objectKey, archivedObjMD,
                authInfo, canonicalID, null, request, false, null,
                ['overhead'], log, 's3:ObjectCreated:Put');

            const storedObjMD = getStoredObjectData();
            assert.strictEqual(storedObjMD['x-amz-meta-scal-version-id'], putVersionId);
        });
    });
});
