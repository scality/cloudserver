const assert = require('assert');
const async = require('async');

const BucketInfo = require('../../../lib/metadata/BucketInfo');

const { cleanup, DummyRequestLogger, makeAuthInfo, TaggingConfigTester } =
    require('../helpers');
const { metadata } = require('../../../lib/metadata/in_memory/metadata');
const DummyRequest = require('../DummyRequest');
const objectDelete = require('../../../lib/api/objectDelete');
const objectPut = require('../../../lib/api/objectPut');
const objectPutACL = require('../../../lib/api/objectPutACL');
const objectPutTagging = require('../../../lib/api/objectPutTagging');
const objectDeleteTagging = require('../../../lib/api/objectDeleteTagging');

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const ownerID = authInfo.getCanonicalID();
const namespace = 'default';
const bucketName = 'source-bucket';
const bucketARN = `arn:aws:s3:::${bucketName}`;
const storageClassType = 'STANDARD';
const keyA = 'key-A';
const keyB = 'key-B';

const deleteReq = new DummyRequest({
    bucketName,
    namespace,
    objectKey: keyA,
    headers: {},
    url: `/${bucketName}/${keyA}`,
});

const objectACLReq = {
    bucketName,
    namespace,
    objectKey: keyA,
    headers: {
        'x-amz-grant-read': `id=${ownerID}`,
        'x-amz-grant-read-acp': `id=${ownerID}`,
    },
    url: `/${bucketName}/${keyA}?acl`,
    query: { acl: '' },
};

// Get an object request with the given key.
function getObjectPutReq(key, hasContent) {
    const bodyContent = hasContent ? 'body content' : '';
    return new DummyRequest({
        bucketName,
        namespace,
        objectKey: key,
        headers: {},
        url: `/${bucketName}/${key}`,
    }, Buffer.from(bodyContent, 'utf8'));
}

const taggingPutReq = new TaggingConfigTester()
    .createObjectTaggingRequest('PUT', bucketName, keyA);
const taggingDeleteReq = new TaggingConfigTester()
    .createObjectTaggingRequest('DELETE', bucketName, keyA);

const emptyReplicationMD = {
    status: '',
    content: [],
    destination: '',
    storageClass: '',
};

// Check that the object key has the expected replication information.
function checkObjectReplicationInfo(key, expected) {
    const objectMD = metadata.keyMaps.get(bucketName).get(key);
    assert.deepStrictEqual(objectMD.replicationInfo, expected);
}

// Put the object key and check the replication information.
function putObjectAndCheckMD(key, expected, cb) {
    return objectPut(authInfo, getObjectPutReq(key, true), undefined, log,
        err => {
            if (err) {
                return cb(err);
            }
            checkObjectReplicationInfo(key, expected);
            return cb();
        });
}

// Create the bucket in metadata.
function createBucket() {
    metadata
        .buckets.set(bucketName, new BucketInfo(bucketName, ownerID, '', ''));
    metadata.keyMaps.set(bucketName, new Map);
}

// Create the bucket in metadata with versioning and a replication config.
function createBucketWithReplication(hasStorageClass) {
    createBucket();
    const config = {
        role: 'arn:aws:iam::account-id:role/resource',
        destination: 'arn:aws:s3:::source-bucket',
        rules: [{
            prefix: keyA,
            enabled: true,
            id: 'test-id',
        }],
    };
    if (hasStorageClass) {
        config.rules[0].storageClass = storageClassType;
    }
    Object.assign(metadata.buckets.get(bucketName), {
        _versioningConfiguration: { status: 'Enabled' },
        _replicationConfiguration: config,
    });
}

describe('Replication object MD without bucket replication config', () => {
    beforeEach(() => {
        cleanup();
        createBucket();
    });

    afterEach(() => cleanup());

    it('should not update object metadata', done =>
        putObjectAndCheckMD(keyA, emptyReplicationMD, done));

    it('should not update object metadata if putting object ACL', done =>
        async.series([
            next => putObjectAndCheckMD(keyA, emptyReplicationMD, next),
            next => objectPutACL(authInfo, objectACLReq, log, next),
        ], err => {
            if (err) {
                return done(err);
            }
            checkObjectReplicationInfo(keyA, emptyReplicationMD);
            return done();
        }));

    describe('Object tagging', () => {
        beforeEach(done => async.series([
            next => putObjectAndCheckMD(keyA, emptyReplicationMD, next),
            next => objectPutTagging(authInfo, taggingPutReq, log, next),
        ], err => done(err)));

        it('should not update object metadata if putting tag', done => {
            checkObjectReplicationInfo(keyA, emptyReplicationMD);
            return done();
        });

        it('should not update object metadata if deleting tag', done =>
            async.series([
                // Put a new version to update replication MD content array.
                next => putObjectAndCheckMD(keyA, emptyReplicationMD, next),
                next => objectDeleteTagging(authInfo, taggingDeleteReq, log,
                    next),
            ], err => {
                if (err) {
                    return done(err);
                }
                checkObjectReplicationInfo(keyA, emptyReplicationMD);
                return done();
            }));
    });
});

[true, false].forEach(hasStorageClass => {
    describe('Replication object MD with bucket replication config ' +
    `${hasStorageClass ? 'with' : 'without'} storage class`, () => {
        const replicationMD = {
            status: 'PENDING',
            content: ['DATA', 'METADATA'],
            destination: bucketARN,
            storageClass: '',
        };
        const newReplicationMD = hasStorageClass ? Object.assign(replicationMD,
            { storageClass: storageClassType }) : replicationMD;
        const replicateMetadataOnly = Object.assign({}, newReplicationMD,
            { content: ['METADATA'] });

        beforeEach(() => {
            cleanup();
            createBucketWithReplication(hasStorageClass);
        });

        afterEach(() => cleanup());

        it('should update metadata when replication config prefix matches ' +
        'an object key', done =>
            putObjectAndCheckMD(keyA, newReplicationMD, done));

        it('should update metadata when replication config prefix matches ' +
        'the start of an object key', done =>
            putObjectAndCheckMD(`${keyA}abc`, newReplicationMD, done));

        it('should not update metadata when replication config prefix does ' +
        'not match the start of an object key', done =>
            putObjectAndCheckMD(`abc${keyA}`, emptyReplicationMD, done));

        it('should not update metadata when replication config prefix does ' +
        'not apply', done =>
            putObjectAndCheckMD(keyB, emptyReplicationMD, done));

        it("should update status to 'PENDING' if putting a new version", done =>
            putObjectAndCheckMD(keyA, newReplicationMD, err => {
                if (err) {
                    return done(err);
                }
                const objectMD = metadata.keyMaps.get(bucketName).get(keyA);
                // Update metadata to a status after replication has occurred.
                objectMD.replicationInfo.status = 'COMPLETED';
                return putObjectAndCheckMD(keyA, newReplicationMD, done);
            }));

        it("should update status to 'PENDING' and content to '['METADATA']' " +
            'if putting 0 byte object', done =>
            objectPut(authInfo, getObjectPutReq(keyA, false), undefined, log,
                err => {
                    if (err) {
                        return done(err);
                    }
                    checkObjectReplicationInfo(keyA, replicateMetadataOnly);
                    return done();
                }));

        it("should update status to 'PENDING' and content to '['METADATA']' " +
            'if putting object ACL', done =>
            async.series([
                next => putObjectAndCheckMD(keyA, newReplicationMD, next),
                next => objectPutACL(authInfo, objectACLReq, log, next),
            ], err => {
                if (err) {
                    return done(err);
                }
                checkObjectReplicationInfo(keyA, replicateMetadataOnly);
                return done();
            }));

        it('should update metadata if putting a delete marker', done =>
            async.series([
                next => putObjectAndCheckMD(keyA, newReplicationMD, err => {
                    if (err) {
                        return next(err);
                    }
                    const objectMD = metadata.keyMaps.get(bucketName).get(keyA);
                    // Set metadata to a status after replication has occurred.
                    objectMD.replicationInfo.status = 'COMPLETED';
                    return next();
                }),
                next => objectDelete(authInfo, deleteReq, log, next),
            ], err => {
                if (err) {
                    return done(err);
                }
                const objectMD = metadata.keyMaps.get(bucketName).get(keyA);
                assert.strictEqual(objectMD.isDeleteMarker, true);
                checkObjectReplicationInfo(keyA, replicateMetadataOnly);
                return done();
            }));

        describe('Object tagging', () => {
            beforeEach(done => async.series([
                next => putObjectAndCheckMD(keyA, newReplicationMD, next),
                next => objectPutTagging(authInfo, taggingPutReq, log, next),
            ], err => done(err)));

            it("should update status to 'PENDING' and content to " +
                "'['METADATA']'if putting tag", done => {
                checkObjectReplicationInfo(keyA, replicateMetadataOnly);
                return done();
            });

            it("should update status to 'PENDING' and content to " +
                "'['METADATA']' if deleting tag", done =>
                async.series([
                    // Put a new version to update replication MD content array.
                    next => putObjectAndCheckMD(keyA, newReplicationMD, next),
                    next => objectDeleteTagging(authInfo, taggingDeleteReq, log,
                        next),
                ], err => {
                    if (err) {
                        return done(err);
                    }
                    checkObjectReplicationInfo(keyA, replicateMetadataOnly);
                    return done();
                }));
        });
    });
});
