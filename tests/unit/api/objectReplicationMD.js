const assert = require('assert');
const async = require('async');
const crypto = require('crypto');

const BucketInfo = require('arsenal').models.BucketInfo;

const { cleanup, DummyRequestLogger, makeAuthInfo, TaggingConfigTester } =
    require('../helpers');
const constants = require('../../../constants');
const { metadata } = require('../../../lib/metadata/in_memory/metadata');
const DummyRequest = require('../DummyRequest');
const objectDelete = require('../../../lib/api/objectDelete');
const objectPut = require('../../../lib/api/objectPut');
const objectCopy = require('../../../lib/api/objectCopy');
const completeMultipartUpload =
    require('../../../lib/api/completeMultipartUpload');
const objectPutACL = require('../../../lib/api/objectPutACL');
const objectPutTagging = require('../../../lib/api/objectPutTagging');
const objectDeleteTagging = require('../../../lib/api/objectDeleteTagging');

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const ownerID = authInfo.getCanonicalID();
const namespace = 'default';
const bucketName = 'source-bucket';
const mpuShadowBucket = `${constants.mpuBucketPrefix}${bucketName}`;
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
    role: '',
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
        role: 'arn:aws:iam::account-id:role/src-resource,' +
            'arn:aws:iam::account-id:role/dest-resource',
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

// Create the shadow bucket in metadata for MPUs with a recent model number.
function createShadowBucket(key, uploadId) {
    const overviewKey = `overview${constants.splitter}` +
        `${key}${constants.splitter}${uploadId}`;
    metadata.buckets
        .set(mpuShadowBucket, new BucketInfo(mpuShadowBucket, ownerID, '', ''));
     // Set modelVersion to use the most recent splitter.
    Object.assign(metadata.buckets.get(mpuShadowBucket), {
        _mdBucketModelVersion: 5,
    });
    metadata.keyMaps.set(mpuShadowBucket, new Map);
    metadata.keyMaps.get(mpuShadowBucket).set(overviewKey, new Map);
    Object.assign(metadata.keyMaps.get(mpuShadowBucket).get(overviewKey), {
        id: uploadId,
        eventualStorageBucket: bucketName,
        initiator: {
            DisplayName: 'accessKey1displayName',
            ID: ownerID },
        key,
        uploadId,
    });
}

// Initiate an MPU, put a part with the given body, and complete the MPU.
function putMPU(key, body, cb) {
    const uploadId = '9a0364b9e99bb480dd25e1f0284c8555';
    createShadowBucket(key, uploadId);
    const partBody = Buffer.from(body, 'utf8');
    const md5Hash = crypto.createHash('md5').update(partBody);
    const calculatedHash = md5Hash.digest('hex');
    const partKey = `${uploadId}${constants.splitter}00001`;
    const obj = {
        partLocations: [{
            key: 1,
            dataStoreName: 'mem',
            dataStoreETag: `1:${calculatedHash}`,
        }],
        key: partKey,
    };
    obj['content-md5'] = calculatedHash;
    obj['content-length'] = body.length;
    metadata.keyMaps.get(mpuShadowBucket).set(partKey, new Map);
    const partMap = metadata.keyMaps.get(mpuShadowBucket).get(partKey);
    Object.assign(partMap, obj);
    const postBody =
        '<CompleteMultipartUpload>' +
            '<Part>' +
                '<PartNumber>1</PartNumber>' +
                `<ETag>"${calculatedHash}"</ETag>` +
            '</Part>' +
        '</CompleteMultipartUpload>';
    const req = {
        bucketName,
        namespace,
        objectKey: key,
        parsedHost: 's3.amazonaws.com',
        url: `/${key}?uploadId=${uploadId}`,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        query: { uploadId },
        post: postBody,
    };
    return completeMultipartUpload(authInfo, req, log, cb);
}

// Copy an object where replication does not apply.
function copyObject(sourceObjectKey, copyObjectKey, hasContent, cb) {
    const req = getObjectPutReq(sourceObjectKey, hasContent);
    return objectPut(authInfo, req, undefined, log, err => {
        if (err) {
            return cb(err);
        }
        const req = new DummyRequest({
            bucketName,
            namespace,
            objectKey: copyObjectKey,
            headers: {},
            url: `/${bucketName}/${sourceObjectKey}`,
        });
        return objectCopy(authInfo, req, bucketName, sourceObjectKey, undefined,
            log, cb);
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

        it('should not update object metadata if completing MPU', done =>
            putMPU(keyA, 'content', err => {
                if (err) {
                    return done(err);
                }
                checkObjectReplicationInfo(keyA, emptyReplicationMD);
                return done();
            }));

        it('should not update object metadata if copying object', done =>
            copyObject(keyB, keyA, true, err => {
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
            role: 'arn:aws:iam::account-id:role/src-resource,' +
                'arn:aws:iam::account-id:role/dest-resource',
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

        describe('Complete MPU', () => {
            it("should update status to 'PENDING' and content to " +
                "'['DATA, METADATA']' if completing MPU", done =>
                putMPU(keyA, 'content', err => {
                    if (err) {
                        return done(err);
                    }
                    checkObjectReplicationInfo(keyA, newReplicationMD);
                    return done();
                }));

            it("should update status to 'PENDING' and content to " +
                "'['METADATA']' if completing MPU with 0 bytes", done =>
                putMPU(keyA, '', err => {
                    if (err) {
                        return done(err);
                    }
                    checkObjectReplicationInfo(keyA, replicateMetadataOnly);
                    return done();
                }));

            it('should not update replicationInfo if key does not apply',
                done => putMPU(keyB, 'content', err => {
                    if (err) {
                        return done(err);
                    }
                    checkObjectReplicationInfo(keyB, emptyReplicationMD);
                    return done();
                }));
        });

        describe('Object copy', () => {
            it("should update status to 'PENDING' and content to " +
                "'['DATA, METADATA']' if copying object", done =>
                copyObject(keyB, keyA, true, err => {
                    if (err) {
                        return done(err);
                    }
                    checkObjectReplicationInfo(keyA, newReplicationMD);
                    return done();
                }));

            it("should update status to 'PENDING' and content to " +
                "'['METADATA']' if copying object with 0 bytes", done =>
                copyObject(keyB, keyA, false, err => {
                    if (err) {
                        return done(err);
                    }
                    checkObjectReplicationInfo(keyA, replicateMetadataOnly);
                    return done();
                }));

            it('should not update replicationInfo if key does not apply',
                done => {
                    const copyKey = `foo-${keyA}`;
                    return copyObject(keyB, copyKey, true, err => {
                        if (err) {
                            return done(err);
                        }
                        checkObjectReplicationInfo(copyKey, emptyReplicationMD);
                        return done();
                    });
                });
        });
    });
});
