const assert = require('assert');
const metadata = require('../../metadataswitch');
const { DummyRequestLogger } = require('../../helpers');
const log = new DummyRequestLogger();
const { BucketInfo, ObjectMD, ObjectMDAmzRestore, ObjectMDArchive } = require('arsenal').models;

const {
    LOCATION_NAME_DMF,
} = require('../../../constants');

const defaultLocation = LOCATION_NAME_DMF;
const defaultOwnerId = '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be';
const restoredEtag = '1234567890abcdef';

const baseMd = {
    'owner-display-name': 'accessKey1displayName',
    'owner-id': defaultOwnerId,
    'content-length': 11,
    'content-md5': 'be747eb4b75517bf6b3cf7c5fbb62f3a',
    'content-language': '',
    'x-amz-version-id': 'null',
    'x-amz-server-version-id': '',
    'x-amz-storage-class': 'STANDARD',
    'x-amz-server-side-encryption': '',
    'x-amz-server-side-encryption-aws-kms-key-id': '',
    'x-amz-server-side-encryption-customer-algorithm': '',
    'x-amz-website-redirect-location': '',
    acl: {
        Canned: 'private',
        FULL_CONTROL: [],
        WRITE_ACP: [],
        READ: [],
        READ_ACP: []
    },
    key: 'objectName',
    location: [
        {
            key: 1,
            size: 11,
            start: 0,
            dataStoreName: 'mem',
            dataStoreETag: '1:be747eb4b75517bf6b3cf7c5fbb62f3a'
        }
    ],
    isDeleteMarker: false,
    tags: {},
    replicationInfo: {
        status: '',
        backends: [],
        content: [],
        destination: '',
        storageClass: '',
        role: '',
        storageType: '',
        dataStoreVersionId: '',
        isNFS: null
    },
    dataStoreName: 'mem',
    originOp: 's3:ObjectCreated:Put',
    'last-modified': '2022-05-10T08:31:51.878Z',
    'md-model-version': 5,
    'x-amz-meta-test': 'some metadata'
};

/**
 * Mocks a Put Object to store custom metadata
 * @param {string} bucketName - the bucket name
 * @param {string|null} location - the bucket location
 * @param {Function} cb - callback once the bucket MD is created
 * @returns {undefined}
 */
function putBucketMock(bucketName, location, cb) {
    const bucket = new BucketInfo(
        bucketName,
        baseMd['owner-id'],
        baseMd['owner-display-name'],
        new Date().toJSON(),
        null,
        null,
        null,
        null,
        null,
        null,
        location);
    return metadata.createBucket(bucketName, bucket, log, cb);
}

/**
 * Mocks a Put Object to store custom metadata
 * @param {string} bucketName - the bucket name
 * @param {string} objectName - the object name
 * @param {ObjectMD} fields - custom MD fields to add to the object
 * @param {Function} cb - callback once the object MD is created
 * @returns {undefined}
 */
function putObjectMock(bucketName, objectName, fields, cb) {
    const md = fields ? fields.getValue() : baseMd;
    return metadata.putObjectMD(bucketName, objectName, md, {}, log, err => {
        assert.ifError(err);
        return cb();
    });
}

/**
 * Computes the 'archive' field of the object MD as an object being restored
 * @returns {ObjectMD} the MD object
 */
function getTransitionInProgressObjectMD() {
    return new ObjectMD(baseMd)
        .setTransitionInProgress(true, new Date(Date.now() - 60))
        .setOriginOp('s3:LifecycleTransition:Start');
}

/**
 * Computes the object MD of an archived object
 * @returns {ObjectMD} the MD object
 */
function getArchivedObjectMD() {
    return getTransitionInProgressObjectMD()
        .setArchive(new ObjectMDArchive(
            { foo: 0, bar: 'stuff' }, // opaque, can be anything...
        ))
        .setDataStoreName(defaultLocation)
        .setAmzStorageClass(defaultLocation)
        .setTransitionInProgress(false)
        .setOriginOp('s3:LifecycleTransition');
}

/**
 * Computes the object MD of an archived object being restored
 * @returns {ObjectMD} the MD object
 */
function getRestoringObjectMD() {
    const archivedObjectMD = getArchivedObjectMD();
    return archivedObjectMD
        .setAmzRestore(new ObjectMDAmzRestore(
            true,
        ))
        .setArchive(new ObjectMDArchive(
            archivedObjectMD.getArchive().getArchiveInfo(),
            new Date(Date.now() - 60),
            5,
        ))
        .setOriginOp('s3:ObjectRestore:Post');
}

/**
 * Computes the object MD as a restored object from cold storage
 * @param {Date} date - the expiry date of the restored object
 * @returns {ObjectMD} the MD object
 */
function getRestoredObjectMD(date) {
    const restoreDays = 5;
    const restoreDate = new Date((date || Date.now()) - 10000);
    const expiryDate = date || new Date(restoreDate.getTime() + 1000 * 60 * 60 * 24 * restoreDays);

    const restoringObjectMD = getRestoringObjectMD();
    const restoreInfo = new ObjectMDAmzRestore(
        false,
        expiryDate,
    );
    restoreInfo['content-md5'] = restoredEtag;

    return restoringObjectMD
        .setAmzRestore(restoreInfo)
        .setArchive(new ObjectMDArchive(
            restoringObjectMD.getArchive().getArchiveInfo(),
            new Date(Date.now() - 60000),
            restoreDays,
            restoreDate,
            expiryDate,
        ))
        .setDataStoreName('mem')
        .setOriginOp('s3:ObjectRestore:Completed');
}

/**
 * Computes the object MD of an restored object from cold storage which has expired
 * @returns {ObjectMD} the MD object
 */
function getExpiredObjectMD() {
    return getRestoredObjectMD(Date.now() - 10000);
}

module.exports = {
    putObjectMock,
    getArchivedObjectMD,
    getRestoringObjectMD,
    getRestoredObjectMD,
    getExpiredObjectMD,
    getTransitionInProgressObjectMD,
    putBucketMock,
    defaultLocation,
    defaultOwnerId,
    restoredEtag,
};
