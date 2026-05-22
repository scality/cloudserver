const { versioning } = require('arsenal');
const versionIdUtils = versioning.VersionID;
const metadata = require('../../../../../lib/metadata/wrapper');
const { config } = require('../../../../../lib/Config');
const { DummyRequestLogger } = require('../../../../unit/helpers');
const log = new DummyRequestLogger();
const nonVersionedObjId = versionIdUtils.getInfVid(config.replicationGroupId);

const { LOCATION_NAME_DMF } = require('../../../../constants');

const isMetadataOrFile = ['file', 'scality'].includes(config.backends.metadata);
/**
 * With null version compat mode the null key should look like
 * 'object1putversion\u000099999999999999999999RG001  '
 * Without it for BucketFile backends it looks like
 * 'object1putversion\x00'
 *
 * The later case does not support ObjectRestore and needs some tests to be skipped.
 *
 * TODO: CLDSRV-721 RING 10 Support ObjectRestore (cold storage) with MD v1
 */
const isNullKeyMetadataV1 = isMetadataOrFile && !config.nullVersionCompatMode;

function decodeVersionId(versionId) {
    let decodedVersionId;
    if (versionId) {
        if (versionId === 'null') {
            decodedVersionId = nonVersionedObjId;
        } else {
            decodedVersionId = versionIdUtils.decode(versionId);
        }
    }
    return decodedVersionId;
}

let metadataInit = false;

function initMetadata(cb) {
    const promise = (async () => {
        if (metadataInit) {
            return;
        }
        await new Promise((resolve, reject) => {
            metadata.setup(err => {
                if (err) {
                    return reject(err);
                }
                metadataInit = true;
                return resolve();
            });
        });
    })();
    return cb ? promise.then(() => cb(), cb) : promise;
}

function getMetadata(bucketName, objectName, versionId, cb) {
    const promise = new Promise((resolve, reject) =>
        metadata.getObjectMD(bucketName, objectName, { versionId: decodeVersionId(versionId) }, log, (err, data) =>
            err ? reject(err) : resolve(data),
        ),
    );
    return cb ? promise.then(res => cb(null, res), cb) : promise;
}

/**
 * updates an object's metadata to show it as transitioning to
 * another location
 * @param {string} bucketName bucket name
 * @param {string} objectName obejct name
 * @param {string} versionId encoded object version id
 * @param {Function} cb callback
 * @returns {undefined}
 */
function fakeMetadataTransition(bucketName, objectName, versionId, cb) {
    const promise = (async () => {
        const objMD = await getMetadata(bucketName, objectName, versionId);
        objMD['x-amz-scal-transition-in-progress'] = true;
        await new Promise((resolve, reject) =>
            metadata.putObjectMD(bucketName, objectName, objMD, { versionId: decodeVersionId(versionId) }, log, err =>
                err ? reject(err) : resolve(),
            ),
        );
    })();
    return cb ? promise.then(() => cb(), cb) : promise;
}

/**
 * changes an object's location to a cold location and
 * adds the archive info object
 * @param {string} bucketName bucket name
 * @param {string} objectName obejct name
 * @param {string} versionId encoded object version id
 * @param {Object} archive archive info object
 * @param {Function} cb callback
 * @returns {undefined}
 */
function fakeMetadataArchive(bucketName, objectName, versionId, archive, cb) {
    const promise = (async () => {
        const objMD = await getMetadata(bucketName, objectName, versionId);
        objMD['x-amz-storage-class'] = LOCATION_NAME_DMF;
        objMD.dataStoreName = LOCATION_NAME_DMF;
        objMD.archive = archive;
        await new Promise((resolve, reject) =>
            metadata.putObjectMD(bucketName, objectName, objMD, { versionId: decodeVersionId(versionId) }, log, err =>
                err ? reject(err) : resolve(),
            ),
        );
    })();
    return cb ? promise.then(() => cb(), cb) : promise;
}

module.exports = {
    isNullKeyMetadataV1,
    initMetadata,
    getMetadata,
    fakeMetadataArchive,
    fakeMetadataTransition,
};
