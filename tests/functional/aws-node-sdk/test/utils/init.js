const { versioning } = require('arsenal');
const versionIdUtils = versioning.VersionID;
const metadata = require('../../../../../lib/metadata/wrapper');
const { config } = require('../../../../../lib/Config');
const { DummyRequestLogger } = require('../../../../unit/helpers');
const log = new DummyRequestLogger();
const nonVersionedObjId =
    versionIdUtils.getInfVid(config.replicationGroupId);

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

function initMetadata(done) {
	if (metadataInit === true) {
		return done();
	}
	return metadata.setup(err => {
		if (err) {
			return done(err);
		}
		metadataInit = true;
		return done();
	});
}

function getMetadata(bucketName, objectName, versionId, cb) {
    return metadata.getObjectMD(bucketName, objectName, { versionId: decodeVersionId(versionId) },
        log, cb);
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
    return getMetadata(bucketName, objectName, versionId, (err, objMD) => {
        if (err) {
			return cb(err);
		}
        /* eslint-disable no-param-reassign */
        objMD['x-amz-scal-transition-in-progress'] = true;
        /* eslint-enable no-param-reassign */
        return metadata.putObjectMD(bucketName, objectName, objMD, { versionId: decodeVersionId(versionId) },
            log, err => cb(err));
    });
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
    return getMetadata(bucketName, objectName, versionId, (err, objMD) => {
        if (err) {
			return cb(err);
		}
        /* eslint-disable no-param-reassign */
        objMD['x-amz-storage-class'] = 'location-dmf-v1';
        objMD.dataStoreName = 'location-dmf-v1';
        objMD.archive = archive;
        /* eslint-enable no-param-reassign */
        return metadata.putObjectMD(bucketName, objectName, objMD, { versionId: decodeVersionId(versionId) },
            log, err => cb(err));
    });
}

module.exports = {
	initMetadata,
	getMetadata,
	fakeMetadataArchive,
    fakeMetadataTransition,
};
