const { versioning } = require('arsenal');
const versionIdUtils = versioning.VersionID;
const metadata = require('../../../../../lib/metadata/wrapper');
const { config } = require('../../../../../lib/Config');
const { DummyRequestLogger } = require('../../../../unit/helpers');
const log = new DummyRequestLogger();
const nonVersionedObjId =
    versionIdUtils.getInfVid(config.replicationGroupId);

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
    let decodedVersionId;
    if (versionId) {
        if (versionId === 'null') {
            decodedVersionId = nonVersionedObjId;
        } else {
            decodedVersionId = versionIdUtils.decode(versionId);
        }
    }
    return metadata.getObjectMD(bucketName, objectName, { versionId: decodedVersionId },
        log, cb);
}

function fakeMetadataRestore(bucketName, objectName, versionId, archive, cb) {
    let decodedVersionId;
    if (versionId) {
        if (versionId === 'null') {
            decodedVersionId = nonVersionedObjId;
        } else {
            decodedVersionId = versionIdUtils.decode(versionId);
        }
    }
    return getMetadata(bucketName, objectName, versionId, (err, objMD) => {
        if (err) {
			return cb(err);
		}
        /* eslint-disable no-param-reassign */
        objMD.dataStoreName = 'location-dmf-v1';
        objMD.archive = archive;
        /* eslint-enable no-param-reassign */
        return metadata.putObjectMD(bucketName, objectName, objMD, { versionId: decodedVersionId },
            log, err => cb(err));
    });
}

module.exports = {
	initMetadata,
	getMetadata,
	fakeMetadataRestore,
};
