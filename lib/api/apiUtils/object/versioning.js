import { errors, versioning } from 'arsenal';

import metadata from '../../../metadata/wrapper';
import config from '../../../Config';

const versionIdUtils = versioning.VersionID;
// Use Arsenal function to generate a version ID used internally by metadata
// for null versions that are created before bucket versioning is configured
const nonVersionedObjId =
    versionIdUtils.getInfVid(config.replicationGroupId);

/** decodedVidResult - decode the version id from a query object
 * @param {object} [reqQuery] - request query object
 * @param {string} [reqQuery.versionId] - version ID sent in request query
 * @return {(Error|string|undefined)} - return Invalid Argument if decryption
 * fails due to improper format, otherwise undefined or the decoded version id
 */
export function decodeVersionId(reqQuery) {
    if (!reqQuery || !reqQuery.versionId) {
        return undefined;
    }
    let versionId = reqQuery.versionId;
    if (versionId === 'null') {
        return versionId;
    }
    versionId = versionIdUtils.decode(versionId);
    if (versionId instanceof Error) {
        return errors.InvalidArgument
            .customizeDescription('Invalid version id specified');
    }
    return versionId;
}

/** getVersionIdResHeader - return encrypted version ID if appropriate
 * @param {object} [verCfg] - bucket versioning configuration
 * @param {object} objectMD - object metadata
 * @return {(string|undefined)} - undefined or encrypted version ID
 * (if not 'null')
 */
export function getVersionIdResHeader(verCfg, objectMD) {
    if (verCfg) {
        if (objectMD.isNull || (objectMD && !objectMD.versionId)) {
            return 'null';
        }
        return versionIdUtils.encode(objectMD.versionId);
    }
    return undefined;
}

/**
 * Checks for versionId in request query and returns error if it is there
 * @param {object} query - request query
 * @return {(Error|undefined)} - customized InvalidArgument error or undefined
 */
export function checkQueryVersionId(query) {
    if (query && query.versionId !== undefined) {
        const customMsg = 'This operation does not accept a version-id.';
        return errors.InvalidArgument.customizeDescription(customMsg);
    }
    return undefined;
}

function _storeNullVersionMD(bucketName, objKey, objMD, options, log, cb) {
    metadata.putObjectMD(bucketName, objKey, objMD, options, log, err => {
        if (err) {
            log.debug('error from metadata storing null version as new version',
            { error: err });
        }
        cb(err, options);
    });
}

/** get location of data for deletion
* @param {string} bucketName - name of bucket
* @param {string} objKey - name of object key
* @param {object} options - metadata options for getting object MD
* @param {string} options.versionId - version to get from metadata
* @param {RequestLogger} log - logger instanceof
* @param {function} cb - callback
* @return {undefined} - and call callback with (err, dataToDelete)
*/
function _getDeleteLocations(bucketName, objKey, options, log, cb) {
    return metadata.getObjectMD(bucketName, objKey, options, log,
        (err, versionMD) => {
            if (err) {
                log.debug('err from metadata getting specified version', {
                    error: err,
                    method: '_getDeleteLocations',
                });
                return cb(err);
            }
            if (!versionMD.location) {
                return cb();
            }
            const dataToDelete = Array.isArray(versionMD.location) ?
                versionMD.location : [versionMD.location];
            return cb(null, dataToDelete);
        });
}

function _deleteNullVersionMD(bucketName, objKey, options, log, cb) {
    // before deleting null version md, retrieve location of data to delete
    return _getDeleteLocations(bucketName, objKey, options, log,
        (err, nullDataToDelete) => {
            if (err) {
                log.warn('could not find null version metadata', {
                    error: err,
                    method: '_deleteNullVersionMD',
                });
                return cb(err);
            }
            return metadata.deleteObjectMD(bucketName, objKey, options, log,
                err => {
                    if (err) {
                        log.warn('metadata error deleting null version',
                        { error: err, method: '_deleteNullVersionMD' });
                        return cb(err);
                    }
                    return cb(null, nullDataToDelete);
                });
        });
}

/** versioningPreprocessing - return versioning information for S3 to handle
 * creation of new versions and manage deletion of old data and metadata
 * @param {string} bucketName - name of bucket
 * @param {object} bucketMD - bucket metadata
 * @param {string} objectKey - name of object
 * @param {object} objMD - obj metadata
 * @param {string} [reqVersionId] - specific version ID sent as part of request
 * @param {RequestLogger} log - logger instance
 * @param {function} callback - callback
 * @return {undefined} and call callback with params (err, options):
 * options.dataToDelete - (array/undefined) location of data to delete
 * options.versionId - specific versionId to overwrite in metadata
 *  ('' overwrites the master version)
 * options.versioning - (true/undefined) metadata instruction to create new ver
 * options.isNull - (true/undefined) whether new version is null or not
 * options.nullVersionId - if storing a null version in version history, the
 *  version id of the null version
 * options.deleteNullVersionData - whether to delete the data of the null ver
 */
export function versioningPreprocessing(bucketName, bucketMD, objectKey,
    objMD, reqVersionId, log, callback) {
    const options = {};
    let mstObjLocation;
    if (objMD && objMD.location) {
        mstObjLocation = Array.isArray(objMD.location) ?
            objMD.location : [objMD.location];
    }
    // bucket is not versioning enabled
    if (!bucketMD.getVersioningConfiguration()) {
        options.dataToDelete = mstObjLocation;
        return callback(null, options);
    }
    // bucket is versioning enabled
    const mstVersionId = objMD ? objMD.versionId : undefined;
    const mstIsNull = objMD ? objMD.isNull : false;
    const vstat = bucketMD.getVersioningConfiguration().Status;
    if (!reqVersionId) {
        // non-version-specific versioning operation
        if (mstVersionId === undefined || mstIsNull) {
            // object does not exist or is not versioned (before versioning)
            if (vstat === 'Suspended') {
                // versioning is suspended, overwrite existing master version
                options.versionId = '';
                options.isNull = true;
                options.dataToDelete = mstObjLocation;
                return callback(null, options);
            }
            // versioning is enabled, create a new version
            options.versioning = true;
            if (objMD) {
                // store master version in a new key
                const versionId = mstIsNull ? mstVersionId : nonVersionedObjId;
                objMD.versionId = versionId; // eslint-disable-line
                objMD.isNull = true; // eslint-disable-line
                options.nullVersionId = versionId;
                return _storeNullVersionMD(bucketName, objectKey, objMD,
                    { versionId }, log, err => callback(err, options));
            }
            return callback(null, options);
        }
        // master is versioned and is not a null version
        const nullVersionId = objMD.nullVersionId;
        if (vstat === 'Suspended') {
            // versioning is suspended, overwrite the existing master version
            options.versionId = '';
            options.isNull = true;
            if (nullVersionId === undefined) {
                return callback(null, options);
            }
            return _deleteNullVersionMD(bucketName, objectKey,
                { versionId: nullVersionId }, log, (err, nullDataToDelete) => {
                    if (err) {
                        log.warn('unexpected error deleting null version md', {
                            error: err,
                            method: 'versioningPreprocessing',
                        });
                        if (err === errors.NoSuchKey) {
                            // it's possible there was a concurrent request to
                            // delete the null version, so proceed with putting
                            // a new version
                            return callback(null, options);
                        }
                        return callback(errors.InternalError);
                    }
                    options.dataToDelete = nullDataToDelete;
                    return callback(err, options);
                });
        }
        // versioning is enabled, put the new version
        options.versioning = true;
        options.nullVersionId = nullVersionId;
        return callback(null, options);
    // NOTE: Currently we do not accept requests with PUTs to specific
    // version ID, but this may change after implementing georeplication.
    } else if (!mstVersionId) {
        // version-specific versioning operation, master is not versioned
        if (vstat === 'Suspended' || reqVersionId === 'null') {
            // object does not exist or is not versioned (before versioning)
            options.versionId = '';
            options.isNull = true;
            options.dataToDelete = mstObjLocation;
            return callback(null, options);
        }
        return callback(errors.BadRequest);
    } else if (mstIsNull) {
        // master is versioned and is a null version
        if (reqVersionId === 'null') {
            // overwrite the existing version, make new version null
            options.versionId = '';
            options.isNull = true;
            options.dataToDelete = mstObjLocation;
            return callback(null, options);
        }
    }
    // master is versioned and is not a null version
    options.versionId = reqVersionId;
    return _getDeleteLocations(bucketName, objectKey,
        { versionId: reqVersionId }, log, (err, dataToDelete) => {
            if (err && err === errors.NoSuchKey) {
                return callback(null, options);
            } else if (err) {
                log.warn('unexpected error getting delete locations', {
                    error: err,
                    method: 'versioningPreprocessing',
                });
                return callback(errors.InternalError);
            }
            options.dataToDelete = dataToDelete;
            return callback(null, options);
        });
}

/** preprocessingVersioningDelete - return versioning information for S3 to
 * manage deletion of objects and versions, including creation of delete markers
 * @param {string} bucketName - name of bucket
 * @param {object} bucketMD - bucket metadata
 * @param {object} objectMD - obj metadata
 * @param {string} [reqVersionId] - specific version ID sent as part of request
 * @param {RequestLogger} log - logger instance
 * @param {function} callback - callback
 * @return {undefined} and call callback with params (err, options):
 * options.deleteData - (true/undefined) whether to delete data (if undefined
 *  means creating a delete marker instead)
 * options.versionId - specific versionId to delete
 */
export function preprocessingVersioningDelete(bucketName, bucketMD, objectMD,
    reqVersionId, log, callback) {
    const options = {};
    // bucket is not versioning enabled
    if (!bucketMD.getVersioningConfiguration()) {
        options.deleteData = true;
        return callback(null, options);
    }
    // bucket is versioning enabled
    if (reqVersionId && reqVersionId !== 'null') {
        // deleting a specific version
        options.deleteData = true;
        options.versionId = reqVersionId;
        return callback(null, options);
    }
    if (reqVersionId) {
        // deleting the 'null' version if it exists
        if (objectMD.versionId === undefined) {
            // object is not versioned, deleting it
            options.deleteData = true;
            return callback(null, options);
        }
        if (objectMD.isNull) {
            // master is the null version
            options.deleteData = true;
            options.versionId = objectMD.versionId;
            return callback(null, options);
        }
        if (objectMD.nullVersionId) {
            // null version exists, deleting it
            options.deleteData = true;
            options.versionId = objectMD.nullVersionId;
            return callback(null, options);
        }
        // null version does not exist, no deletion
        // TODO check AWS behaviour for no deletion (seems having no error)
        return callback(errors.NoSuchKey);
    }
    // not deleting any specific version, making a delete marker instead
    return callback(null, options);
}
