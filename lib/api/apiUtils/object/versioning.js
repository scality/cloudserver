const { errors, versioning } = require('arsenal');
const async = require('async');

const metadata = require('../../../metadata/wrapper');
const { config } = require('../../../Config');

const { scaledMsPerDay } = config.getTimeOptions();

const versionIdUtils = versioning.VersionID;
// Use Arsenal function to generate a version ID used internally by metadata
// for null versions that are created before bucket versioning is configured
const nonVersionedObjId =
    versionIdUtils.getInfVid(config.replicationGroupId);

/** decodeVID - decode the version id
 * @param {string} versionId - version ID
 * @return {(Error|string|undefined)} - return Invalid Argument if decryption
 * fails due to improper format, otherwise undefined or the decoded version id
 */
function decodeVID(versionId) {
    if (versionId === 'null') {
        return versionId;
    }

    let decoded;
    const invalidErr = errors.InvalidArgument.customizeDescription('Invalid version id specified');
    try {
        decoded = versionIdUtils.decode(versionId);
    } catch (err) {
        return invalidErr;
    }

    if (decoded instanceof Error) {
        return invalidErr;
    }

    return decoded;
}

/** decodeVersionId - decode the version id from a query object
 * @param {object} [reqQuery] - request query object
 * @param {string} [reqQuery.versionId] - version ID sent in request query
 * @return {(Error|string|undefined)} - return Invalid Argument if decryption
 * fails due to improper format, otherwise undefined or the decoded version id
 */
function decodeVersionId(reqQuery) {
    if (!reqQuery || !reqQuery.versionId) {
        return undefined;
    }
    return decodeVID(reqQuery.versionId);
}

/** getVersionIdResHeader - return encrypted version ID if appropriate
 * @param {object} [verCfg] - bucket versioning configuration
 * @param {object} objectMD - object metadata
 * @return {(string|undefined)} - undefined or encrypted version ID
 * (if not 'null')
 */
function getVersionIdResHeader(verCfg, objectMD) {
    if (verCfg) {
        if (objectMD.isNull || !objectMD.versionId) {
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
function checkQueryVersionId(query) {
    if (query && query.versionId !== undefined) {
        const customMsg = 'This operation does not accept a version-id.';
        return errors.InvalidArgument.customizeDescription(customMsg);
    }
    return undefined;
}

function _storeNullVersionMD(bucketName, objKey, nullVersionId, objMD, log, cb) {
    // In compatibility mode, create null versioned keys instead of null keys
    let versionId;
    let nullVersionMD;
    if (config.nullVersionCompatMode) {
        versionId = nullVersionId;
        nullVersionMD = Object.assign({}, objMD, {
            versionId: nullVersionId,
            isNull: true,
        });
    } else {
        versionId = 'null';
        nullVersionMD = Object.assign({}, objMD, {
            versionId: nullVersionId,
            isNull: true,
            isNull2: true,
        });
    }
    metadata.putObjectMD(bucketName, objKey, nullVersionMD, { versionId }, log, err => {
        if (err) {
            log.debug('error from metadata storing null version as new version',
            { error: err });
        }
        cb(err);
    });
}

/** check existence and get location of null version data for deletion
* @param {string} bucketName - name of bucket
* @param {string} objKey - name of object key
* @param {object} options - metadata options for getting object MD
* @param {string} options.versionId - version to get from metadata
* @param {object} mst - info about the master version
* @param {string} mst.versionId - the master version's version id
* @param {RequestLogger} log - logger instanceof
* @param {function} cb - callback
* @return {undefined} - and call callback with (err, dataToDelete)
*/
function _prepareNullVersionDeletion(bucketName, objKey, options, mst, log, cb) {
    const nullOptions = {};
    if (!options.deleteData) {
        return process.nextTick(cb, null, nullOptions);
    }
    if (options.versionId === mst.versionId) {
        // no need to get another key as the master is the target
        nullOptions.dataToDelete = mst.objLocation;
        return process.nextTick(cb, null, nullOptions);
    }
    if (options.versionId === 'null') {
        // deletion of the null key will be done by the main metadata
        // PUT via this option
        nullOptions.deleteNullKey = true;
    }
    return metadata.getObjectMD(bucketName, objKey, options, log,
        (err, versionMD) => {
            if (err) {
                // the null key may not exist, hence it's a normal
                // situation to have a NoSuchKey error, in which case
                // there is nothing to delete
                if (err.is.NoSuchKey) {
                    log.debug('null version does not exist', {
                        method: '_prepareNullVersionDeletion',
                    });
                } else {
                    log.warn('could not get null version metadata', {
                        error: err,
                        method: '_prepareNullVersionDeletion',
                    });
                }
                return cb(err);
            }
            if (versionMD.location) {
                const dataToDelete = Array.isArray(versionMD.location) ?
                      versionMD.location : [versionMD.location];
                nullOptions.dataToDelete = dataToDelete;
            }
            return cb(null, nullOptions);
        });
}

function _deleteNullVersionMD(bucketName, objKey, options, log, cb) {
    return metadata.deleteObjectMD(bucketName, objKey, options, log, err => {
        if (err) {
            log.warn('metadata error deleting null versioned key',
                     { bucketName, objKey, error: err, method: '_deleteNullVersionMD' });
        }
        return cb(err);
    });
}

/**
 * Process state from the master version of an object and the bucket
 * versioning configuration, return a set of options objects
 *
 * @param {object} mst - state of master version, as returned by
 * getMasterState()
 * @param {string} vstat - bucket versioning status: 'Enabled' or 'Suspended'
 * @param {boolean} nullVersionCompatMode - if true, behaves in null
 * version compatibility mode and return appropriate values: this mode
 * does not attempt to create null keys but create null versioned keys
 * instead
 *
 * @return {object} result object with the following attributes:
 * - {object} options: versioning-related options to pass to the
     services.metadataStoreObject() call
 * - {object} [options.extraMD]: extra attributes to set in object metadata
 * - {string} [nullVersionId]: null version key to create, if needed
 * - {object} [delOptions]: options for metadata to delete the null
     version key, if needed
 */
function processVersioningState(mst, vstat, nullVersionCompatMode) {
    const versioningSuspended = (vstat === 'Suspended');
    const masterIsNull = mst.exists && (mst.isNull || !mst.versionId);

    if (versioningSuspended) {
        // versioning is suspended: overwrite the existing null version
        const options = { versionId: '', isNull: true };
        if (masterIsNull) {
            // if the null version exists, clean it up prior to put
            if (mst.objLocation) {
                options.dataToDelete = mst.objLocation;
            }
            // backward-compat: a null version key may exist even with
            // a null master (due to S3C-7526), if so, delete it (its
            // data will be deleted as part of the master cleanup, so
            // no "deleteData" param is needed)
            //
            // "isNull2" attribute is set in master metadata when
            // null keys are used, which is used as an optimization to
            // avoid having to check the versioned key since there can
            // be no more versioned key to clean up
            if (mst.isNull && !mst.isNull2) {
                const delOptions = { versionId: mst.versionId };
                return { options, delOptions };
            }
            return { options };
        }
        if (mst.nullVersionId) {
            // backward-compat: delete the null versioned key and data
            const delOptions = { versionId: mst.nullVersionId, deleteData: true };
            if (mst.nullUploadId) {
                delOptions.replayId = mst.nullUploadId;
            }
            return { options, delOptions };
        }
        // clean up the eventual null key's location data prior to put

        // NOTE: due to metadata v1 internal format, we cannot guess
        // from the master key whether there is an associated null
        // key, because the master key may be removed whenever the
        // latest version becomes a delete marker. Hence we need to
        // pessimistically try to get the null key metadata and delete
        // it if it exists.
        const delOptions = { versionId: 'null', deleteData: true };
        return { options, delOptions };
    }

    // versioning is enabled: create a new version
    const options = { versioning: true };
    if (masterIsNull) {
        // if master is a null version or a non-versioned key,
        // copy it to a new null key
        const nullVersionId = mst.isNull ? mst.versionId : nonVersionedObjId;
        if (nullVersionCompatMode) {
            options.extraMD = {
                nullVersionId,
            };
            if (mst.uploadId) {
                options.extraMD.nullUploadId = mst.uploadId;
            }
            return { options, nullVersionId };
        }
        if (mst.isNull && !mst.isNull2) {
            // if master null version was put with an older
            // Cloudserver (or in compat mode), there is a
            // possibility that it also has a null versioned key
            // associated, so we need to delete it as we write the
            // null key
            const delOptions = {
                versionId: nullVersionId,
            };
            return { options, nullVersionId, delOptions };
        }
        return { options, nullVersionId };
    }
    // backward-compat: keep a reference to the existing null
    // versioned key
    if (mst.nullVersionId) {
        options.extraMD = {
            nullVersionId: mst.nullVersionId,
        };
        if (mst.nullUploadId) {
            options.extraMD.nullUploadId = mst.nullUploadId;
        }
    }
    return { options };
}

/**
 * Build the state of the master version from its object metadata
 *
 * @param {object} objMD - object metadata parsed from JSON
 *
 * @return {object} state of master version, with the following attributes:
 * - {boolean} exists - true if the object exists (i.e. if `objMD` is truish)
 * - {string} versionId - version ID of the master key
 * - {boolean} isNull - whether the master version is a null version
 * - {string} nullVersionId - if not a null version, reference to the
 *   null version ID
 * - {array} objLocation - array of data locations
 */
function getMasterState(objMD) {
    if (!objMD) {
        return {};
    }
    const mst = {
        exists: true,
        versionId: objMD.versionId,
        uploadId: objMD.uploadId,
        isNull: objMD.isNull,
        isNull2: objMD.isNull2,
        nullVersionId: objMD.nullVersionId,
        nullUploadId: objMD.nullUploadId,
    };
    if (objMD.location) {
        mst.objLocation = Array.isArray(objMD.location) ?
            objMD.location : [objMD.location];
    }
    return mst;
}
/** versioningPreprocessing - return versioning information for S3 to handle
 * creation of new versions and manage deletion of old data and metadata
 * @param {string} bucketName - name of bucket
 * @param {object} bucketMD - bucket metadata
 * @param {string} objectKey - name of object
 * @param {object} objMD - obj metadata
 * @param {RequestLogger} log - logger instance
 * @param {function} callback - callback
 * @return {undefined} and call callback with params (err, options):
 * options.dataToDelete - (array/undefined) location of data to delete
 * options.versionId - specific versionId to overwrite in metadata
 *  ('' overwrites the master version)
 * options.versioning - (true/undefined) metadata instruction to create new ver
 * options.isNull - (true/undefined) whether new version is null or not
 */
function versioningPreprocessing(bucketName, bucketMD, objectKey, objMD,
    log, callback) {
    const mst = getMasterState(objMD);
    const vCfg = bucketMD.getVersioningConfiguration();
    // bucket is not versioning configured
    if (!vCfg) {
        const options = { dataToDelete: mst.objLocation };
        return process.nextTick(callback, null, options);
    }
    // bucket is versioning configured
    const { options, nullVersionId, delOptions } =
          processVersioningState(mst, vCfg.Status, config.nullVersionCompatMode);
    return async.series([
        function storeNullVersionMD(next) {
            if (!nullVersionId) {
                return process.nextTick(next);
            }
            return _storeNullVersionMD(bucketName, objectKey, nullVersionId, objMD, log, next);
        },
        function prepareNullVersionDeletion(next) {
            if (!delOptions) {
                return process.nextTick(next);
            }
            return _prepareNullVersionDeletion(
                bucketName, objectKey, delOptions, mst, log,
                (err, nullOptions) => {
                    if (err) {
                        return next(err);
                    }
                    Object.assign(options, nullOptions);
                    return next();
                });
        },
        function deleteNullVersionMD(next) {
            if (delOptions &&
                delOptions.versionId &&
                delOptions.versionId !== 'null') {
                // backward-compat: delete old null versioned key
                return _deleteNullVersionMD(
                    bucketName, objectKey, { versionId: delOptions.versionId }, log, next);
            }
            return process.nextTick(next);
        },
    ], err => {
        // it's possible there was a prior request that deleted the
        // null version, so proceed with putting a new version
        if (err && err.is.NoSuchKey) {
            return callback(null, options);
        }
        return callback(err, options);
    });
}

/** Return options to pass to Metadata layer for version-specific
 * operations with the given requested version ID
 *
 * @param {object} objectMD - object metadata
 * @param {boolean} nullVersionCompatMode - if true, behaves in null
 * version compatibility mode
 * @return {object} options object with params:
 * {string} [options.versionId] - specific versionId to update
 * {boolean} [options.isNull=true|false|undefined] - if set, tells the
 * Metadata backend if we're updating or deleting a new-style null
 * version (stored in master or null key), or not a null version.
 */
function getVersionSpecificMetadataOptions(objectMD, nullVersionCompatMode) {
    // Use the internal versionId if it is a "real" null version (not
    // non-versioned)
    //
    // If the target object is non-versioned: do not specify a
    // "versionId" attribute nor "isNull"
    //
    // If the target version is a null version, i.e. has the "isNull"
    // attribute:
    //
    // - send the "isNull=true" param to Metadata if the version is
    //   already a null key put by a non-compat mode Cloudserver, to
    //   let Metadata know that the null key is to be updated or
    //   deleted. This is the case if the "isNull2" metadata attribute
    //   exists
    //
    // - otherwise, do not send the "isNull" parameter to hint
    //   Metadata that it is a legacy null version
    //
    // If the target version is not a null version and is versioned:
    //
    // - send the "isNull=false" param to Metadata in non-compat
    //   mode (mandatory for v1 format)
    //
    // - otherwise, do not send the "isNull" parameter to hint
    //   Metadata that an existing null version may not be stored in a
    //   null key
    //
    //
    if (objectMD.versionId === undefined) {
        return {};
    }
    const options = { versionId: objectMD.versionId };
    if (objectMD.isNull) {
        if (objectMD.isNull2) {
            options.isNull = true;
        }
    } else if (!nullVersionCompatMode) {
        options.isNull = false;
    }
    return options;
}

/** preprocessingVersioningDelete - return versioning information for S3 to
 * manage deletion of objects and versions, including creation of delete markers
 * @param {string} bucketName - name of bucket
 * @param {object} bucketMD - bucket metadata
 * @param {object} objectMD - obj metadata
 * @param {string} [reqVersionId] - specific version ID sent as part of request
 * @param {boolean} nullVersionCompatMode - if true, behaves in null version compatibility mode
 * @return {object} options object with params:
 * {boolean} [options.deleteData=true|undefined] - whether to delete data (if undefined
 *  means creating a delete marker instead)
 * {string} [options.versionId] - specific versionId to delete
 * {boolean} [options.isNull=true|false|undefined] - if set, tells the
 * Metadata backend if we're deleting a new-style null version (stored
 * in master or null key), or not a null version.
 */
function preprocessingVersioningDelete(bucketName, bucketMD, objectMD, reqVersionId, nullVersionCompatMode) {
    let options = {};
    if (bucketMD.getVersioningConfiguration() && reqVersionId) {
        options = getVersionSpecificMetadataOptions(objectMD, nullVersionCompatMode);
    }
    if (!bucketMD.getVersioningConfiguration() || reqVersionId) {
        // delete data if bucket is non-versioned or the request
        // deletes a specific version
        options.deleteData = true;
    }
    return options;
}

/**
 * Keep metadatas when the object is restored from cold storage
 * but remove the specific ones we don't want to keep
 * @param {object} objMD - obj metadata
 * @param {object} metadataStoreParams - custom built object containing resource details.
 * @return {undefined}
 */
function restoreMetadata(objMD, metadataStoreParams) {
    /* eslint-disable no-param-reassign */
    const userMDToSkip = ['x-amz-meta-scal-s3-restore-attempt'];
    // We need to keep user metadata and tags
    Object.keys(objMD).forEach(key => {
        if (key.startsWith('x-amz-meta-') && !userMDToSkip.includes(key)) {
            metadataStoreParams.metaHeaders[key] = objMD[key];
        }
    });

    if (objMD['x-amz-website-redirect-location']) {
        if (!metadataStoreParams.headers) {
            metadataStoreParams.headers = {};
        }
        metadataStoreParams.headers['x-amz-website-redirect-location'] = objMD['x-amz-website-redirect-location'];
    }

    if (objMD.replicationInfo) {
        metadataStoreParams.replicationInfo = objMD.replicationInfo;
    }

    if (objMD.legalHold) {
        metadataStoreParams.legalHold = objMD.legalHold;
    }

    if (objMD.acl) {
        metadataStoreParams.acl = objMD.acl;
    }

    metadataStoreParams.creationTime = objMD['creation-time'];
    metadataStoreParams.lastModifiedDate = objMD['last-modified'];
    metadataStoreParams.taggingCopy = objMD.tags;
}

/** overwritingVersioning - return versioning information for S3 to handle
 * storing version metadata with a specific version id.
 * @param {object} objMD - obj metadata
 * @param {object} metadataStoreParams - custom built object containing resource details.
 * @return {object} options
 * options.versionId - specific versionId to overwrite in metadata
 * options.isNull - (true/undefined) whether new version is null or not
 * options.nullVersionId - if storing a null version in version history, the
 *  version id of the null version
 */
function overwritingVersioning(objMD, metadataStoreParams) {
    metadataStoreParams.updateMicroVersionId = true;
    metadataStoreParams.amzStorageClass = objMD['x-amz-storage-class'];

    // set correct originOp
    metadataStoreParams.originOp = 's3:ObjectRestore:Completed';

    // update restore
    const days = objMD.archive?.restoreRequestedDays;
    const now = Date.now();
    metadataStoreParams.archive = {
        archiveInfo: objMD.archive?.archiveInfo,
        restoreRequestedAt: objMD.archive?.restoreRequestedAt,
        restoreRequestedDays: objMD.archive?.restoreRequestedDays,
        restoreCompletedAt: new Date(now),
        restoreWillExpireAt: new Date(now + (days * scaledMsPerDay)),
    };

    /* eslint-enable no-param-reassign */

    const versionId = objMD.versionId || undefined;
    const options = {
        versionId,
        isNull: objMD.isNull,
    };
    if (objMD.nullVersionId) {
        options.extraMD = {
            nullVersionId: objMD.nullVersionId,
        };
    }

    restoreMetadata(objMD, metadataStoreParams);

    return options;
}

module.exports = {
    decodeVersionId,
    getVersionIdResHeader,
    checkQueryVersionId,
    processVersioningState,
    getMasterState,
    versioningPreprocessing,
    getVersionSpecificMetadataOptions,
    preprocessingVersioningDelete,
    overwritingVersioning,
    decodeVID,
};
