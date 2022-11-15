/*
 * Code based on Yutaka Oishi (Fujifilm) contributions
 * Date: 11 Sep 2020
 */
const ObjectMDArchive = require('arsenal').models.ObjectMDArchive;
const errors = require('arsenal').errors;
const { config } = require('../../../Config');
const { locationConstraints } = config;

/**
 * Get response header "x-amz-restore"
 * Be called by objectHead.js
 * @param {object} objMD - object's metadata
 * @returns {string|undefined} x-amz-restore
 */
function getAmzRestoreResHeader(objMD) {
    if (objMD.archive &&
        objMD.archive.restoreRequestedAt &&
        !objMD.archive.restoreCompletedAt) {
        // Avoid race condition by relying on the `archive` MD of the object
        // and return the right header after a RESTORE request.
        // eslint-disable-next-line
        return `ongoing-request="true"`;
    }
    if (objMD['x-amz-restore']) {
        if (objMD['x-amz-restore']['expiry-date']) {
            const utcDateTime = new Date(objMD['x-amz-restore']['expiry-date']).toUTCString();
            // eslint-disable-next-line
            return `ongoing-request="${objMD['x-amz-restore']['ongoing-request']}", expiry-date="${utcDateTime}"`;
        }
    }
    return undefined;
}


/**
 * Check if restore can be done.
 *
 * @param {ObjectMD} objectMD - object metadata
 * @param {object} log - werelogs logger
 * @return {ArsenalError|undefined} - undefined if the conditions for RestoreObject are fulfilled
 */
function _validateStartRestore(objectMD, log) {
    const isLocationCold = locationConstraints[objectMD.dataStoreName]?.isCold;
    if (!isLocationCold) {
        // return InvalidObjectState error if the object is not in cold storage,
        // not in cold storage means either location cold flag not exists or cold flag is explicit false
        log.debug('The bucket of the object is not in a cold storage location.',
            {
                isLocationCold,
                method: '_validateStartRestore',
            });
        return errors.InvalidObjectState;
    }
    if (objectMD.archive?.restoreCompletedAt
        && new Date(objectMD.archive?.restoreWillExpireAt) < new Date(Date.now())) {
        // return InvalidObjectState error if the restored object is expired
        // but restore info md of this object has not yet been cleared
        log.debug('The restored object already expired.',
            {
                archive: objectMD.archive,
                method: '_validateStartRestore',
            });
        return errors.InvalidObjectState;
    }
    if (objectMD.archive?.restoreRequestedAt && !objectMD.archive?.restoreCompletedAt) {
        // return RestoreAlreadyInProgress error if the object is currently being restored
        // check if archive.restoreRequestAt exists and archive.restoreCompletedAt not yet exists
        log.debug('The object is currently being restored.',
            {
                archive: objectMD.archive,
                method: '_validateStartRestore',
            });
        return errors.RestoreAlreadyInProgress;
    }
    return undefined;
}

/**
 * Check if "put version id" is allowed
 *
 * @param {ObjectMD} objMD - object metadata
 * @param {string} versionId - object's version id
 * @param {object} log - werelogs logger
 * @return {ArsenalError|undefined} - undefined if "put version id" is allowed
 */
function validatePutVersionId(objMD, versionId, log) {
    if (!objMD) {
        const err = versionId ? errors.NoSuchVersion : errors.NoSuchKey;
        log.error('error no object metadata found', { method: 'validatePutVersionId', versionId });
        return err;
    }

    if (objMD.isDeleteMarker) {
        log.error('version is a delete marker', { method: 'validatePutVersionId', versionId });
        return errors.MethodNotAllowed;
    }

    const isLocationCold = locationConstraints[objMD.dataStoreName]?.isCold;
    if (!isLocationCold) {
        log.error('The object data is not stored in a cold storage location.',
            {
                isLocationCold,
                dataStoreName: objMD.dataStoreName,
                method: 'validatePutVersionId',
            });
        return errors.InvalidObjectState;
    }

    // make sure object archive restoration is in progress
    // NOTE: we do not use putObjectVersion to update the restoration period.
    if (!objMD.archive || !objMD.archive.restoreRequestedAt || !objMD.archive.restoreRequestedDays
        || objMD.archive.restoreCompletedAt || objMD.archive.restoreWillExpireAt) {
        log.error('object archive restoration is not in progress',
            { method: 'validatePutVersionId', versionId });
        return errors.InvalidObjectState;
    }

    return undefined;
}

/**
 * Check if the object is already restored
 *
 * @param {ObjectMD} objectMD - object metadata
 * @param {object} log - werelogs logger
 * @return {boolean} - true if the object is already restored
 */
function isObjectAlreadyRestored(objectMD, log) {
    //  check if restoreCompletedAt field exists
    //  and archive.restoreWillExpireAt > current time
    const isObjectAlreadyRestored = objectMD.archive?.restoreCompletedAt
        && new Date(objectMD.archive?.restoreWillExpireAt) >= new Date(Date.now());
    log.debug('The restore status of the object.',
        {
            isObjectAlreadyRestored,
            method: 'isObjectAlreadyRestored'
        });
    return isObjectAlreadyRestored;
}

/**
 * update restore expiration date.
 *
 * @param {ObjectMD} objectMD - objectMD instance
 * @param {object} restoreParam - restore param
 * @param {object} log - werelogs logger
 * @return {ArsenalError|undefined} internal error if object MD is not valid
 *
 */
function _updateRestoreInfo(objectMD, restoreParam, log) {
    if (!objectMD.archive) {
        log.debug('objectMD.archive doesn\'t exits', {
            objectMD,
            method: '_updateRestoreInfo'
        });
        return errors.InternalError.customizeDescription('Archive metadata is missing.');
    }
    /* eslint-disable no-param-reassign */
    objectMD.archive.restoreRequestedAt = new Date();
    objectMD.archive.restoreRequestedDays = restoreParam.days;
    objectMD.originOp = 's3:ObjectRestore:Post';
    /* eslint-enable no-param-reassign */
    if (!ObjectMDArchive.isValid(objectMD.archive)) {
        log.debug('archive is not valid', {
            archive: objectMD.archive,
            method: '_updateRestoreInfo'
        });
        return errors.InternalError.customizeDescription('Invalid archive metadata.');
    }
    return undefined;
}

/**
 * start to restore object.
 * If not exist x-amz-restore, add it to objectMD.(x-amz-restore = false)
 * calculate restore expiry-date and add it to objectMD.
 * Be called by objectRestore.js
 *
 * @param {ObjectMD} objectMD - objectMd instance
 * @param {object} restoreParam - bucket name
 * @param {object} log - werelogs logger
 * @param {function} cb - bucket name
 * @return {undefined}
 *
 */
function startRestore(objectMD, restoreParam, log, cb) {
    log.info('Validating if restore can be done or not.');
    const checkResultError = _validateStartRestore(objectMD, log);
    if (checkResultError) {
        return cb(checkResultError);
    }
    log.info('Updating restore information.');
    const updateResultError = _updateRestoreInfo(objectMD, restoreParam, log);
    if (updateResultError) {
        return cb(updateResultError);
    }
    return cb(null, isObjectAlreadyRestored(objectMD, log));
}


module.exports = {
    startRestore,
    getAmzRestoreResHeader,
    validatePutVersionId,
};
