/*
 * Code based on Yutaka Oishi (Fujifilm) contributions
 * Date: 11 Sep 2020
 */
const { ObjectMDArchive } = require('arsenal').models;
const errors = require('arsenal').errors;
const { config } = require('../../../Config');
const { locationConstraints } = config;

const { scaledMsPerDay } = config.getTimeOptions();

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
 * set archiveInfo headers to target header object
 * @param {object} objMD - object metadata
 * @returns {object} headers - target header object
 */
function setArchiveInfoHeaders(objMD) {
    const headers = {};

    if (objMD['x-amz-scal-transition-in-progress']) {
        headers['x-amz-scal-transition-in-progress'] = true;
        headers['x-amz-scal-transition-time'] = new Date(objMD['x-amz-scal-transition-time']).toUTCString();
    }

    if (objMD.archive) {
        headers['x-amz-scal-archive-info'] = JSON.stringify(objMD.archive.archiveInfo);

        if (objMD.archive.restoreRequestedAt) {
            headers['x-amz-scal-restore-requested-at'] = new Date(objMD.archive.restoreRequestedAt).toUTCString();
            headers['x-amz-scal-restore-requested-days'] = objMD.archive.restoreRequestedDays;
        }

        if (objMD.archive.restoreCompletedAt) {
            headers['x-amz-scal-restore-completed-at'] = new Date(objMD.archive.restoreCompletedAt).toUTCString();
            headers['x-amz-scal-restore-will-expire-at'] = new Date(objMD.archive.restoreWillExpireAt).toUTCString();
        }
    }

    // Always get the "real" storage class (even when STANDARD) in this case
    headers['x-amz-storage-class'] = objMD['x-amz-storage-class'] || objMD.dataStoreName;

    return headers;
}

/**
 * Check if restore can be done.
 *
 * @param {ObjectMD} objectMD - object metadata
 * @param {object} log - werelogs logger
 * @return {ArsenalError|undefined} - undefined if the conditions for RestoreObject are fulfilled
 */
function _validateStartRestore(objectMD, log) {
    if (objectMD.archive?.restoreCompletedAt) {
        if (new Date(objectMD.archive?.restoreWillExpireAt) < new Date(Date.now())) {
            // return InvalidObjectState error if the restored object is expired
            // but restore info md of this object has not yet been cleared
            log.debug('The restored object already expired.',
                {
                    archive: objectMD.archive,
                    method: '_validateStartRestore',
                });
            return errors.InvalidObjectState;
        }

        // If object is already restored, no further check is needed
        // Furthermore, we cannot check if the location is cold, as the `dataStoreName` would have
        // been reset.
        return undefined;
    }
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
    if (objectMD.archive?.restoreRequestedAt) {
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
 * Check if the object is already restored, and update the expiration date accordingly:
 * > After restoring an archived object, you can update the restoration period by reissuing the
 * > request with a new period. Amazon S3 updates the restoration period relative to the current
 * > time.
 *
 * @param {ObjectMD} objectMD - object metadata
 * @param {object} log - werelogs logger
 * @return {boolean} - true if the object is already restored
 */
function _updateObjectExpirationDate(objectMD, log) {
    // Check if restoreCompletedAt field exists
    // Normally, we should check `archive.restoreWillExpireAt > current time`; however this is
    // checked earlier in the process, so checking again here would create weird states
    const isObjectAlreadyRestored = !!objectMD.archive.restoreCompletedAt;
    log.debug('The restore status of the object.', {
        isObjectAlreadyRestored,
        method: 'isObjectAlreadyRestored'
    });
    if (isObjectAlreadyRestored) {
        const expiryDate = new Date(objectMD.archive.restoreRequestedAt);
        expiryDate.setTime(expiryDate.getTime() + (objectMD.archive.restoreRequestedDays * scaledMsPerDay));

        /* eslint-disable no-param-reassign */
        objectMD.archive.restoreWillExpireAt = expiryDate;
        objectMD['x-amz-restore'] = {
            'ongoing-request': false,
            'expiry-date': expiryDate,
        };
        /* eslint-enable no-param-reassign */
    }
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
    const isObjectAlreadyRestored = _updateObjectExpirationDate(objectMD, log);
    return cb(null, isObjectAlreadyRestored);
}

/**
 * checks if object data is available or if it's in cold storage
 * @param {ObjectMD} objMD Object metadata
 * @returns {ArsenalError|null} error if object data is not available
 */
function verifyColdObjectAvailable(objMD) {
    // return error when object is cold
    if (objMD.archive &&
        // Object is in cold backend
        (!objMD.archive.restoreRequestedAt ||
            // Object is being restored
            (objMD.archive.restoreRequestedAt && !objMD.archive.restoreCompletedAt))) {
        const err = errors.InvalidObjectState
            .customizeDescription('The operation is not valid for the object\'s storage class');
        return err;
    }
    return null;
}

module.exports = {
    startRestore,
    getAmzRestoreResHeader,
    validatePutVersionId,
    verifyColdObjectAvailable,
    setArchiveInfoHeaders,
};
