const coldstorage = require('../../../coldstorage/wrapper');
const { metadataGetObject } = require('../../../metadata/metadataUtils');
const errors = require('arsenal').errors;

/**
 * Get response header "x-amz-restore"
 * Be called by objectHead.js
 * @param {object} objMD - object's metadata
 * @returns {string} x-amz-restore
 */
function getAmzRestoreResHeader(objMD){

    let value;
    if(objMD['x-amz-restore']){
        if(objMD['x-amz-restore']['ongoing-request']){
            value = `ongoing-request="${objMD['x-amz-restore']['ongoing-request']}"`;
        }
        
        // expiry-date is transformed to format of RFC2822
        if (objMD['x-amz-restore']['expiry-date']) {
            const utcDateTime = alDateUtils.toUTCString(new Date(objMD['x-amz-restore']['expiry-date']));
            value = `${value}, ${expiry-date}="${utcDateTime}"`;
        }
    }

    return value;

}

/**
 * Check object metadata if GET request is possible
 * Be called by objectGet.js
 * @param {object} objMD - object's metadata
 * @return {boolean} true if the GET request is accepted, false if not
 */
function validateAmzRestoreForGet(objMD){

    if(!objMD) {
        return false;
    }

    if(objMD['x-amz-storage-class'] === 'GLACIER'){
        return false;
    }

    if(objMD['x-amz-restore']['ongoing-request']){
        return false;
    }

    return true;

}

/**
 * Start to archive to GLACIER
 * ( Be called by Lifecycle batch? ) 
 */
function startGlacier(bucketName, objName, versionId, log, cb){
    
    return completeGlacier(bucketName, objName, versionId, log, cb);
}

/**
 * Complete to archive to GLACIER
 * ( Be called by Lifecycle batch? ) 
 * update x-amz-storage-class to "GLACIER".
 */
function completeGlacier(bucketName, objName, versionId, log, cb){

    metadataGetObject(bucketName, objectKey, versionId, log,
        (err, objMD) => {
            if(err){
                log.trace('error processing get metadata', {
                    error: err,
                    method: 'metadataGetObject',
                });
                return cb(err);
            }

            const storageClass = 'GLACIER';

            // FIXME: return error NotImplemented when using "ColdStorageFileInterface"
            coldstorage.updateAmzStorageClass(bucketName, objName, objMD, storageClass, log, cb);
        }
    );



}

/**
 * start to restore object.
 * If not exist x-amz-restore, add it to objectMD.(x-amz-restore = false)
 * calculate restore expiry-date and add it to objectMD.
 * Be called by objectRestore.js
 * 
 * FIXME: After restore is started, there is no timing to update restore parameter to the content of complete restore.
 */
function startRestore(bucketName, objName, objectMD, restoreParam, cb){

    let checkResult = _validateStartRestore(objectMD);
    if(checkResult instanceof errors){
        return cb(checkResult);
    };

    // update restore parameter to the content of doing restore.
    _updateRestoreExpiration(bucketName, objName, objMD, restoreParam, log, cb);


    return cb(objectMD, restoreParam);
    
}

/**
 * complete to restore object.
 * Update restore-ongoing to false.
 * ( Be called by batch to check if the restore is complete? )
 * 
 */
function completeRestore(bucketName, objName, objMD){

    const updateParam = false;

    // FIXME: return error NotImplemented when using "ColdStorageFileInterface"
    return coldstorage.updateRestoreOngoing(bucketName, objName, objMD, updateParam, log, cb);
}


/**
 * expire to restore object.
 * Delete x-amz-restore.
 * ( Be called by batch to check if the restore is expire? )
 */
function expireRestore(bucketName, objName, objMD){

    // FIXME: return error NotImplemented when using "ColdStorageFileInterface"
    return coldstorage.deleteAmzRestore(bucketName, objName, objMD, log, cb);
}



/**
 * Check if restore has already started.
 */
function _validateStartRestore(objectMD){
    
    if(objectMD['x-amz-restore'] && objMD['x-amz-restore']['ongoing-request']){
        return errors.RestoreAlreadyInProgress;
    }
    else{
        return undefined;
    }

}

/**
 * update restore expiration date.
 */
function _updateRestoreExpiration(bucketName, objName, objMD, restoreParam, log, cb){

    if(objMD['x-amz-restore'] && !objMD['x-amz-restore']['ongoing-request']){

        // FIXME: return error NotImplemented when using "ColdStorageFileInterface"
        return coldstorage.updateRestoreExpiration(bucketName, objName, objMD, restoreParam, log, cb);
    }
    else{
        log.debug('do not updateRestoreExpiration', { method: '_updateRestoreExpiration' });
        return undefined;
    }

}





module.exports = {
    getAmzRestoreResHeader,
    validateAmzRestoreForGet,
    startGlacier,
    completeGlacier,
    startRestore,
    completeRestore,
    expireRestore,
};