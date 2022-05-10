/*
 * Code based on Yutaka Oishi (Fujifilm) contributions
 * Date: 11 Sep 2020
 */

/**
 * Get response header "x-amz-restore"
 * Be called by objectHead.js
 * @param {object} objMD - object's metadata
 * @returns {string} x-amz-restore
 */
function getAmzRestoreResHeader(objMD){
    let value;
    if (objMD.archive &&
        objMD.archive.restoreRequestedAt &&
        !objMD.archive.restoreCompletedAt) {
        // Avoid race condition by relying on the `archive` MD of the object
        // and return the right header after a RESTORE request.
        value = `ongoing-request="true"`;
    } else if (objMD['x-amz-restore']) {
        const utcDateTime = new Date(objMD['x-amz-restore']['expiry-date']).toUTCString();
        if (objMD['x-amz-restore']['expiry-date']) {
            value = `ongoing-request="${objMD['x-amz-restore']['ongoing-request']}", expiry-date="${utcDateTime}"`;
        }
    }
    return value;
}

module.exports = {
    getAmzRestoreResHeader,
};