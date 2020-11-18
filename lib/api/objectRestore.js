/**
 * This module handles POST Object restore.
 *
 * @module api/objectRestore
 */

const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const metadata = require('../metadata/wrapper');
const metadataUtils = require('../metadata/metadataUtils');

const { decodeVersionId, getVersionIdResHeader } =
    require('./apiUtils/object/versioning');

const sdtObjectRestore = require('./apiUtils/object/objectRestore');

/**
 * Process POST Object restore request.
 *
 * @param {AuthInfo} userInfo Instance of AuthInfo class with requester's info
 * @param {IncomingMessage} request normalized request object
 * @param {werelogs.Logger} log werelogs request instance
 * @param {module:api/objectRestore~NoBodyResultCallback} callback
 * callback to function in route
 * @return {void}
 */
function objectRestore(userInfo, request, log, callback) {
    const func = {
        decodeVersionId,
        collectCorsHeaders,
        getVersionIdResHeader,
    };

    return sdtObjectRestore(metadata, metadataUtils, func, userInfo, request,
        log, callback);
}

/**
 * @callback module:api/objectRestore~NoBodyResultCallback
 * @param {ArsenalError} error ArsenalError instance in case of error
 * @param {object} responseHeaders Response header object
 */

module.exports = objectRestore;