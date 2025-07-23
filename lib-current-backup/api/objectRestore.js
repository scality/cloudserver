/*
 * Code based on Yutaka Oishi (Fujifilm) contributions
 * Date: 11 Sep 2020
 *
 * This module handles POST Object restore.
 *
 * @module api/objectRestore
 */

const metadata = require('../metadata/wrapper');
const metadataUtils = require('../metadata/metadataUtils');

const sdtObjectRestore = require('./apiUtils/object/objectRestore');

/**
 * Process POST Object restore request.
 *
 * @param {AuthInfo} userInfo Instance of AuthInfo class with requester's info
 * @param {object} request http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function objectRestore(userInfo, request, log, callback) {
    return sdtObjectRestore(metadata, metadataUtils, userInfo, request,
        log, callback);
}

module.exports = objectRestore;
