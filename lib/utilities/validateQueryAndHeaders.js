const { errors } = require('arsenal');

const constants = require('../../constants');

function _validateKeys(unsupportedKeys, obj) {
    let unsupportedKey;
    unsupportedKeys.some(key => {
        if (obj[key] !== undefined) {
            unsupportedKey = key;
            return true;
        }
        return false;
    });
    return unsupportedKey;
}


/**
 * validateQueryAndHeaders - Check request for unsupported queries or headers
 * @param {string} reqMethod - request method
 * @param {object} reqQuery - request query object
 * @param {object} reqHeaders - request headers object
 * @param {object} log - Werelogs logger
 * @return {object} - empty object or object with error boolean property
 */
function validateQueryAndHeaders(reqMethod, reqQuery, reqHeaders, log) {
    const unsupportedQuery = _validateKeys(constants.unsupportedQueries,
        reqQuery);
    if (unsupportedQuery) {
        log.debug('encountered unsupported query', {
            query: unsupportedQuery,
            method: 'validateQueryAndHeaders',
        });
        return { error: errors.NotImplemented };
    }
    const unsupportedHeader = _validateKeys(constants.unsupportedHeaders,
        reqHeaders);
    if (unsupportedHeader) {
        log.debug('encountered unsupported header', {
            header: unsupportedHeader,
            method: 'validateQueryAndHeaders',
        });
        // for now only unsupported headers are encryption headers and we only
        // return NotImplemented on a PUT
        if (reqMethod.toUpperCase() === 'PUT') {
            return { error: errors.NotImplemented };
        }
    }
    return {};
}

module.exports = validateQueryAndHeaders;
