const crypto = require('crypto');
const { errors } = require('arsenal');

const { config } = require('./Config');
const constants = require('../constants');

const utils = {};

/**
 * Get all valid endpoints, according to our configuration
 *
 * @returns {string[]} - list of valid endpoints
 */
utils.getAllEndpoints = function getAllEndpoints() {
    return Object.keys(config.restEndpoints);
};

utils.getContentMD5 = function getContentMD5(requestBody) {
    return crypto.createHash('md5')
        .update(requestBody, 'binary').digest('base64');
};

utils.validateWebsiteHeader = function validateWebsiteHeader(header) {
    return (!header || header.startsWith('/') ||
    header.startsWith('http://') || header.startsWith('https://'));
};

/**
 * Pull user provided meta headers from request headers
 * @param {object} headers - headers attached to the http request (lowercased)
 * @return {(object|Error)} all user meta headers or MetadataTooLarge
 */
utils.getMetaHeaders = function getMetaHeaders(headers) {
    const metaHeaders = Object.create(null);
    let totalLength = 0;
    const metaHeaderKeys = Object.keys(headers).filter(h =>
        h.startsWith('x-amz-meta-'));
    const validHeaders = metaHeaderKeys.every(k => {
        totalLength += k.length;
        totalLength += headers[k].length;
        metaHeaders[k] = headers[k];
        return (totalLength <= constants.maximumMetaHeadersSize);
    });
    if (validHeaders) {
        return metaHeaders;
    }
    return errors.MetadataTooLarge;
};

/**
 * Create a unique key for either a bucket or an object
 * @param {string} namespace - namespace of request
 * @param {string} resource - either bucketname or bucketname + objectname
 * @return {string} hash to use as bucket key or object key
 */
utils.getResourceUID = function getResourceUID(namespace, resource) {
    return crypto.createHash('md5')
        .update(namespace + resource, 'binary').digest('hex');
};

utils.mapHeaders = function mapHeaders(headers, addHeaders) {
    /* eslint-disable no-param-reassign */
    if (addHeaders['response-content-type']) {
        headers['Content-Type'] = addHeaders['response-content-type'];
    }
    if (addHeaders['response-content-language']) {
        headers['Content-Language'] = addHeaders['response-content-language'];
    }
    if (addHeaders['response-expires']) {
        headers.Expires = addHeaders['response-expires'];
    }
    if (addHeaders['response-cache-control']) {
        headers['Cache-Control'] = addHeaders['response-cache-control'];
    }
    if (addHeaders['response-content-disposition']) {
        headers['Content-Disposition'] =
        addHeaders['response-content-disposition'];
    }
    if (addHeaders['response-content-encoding']) {
        headers['Content-Encoding'] = addHeaders['response-content-encoding'];
    }
    /* eslint-enable no-param-reassign */
    return headers;
};

module.exports = utils;
