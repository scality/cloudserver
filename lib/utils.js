import url from 'url';
import crypto from 'crypto';
import { ipCheck } from 'arsenal';

import config from './Config';
import constants from '../constants';

const utils = {};

/**
 * Get bucket name from the request of a virtually hosted bucket
 * @param {object} request - HTTP request object
 * @return {string|undefined} - returns bucket name if dns-style query
 *                              returns undefined if path-style query
 * @throws {Error} in case the type of query could not be infered
 */
utils.getBucketNameFromHost = function getBucketNameFromHost(request) {
    const headers = request.headers;
    if (headers === undefined || headers.host === undefined) {
        throw new Error('bad request: no host in headers');
    }
    const reqHost = headers.host;
    const bracketIndex = reqHost.indexOf(']');
    const colonIndex = reqHost.lastIndexOf(':');

    const hostLength = colonIndex > bracketIndex ? colonIndex : reqHost.length;
    // If request is made using IPv6 (indicated by presence of brackets),
    // surrounding brackets should not be included in host var
    const host = bracketIndex > -1 ?
        reqHost.slice(1, hostLength - 1) : reqHost.slice(0, hostLength);
    // parseIp returns empty object if host is not valid IP
    // If host is an IP address, it's path-style
    if (Object.keys(ipCheck.parseIp(host)).length !== 0) {
        return undefined;
    }

    // All endpoints from all regions + `websiteEndpoints
    const validHosts = utils.getAllEndpoints().concat(
        config.websiteEndpoints);

    let bucketName;
    for (let i = 0; i < validHosts.length; ++i) {
        if (host === validHosts[i]) {
            // It's path-style
            return undefined;
        } else if (host.endsWith(`.${validHosts[i]}`)) {
            const potentialBucketName = host.split(`.${validHosts[i]}`)[0];
            if (!bucketName) {
                bucketName = potentialBucketName;
            } else {
                // bucketName should be shortest so that takes into account
                // most specific potential hostname
                bucketName = potentialBucketName.length < bucketName.length ?
                    potentialBucketName : bucketName;
            }
        }
    }
    if (bucketName) {
        return bucketName;
    }
    throw new Error(`bad request: hostname ${host} is not in valid endpoints`);
};

/**
 * Get all valid regions, according to our configuration.
 * Valid regions are Amazon official regions + custom regions declared in conf.
 *
 * @returns {string[]} - list of valid regions
 */
utils.getAllRegions = function getAllRegions() {
    const awsOfficialRegions = [
        'ap-northeast-1', 'ap-southeast-1', 'ap-southeast-2', 'eu-central-1',
        'eu-west-1', 'sa-east-1', 'us-east-1', 'us-west-1', 'us-west-2',
        'us-gov-west-1'];
    return Object.keys(config.regions).concat(awsOfficialRegions);
};

/**
 * Get all valid endpoints, according to our configuration
 *
 * @returns {string[]} - list of valid endpoints
 */
utils.getAllEndpoints = function getAllEndpoints() {
    return Object.keys(config.regions)
        .map(r => config.regions[r])
        .reduce((a, b) => a.concat(b));
};

/**
 * Get bucket name and object name from the request
 * @param {object} request - http request object
 * @returns {object} result - returns object containing bucket
 * name and objectKey as key
 */
utils.getResourceNames = function getResourceNames(request) {
    return this.getNamesFromReq(request, utils.getBucketNameFromHost(request));
};

/**
 * Get bucket name and/or object name from the path of a request
 * @param {object} request - http request object
 * @param {string} bucketNameFromHost - name of bucket obtained from host name
 * @returns {object} resources - returns object with bucket and object as keys
 */
utils.getNamesFromReq = function getNamesFromReq(request, bucketNameFromHost) {
    const resources = {
        bucket: undefined,
        object: undefined,
        host: undefined,
        gotBucketNameFromHost: undefined,
        path: undefined,
    };
    const pathname = url.parse(request.url).pathname;
    // If there are spaces in a key name, s3cmd sends them as "+"s.
    // Actual "+"s are uri encoded as "%2B" so by switching "+"s to
    // spaces here, you still retain any "+"s in the final decoded path
    const pathWithSpacesInsteadOfPluses = pathname.replace(/\+/g, ' ');
    const path = decodeURIComponent(pathWithSpacesInsteadOfPluses);
    resources.path = path;

    let fullHost;
    if (request.headers && request.headers.host) {
        const reqHost = request.headers.host;
        const bracketIndex = reqHost.indexOf(']');
        const colonIndex = reqHost.lastIndexOf(':');
        const hostLength = colonIndex > bracketIndex ?
            colonIndex : reqHost.length;
        fullHost = reqHost.slice(0, hostLength);
    } else {
        fullHost = undefined;
    }

    if (bucketNameFromHost) {
        resources.bucket = bucketNameFromHost;
        const bucketNameLength = bucketNameFromHost.length;
        resources.host = fullHost.slice(bucketNameLength + 1);
        // Slice off leading '/'
        resources.object = path.slice(1);
        resources.gotBucketNameFromHost = true;
    } else {
        resources.host = fullHost;
        const urlArr = path.split('/');
        if (urlArr.length > 1) {
            resources.bucket = urlArr[1];
            resources.object = urlArr.slice(2).join('/');
        } else if (urlArr.length === 1) {
            resources.bucket = urlArr[0];
        }
    }
    // remove any empty strings or nulls
    if (resources.bucket === '' || resources.bucket === null) {
        resources.bucket = undefined;
    }
    if (resources.object === '' || resources.object === null) {
        resources.object = undefined;
    }
    return resources;
};

/**
 * Validate bucket name per naming rules and restrictions
 * @param {string} bucketname - name of the bucket to be created
 * @return {boolean} - returns true/false by testing
 * bucket name against validation rules
 */
utils.isValidBucketName = function isValidBucketName(bucketname) {
    const ipAddressRegex = new RegExp(/^(\d+\.){3}\d+$/);
    const dnsRegex = new RegExp(/^[a-z0-9]+([\.\-]{1}[a-z0-9]+)*$/);
    // Must be at least 3 and no more than 63 characters long.
    if (bucketname.length < 3 || bucketname.length > 63) {
        return false;
    }
    // Must not start with the mpuBucketPrefix since this is
    // reserved for the shadow bucket used for multipart uploads
    if (bucketname.startsWith(constants.mpuBucketPrefix)) {
        return false;
    }
    // Must not contain more than one consecutive period
    if (bucketname.indexOf('..') > 1) {
        return false;
    }
    // Must not be an ip address
    if (bucketname.match(ipAddressRegex)) {
        return false;
    }
    // Must be dns compatible
    return !!bucketname.match(dnsRegex);
};

utils.getContentMD5 = function getContentMD5(requestBody) {
    return crypto.createHash('md5')
        .update(requestBody, 'binary').digest('base64');
};

/**
 * Parse content-md5 from meta headers
 * @param {string} headers - request headers
 * @return {string} - returns content-md5 string
 */
utils.parseContentMD5 = function parseContentMD5(headers) {
    if (headers['x-amz-meta-s3cmd-attrs']) {
        const metaHeadersArr = headers['x-amz-meta-s3cmd-attrs'].split('/');
        for (let i = 0; i < metaHeadersArr.length; i++) {
            const tmpArr = metaHeadersArr[i].split(':');
            if (tmpArr[0] === 'md5') {
                return tmpArr[1];
            }
        }
    }
    return '';
};


utils.validateWebsiteHeader = function validateWebsiteHeader(header) {
    return (!header || header.startsWith('/') ||
    header.startsWith('http://') || header.startsWith('https://'));
};

/**
 * Pull user provided meta headers from request headers
 * @param {object} headers - headers attached to the http request (lowercased)
 * @return {object} all user meta headers
 */
utils.getMetaHeaders = function getMetaHeaders(headers) {
    const metaHeaders = Object.create(null);
    Object.keys(headers).filter(h => (h.startsWith('x-amz-meta-') ||
    h.startsWith('x-amz-website'))).forEach(k => {
        metaHeaders[k] = headers[k];
    });
    return metaHeaders;
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


/**
 * Modify http request object
 * @param {object} request - http request object
 * @return {object} request object with additional attributes
 */
utils.normalizeRequest = function normalizeRequest(request) {
    /* eslint-disable no-param-reassign */
    request.query = url.parse(request.url, true).query;
    // TODO: make the namespace come from a config variable.
    request.namespace = 'default';
    // Parse bucket and/or object names from request
    const resources = this.getResourceNames(request);
    request.gotBucketNameFromHost = resources.gotBucketNameFromHost;
    request.bucketName = resources.bucket;
    request.objectKey = resources.object;
    request.parsedHost = resources.host;
    request.path = resources.path;
    // For streaming v4 auth, the total body content length
    // without the chunk metadata is sent as
    // the x-amz-decoded-content-length
    const contentLength = request.headers['x-amz-decoded-content-length'] ?
        request.headers['x-amz-decoded-content-length'] :
        request.headers['content-length'];
    request.parsedContentLength =
        Number.parseInt(contentLength, 10);

    if (process.env.ALLOW_INVALID_META_HEADERS) {
        const headersArr = Object.keys(request.headers);
        const length = headersArr.length;
        if (headersArr.indexOf('x-invalid-metadata') > 1) {
            for (let i = 0; i < length; i++) {
                const headerName = headersArr[i];
                if (headerName.startsWith('x-amz-')) {
                    const translatedHeaderName =
                        headerName.replace(/\|\+2f/g, '/');
                    request.headers[translatedHeaderName] =
                        request.headers[headerName];
                    if (translatedHeaderName !== headerName) {
                        delete request.headers[headerName];
                    }
                }
            }
        }
    }
    return request;
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

export default utils;
