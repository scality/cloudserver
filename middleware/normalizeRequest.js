const url = require('url');
const invalidBucketName = require('./invalidBucketName');
/**
 * Get bucket name from the request of a virtually hosted bucket
 * @param {object} request - HTTP request object
 * @return {string|undefined} - returns bucket name if dns-style query
 *                              returns undefined if path-style query
 * @throws {Error} in case the type of query could not be infered
 */
function getBucketNameFromHost(request) {
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
    const validHosts = getAllEndpoints().concat(config.websiteEndpoints);

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
 * Get all valid endpoints, according to our configuration
 *
 * @returns {string[]} - list of valid endpoints
 */
function getAllEndpoints() {
    return Object.keys(config.restEndpoints);
};

/**
 * Get bucket name and object name from the request
 * @param {object} request - http request object
 * @param {string} pathname - http request path parsed from request url
 * @returns {object} result - returns object containing bucket
 * name and objectKey as key
 */
function getResourceNames(request, pathname) {
    return this.getNamesFromReq(request, pathname,
        utils.getBucketNameFromHost(request));
};

/**
 * Get bucket name and/or object name from the path of a request
 * @param {object} request - http request object
 * @param {string} pathname - http request path parsed from request url
 * @param {string} bucketNameFromHost - name of bucket obtained from host name
 * @returns {object} resources - returns object with bucket and object as keys
 */
function getNamesFromReq(request, pathname,
    bucketNameFromHost) {
    const resources = {
        bucket: undefined,
        object: undefined,
        host: undefined,
        gotBucketNameFromHost: undefined,
        path: undefined,
    };
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

function normalizeRequest(request) {
    /* eslint-disable no-param-reassign */
    const parsedUrl = url.parse(request.url, true);
    request.query = parsedUrl.query;
    // TODO: make the namespace come from a config variable.
    request.namespace = 'default';
    // Parse bucket and/or object names from request
    const resources = this.getResourceNames(request, parsedUrl.pathname);
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
}

module.exports = normalizeRequest;
