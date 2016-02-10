import url from 'url';
import crypto from 'crypto';
import xmlService from 'xml';

import Config from './Config';
import constants from '../constants';

const config = new Config();

const utils = {};

/**
 * Get bucket name from the request of a virtually hosted bucket
 * @param {object} request - HTTP request object
 * @return {string} result - returns bucket name if dns-style query
 *                           returns undefined if path-style query
 * @throws {Error} in case the type of query could not be infered
 */
utils.getBucketNameFromHost = function getBucketNameFromHost(request) {
    let headers = request.lowerCaseHeaders;
    if (headers === undefined) {
        headers = request.headers;
    }

    if (headers === undefined || headers.host === undefined) {
        throw new Error('bad request: no host in headers');
    }

    const host = headers.host.split(':')[0];

    // If host is an IP address, it's path-style
    if (/^[0-9.]+$/.test(host)) {
        return undefined;
    }

    // All endpoints from all regions + `s3-website-$REGION...`
    const validHosts = utils.getAllEndpoints().concat(
        utils.getAllRegions().map(r => `s3-website-${r}.amazonaws.com`));

    for (let i = 0; i < validHosts.length; i++) {
        if (host === validHosts[i]) {
            // It's path-style
            return undefined;
        } else if (host.endsWith(`.${validHosts[i]}`)) {
            return host.split(`.${validHosts[i]}`)[0];
        }
    }
    throw new Error('bad request:' +
                    `hostname "${host}" is not in valid endpoints`);
};

/**
 * Get all valid regions, according to our configuration
 *
 * @returns {string[]} - list of valid regions
 */
utils.getAllRegions = function getAllRegions() {
    return Object.keys(config.regions);
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
    };

    const path = url.parse(request.url).pathname;
    const fullHost = request.headers && request.headers.host
        ? request.headers.host.split(':')[0] : undefined;

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
            resources.object = urlArr.slice(2).join("/");
        } else if (urlArr.length === 1) {
            resources.bucket = urlArr[1];
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
    const ipAddressRegex = new RegExp(/(\d+\.){3}\d+/);
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
    // Must not be an ip address
    if (bucketname.match(ipAddressRegex)) {
        return false;
    }
    // Must be dns compatible
    return bucketname.match(dnsRegex) ? true : false;
};

utils.getContentMD5 = function getContentMD5(requestBody) {
    return crypto.createHash('md5').update(requestBody).digest('base64');
};

/**
 * Parse content-md5 from meta headers
 * @param {string} headers - request headers
 * @return {string} - returns content-md5 string
 */
utils.parseContentMD5 = function parseContentMD5(headers) {
    if (headers['x-amz-meta-s3cmd-attrs']) {
        const metaHeadersArr
            = headers['x-amz-meta-s3cmd-attrs']
                .split('/');
        let tmpArr;
        for (let i = 0; i < metaHeadersArr.length; i++) {
            tmpArr = metaHeadersArr[i].split(':');
            if (tmpArr[0] === 'md5') {
                return tmpArr[1];
            }
        }
    }
    return '';
};


/**
 * Pull user provided meta headers from request headers
 * @param {object} headers - headers attached to the http request (lowercased)
 * @return {object} all user meta headers
 */

utils.getMetaHeaders = function getMetaHeaders(headers) {
    const metaHeaders = {};
    Object.keys(headers).forEach((k) => {
        if (k.substr(0, 11) === 'x-amz-meta-') {
            metaHeaders[k] = headers[k];
        }
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
    return crypto.createHash('md5').update(namespace + resource).digest('hex');
};


/**
 * Modify http request object
 * @param {object} request - http request object
 * @return {object} request object with additional attributes
 */

utils.normalizeRequest = function normalizeRequest(request) {
    request.lowerCaseHeaders = {};
    Object.keys(request.headers).forEach((key) => {
        request.lowerCaseHeaders[key.toLowerCase()] = request.headers[key];
    });
    request.query = this.decodeQuery(url.parse(request.url, true).query);
    // TODO: make the namespace come from a config variable.
    request.namespace = 'default';
    // Parse bucket and/or object names from request
    const resources = this.getResourceNames(request);
    request.gotBucketNameFromHost = resources.gotBucketNameFromHost;
    request.bucketName = resources.bucket;
    request.objectKey = resources.object;
    request.parsedHost = resources.host;
    return request;
};

utils.parseGrant = function parseGrant(grantHeader, grantType) {
    if (grantHeader === undefined) {
        return undefined;
    }
    const grantArray = grantHeader.split(',');
    let itemArray;
    let userIDType;
    let identifier;
    return grantArray.map((item) => {
        itemArray = item.split('=');
        userIDType = itemArray[0].trim();
        identifier = itemArray[1].trim();
        if (identifier[0] === '"') {
            identifier = identifier.substr(1, identifier.length - 2);
        }
        return {
            userIDType,
            identifier,
            grantType,
        };
    });
};

utils.reconstructUsersIdentifiedByEmail =
    function reconstruct(userInfofromVault, userGrantInfo) {
        return userInfofromVault.map((item) => {
            const userEmail = item.email;
            // Find the full user grant info based on email
            const user = userGrantInfo
                .find(elem => elem.identifier.toLowerCase() === userEmail);
            // Set the identifier to be the canonicalID instead of email
            user.identifier = item.canonicalID;
            user.userIDType = 'id';
            return user;
        });
    };

utils.sortHeaderGrants =
    function sortHeaderGrants(allGrantHeaders, addACLParams) {
        allGrantHeaders.forEach((item) => {
            if (item) {
                addACLParams[item.grantType].push(item.identifier);
            }
        });
        return addACLParams;
    };

utils.getPermissionType = function getPermissionType(identifier, resourceACL,
        resourceType) {
    const fullControlIndex = resourceACL.FULL_CONTROL.indexOf(identifier);
    let writeIndex;
    if (resourceType === 'bucket') {
        writeIndex = resourceACL.WRITE.indexOf(identifier);
    }
    const writeACPIndex = resourceACL.WRITE_ACP.indexOf(identifier);
    const readACPIndex = resourceACL.READ_ACP.indexOf(identifier);
    const readIndex = resourceACL.READ.indexOf(identifier);
    let permission = '';
    if (fullControlIndex > -1) {
        permission = 'FULL_CONTROL';
        resourceACL.FULL_CONTROL.splice(fullControlIndex, 1);
    } else if (writeIndex > -1) {
        permission = 'WRITE';
        resourceACL.WRITE.splice(writeIndex, 1);
    } else if (writeACPIndex > -1) {
        permission = 'WRITE_ACP';
        resourceACL.WRITE_ACP.splice(writeACPIndex, 1);
    } else if (readACPIndex > -1) {
        permission = 'READ_ACP';
        resourceACL.READ_ACP.splice(readACPIndex, 1);
    } else if (readIndex > -1) {
        permission = 'READ';
        resourceACL.READ.splice(readIndex, 1);
    }
    return permission;
};

utils.mapHeaders = function mapHeaders(headers, addHeaders) {
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
    return headers;
};

utils.constructGetACLsJson = function constrctGetACLsJson(grantInfo) {
    const {grants, ownerInfo} = grantInfo;
    const accessControlList = grants.map((grant) => {
        let grantIdentifier;
        let type;
        if (grant.ID) {
            grantIdentifier = { ID: grant.ID };
            type = 'CanonicalUser';
        }
        if (grant.URI) {
            grantIdentifier = { URI: grant.URI };
            type = 'Group';
        }
        const grantItem = {
            Grant: [
                { Grantee: [ { _attr: {'xmlns:xsi':
                    'http://www.w3.org/2001/XMLSchema-instance',
                    'xsi:type': type
                    }
                },
                    grantIdentifier ] },
                { Permission: grant.permission },
            ]
        };
        if (grant.displayName) {
            grantItem.Grant[0].Grantee.
                push({ DisplayName: grant.displayName });
        }
        return grantItem;
    });

    return {
        AccessControlPolicy: [{
            Owner: [
                    { ID: ownerInfo.ID },
                    { DisplayName: ownerInfo.displayName },
            ]
        }, { AccessControlList: accessControlList, }, ]};
};

utils.convertToXml = function convertToXml(infoToConvert, jsonConstructer) {
    const constructedJSON = jsonConstructer(infoToConvert);
    return xmlService(constructedJSON,
        { declaration: { standalone: 'yes', encoding: 'UTF-8' }});
};

utils.decodeQuery = function decodeQuery(query) {
    const decodedQuery = {};
    Object.keys(query).forEach(x => {
        const key = decodeURIComponent(x);
        const value = decodeURIComponent(query[x]);
        decodedQuery[key] = value;
    });
    return decodedQuery;
};


export default utils;
