import parseString from 'xml2js';
import url from 'url';
import crypto from 'crypto';
import UUID from 'node-uuid';
import S3ERRORS from './data/s3Errors.json';
import xmlService from 'xml';

const utils = {};

const awsRegions = [
    {
        "endpoint": "s3.amazonaws.com",
        "region": "us-east-1"
    },
    {
        "endpoint": "s3-external-1.amazonaws.com",
        "region": "us-east-1"
    },
    {
        "endpoint": "s3.eu-west-1.amazonaws.com",
        "region": "eu-west-1"
    },
    {
        "endpoint": "s3.ap-southeast-1.amazonaws.com",
        "region": "ap-southeast-1"
    },
    {
        "endpoint": "s3.ap-southeast-2.amazonaws.com",
        "region": "ap-southeast-2"
    },
    {
        "endpoint": "s3.eu-central-1.amazonaws.com",
        "region": "eu-central-1"
    },
    {
        "endpoint": "s3.eu.central-1.amazonaws.com",
        "region": "eu-central-1"
    },
    {
        "endpoint": "s3.ap-northeast-1.amazonaws.com",
        "region": "ap-northeast-1"
    },
    {
        "endpoint": "s3.us-east-1.amazonaws.com",
        "region": "us-east-1"
    },
    {
        "endpoint": "s3.sa-east-1.amazonaws.com",
        "region": "sa-east-1"
    },
    {
        "endpoint": "s3.us-west-1.amazonaws.com",
        "region": "us-west-1"
    },
    {
        "endpoint": "ec2.us-west-2.amazonaws.com",
        "region": "us-west-2"
    },
    {
        "endpoint": "s3-us-gov-west-1.amazonaws.com",
        "region": "us-gov-west-1"
    },
    {
        "endpoint": "s3-fips-us-gov-west-1.amazonaws.com",
        "region": "us-gov-west-1"
    }
];

/**
 * Get bucket name from the request of a virtually hosted bucket
 * @param {object} request - http request object
 * @return {string} result - returns bucket name if found or undefined
 */
export function getBucketNameFromHost(request) {
    let endpoint;
    let hostArr;
    let websiteEndpoint;

    if (request.headers === undefined || request.headers.host === undefined) {
        return false;
    }
    const host = request.headers.host.split(':')[0];
    for (let i = 0; i < awsRegions.length; i++) {
        endpoint = awsRegions[i].endpoint;
        websiteEndpoint = `s3-website-${awsRegions[i].region}.amazonaws.com`;
        if (host !== endpoint && host.indexOf(endpoint) !== -1) {
            hostArr = host.split('.' + endpoint);
            return hostArr[0];
        }

        if (host !== endpoint && host.indexOf(websiteEndpoint) !== -1) {
            hostArr = host.split('.' + websiteEndpoint);
            return hostArr[0];
        }
    }
    return false;
}

/**
 * Get region
 * @param {object} request - http request object
 * @param {function} callback - callback from the calling function
 * @returns {function} - returns callback function with err and result
 */
utils.getRegion = function getRegion(request, callback) {
    if (request.headers.host === undefined) {
        callback("Host is undefined");
    }
    const host = request.headers.host.split(':')[0];

    if (host === 's3.amazonaws.com') {
        return callback(null, 'us-east-1');
    }

    if (request.body) {
        parseString(request.body, function parseXmlStringRes(err, result) {
            if (err) {
                return callback('MalformedXML');
            }
            for (let i = 0; i < awsRegions.length; i++) {
                if (awsRegions[i].region === result) {
                    return callback(null, result);
                }
            }
            return callback('Region is invalid');
        });
    }

    for (let i = 0; i < awsRegions.length; i++) {
        const a = awsRegions[i];
        const endpoint = `s3-website-${awsRegions[i].region}.amazonaws.com`;
        if (host.indexOf(a.endpoint) !== -1 || host.indexOf(endpoint)) {
            return callback(null, a.region);
        }
    }
    return callback('Unable to set region');
};

/**
 * Get bucket name and object name from the request
 * @param {object} request - http request object
 * @returns {object} result - returns object containing bucket
 * name and objectKey as key
 */
utils.getResourceNames = function getResourceNames(request) {
    return this.getNamesFromReq(request, getBucketNameFromHost(request));
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
    request.query = url.parse(request.url, true).query;
    // TODO: make the namespace come from a config variable.
    request.namespace = 'default';
    return request;
};


/**
 * Add to http response headers
 * @param {object} response - http response object
 * @param {object} headers - key and value of new headers to add
 * @return {object} response object with additional headers
 */
utils.buildResponseHeaders = function buildResponseHeaders(response, headers) {
    Object.keys(headers).forEach((key) => {
        response.setHeader(key, headers[key]);
    });

    // to be expanded in further implementation of logging of requests
    response.setHeader('x-amz-id-2', UUID.v4());
    response.setHeader('x-amz-request-id', UUID.v4());
    return response;
};

/**
 * Modify response headers for an objectGet or objectHead request
 * @param {object} headers - lowercased headers from request object
 * @param {object} response - response object
 * @param {object} responseMetaHeaders - object with additional headers
 *  to add to response object
 * @return {object} response - modified response object
 */
utils.buildSuccessResponse =
function buildSuccessResponse(headers, response, responseMetaHeaders) {
    const addHeaders = {};
    // TODO: If retrieved object is a delete marker,
    // return x-amx-delete-marker header set to true.
    if (headers['response-content-type']) {
        addHeaders['Content-Type'] = headers['response-content-type'];
    }
    if (headers['response-content-language']) {
        addHeaders['Content-Language'] = headers['response-content-language'];
    }
    if (headers['response-expires']) {
        addHeaders.Expires = headers['response-expires'];
    }
    if (headers['response-cache-control']) {
        addHeaders['Cache-Control'] = headers['response-cache-control'];
    }
    if (headers['response-content-disposition']) {
        addHeaders['Content-Disposition'] =
        headers['response-content-disposition'];
    }
    if (headers['response-content-encoding']) {
        addHeaders['Content-Encoding'] = headers['response-content-encoding'];
    }
    Object.keys(responseMetaHeaders).forEach(function headersKey(k) {
        addHeaders[k] = responseMetaHeaders[k];
    });

    utils.buildResponseHeaders(response, addHeaders);
    response.writeHead(200);
    return response;
};


utils.buildResponseErrorXML = function buildResponseErrorXML(errorCode) {
    const result = { xml: '', httpCode: 500 };
    /*
    <?xml version="1.0" encoding="UTF-8"?>
     <Error>
     <Code>NoSuchKey</Code>
     <Message>The resource you requested does not exist</Message>
     <Resource>/mybucket/myfoto.jpg</Resource>
     <RequestId>4442587FB7D0A2F9</RequestId>
     </Error>
     */
    const errObj = S3ERRORS[errorCode] ? S3ERRORS[errorCode]
        : S3ERRORS.InternalError;
    const errXmlObj = [
        {
            'Error': [
                {
                    'Code': errorCode
                }, {
                    'Message': errObj.description
                }, {
                    'Resource': ''
                }, {
                    'RequestId': ''
                }
            ]
        }
    ];
    result.xml = xmlService(errXmlObj, { declaration: { encoding: 'UTF-8' }});
    result.httpCode = errObj.httpCode;
    return result;
};

utils.okHeaderResponse = function okHeaderResponse(response, code = 500) {
    const httpCode = code;
    response.writeHead(httpCode);
    return response.end();
};

utils.okXmlResponse = function okXmlResponse(response, xml) {
    response.writeHead(200, {
        'Content-type': 'application/xml'
    });
    return response.end(xml, 'utf8');
};

utils.errorXmlResponse = function errorXmlResponse(response, err) {
    const errorXmlRes = this.buildResponseErrorXML(err);
    response.writeHead(errorXmlRes.httpCode, {
        'Content-type': 'application/xml'
    });
    return response.end(errorXmlRes.xml, 'utf8');
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

export default utils;
