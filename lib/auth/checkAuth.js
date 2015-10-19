import crypto from 'crypto';
import url from 'url';
import utf8 from 'utf8';
import {getBucketNameFromHost} from '../utils.js';
import {secretKeys} from '../testdata/vault.json';

const Auth = {
    _hashSignature(stringToSign, secretKey, algorithm) {
        const utf8stringToSign = utf8.encode(stringToSign);
        const hmacObject = crypto.createHmac(algorithm, secretKey);
        const hashedSignature =
            hmacObject.update(utf8stringToSign).digest('base64');
        return hashedSignature;
    },

    _getCanonicalizedAmzHeaders(headers) {
        let headerString = '';
        const amzHeaders = [];

        /*
        Iterate through headers and pull any headers that are x-amz headers.
        Need to include "x-amz-date" here even though AWS docs
        ambiguous on this.
        */
        let key;
        let value;
        for (key in headers) {
            if (key.substr(0, 6) === 'x-amz-') {
                value = headers[key].trim();

            /*
            AWS docs state that duplicate headers should be combined
            in the same header with values concatenated with
            a comma separation.
            Node combines duplicate headers and concatenates the values
            with a comma AND SPACE separation.
            Could replace all occurrences of ", " with "," but this
            would remove spaces that might be desired
            (for instance, in date header).
            Opted to proceed without this parsing since it does not appear
            that the AWS clients use duplicate headers.
            */
                amzHeaders.push([key.trim(), value]);
            }
        }

        // If there are no amz headers, just return an empty string
        if (amzHeaders.length === 0) {
            return headerString;
        }

        // Sort the amz headers by key (first item in tuple)
        amzHeaders.sort(function sorting(a, b) {return a[0] > b[0];});

        // Build headerString
        for (let j = 0; j < amzHeaders.length; j += 1) {
            headerString += amzHeaders[j][0] + ":" + amzHeaders[j][1] + "\n";
        }
        return headerString;
    },

    _getCanonicalizedResource(request) {
        let resourceString = "";

        /*
        This variable is used to determine whether to insert
        a "?" or "&".  Once have added a query parameter to the resourceString,
        switch haveAddedQueries to true and add
        "&" before any new query parameter.
        */
        let haveAddedQueries = false;
        // If bucket specified in hostname, add to resourceString
        const bucket = getBucketNameFromHost(request);
        if (bucket !== undefined) {
            resourceString += bucket;
        }
        // Add the path to the resourceString
        resourceString += url.parse(request.url).pathname;

        /*
        If request includes a specified subresource,
        add to the resourceString: (a) a "?", (b) the subresource,
        and (c) its value (if any).
        Separate multiple subresources with "&".
        Subresources must be in alphabetical order.
        */

        // Specified subresources:
        const subresources = [
            "acl",
            "lifecycle",
            "location",
            "logging",
            "notification",
            "partNumber",
            "policy",
            "requestPayment",
            "torrent",
            "uploadId",
            "uploads",
            "versionId",
            "versioning",
            "versions",
            "website"];

        const presentSubresources = [];

        // Check which specified subresources are present in query string,
        // build array with them
        let param;
        for (param in request.query) {
        // If need to pick up existing query parameters that do not have values,
        // add "|| subresources.indexOf(param + "/")" to conditional below
            if (subresources.indexOf(param) > -1) {
                presentSubresources.push(param);
            }
        }
    // Sort the array and add the subresources and their value (if any)
    // to the resourceString
        if (presentSubresources.length > 0) {
            presentSubresources.sort();
            let subresource;
            for (let i = 0; i < presentSubresources.length; i += 1) {
                resourceString += (i === 0 ? "?" : "&");
                subresource = presentSubresources[i];
                resourceString += subresource;
                haveAddedQueries = true;
                if (request.query[subresource] !== "") {
                    resourceString += "=" + request.query[subresource];
                }
            }
        }
    /*
    If the request includes parameters in the query string,
    that override the headers, include
    them in the resourceString
    along with their values.
    AWS is ambiguous about format.  Used alphabetical order.
    */

        const overridingParams = [
            "response-cache-control",
            "response-content-disposition",
            "response-content-encoding",
            "response-content-language",
            "response-content-type",
            "response-expires",
        ];

        for (let j = 0; j < overridingParams.length; j += 1) {
            if (request.query[overridingParams[j]]) {
                // Addresses adding "?" instead of "&" if no subresources added.
                resourceString += (haveAddedQueries ? "&" : "?");
                resourceString += overridingParams[j] + "="
                + request.query[overridingParams[j]];
                haveAddedQueries = true;
            }
        }
    /*
    Per AWS, the delete query string parameter must be included when
    you create the CanonicalizedResource for a multi-object Delete request.
    Unclear what this means for a single item delete request.
    */

        if (request.query.delete) {
            // Addresses adding "?" instead of "&" if no other params added.
            resourceString += (haveAddedQueries ? "&" : "?");
            resourceString += "delete=" + request.query.delete;
        }
        return resourceString;
    },

    _reconstructSignature(secretKey, request) {
    /*
    Build signature per AWS requirements:
    StringToSign = HTTP-Verb + "\n" +
    Content-MD5 + "\n" +
    Content-Type + "\n" +
    Date (or Expiration for query Auth) + "\n" +
    CanonicalizedAmzHeaders +
    CanonicalizedResource;
    */

        let stringToSign = request.method + "\n";

        const contentMD5 =
            request.lowerCaseHeaders['content-md5'] || request.query['Content-MD5'];
        stringToSign += (contentMD5 ? contentMD5 + '\n' : '\n');

        const contentType =
            request.lowerCaseHeaders['content-type'] ||
        request.query['Content-Type'];
        stringToSign += (contentType ? contentType + '\n' : '\n');

        /*
        AWS docs are conflicting on whether to include x-amz-date header here
        if present in request.
        s3cmd includes x-amz-date in amzHeaders rather
        than here in stringToSign so we have replicated that.
        */
        const date =
         request.lowerCaseHeaders.date || request.query.Expires;
        stringToSign += (date ? date + '\n' : '\n');
        stringToSign +=
            this._getCanonicalizedAmzHeaders(request.lowerCaseHeaders);
        stringToSign += this._getCanonicalizedResource(request);
        const hashedSignature =
            this._hashSignature(stringToSign, secretKey, "sha1");
        return hashedSignature;
    },


    _checkSignatureMatch(accessKey, secretKey, signature, request, callback) {
        const reconstructedSignature =
            this._reconstructSignature(secretKey, request);
        if (reconstructedSignature === signature) {
            return callback(null, accessKey);
        }
        return callback('SignatureDoesNotMatch');
    },

    _getSecretKey(accessKey, signature, request, callback) {
    // Retrieve secret key based on accessKey.
        process.nextTick(function retrieveKey() {
            const secretKey = secretKeys[accessKey];
            if (!secretKey) {
                return callback('InvalidAccessKeyId');
            }
            this._checkSignatureMatch(
            accessKey, secretKey, signature, request, callback);
        }.bind(this));
    },


    _checkTimestamp(timestamp) {
        // If timestamp is not within 15 minutes of current time, return true
        const currentTime = Date.now();
        const fifteenMinutes = (15 * 60 * 1000);
        if ((currentTime - timestamp) > fifteenMinutes ||
                (currentTime + fifteenMinutes) < timestamp) {
            return true;
        }
        return false;
    },

    _v2HeaderAuthCheck(request, callback) {
        // Check to make sure timestamp is within 15 minutes of current time
        let timestamp =
        request.lowerCaseHeaders['x-amz-date'] || request.lowerCaseHeaders.date;
        timestamp = Date.parse(timestamp);
        if (!timestamp || isNaN(timestamp)) {
            return callback('MissingSecurityHeader');
        }
        const timeOut = this._checkTimestamp(timestamp);
        if (timeOut) {
            return callback('RequestTimeTooSkewed');
        }
        // Authorization Header should be
        // in the format of "AWS AccessKey:Signature"
        const authorizationInfo = request.lowerCaseHeaders.authorization;

        if (!authorizationInfo) {
            return callback('MissingSecurityHeader');
        }
        const semicolonIndex = authorizationInfo.indexOf(":");
        if (semicolonIndex < 0) {
            return callback('MissingSecurityHeader');
        }
        const accessKey =
            authorizationInfo.substring(4, semicolonIndex).trim();
        const signature =
            authorizationInfo.substring(semicolonIndex + 1).trim();
        this._getSecretKey(accessKey, signature, request, callback);
    },

    _v2QueryAuthCheck(request, callback) {
        if (request.method === "POST") {
            return callback(
                "Query string authentication is not supported for POST");
        }

        /*
        Check whether request has expired or if
        expires parameter is more than 15 minutes in the future.
        Expires time is provided in seconds so need to
        multiply by 1000 to obtain
        milliseconds to compare to Date.now()
        */
        let expirationTime = parseInt(request.query.Expires, 10);
        if (isNaN(expirationTime)) {
            return callback("Missing or invalid Expires query parameter");
        }
        expirationTime = parseInt(request.query.Expires, 10) * 1000;
        const currentTime = Date.now();
        const fifteenMinutes = (15 * 60 * 1000);
        if (currentTime > expirationTime ||
            currentTime + fifteenMinutes < expirationTime) {
            return callback('RequestTimeTooSkewed');
        }
        const accessKey = request.query.AWSAccessKeyId;
        const signature = request.query.Signature;
        if (!accessKey || !signature) {
            return callback('MissingSecurityHeader');
        }
        this._getSecretKey(accessKey, signature, request, callback);
    },

    _runParticularAuthCheck(request, callback) {
        const authHeader = request.lowerCaseHeaders.authorization;

        // Check whether signature is in header
        if (authHeader) {
        // TODO: Check for security token header to
        // handle temporary security credentials

            // Check if v2
            if (authHeader.substr(0, 4) === "AWS ") {
                this._v2HeaderAuthCheck(request, callback);
            } else {
                // TODO: Deal with v4HeaderAuth
            }

        // Check whether signature is in query string
        } else if (request.query.Signature) {
            this._v2QueryAuthCheck(request, callback);
        } else if (request.query["X-Amz-Algorithm"]) {
        // TODO: Handle v4 query scenario
        } else {
            return callback('MissingSecurityHeader');
        }
    },

    checkAuth(request, callback) {
        Auth._runParticularAuthCheck(request, callback);
    },
};

export default Auth;
