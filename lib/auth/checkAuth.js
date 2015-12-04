import url from 'url';
import utf8 from 'utf8';

import vault from './vault';
import { getBucketNameFromHost } from '../utils';

const Auth = {

    _getCanonicalizedAmzHeaders(headers) {
        /*
        Iterate through headers and pull any headers that are x-amz headers.
        Need to include "x-amz-date" here even though AWS docs
        ambiguous on this.
        */
        const amzHeaders = Object.keys(headers)
            .filter(val => val.substr(0, 6) === 'x-amz-')
            .map(val => [val.trim(), headers[val].trim()]);
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

        // If there are no amz headers, just return an empty string
        if (amzHeaders.length === 0) {
            return '';
        }

        // Sort the amz headers by key (first item in tuple)
        amzHeaders.sort((a, b) => a[0] > b[0]);

        // Build headerString
        return amzHeaders.reduce((headerStr, current) => {
            return `${headerStr}${current[0]}:${current[1]}\n`;
        }, '');
    },

    _getCanonicalizedResource(request) {
        /*
        This variable is used to determine whether to insert
        a "?" or "&".  Once have added a query parameter to the resourceString,
        switch haveAddedQueries to true and add
        "&" before any new query parameter.
        */
        let queryChar = '?';
        // If bucket specified in hostname, add to resourceString
        const bucket = getBucketNameFromHost(request);
        let resourceString = (bucket ? `/${bucket}` : '');
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
            "website",
        ];

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

        // Check which specified subresources are present in query string,
        // build array with them
        const query = request.query;
        const presentSubresources = Object.keys(query).filter((val) => {
            return subresources.indexOf(val) !== -1;
        });
        // Sort the array and add the subresources and their value (if any)
        // to the resourceString
        resourceString = presentSubresources.reduce((prev, current) => {
            const ch = (query[current] !== '' ? '=' : '');
            const ret = `${prev}${queryChar}${current}${ch}${query[current]}`;
            queryChar = '&';
            return ret;
        }, resourceString);
        // Add the overriding parameters to our resourceString
        resourceString = overridingParams.reduce((prev, current) => {
            if (query[current]) {
                const ret = `${prev}${queryChar}${current}=${query[current]}`;
                queryChar = '&';
                return ret;
            }
            return prev;
        }, resourceString);

        /*
        Per AWS, the delete query string parameter must be included when
        you create the CanonicalizedResource for a multi-object Delete request.
        Unclear what this means for a single item delete request.
        */
        if (request.query.delete) {
            // Addresses adding "?" instead of "&" if no other params added.
            resourceString += `${queryChar}delete=${query.delete}`;
        }
        return resourceString;
    },

    _constructStringToSign(request) {
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

        const contentMD5 = request.lowerCaseHeaders['content-md5']
            || request.query['Content-MD5'];
        stringToSign += (contentMD5 ? contentMD5 + '\n' : '\n');

        const contentType = request.lowerCaseHeaders['content-type']
            || request.query['Content-Type'];
        stringToSign += (contentType ? contentType + '\n' : '\n');

        /*
        AWS docs are conflicting on whether to include x-amz-date header here
        if present in request.
        s3cmd includes x-amz-date in amzHeaders rather
        than here in stringToSign so we have replicated that.
        */
        const date = request.lowerCaseHeaders.date || request.query.Expires;
        stringToSign += (date ? date + '\n' : '\n')
            + this._getCanonicalizedAmzHeaders(request.lowerCaseHeaders)
            + this._getCanonicalizedResource(request);
        return utf8.encode(stringToSign);
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
        let timestamp = request.lowerCaseHeaders['x-amz-date']
            || request.lowerCaseHeaders.date;
        timestamp = Date.parse(timestamp);
        if (!timestamp) {
            return callback('MissingSecurityHeader');
        }
        const timeOut = this._checkTimestamp(timestamp);
        if (timeOut) {
            return callback('RequestTimeTooSkewed');
        }
        // Authorization Header should be
        // in the format of "AWS AccessKey:Signature"
        const authInfo = request.lowerCaseHeaders.authorization;

        if (!authInfo) {
            return callback('MissingSecurityHeader');
        }
        const semicolonIndex = authInfo.indexOf(":");
        if (semicolonIndex < 0) {
            return callback('MissingSecurityHeader');
        }
        const accessKey = authInfo.substring(4, semicolonIndex).trim();
        const signatureFromRequest =
            authInfo.substring(semicolonIndex + 1).trim();
        const stringToSign = this._constructStringToSign(request);
        vault.authenticateV2Request(accessKey,
            signatureFromRequest, stringToSign, (err, accountInfo) => {
                return callback(err, accountInfo);
            });
    },

    _v2QueryAuthCheck(request, callback) {
        if (request.method === "POST") {
            return callback("Query string auth not supported for POST");
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
        if (currentTime > expirationTime
                || currentTime + fifteenMinutes < expirationTime) {
            return callback('RequestTimeTooSkewed');
        }
        const accessKey = request.query.AWSAccessKeyId;
        const signatureFromRequest = request.query.Signature;
        if (!accessKey || !signatureFromRequest) {
            return callback('MissingSecurityHeader');
        }
        const stringToSign = this._constructStringToSign(request);
        vault.authenticateV2Request(accessKey,
            signatureFromRequest, stringToSign, (err, accountInfo) => {
                return callback(err, accountInfo);
            });
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
            } else if (authHeader.substr(0, 4) === "AWS4") {
                // TODO: Deal with v4HeaderAuth
                return callback('NotImplemented');
            } else {
                return callback('MissingSecurityHeader');
            }
        // Check whether signature is in query string
        } else if (request.query.Signature) {
            this._v2QueryAuthCheck(request, callback);
        } else if (request.query["X-Amz-Algorithm"]) {
        // TODO: Handle v4 query scenario
            return callback('NotImplemented');
        } else {
            // If no auth information is provided in request, then
            // user is part of "All Users Group" so send back this
            // group as the accessKey. This means the user will only
            // be able to perform actions that are public.
            return callback(null,
                'http://acs.amazonaws.com/groups/global/AllUsers');
        }
    },

    checkAuth(request, callback) {
        Auth._runParticularAuthCheck(request, callback);
    },
};

export default Auth;
