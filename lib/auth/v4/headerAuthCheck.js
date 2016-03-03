import { errors } from 'arsenal';

import vault from '../vault';
import constructStringToSign from './constructStringToSign';
import { checkTimeSkew, convertUTCtoISO8601 } from './timeUtils';
import { extractAuthItems, validateCredentials } from './validateInputs';

/**
 * V4 header auth check
 * @param {object} request - HTTP request object
 * @param {object} log - logging object
 * @param {function} callback - callback to auth checking function
 * @return {callback} calls callback
 */
function headerAuthCheck(request, log, callback) {
    log.trace('running header auth check');
    // authorization header
    const authHeader = request.headers.authorization;
    if (!authHeader) {
        log.warn('missing authorization header');
        return callback(errors.MissingSecurityHeader);
    }

    const payloadChecksum = request.headers['x-amz-content-sha256'];
    if (!payloadChecksum) {
        log.warn('missing payload checksum');
        return callback(errors.MissingSecurityHeader);
    }
    if (payloadChecksum === 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD') {
        log.trace('requesting streaming v4 auth');
        // TODO: Implement this option
        return callback('NotImplemented');
    }

    log.trace('authorization header from request', { authHeader });

    const authHeaderItems = extractAuthItems(authHeader, log);
    if (Object.keys(authHeaderItems).length < 3) {
        return callback(errors.MissingSecurityHeader);
    }
    const { signatureFromRequest, credentialsArr,
        signedHeaders } = authHeaderItems;

    let timestamp;
    // check request timestamp
    if (request.headers['x-amz-date']) {
        // format of x-amz- date is ISO 8601: YYYYMMDDTHHMMSSZ
        timestamp = request.headers['x-amz-date'];
    } else if (request.headers.date) {
        timestamp = convertUTCtoISO8601(request.headers.date);
    }
    if (!timestamp) {
        log.warn('missing date header');
        return callback(errors.MissingSecurityHeader);
    }

    if (!validateCredentials(credentialsArr, timestamp, log)) {
        log.warn('credentials in improper format', { credentialsArr });
        return callback(errors.InvalidArgument);
    }
    // credentialsArr is [accessKey, date, region, aws-service, aws4_request]
    const scopeDate = credentialsArr[1];
    const region = credentialsArr[2];
    const accessKey = credentialsArr.shift();
    const credentialScope = credentialsArr.join('/');


    // In Signature Version 4, the signing key is valid for up to seven days
    // (see Introduction to Signing Requests.
    // Therefore, a signature is also valid for up to seven days or
    // less if specified by a bucket policy.
    // See http://docs.aws.amazon.com/AmazonS3/latest/API/
    // bucket-policy-s3-sigv4-conditions.html
    // TODO: When implementing bucket policies,
    // note that expiration can be shortened so
    // expiry is less than 7 days

    // 7 days in seconds
    const expiry = (7 * 24 * 60 * 60);
    const isTimeSkewed = checkTimeSkew(timestamp, expiry, log);
    if (isTimeSkewed) {
        return callback(errors.RequestTimeTooSkewed);
    }

    const stringToSign = constructStringToSign({
        log,
        request,
        query: request.query,
        signedHeaders,
        credentialScope,
        timestamp,
        payloadChecksum,
    });
    log.trace('constructed stringToSign', { stringToSign });
    if (stringToSign instanceof Error) {
        return callback(stringToSign);
    }
    const vaultParams = {
        accessKey,
        signatureFromRequest,
        region,
        scopeDate,
        stringToSign,
        log,
    };
    vault.authenticateV4Request(vaultParams, callback);
}

export default headerAuthCheck;
