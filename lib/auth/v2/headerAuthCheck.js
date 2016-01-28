import vault from '../vault';
import constructStringToSign from './constructStringToSign';
import checkRequestExpiry from './checkRequestExpiry';
import algoCheck from './algoCheck';

function headerAuthCheck(request, log, callback) {
    log.debug('Running Header Auth check');
    const headers = request.lowerCaseHeaders;

    // Check to make sure timestamp is within 15 minutes of current time
    let timestamp = headers['x-amz-date'] ?
        headers['x-amz-date'] : headers.date;
    timestamp = Date.parse(timestamp);
    if (!timestamp) {
        log.error('Missing Security Header: Invalid Date/Timestamp');
        return callback('MissingSecurityHeader');
    }

    const timeout = checkRequestExpiry(timestamp, log);
    if (timeout) {
        log.error(`Request time too skewed: {$timestamp}`);
        return callback('RequestTimeTooSkewed');
    }
    // Authorization Header should be
    // in the format of 'AWS AccessKey:Signature'
    const authInfo = headers.authorization;

    if (!authInfo) {
        log.error('Missing Authorization Security Header');
        return callback('MissingSecurityHeader');
    }
    const semicolonIndex = authInfo.indexOf(':');
    if (semicolonIndex < 0) {
        log.error(`Invalid Authorization Header: ${authInfo}`);
        return callback('MissingSecurityHeader');
    }
    const accessKey = authInfo.substring(4, semicolonIndex).trim();
    log.debug(`Access Key from request:${accessKey}`);

    const signatureFromRequest =
        authInfo.substring(semicolonIndex + 1).trim();
    log.debug(`Signature from request: ${signatureFromRequest}`);
    const stringToSign = constructStringToSign(request, log);
    log.debug(`Constructed String to Sign: ${stringToSign}`);
    const algo = algoCheck(signatureFromRequest.length);
    log.debug(`Algo for calculating signature: ${algo}`);
    if (algo === undefined) {
        return callback('InvalidArgument');
    }
    vault.authenticateV2Request(accessKey,
        signatureFromRequest, stringToSign, algo, log, (err, accountInfo) => {
            // For now, I am just sending back the canonicalID.
            // TODO: Refactor so that the accessKey information
            // passed to the API is the full accountInfo Object
            // rather than just the canonicalID string.
            // This is GH Issue#75
            if (err) {
                return callback(err);
            }
            return callback(null, accountInfo.canonicalID);
        });
}

export default headerAuthCheck;
