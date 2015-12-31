import vault from '../vault';
import constructStringToSign from './constructStringToSign';
import checkRequestExpiry from './checkRequestExpiry';

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

    vault.authenticateV2Request(accessKey,
        signatureFromRequest, stringToSign, log, (err, accountInfo) => {
            return callback(err, accountInfo);
        });
}

export default headerAuthCheck;
