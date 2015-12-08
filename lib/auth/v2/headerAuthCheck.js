import vault from '../vault';
import constructStringToSign from './constructStringToSign';
import checkRequestExpiry from './checkRequestExpiry';

function headerAuthCheck(request, callback) {
    const headers = request.lowerCaseHeaders;
    // Check to make sure timestamp is within 15 minutes of current time
    let timestamp = headers['x-amz-date'] ?
        headers['x-amz-date'] : headers.date;
    timestamp = Date.parse(timestamp);
    if (!timestamp) {
        return callback('MissingSecurityHeader');
    }
    const timeout = checkRequestExpiry(timestamp);
    if (timeout) {
        return callback('RequestTimeTooSkewed');
    }
    // Authorization Header should be
    // in the format of 'AWS AccessKey:Signature'
    const authInfo = headers.authorization;

    if (!authInfo) {
        return callback('MissingSecurityHeader');
    }
    const semicolonIndex = authInfo.indexOf(':');
    if (semicolonIndex < 0) {
        return callback('MissingSecurityHeader');
    }
    const accessKey = authInfo.substring(4, semicolonIndex).trim();
    const signatureFromRequest =
        authInfo.substring(semicolonIndex + 1).trim();
    const stringToSign = constructStringToSign(request);
    vault.authenticateV2Request(accessKey,
        signatureFromRequest, stringToSign, (err, accountInfo) => {
            return callback(err, accountInfo);
        });
}

export default headerAuthCheck;
