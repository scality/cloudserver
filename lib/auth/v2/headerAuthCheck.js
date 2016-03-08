import { errors } from 'arsenal';
import vault from '../vault';
import constructStringToSign from './constructStringToSign';
import checkRequestExpiry from './checkRequestExpiry';
import algoCheck from './algoCheck';

function headerAuthCheck(request, log, callback) {
    log.trace('running header auth check');
    const headers = request.headers;

    // Check to make sure timestamp is within 15 minutes of current time
    let timestamp = headers['x-amz-date'] ?
        headers['x-amz-date'] : headers.date;
    timestamp = Date.parse(timestamp);
    if (!timestamp) {
        log.warn('missing security header: invalid date/timestamp');
        return callback(errors.MissingSecurityHeader);
    }

    const timeout = checkRequestExpiry(timestamp, log);
    if (timeout) {
        log.warn('request time too skewed', { timestamp });
        return callback(errors.RequestTimeTooSkewed);
    }
    // Authorization Header should be
    // in the format of 'AWS AccessKey:Signature'
    const authInfo = headers.authorization;

    if (!authInfo) {
        log.warn('missing authorization security header');
        return callback(errors.MissingSecurityHeader);
    }
    const semicolonIndex = authInfo.indexOf(':');
    if (semicolonIndex < 0) {
        log.warn('invalid authorization header', { authInfo });
        return callback(errors.MissingSecurityHeader);
    }
    const accessKey = authInfo.substring(4, semicolonIndex).trim();
    log.addDefaultFields({ accessKey });

    const signatureFromRequest = authInfo.substring(semicolonIndex + 1).trim();
    log.trace('signature from request', { signatureFromRequest });
    const stringToSign = constructStringToSign(request, log);
    log.trace('constructed string to sign', { stringToSign });
    const algo = algoCheck(signatureFromRequest.length);
    log.trace('algo for calculating signature', { algo });
    if (algo === undefined) {
        return callback(errors.InvalidArgument);
    }
    vault.authenticateV2Request(accessKey,
        signatureFromRequest, stringToSign, algo, log, (err, authInfo) => {
            if (err) {
                return callback(err);
            }
            return callback(null, authInfo);
        });
}

export default headerAuthCheck;
