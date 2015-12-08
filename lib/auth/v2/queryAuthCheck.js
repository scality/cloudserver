import vault from '../vault';
import constructStringToSign from './constructStringToSign';
import checkRequestExpiry from './checkRequestExpiry';
function queryAuthCheck(request, callback) {
    if (request.method === 'POST') {
        return callback('Query string auth not supported for POST');
    }

    /*
    Check whether request has expired or if
    expires parameter is more than 15 minutes in the future.
    Expires time is provided in seconds so need to
    multiply by 1000 to obtain
    milliseconds to compare to Date.now()
    */
    const expirationTime = parseInt(request.query.Expires, 10) * 1000;
    if (isNaN(expirationTime)) {
        return callback('MissingSecurityHeader');
    }
    const timeout = checkRequestExpiry(expirationTime);
    if (timeout) {
        return callback('RequestTimeTooSkewed');
    }
    const accessKey = request.query.AWSAccessKeyId;
    const signatureFromRequest = request.query.Signature;
    if (!accessKey || !signatureFromRequest) {
        return callback('MissingSecurityHeader');
    }
    const stringToSign = constructStringToSign(request);
    vault.authenticateV2Request(accessKey,
        signatureFromRequest, stringToSign, (err, accountInfo) => {
            return callback(err, accountInfo);
        }
    );
}

export default queryAuthCheck;
