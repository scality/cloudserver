import algoCheck from './algoCheck';
import vault from '../vault';
import constructStringToSign from './constructStringToSign';
import checkRequestExpiry from './checkRequestExpiry';

function queryAuthCheck(request, log, callback) {
    log.debug('Running query auth check');
    if (request.method === 'POST') {
        log.error('Query string auth not supported for POST requests');
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
        log.error(`Invalid Expires parameter: ${request.query.Expires}`);
        return callback('MissingSecurityHeader');
    }
    const timeout = checkRequestExpiry(expirationTime, log);
    if (timeout) {
        log.error('Request time too skewed');
        return callback('RequestTimeTooSkewed');
    }
    const accessKey = request.query.AWSAccessKeyId;
    log.debug(`Access Key from request: ${accessKey}`);

    const signatureFromRequest = decodeURIComponent(request.query.Signature);
    log.debug(`Signature from request: ${signatureFromRequest}`);
    if (!accessKey || !signatureFromRequest) {
        log.error('Invalid Access Key/Signature parameters');
        return callback('MissingSecurityHeader');
    }
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
        }
    );
}

export default queryAuthCheck;
