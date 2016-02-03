import algoCheck from './algoCheck';
import vault from '../vault';
import constructStringToSign from './constructStringToSign';
import checkRequestExpiry from './checkRequestExpiry';

function queryAuthCheck(request, log, callback) {
    log.trace('running query auth check');
    if (request.method === 'POST') {
        log.warn('query string auth not supported for post requests');
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
        log.warn('invalid expires parameter',
            { expires: request.query.Expires });
        return callback('MissingSecurityHeader');
    }
    const timeout = checkRequestExpiry(expirationTime, log);
    if (timeout) {
        return callback('RequestTimeTooSkewed');
    }
    const accessKey = request.query.AWSAccessKeyId;
    log.addDefaultFields({ accessKey });

    const signatureFromRequest = decodeURIComponent(request.query.Signature);
    log.trace('signature from request', { signatureFromRequest });
    if (!accessKey || !signatureFromRequest) {
        log.warn('invalid access key/signature parameters');
        return callback('MissingSecurityHeader');
    }
    const stringToSign = constructStringToSign(request, log);
    log.trace('constructed string to sign', { stringToSign });
    const algo = algoCheck(signatureFromRequest.length);
    log.trace('algo for calculating signature', { algo });
    if (algo === undefined) {
        return callback('InvalidArgument');
    }
    vault.authenticateV2Request(accessKey,
        signatureFromRequest, stringToSign, algo, log, (err, authInfo) => {
            if (err) {
                return callback(err);
            }
            return callback(null, authInfo);
        }
    );
}

export default queryAuthCheck;
