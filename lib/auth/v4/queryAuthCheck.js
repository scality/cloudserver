import vault from '../vault';
import constructStringToSign from './constructStringToSign';
import { checkTimeSkew } from './timeUtils';
import { validateCredentials, extractQueryParams } from './validateInputs';


/**
 * V4 query auth check
 * @param {object} request - HTTP request object
 * @param {object} log - logging object
 * @param {function} callback - callback to auth checking function
 * @return {callback} calls callback
 */
function queryAuthCheck(request, log, callback) {
    const authParams = extractQueryParams(request.query, log);

    if (Object.keys(authParams).length !== 5) {
        return callback('InvalidArgument');
    }
    const { signedHeaders, signatureFromRequest, timestamp,
        expiry, credential } = authParams;

    if (!validateCredentials(credential, timestamp, log)) {
        log.warn('credential param format incorrect', { credential });
        return callback('InvalidArgument');
    }
    const [accessKey, scopeDate, region, service, requestType] =
        credential;

    const isTimeSkewed = checkTimeSkew(timestamp, expiry, log);
    if (isTimeSkewed) {
        return callback('RequestTimeTooSkewed');
    }

    // In query v4 auth, the canonical request needs
    // to include the query params OTHER THAN
    // the signature so create a
    // copy of the query object and remove
    // the X-Amz-Signature property.
    const queryWithoutSignature = Object.assign({}, request.query);
    delete queryWithoutSignature['X-Amz-Signature'];

    // For query auth, instead of a
    // checksum of the contents, the
    // string 'UNSIGNED-PAYLOAD' should be
    // added to the canonicalRequest in
    // building string to sign
    const payloadChecksum = 'UNSIGNED-PAYLOAD';

    const stringToSign = constructStringToSign({
        log,
        request,
        query: queryWithoutSignature,
        signedHeaders,
        payloadChecksum,
        timestamp,
        credentialScope:
            `${scopeDate}/${region}/${service}/${requestType}`,
    });
    if (stringToSign instanceof Error) {
        return callback(stringToSign);
    }
    log.trace('constructed stringToSign', { stringToSign });
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

export default queryAuthCheck;
