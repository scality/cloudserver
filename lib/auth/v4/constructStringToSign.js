import crypto from 'crypto';

import createCanonicalRequest from './createCanonicalRequest';

/**
 * constructStringToSign - creates V4 stringToSign
 * @param {object} params - params object
 * @returns {string} - stringToSign
 */
function constructStringToSign(params) {
    const { request, signedHeaders, payloadChecksum,
        credentialScope, timestamp, query, log } = params;

    const canonicalReqResult = createCanonicalRequest({
        pHttpVerb: request.method,
        pResource: request.path,
        pQuery: query,
        pHeaders: request.headers,
        pSignedHeaders: signedHeaders,
        payloadChecksum,
    });

    if (canonicalReqResult instanceof Error) {
        log.error('error creating canonicalRequest');
        return canonicalReqResult;
    }
    log.debug('constructed canonicalRequest', { canonicalReqResult });
    const sha256 = crypto.createHash('sha256');
    const stringToSign = `AWS4-HMAC-SHA256\n${timestamp}\n` +
    `${credentialScope}\n${sha256.update(canonicalReqResult).digest('hex')}`;
    return stringToSign;
}

export default constructStringToSign;
