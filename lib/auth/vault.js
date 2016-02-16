import vaultclient from 'vaultclient';

import AuthInfo from './AuthInfo';
import Config from '../Config';
import backend from './in_memory/backend';

let client;

if (process.env.S3BACKEND && process.env.S3BACKEND === 'mem') {
    client = backend;
} else {
    const config = new Config();
    client = new vaultclient.Client(config.vaultd.host, config.vaultd.port);
}

/**
 * @param {number} errorCode - the error code sent from Vault
 * (or in memory vault)
 * @return {string} errorMessage to send back to user
 */
function _findErrorMessage(errorCode) {
    const cases = {
        400: 'InvalidArgument',
        403: 'SignatureDoesNotMatch',
        404: 'NoSuchKey',
        500: 'InternalError',
    };
    return cases[errorCode] ? cases[errorCode] : cases[500];
}

/** vaultSignatureCb parses message from Vault and instantiates
 * @param {object} err - error from vault
 * @param {object} userInfo - info from vault
 * @param {object} log - log for request
 * @param {function} callback - callback to authCheck functions
 * @return {undefined}
 */
function vaultSignatureCb(err, userInfo, log, callback) {
    // vaultclient API guarantees that it returns:
    // - either `err`, an Error object with `code` and `message` properties set
    // - or `err == null` and `info` is an object with `message.code` and
    //   `message.message` properties set.
    if (err) {
        log.error('received error message from vault', { errorMessage: err });
        const errmsg = _findErrorMessage(err.code);
        log.error('translated S3 error message', { errorMessage: errmsg });
        return callback(errmsg);
    }

    log.debug('received user info from Vault', { userInfo });
    return callback(null, new AuthInfo(userInfo.message.body));
}

const vault = {};

/**
 * authenticateV2Request
 *
 * @param {string} accessKey - user's accessKey
 * @param {string} signatureFromRequest - signature sent with request
 * @param {string} stringToSign - string to sign built per AWS rules
 * @param {string} algo - either SHA256 or SHA1
 * @param {function} callback - callback with either error or user info
 * @return {undefined}
 */
vault.authenticateV2Request = (accessKey, signatureFromRequest,
    stringToSign, algo, log, callback) => {
    log.debug('authenticating V2 request');
    client.verifySignatureV2(stringToSign, signatureFromRequest, accessKey,
        { algo, reqUid: log.getSerializedUids() },
    (err, userInfo)  => vaultSignatureCb(err, userInfo, log, callback));
};

/** authenticateV4Request
 * @param {object} params - contains accessKey (string),
 * signatureFromRequest (string), region (string),
 * stringToSign (string) and log (object)
 * @param {function} callback - callback with either error or user info
 * @return {undefined}
*/
vault.authenticateV4Request = (params, callback) => {
    const { accessKey, signatureFromRequest, region, scopeDate,
        stringToSign, log }
        = params;
    log.debug('authenticating V4 request');
    client.verifySignatureV4(stringToSign, signatureFromRequest,
        accessKey, region, scopeDate, { reqUid: log.getSerializedUids() },
    (err, userInfo) => vaultSignatureCb(err, userInfo, log, callback));
};

export default vault;
