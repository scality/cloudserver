import vaultclient from 'vaultclient';

import AuthInfo from './AuthInfo';
import Config from '../Config';
import backend from './backend';
import checkStringParse from '../utilities/checkStringParse';

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
vault.authenticateV2Request = (accessKey, signatureFromRequest, stringToSign,
                               algo, log, callback) => {
    log.debug('authenticating V2 request');
    client.verifySignatureV2(stringToSign, signatureFromRequest, accessKey,
        { algo, reqUid: log.getSerializedUids() },
        function rcvVaultV2Sig(err, userInfo) {
            if (err) {
                const parsedErr = checkStringParse(err);
                if (parsedErr instanceof Error) {
                    return callback(parsedErr);
                }
                log.error('received error message from vault',
                          { errorMessage: parsedErr });
                const errmsg = _findErrorMessage(parsedErr.message.code);
                log.error('translated S3 error message',
                          { errorMessage: errmsg });
                return callback(errmsg);
            }
            const parsedInfo = checkStringParse(userInfo);
            if (parsedInfo instanceof Error) {
                return callback(parsedInfo);
            }
            log.debug('received user info from Vault',
                      { userInfo: parsedInfo });
            const authInfo = new AuthInfo(parsedInfo.message.body);
            return callback(null, authInfo);
        });
};

export default vault;
