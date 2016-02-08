import AuthInfo from './AuthInfo';
import VaultClient from 'vaultclient';

import Config from '../Config';
import backend from './backend';
import checkStringParse from '../utilities/checkStringParse';

let client;

if (process.env.S3BACKEND && process.env.S3BACKEND === 'mem') {
    client = backend;
} else {
    const config = new Config();
    client = new VaultClient(config.vaultd);
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

const vault = {
    /** authenticateV2Request
    * @param {string} accessKey - user's accessKey
    * @param {string} signatureFromRequest - signature sent with request
    * @param {string} stringToSign - string to sign built per AWS rules
    * @param {string} algo - either SHA256 or SHA1
    * @param {function} callback - callback with either error or user info
    * @return {function} calls callback
    */
    authenticateV2Request: (accessKey, signatureFromRequest,
        stringToSign, algo, log, callback) => {
        log.debug('Authenticating V2 Request');
        client.verifySignatureV2({
            stringToSign,
            accessKey,
            signatureFromRequest,
            hashAlgorithm: algo,
        },
        function rcvVaultV2Sig(err, userInfo) {
            if (err) {
                const parsedErr = checkStringParse(err);
                if (parsedErr instanceof Error) {
                    return callback(parsedErr);
                }
                log.error(`Error message from Vault: ${parsedErr}`);
                const errmsg = _findErrorMessage(parsedErr.message.code);
                log.error(`Translated S3 Error Message: ${errmsg}`);
                return callback(errmsg);
            }
            const parsedInfo = checkStringParse(userInfo);
            if (parsedInfo instanceof Error) {
                return callback(parsedInfo);
            }
            log.debug(`User Info from Vault: ${parsedInfo}`);
            const authInfo = new AuthInfo(parsedInfo.message.body);
            return callback(null, authInfo);
        });
    }
};

export default vault;
