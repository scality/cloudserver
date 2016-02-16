import crypto from 'crypto';

import { accountsKeyedbyAccessKey } from './vault.json';

/** hashSignature
 * @param {string} stringToSign - built string to sign per AWS rules
 * @param {string} secretKey - user's secretKey
 * @param {string} algorithm - either SHA256 or SHA1
 * @return {string} reconstructed signature
 */
export function hashSignature(stringToSign, secretKey, algorithm) {
    const hmacObject = crypto.createHmac(algorithm, secretKey);
    return hmacObject.update(stringToSign).digest('base64');
}


const backend = {
    /** verifySignatureV2
     * @param {object} params - items needed to check auth
     * @param {function} callback - callback with either error or user info
     * @return {function} calls callback
     */
    verifySignatureV2(stringToSign, signatureFromRequest, accessKey, options,
                      callback) {
        const account = accountsKeyedbyAccessKey[accessKey];
        if (!account) {
            return callback({message: {code: 400}});
        }
        const secretKey = account.secretKey;
        const reconstructedSig =
            hashSignature(stringToSign, secretKey, options.algo);
        if (signatureFromRequest !== reconstructedSig) {
            return callback({message: {code: 403}});
        }
        const userInfoToSend = {
            accountDisplayName: account.displayName,
            canonicalID: account.canonicalID,
            arn: account.arn,
            IAMdisplayName: account.IAMdisplayName,
        };
        const vaultReturnObject = {
            message: {
                body: userInfoToSend,
            }
        };
        return callback(null, vaultReturnObject);
    }
};

export default backend;
