import crypto from 'crypto';

import { accountsKeyedbyAccessKey } from './vault.json';
import { calculateSigningKey, hashSignature, } from './vaultUtilities';

const backend = {
    /** verifySignatureV2
    * @param {string} stringToSign - string to sign built per AWS rules
    * @param {string} signatureFromRequest - signature sent with request
    * @param {string} accessKey - user's accessKey
    * @param {object} options - contains algorithm (SHA1 or SHA256)
    * @param {function} callback - callback with either error or user info
    * @return {function} calls callback
    */
    verifySignatureV2: (stringToSign, signatureFromRequest,
        accessKey, options, callback) => {
        const account = accountsKeyedbyAccessKey[accessKey];
        if (!account) {
            const fakeVaultclientErr = new Error('');
            fakeVaultclientErr.code = 400;
            return callback(fakeVaultclientErr);
        }
        const secretKey = account.secretKey;
        const reconstructedSig =
            hashSignature(stringToSign, secretKey, options.algo);
        if (signatureFromRequest !== reconstructedSig) {
            const fakeVaultclientErr = new Error('');
            fakeVaultclientErr.code = 403;
            return callback(fakeVaultclientErr);
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
            },
        };
        return callback(null, vaultReturnObject);
    },


    /** verifySignatureV4
     * @param {string} stringToSign - string to sign built per AWS rules
     * @param {string} signatureFromRequest - signature sent with request
     * @param {string} accessKey - user's accessKey
     * @param {string} region - region specified in request credential
     * @param {string} scopeDate - date specified in request credential
     * @param {object} options - options to send to Vault
     * (just contains reqUid for logging in Vault)
     * @param {function} callback - callback with either error or user info
     * @return {function} calls callback
     */
    verifySignatureV4: (stringToSign, signatureFromRequest, accessKey,
        region, scopeDate, options, callback) => {
        const account = accountsKeyedbyAccessKey[accessKey];
        if (!account) {
            return callback({ message: { code: 400 } });
        }
        const secretKey = account.secretKey;
        const signingKey = calculateSigningKey(secretKey, region, scopeDate);
        const reconstructedSig = crypto.createHmac(`sha256`, signingKey)
            .update(stringToSign).digest(`hex`);
        if (signatureFromRequest !== reconstructedSig) {
            return callback({ message: { code: 403 } });
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
            },
        };
        return callback(null, vaultReturnObject);
    },
};

export default backend;
