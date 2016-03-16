import { errors } from 'arsenal';
import crypto from 'crypto';

import { accountsKeyedbyAccessKey, accountsKeyedbyCanID,
    accountsKeyedbyEmail } from './vault.json';
import { calculateSigningKey, hashSignature } from './vaultUtilities';

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
            return callback(errors.InvalidAccessKeyId);
        }
        const secretKey = account.secretKey;
        const reconstructedSig =
            hashSignature(stringToSign, secretKey, options.algo);
        if (signatureFromRequest !== reconstructedSig) {
            return callback(errors.SignatureDoesNotMatch);
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
            return callback(errors.InvalidAccessKeyId);
        }
        const secretKey = account.secretKey;
        const signingKey = calculateSigningKey(secretKey, region, scopeDate);
        const reconstructedSig = crypto.createHmac('sha256', signingKey)
            .update(stringToSign).digest('hex');
        if (signatureFromRequest !== reconstructedSig) {
            return callback(errors.SignatureDoesNotMatch);
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

    /**
     * Gets canonical ID's for a list of accounts
     * based on email associated with account
     * @param {array} emails - list of email addresses
     * @param {object} log - log object
     * @param {function} cb - callback to calling function
     * @returns {function} callback with either error or
     * object with email addresses as keys and canonical IDs
     * as values
     */
    getCanonicalIds: (emails, log, cb) => {
        const results = {};
        emails.forEach((email) => {
            const lowercasedEmail = email.toLowerCase();
            if (!accountsKeyedbyEmail[lowercasedEmail]) {
                results[email] = 'NotFound';
            } else {
                results[email] =
                    accountsKeyedbyEmail[lowercasedEmail].canonicalID;
            }
        });
        const vaultReturnObject = {
            message: {
                body: results,
            },
        };
        return cb(null, vaultReturnObject);
    },

    /**
     * Gets email addresses (referred to as diplay names for getACL's)
     * for a list of accounts
     * based on canonical IDs associated with account
     * @param {array} canonicalIDs - list of canonicalIDs
     * @param {object} options - to send log id to vault
     * @param {function} cb - callback to calling function
     * @returns {function} callback with either error or
     * an object from Vault containing account canonicalID
     * as each object key and an email address as the value (or "NotFound")
     */
    getEmailAddresses: (canonicalIDs, options, cb) => {
        const results = {};
        canonicalIDs.forEach((canonicalId) => {
            const foundAccount = accountsKeyedbyCanID[canonicalId];
            if (!foundAccount || !foundAccount.email) {
                results[canonicalId] = 'NotFound';
            } else {
                results[canonicalId] = foundAccount.email;
            }
        });
        const vaultReturnObject = {
            message: {
                body: results,
            },
        };
        return cb(null, vaultReturnObject);
    },
};

export default backend;
