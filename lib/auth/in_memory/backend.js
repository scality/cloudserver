import { errors } from 'arsenal';
import crypto from 'crypto';

import config from '../../Config';
import Indexer from './indexer';

import { calculateSigningKey, hashSignature } from './vaultUtilities';

const authIndex = new Indexer(config.authData);

function _buildArn(generalResource, specificResource) {
    return `arn:aws:s3:::${generalResource}/${specificResource}`;
}

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
        const entity = authIndex.getByKey(accessKey);
        if (!entity) {
            return callback(errors.InvalidAccessKeyId);
        }
        const secretKey = entity.keys
            .filter(kv => kv.access === accessKey)[0].secret;
        const reconstructedSig =
            hashSignature(stringToSign, secretKey, options.algo);
        if (signatureFromRequest !== reconstructedSig) {
            return callback(errors.SignatureDoesNotMatch);
        }
        const userInfoToSend = {
            accountDisplayName: entity.accountDisplayName,
            canonicalID: entity.canonicalID,
            arn: entity.arn,
            IAMdisplayName: entity.IAMdisplayName,
        };
        const vaultReturnObject = {
            message: {
                body: { userInfo: userInfoToSend },
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
        const entity = authIndex.getByKey(accessKey);
        if (!entity) {
            return callback(errors.InvalidAccessKeyId);
        }
        const secretKey = entity.keys
            .filter(kv => kv.access === accessKey)[0].secret;
        const signingKey = calculateSigningKey(secretKey, region, scopeDate);
        const reconstructedSig = crypto.createHmac('sha256', signingKey)
            .update(stringToSign, 'binary').digest('hex');
        if (signatureFromRequest !== reconstructedSig) {
            return callback(errors.SignatureDoesNotMatch);
        }
        const userInfoToSend = {
            accountDisplayName: entity.accountDisplayName,
            canonicalID: entity.canonicalID,
            arn: entity.arn,
            IAMdisplayName: entity.IAMdisplayName,
        };
        const vaultReturnObject = {
            message: {
                body: { userInfo: userInfoToSend },
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
        emails.forEach(email => {
            if (!authIndex.getByEmail(email)) {
                results[email] = 'NotFound';
            } else {
                results[email] = authIndex.getByEmail(email).canonicalID;
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
        canonicalIDs.forEach(canonicalId => {
            const foundAccount = authIndex.getByCanId(canonicalId);
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

    /**
     * Mocks Vault's response to a policy evaluation request
     * Since policies not actually implemented in memory backend,
     * we allow users to proceed with request.
     * @param {object} requestContextParams - parameters needed to construct
     * requestContext in Vault
     * @param {object} requestContextParams.constantParams -
     * params that have the
     * same value for each requestContext to be constructed in Vault
     * @param {object} requestContextParams.paramaterize - params that have
     * arrays as values since a requestContext needs to be constructed with
     * each option in Vault
     * @param {string} userArn - arn of requesting user
     * @param {object} log - log object
     * @param {function} cb - callback with either error or an array
     * of authorization results
     * @returns {callback} with either error or array of authorization results
     */
    checkPolicies: (requestContextParams, userArn, log, cb) => {
        const results = [];
        const generalResourceParams =
            requestContextParams.parameterize.generalResource;
        const specificResourceParams =
            requestContextParams.parameterize.specificResource;
        if (generalResourceParams) {
            if (specificResourceParams) {
                // both bucket and object are parameterized
                generalResourceParams.forEach(bucket => {
                    specificResourceParams.forEach(obj => {
                        results.push({
                            isAllowed: true,
                            arn: _buildArn(bucket, obj),
                        });
                    });
                });
            } else {
                // just bucket is parameterized
                generalResourceParams.forEach(bucket => {
                    results.push({
                        isAllowed: true,
                        arn: _buildArn(bucket,
                        requestContextParams
                        .constantParams.specificResource),
                    });
                });
            }
        } else if (specificResourceParams) {
            // just object is parameterized
            specificResourceParams.forEach(obj => {
                results.push({
                    isAllowed: true,
                    arn: _buildArn(requestContextParams
                        .constantParams.generalResource, obj),
                });
            });
        } else {
            results.push({
                isAllowed: true,
                arn: _buildArn(requestContextParams
                    .constantParams.generalResource, requestContextParams
                    .constantParams.specificResource),
            });
        }
        const vaultReturnObject = {
            message: {
                body: results,
            },
        };
        return cb(null, vaultReturnObject);
    },
};

export default backend;
