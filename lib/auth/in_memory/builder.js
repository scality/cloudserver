const serviceAccountPrefix =
    require('arsenal').constants.zenkoServiceAccount;

/** build simple authdata with only one account
 * @param {string} accessKey - account's accessKey
 * @param {string} secretKey - account's secretKey
 * @param {string} canonicalId - account's canonical id
 * @param {string} [serviceName] - service name to use to generate can id
 * @param {string} userName - account's user name
 * @return {object} authdata - authdata with account's accessKey and secretKey
 */
function buildAuthDataAccount(accessKey, secretKey, canonicalId, serviceName,
userName) {
    // TODO: remove specific check for clueso and generate unique
    // canonical id's for accounts
    const finalCanonicalId = canonicalId ||
        (serviceName ? `${serviceAccountPrefix}/${serviceName}` :
            '12349df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47qwer');
    const shortid = '123456789012';
    return {
        accounts: [{
            name: userName || 'CustomAccount',
            email: 'customaccount1@setbyenv.com',
            arn: `arn:aws:iam::${shortid}:root`,
            canonicalID: finalCanonicalId,
            shortid,
            keys: [{
                access: accessKey,
                secret: secretKey,
            }],
        }],
    };
}

module.exports = {
    buildAuthDataAccount,
};
