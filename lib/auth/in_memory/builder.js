const serviceAccountPrefix =
    require('arsenal').constants.zenkoServiceAccount;

/** build simple authdata with only one account
 * @param {string} accessKey - account's accessKey
 * @param {string} secretKey - account's secretKey
 * @param {string} [serviceName] - service name to use to generate can id
 * @return {object} authdata - authdata with account's accessKey and secretKey
 */
function buildAuthDataAccount(accessKey, secretKey, serviceName) {
    // TODO: remove specific check for clueso and generate unique
    // canonical id's for accounts
    const canonicalID = serviceName && serviceName === 'clueso' ?
        `${serviceAccountPrefix}/${serviceName}` : '12349df900b949e' +
            '55d96a1e698fbacedfd6e09d98eacf8f8d52' +
            '18e7cd47qwer';
    const shortid = '123456789012';
    return {
        accounts: [{
            name: 'CustomAccount',
            email: 'customaccount1@setbyenv.com',
            arn: `arn:aws:iam::${shortid}:root`,
            canonicalID,
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
