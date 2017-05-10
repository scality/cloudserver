/** build simple authdata with only one account
 * @param {string} accessKey - account's accessKey
 * @param {string} secretKey - account's secretKey
 * @return {object} authdata - authdata with account's accessKey and secretKey
 */
function buildAuthDataAccount(accessKey, secretKey) {
    return {
        accounts: [{
            name: 'CustomAccount',
            email: 'customaccount1@setbyenv.com',
            arn: 'aws::iam:123456789012:root',
            canonicalID: '12349df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d52' +
            '18e7cd47qwer',
            shortid: '123456789012',
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
