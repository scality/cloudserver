const { errorInstances, models } = require('@scality/arsenal');

/**
 * Return the account of the requester ARN, which Vault always gives with a
 * 12-digit account. Never the short ID, which is the user or role ID.
 * @param {AuthInfo} authInfo - the requester
 * @return {string} - the account ID
 */
function requesterAccountId(authInfo) {
    return models.ARN.createFromString(authInfo.getArn()).getAccountId();
}

/**
 * The error that a destination name outside the grammar gets.
 * @param {*} name - the name query parameter
 * @return {ArsenalError|null} - InvalidDestination, or null when the name
 * is valid
 */
function nameError(name) {
    if (models.NotificationDestination.isValidName(name)) {
        return null;
    }
    return errorInstances.InvalidDestination.customizeDescription(
        'A bucket notification destination name has 1 to ' +
            `${models.NotificationDestination.nameMaxLength} letters, digits and +=,.@_- or spaces.`,
    );
}

// The version of the destination records that this server writes. Create
// and Update both write this version.
const recordModelVersion = 1;

module.exports = {
    nameError,
    recordModelVersion,
    requesterAccountId,
};
