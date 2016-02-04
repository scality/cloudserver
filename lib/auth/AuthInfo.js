import constants from '../../constants';

/**
 * Class containing requester's information received from Vault
 * @param {object} info from Vault including arn, canonicalID,
 * shortid, email, accountDisplayName and IAMdisplayName (if applicable)
 * @return {AuthInfo} an AuthInfo instance
 */

export default class AuthInfo {
    constructor(objectFromVault) {
        const {arn, canonicalID, shortid, email,
            accountDisplayName, IAMdisplayName} = objectFromVault;
        // amazon resource name for IAM user (if applicable)
        this.arn = arn;
        // account canonicalID
        this.canonicalID = canonicalID;
        // shortid for account (also contained in ARN)
        this.shortid = shortid;
        // email for account or user as applicable
        this.email = email;
        // display name for account
        this.accountDisplayName = accountDisplayName;
        // display name for user (if applicable)
        this.IAMdisplayName = IAMdisplayName;
    }
    getArn() {
        return this.arn;
    }
    getCanonicalID() {
        return this.canonicalID;
    }
    getShortid() {
        return this.shortid;
    }
    getEmail() {
        return this.email;
    }
    getAccountDisplayName() {
        return this.accountDisplayName;
    }
    getIAMdisplayName() {
        return this.IAMdisplayName;
    }
    // Check whether requester is an IAM user versus an account
    isRequesterAnIAMUser() {
        return this.IAMdisplayName ? true : false;
    }
    isRequesterPublicUser() {
        return this.canonicalID === constants.publicId;
    }
}
