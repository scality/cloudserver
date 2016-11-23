/**
 * Class that provides an internal indexing over the simple data provided by
 * the authentication configuration file for the memory backend. This allows
 * accessing the different authentication entities through various types of
 * keys.
 *
 * @class Index
 */
export default class Index {
    /**
     * @constructor
     * @param {object} authdata - the authentication config file's data
     * @return {undefined}
     */
    constructor(authdata) {
        this.accountsBy = {
            canId: {},
            accessKey: {},
            email: {},
        };
        this.usersBy = {
            accessKey: {},
            email: {},
        };

        /*
         * This may happen if the file backend is not configured for S3.
         * As such, we're managing the error here to avoid screwing up there.
         */
        if (!authdata) {
            return ;
        }

        this._build(authdata);
    }

    _indexUser(account, user) {
        const userData = {
            arn: account.arn,
            canonicalID: account.canonicalID,
            shortid: account.shortid,
            accountDisplayName: account.name,
            IAMdisplayName: user.name,
            email: user.email.toLowerCase(),
            keys: [],
        };
        this.usersBy.email[userData.email] = userData;
        user.keys.forEach(key => {
            userData.keys.push(key);
            this.usersBy.accessKey[key.access] = userData;
        });
    }

    _indexAccount(account) {
        const accountData = {
            arn: account.arn,
            canonicalID: account.canonicalID,
            shortid: account.shortid,
            accountDisplayName: account.name,
            email: account.email.toLowerCase(),
            keys: [],
        };
        this.accountsBy.canId[accountData.canonicalID] = accountData;
        this.accountsBy.email[accountData.email] = accountData;
        if (account.keys !== undefined) {
            account.keys.forEach(key => {
                accountData.keys.push(key);
                this.accountsBy.accessKey[key.access] = accountData;
            });
        }
        if (account.users !== undefined) {
            account.users.forEach(user => {
                this._indexUser(accountData, user);
            });
        }
    }

    _build(authdata) {
        authdata.accounts.forEach(account => {
            this._indexAccount(account);
        });
    }

    /**
     * This method returns the account associated to a canonical ID.
     *
     * @param {string} canId - The canonicalId of the account
     * @return {Object} account - The account object
     * @return {Object} account.arn - The account's ARN
     * @return {Object} account.canonicalID - The account's canonical ID
     * @return {Object} account.shortid - The account's internal shortid
     * @return {Object} account.accountDisplayName - The account's display name
     * @return {Object} account.email - The account's lowercased email
     */
    getByCanId(canId) {
        return this.accountsBy.canId[canId];
    }

    /**
     * This method returns the entity (either an account or a user) associated
     * to a canonical ID.
     *
     * @param {string} key - The accessKey of the entity
     * @return {Object} entity - The entity object
     * @return {Object} entity.arn - The entity's ARN
     * @return {Object} entity.canonicalID - The canonical ID for the entity's
     *                                       account
     * @return {Object} entity.shortid - The entity's internal shortid
     * @return {Object} entity.accountDisplayName - The entity's account
     *                                              display name
     * @return {Object} entity.IAMDisplayName - The user's display name
     *                                          (if the entity is an user)
     * @return {Object} entity.email - The entity's lowercased email
     */
    getByKey(key) {
        if (this.accountsBy.accessKey.hasOwnProperty(key)) {
            return this.accountsBy.accessKey[key];
        }
        return this.usersBy.accessKey[key];
    }

    /**
     * This method returns the entity (either an account or a user) associated
     * to an email address.
     *
     * @param {string} email - The email address
     * @return {Object} entity - The entity object
     * @return {Object} entity.arn - The entity's ARN
     * @return {Object} entity.canonicalID - The canonical ID for the entity's
     *                                       account
     * @return {Object} entity.shortid - The entity's internal shortid
     * @return {Object} entity.accountDisplayName - The entity's account
     *                                              display name
     * @return {Object} entity.IAMDisplayName - The user's display name
     *                                          (if the entity is an user)
     * @return {Object} entity.email - The entity's lowercased email
     */
    getByEmail(email) {
        const lowerCasedEmail = email.toLowerCase();
        if (this.usersBy.email.hasOwnProperty(lowerCasedEmail)) {
            return this.usersBy.email[lowerCasedEmail];
        }
        return this.accountsBy.email[lowerCasedEmail];
    }
}
