import { errors } from 'arsenal';

import { accountsKeyedbyEmail, accountsKeyedbyCanID, } from
    '../auth/in_memory/vault.json';
import constants from '../../constants';
import metadata from '../metadata/wrapper';
import utils from '../utils';

const acl = {
    /**
     * Gets canonical ID of account based on email associated with account
     * @param {string} email - account's email address
     * @param {object} log - Werelogs logger
     * @param {function} cb - callback to bucketPutACL.js
     * @returns {function} callback with either error or
     * canonical ID response from Vault
     */
    getCanonicalID(email, log, cb) {
        const lowercasedEmail = email.toLowerCase();
        // Placeholder for actual request to Vault/Metadata
        process.nextTick(() => {
            if (accountsKeyedbyEmail[lowercasedEmail] === undefined) {
                return cb(errors.UnresolvableGrantByEmailAddress);
            }
            // if more than one canonical ID associated
            // with email address, return callback with
            // error 'AmbiguousGrantByEmailAddres'
            // AWS has this error as a possibility.  If we will not have
            // an email address associated with multiple accounts, then
            // error not needed.
            return cb(null, accountsKeyedbyEmail[lowercasedEmail].canonicalID);
        });
    },

    /**
     * Gets canonical ID's for a list of accounts
     * based on email associated with account
     * @param {array} emails - list of email addresses
     * @param {object} log - Werelogs logger
     * @param {function} cb - callback to calling function
     * @returns {function} callback with either error or
     * canonical ID response from Vault
     */
    getManyCanonicalIDs(emails, log, cb) {
        process.nextTick(function getIDs() {
            let canonicalID;
            let lowercasedEmail;
            const results = [];
            for (let i = 0; i < emails.length; i++) {
                lowercasedEmail = emails[i].toLowerCase();
                if (!accountsKeyedbyEmail[lowercasedEmail]) {
                    log.warn('canonical id not found matching the email',
                        { email: lowercasedEmail });
                    log.warn('unresolvablegrantbyemailaddress');
                    return cb(errors.UnresolvableGrantByEmailAddress);
                }
                canonicalID = accountsKeyedbyEmail[lowercasedEmail].canonicalID;
                results.push({
                    email: lowercasedEmail,
                    canonicalID,
                });
            }
            return cb(null, results);
        });
    },

    /**
     * Gets email addresses (referred to as diplay names for getACL's)
     * for a list of accounts
     * based on canonical IDs associated with account
     * @param {array} canonicalIDs - list of canonicalIDs
     * @param {function} cb - callback to calling function
     * @returns {function} callback with either error or
     * array of account objects from Vault containing account canonicalID
     * and email address for each account
     */
    getManyDisplayNames(canonicalIDs, cb) {
        process.nextTick(() => {
            let foundAccount;
            const results = [];
            for (let i = 0; i < canonicalIDs.length; i++) {
                foundAccount = accountsKeyedbyCanID[canonicalIDs[i]];
                // TODO: Determine whether want to return an error message
                // if user no longer found or just skip as done here
                if (!foundAccount) {
                    continue;
                }
                results.push({
                    displayName: foundAccount.email,
                    canonicalID: canonicalIDs[i],
                });
            }
            // TODO: Send back error if no response from Vault
            return cb(null, results);
        });
    },

    addACL(bucket, addACLParams, log, cb) {
        log.trace('updating bucket acl in metadata');
        bucket.acl = addACLParams;
        metadata.updateBucket(bucket.name, bucket, log, cb);
    },

    addObjectACL(bucket, objectKey, objectMD, addACLParams, log, cb) {
        log.trace('updating object acl in metadata');
        objectMD.acl = addACLParams;
        metadata.putObjectMD(bucket.name, objectKey, objectMD, log, (err) => {
            if (err) {
                log.warn('error from metadata', { error: err });
                return cb(err);
            }
            return cb();
        });
    },

    parseAclFromHeaders(params, cb) {
        const headers = params.headers;
        const resourceType = params.resourceType;
        const currentResourceACL = params.acl;
        const log = params.log;
        const resourceACL = {
            Canned: '',
            FULL_CONTROL: [],
            WRITE: [],
            WRITE_ACP: [],
            READ: [],
            READ_ACP: [],
        };
        let validCannedACL = [];
        if (resourceType === 'bucket') {
            validCannedACL =
                ['private', 'public-read', 'public-read-write',
                'authenticated-read', 'log-delivery-write'];
        } else if (resourceType === 'object') {
            validCannedACL =
                ['private', 'public-read', 'public-read-write',
                'authenticated-read', 'bucket-owner-read',
                'bucket-owner-full-control'];
        }

        // parse canned acl
        if (headers['x-amz-acl']) {
            const newCannedACL = headers['x-amz-acl'];
            if (validCannedACL.indexOf(newCannedACL) > -1) {
                resourceACL.Canned = newCannedACL;
                return cb(null, resourceACL);
            }
            return cb(errors.InvalidArgument);
        }

        // parse grant headers
        const grantReadHeader =
            utils.parseGrant(headers['x-amz-grant-read'], 'READ');
        let grantWriteHeader = [];
        if (resourceType === 'bucket') {
            grantWriteHeader = utils
            .parseGrant(headers['x-amz-grant-write'], 'WRITE');
        }
        const grantReadACPHeader = utils
        .parseGrant(headers['x-amz-grant-read-acp'], 'READ_ACP');
        const grantWriteACPHeader = utils
        .parseGrant(headers['x-amz-grant-write-acp'], 'WRITE_ACP');
        const grantFullControlHeader = utils
        .parseGrant(headers['x-amz-grant-full-control'], 'FULL_CONTROL');
        const allGrantHeaders =
            [].concat(grantReadHeader, grantWriteHeader,
            grantReadACPHeader, grantWriteACPHeader,
            grantFullControlHeader).filter(item => item !== undefined);
        if (allGrantHeaders.length === 0) {
            return cb(null, currentResourceACL);
        }

        const usersIdentifiedByEmail = allGrantHeaders
        .filter(it => it && it.userIDType.toLowerCase() === 'emailaddress');
        const usersIdentifiedByGroup = allGrantHeaders
        .filter(item => item && item.userIDType.toLowerCase() === 'uri');
        const justEmails = usersIdentifiedByEmail.map(item => item.identifier);
        const validGroups = [
            constants.allAuthedUsersId,
            constants.publicId,
            constants.logId,
        ];

        for (let i = 0; i < usersIdentifiedByGroup.length; i++) {
            if (validGroups.indexOf(usersIdentifiedByGroup[i].identifier) < 0) {
                return cb(errors.InvalidArgument);
            }
        }
        const usersIdentifiedByID = allGrantHeaders
        .filter(item => item && item.userIDType.toLowerCase() === 'id');
        // TODO: Consider whether want to verify with Vault
        // whether canonicalID is associated with existing
        // account before adding to ACL

        // If have to lookup canonicalID's do that asynchronously
        // then add grants to bucket
        if (justEmails.length > 0) {
            this.getManyCanonicalIDs(justEmails, log, (err, results) => {
                if (err) {
                    return cb(err);
                }
                const reconstructedUsersIdentifiedByEmail = utils.
                    reconstructUsersIdentifiedByEmail(results,
                        usersIdentifiedByEmail);
                const allUsers = [].concat(
                    reconstructedUsersIdentifiedByEmail,
                    usersIdentifiedByGroup,
                    usersIdentifiedByID);
                const revisedACL =
                    utils.sortHeaderGrants(allUsers, resourceACL);
                return cb(null, revisedACL);
            });
        } else {
            // If don't have to look up canonicalID's just sort grants
            // and add to bucket
            const revisedACL = utils
            .sortHeaderGrants(allGrantHeaders, resourceACL);
            return cb(null, revisedACL);
        }
    },
};

export default acl;
