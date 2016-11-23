import { errors } from 'arsenal';

import aclUtils from '../utilities/aclUtils';
import constants from '../../constants';
import metadata from '../metadata/wrapper';
import vault from '../auth/vault';

const acl = {
    addACL(bucket, addACLParams, log, cb) {
        log.trace('updating bucket acl in metadata');
        bucket.setFullAcl(addACLParams);
        metadata.updateBucket(bucket.getName(), bucket, log, cb);
    },

    addObjectACL(bucket, objectKey, objectMD, addACLParams, log, cb) {
        log.trace('updating object acl in metadata');
        objectMD.acl = addACLParams;
        metadata.putObjectMD(bucket.getName(), objectKey, objectMD, log, cb);
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
            aclUtils.parseGrant(headers['x-amz-grant-read'], 'READ');
        let grantWriteHeader = [];
        if (resourceType === 'bucket') {
            grantWriteHeader = aclUtils
            .parseGrant(headers['x-amz-grant-write'], 'WRITE');
        }
        const grantReadACPHeader = aclUtils
        .parseGrant(headers['x-amz-grant-read-acp'], 'READ_ACP');
        const grantWriteACPHeader = aclUtils
        .parseGrant(headers['x-amz-grant-write-acp'], 'WRITE_ACP');
        const grantFullControlHeader = aclUtils
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
            return vault.getCanonicalIds(justEmails, log, (err, results) => {
                if (err) {
                    return cb(err);
                }
                const reconstructedUsersIdentifiedByEmail = aclUtils.
                    reconstructUsersIdentifiedByEmail(results,
                        usersIdentifiedByEmail);
                const allUsers = [].concat(
                    reconstructedUsersIdentifiedByEmail,
                    usersIdentifiedByGroup,
                    usersIdentifiedByID);
                const revisedACL =
                    aclUtils.sortHeaderGrants(allUsers, resourceACL);
                return cb(null, revisedACL);
            });
        }
        // If don't have to look up canonicalID's just sort grants
        // and add to bucket
        const revisedACL = aclUtils
        .sortHeaderGrants(allGrantHeaders, resourceACL);
        return cb(null, revisedACL);
    },
};

export default acl;
