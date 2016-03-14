import { parseString } from 'xml2js';

import { errors } from 'arsenal';
import constants from '../../constants';

const aclUtils = {};

/**
 * handleCannedGrant - Populate grantInfo for a bucketGetACL or objectGetACL
 * @param  {string} grantType - canned grant type
 * @param  {object} ownerGrant - contains owner grant defaults
 * @param  {object} separateBucketOwner - bucket metadata
 * (only needed for just objectGetACL and only if bucket owner and object owner
 * are different)
 * @returns {array} cannedGrants - containing canned ACL settings
 */
aclUtils.handleCannedGrant =
    function handleCannedGrant(grantType,
        ownerGrant, separateBucketOwner) {
        const cannedGrants = [];
        const actions = {
            'private': () => {
                cannedGrants.push(ownerGrant);
            },
            'public-read': () => {
                const publicGrant = {
                    URI: constants.publicId,
                    permission: 'READ',
                };
                cannedGrants.push(ownerGrant, publicGrant);
            },
            'public-read-write': () => {
                const publicReadGrant = {
                    URI: constants.publicId,
                    permission: 'READ',
                };
                const publicWriteGrant = {
                    URI: constants.publicId,
                    permission: 'WRITE',
                };
                cannedGrants.
                    push(ownerGrant, publicReadGrant, publicWriteGrant);
            },
            'authenticated-read': () => {
                const authGrant = {
                    URI: constants.allAuthedUsersId,
                    permission: 'READ',
                };
                cannedGrants.push(ownerGrant, authGrant);
            },
            // Note: log-delivery-write is just for bucketGetACL
            'log-delivery-write': () => {
                const logWriteGrant = {
                    URI: constants.logId,
                    permission: 'WRITE',
                };
                const logReadACPGrant = {
                    URI: constants.logId,
                    permission: 'READ_ACP',
                };
                cannedGrants.
                    push(ownerGrant, logWriteGrant, logReadACPGrant);
            },
            // Note: bucket-owner-read is just for objectGetACL
            'bucket-owner-read': () => {
                // If the bucket owner and object owner are different,
                // add separate entries for each
                if (separateBucketOwner) {
                    const bucketOwnerReadGrant = {
                        ID: separateBucketOwner.owner,
                        displayName: separateBucketOwner.ownerDisplayName,
                        permission: 'READ',
                    };
                    cannedGrants.push(ownerGrant, bucketOwnerReadGrant);
                } else {
                    cannedGrants.push(ownerGrant);
                }
            },
            // Note: bucket-owner-full-control is just for objectGetACL
            'bucket-owner-full-control': () => {
                if (separateBucketOwner) {
                    const bucketOwnerFCGrant = {
                        ID: separateBucketOwner.owner,
                        displayName: separateBucketOwner.ownerDisplayName,
                        permission: 'FULL_CONTROL',
                    };
                    cannedGrants.push(ownerGrant, bucketOwnerFCGrant);
                } else {
                    cannedGrants.push(ownerGrant);
                }
            },
        };
        actions[grantType]();
        return cannedGrants;
    };


aclUtils.parseAclXml = function parseAclXml(toBeParsed, log, next) {
    return parseString(toBeParsed, function parseXML(err, result) {
        if (err) {
            log.warn('invalid xml', { xmlObj: toBeParsed });
            return next(errors.MalformedXML);
        }
        if (!result.AccessControlPolicy
                || !result.AccessControlPolicy.AccessControlList
                || !result.AccessControlPolicy
                .AccessControlList[0].Grant) {
            log.warn('invalid acl', { acl: result });
            return next(errors.MalformedACLError);
        }
        const jsonGrants = result
            .AccessControlPolicy.AccessControlList[0].Grant;
        log.trace('acl grants', { aclGrants: jsonGrants });
        return next(null, jsonGrants);
    });
};

aclUtils.constructGetACLsJson = function constrctGetACLsJson(grantInfo) {
    const { grants, ownerInfo } = grantInfo;
    const accessControlList = grants.map((grant) => {
        let grantIdentifier;
        let type;
        if (grant.ID) {
            grantIdentifier = { ID: grant.ID };
            type = 'CanonicalUser';
        }
        if (grant.URI) {
            grantIdentifier = { URI: grant.URI };
            type = 'Group';
        }
        const grantItem = {
            Grant: [
                { Grantee: [{ _attr: { 'xmlns:xsi':
                    'http://www.w3.org/2001/XMLSchema-instance',
                    'xsi:type': type,
                    },
                },
                    grantIdentifier] },
                { Permission: grant.permission },
            ],
        };
        if (grant.displayName) {
            grantItem.Grant[0].Grantee.push({ DisplayName: grant.displayName });
        }
        return grantItem;
    });

    return {
        AccessControlPolicy: [{
            Owner: [
                { ID: ownerInfo.ID },
                { DisplayName: ownerInfo.displayName },
            ],
        },
        { AccessControlList: accessControlList }],
    };
};

aclUtils.getPermissionType = function getPermissionType(identifier, resourceACL,
        resourceType) {
    const fullControlIndex = resourceACL.FULL_CONTROL.indexOf(identifier);
    let writeIndex;
    if (resourceType === 'bucket') {
        writeIndex = resourceACL.WRITE.indexOf(identifier);
    }
    const writeACPIndex = resourceACL.WRITE_ACP.indexOf(identifier);
    const readACPIndex = resourceACL.READ_ACP.indexOf(identifier);
    const readIndex = resourceACL.READ.indexOf(identifier);
    let permission = '';
    if (fullControlIndex > -1) {
        permission = 'FULL_CONTROL';
        resourceACL.FULL_CONTROL.splice(fullControlIndex, 1);
    } else if (writeIndex > -1) {
        permission = 'WRITE';
        resourceACL.WRITE.splice(writeIndex, 1);
    } else if (writeACPIndex > -1) {
        permission = 'WRITE_ACP';
        resourceACL.WRITE_ACP.splice(writeACPIndex, 1);
    } else if (readACPIndex > -1) {
        permission = 'READ_ACP';
        resourceACL.READ_ACP.splice(readACPIndex, 1);
    } else if (readIndex > -1) {
        permission = 'READ';
        resourceACL.READ.splice(readIndex, 1);
    }
    return permission;
};

aclUtils.parseGrant = function parseGrant(grantHeader, grantType) {
    if (grantHeader === undefined) {
        return undefined;
    }
    const grantArray = grantHeader.split(',');
    let itemArray;
    let userIDType;
    let identifier;
    return grantArray.map((item) => {
        itemArray = item.split('=');
        userIDType = itemArray[0].trim();
        identifier = itemArray[1].trim();
        if (identifier[0] === '"') {
            identifier = identifier.substr(1, identifier.length - 2);
        }
        return {
            userIDType,
            identifier,
            grantType,
        };
    });
};

aclUtils.reconstructUsersIdentifiedByEmail =
    function reconstruct(userInfofromVault, userGrantInfo) {
        return userInfofromVault.map((item) => {
            const userEmail = item.email.toLowerCase();
            // Find the full user grant info based on email
            const user = userGrantInfo
                .find(elem => elem.identifier.toLowerCase() === userEmail);
            // Set the identifier to be the canonicalID instead of email
            user.identifier = item.canonicalID;
            user.userIDType = 'id';
            return user;
        });
    };

aclUtils.sortHeaderGrants =
    function sortHeaderGrants(allGrantHeaders, addACLParams) {
        allGrantHeaders.forEach((item) => {
            if (item) {
                addACLParams[item.grantType].push(item.identifier);
            }
        });
        return addACLParams;
    };

export default aclUtils;
