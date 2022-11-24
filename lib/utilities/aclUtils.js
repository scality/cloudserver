const { parseString } = require('xml2js');
const { errors, s3middleware } = require('arsenal');

const constants = require('../../constants');
const escapeForXml = s3middleware.escapeForXml;

const possibleGrantHeaders = ['x-amz-grant-read', 'x-amz-grant-write',
    'x-amz-grant-read-acp', 'x-amz-grant-write-acp',
    'x-amz-grant-full-control'];

const regexpEmailAddress = /^\S+@\S+.\S+$/;

const aclUtils = {};
const grantsByURI = [
    constants.publicId,
    constants.allAuthedUsersId,
    constants.logId,
];

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
                        ID: separateBucketOwner.getOwner(),
                        displayName: separateBucketOwner.getOwnerDisplayName(),
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
                        ID: separateBucketOwner.getOwner(),
                        displayName: separateBucketOwner.getOwnerDisplayName(),
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
    return parseString(toBeParsed, (err, result) => {
        if (err) {
            log.debug('invalid xml', { xmlObj: toBeParsed });
            return next(errors.MalformedXML);
        }
        if (!result.AccessControlPolicy
            || !result.AccessControlPolicy.AccessControlList
            || result.AccessControlPolicy.AccessControlList.length !== 1
            || (result.AccessControlPolicy.AccessControlList[0] !== '' &&
                Object.keys(result.AccessControlPolicy.AccessControlList[0])
                    .some(listKey => listKey !== 'Grant'))) {
            log.debug('invalid acl', { acl: result });
            return next(errors.MalformedACLError);
        }
        const jsonGrants = result
            .AccessControlPolicy.AccessControlList[0].Grant;
        log.trace('acl grants', { aclGrants: jsonGrants });

        if (!Array.isArray(result.AccessControlPolicy.Owner)
            || result.AccessControlPolicy.Owner.length !== 1
            || !Array.isArray(result.AccessControlPolicy.Owner[0].ID)
            || result.AccessControlPolicy.Owner[0].ID.length !== 1
            || result.AccessControlPolicy.Owner[0].ID[0] === '') {
            return next(errors.MalformedACLError);
        }
        const ownerID = result.AccessControlPolicy.Owner[0].ID[0];

        return next(null, jsonGrants, ownerID);
    });
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
    return grantArray.map(item => {
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

aclUtils.isValidCanonicalId = function isValidCanonicalId(canonicalID) {
    return /^(?=.*?[a-f])(?=.*?[0-9])[a-f0-9]{64}$/.test(canonicalID);
};

aclUtils.reconstructUsersIdentifiedByEmail =
    function reconstruct(userInfofromVault, userGrantInfo) {
        return userGrantInfo.map(item => {
            const userEmail = item.identifier.toLowerCase();
            const user = {};
            // Find the full user grant info based on email
            const userId = userInfofromVault
                .find(elem => elem.email.toLowerCase() === userEmail);
            // Set the identifier to be the canonicalID instead of email
            user.identifier = userId.canonicalID;
            user.userIDType = 'id';
            // copy over ACL grant type: i.e. READ/WRITE...
            user.grantType = item.grantType;
            return user;
        });
    };

aclUtils.sortHeaderGrants =
    function sortHeaderGrants(allGrantHeaders, addACLParams) {
        allGrantHeaders.forEach(item => {
            if (item) {
                addACLParams[item.grantType].push(item.identifier);
            }
        });
        return addACLParams;
    };

/**
 * convertToXml - Converts the `grantInfo` object (defined in `objectGetACL()`)
 * to an XML DOM string
 * @param {object} grantInfo - The `grantInfo` object defined in
 * `objectGetACL()`
 * @return {string} xml.join('') - The XML DOM string
 */
aclUtils.convertToXml = grantInfo => {
    const { grants, ownerInfo } = grantInfo;
    const xml = [];

    xml.push('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
        '<AccessControlPolicy>',
        '<Owner>',
        `<ID>${ownerInfo.ID}</ID>`,
        `<DisplayName>${escapeForXml(ownerInfo.displayName)}` +
        '</DisplayName>',
        '</Owner>',
        '<AccessControlList>'
    );

    grants.forEach(grant => {
        xml.push('<Grant>');

        // The `<Grantee>` tag has different attributes depending on whether the
        // grant has an ID or URI
        if (grant.ID) {
            xml.push('<Grantee xmlns:xsi="http://www.w3.org/2001/' +
                'XMLSchema-instance" xsi:type="CanonicalUser">',
                `<ID>${grant.ID}</ID>`
            );
        } else if (grant.URI) {
            xml.push('<Grantee xmlns:xsi="http://www.w3.org/2001/' +
                'XMLSchema-instance" xsi:type="Group">',
                `<URI>${escapeForXml(grant.URI)}</URI>`
            );
        }

        if (grant.displayName) {
            xml.push(`<DisplayName>${escapeForXml(grant.displayName)}` +
                '</DisplayName>'
            );
        }

        xml.push('</Grantee>',
            `<Permission>${grant.permission}</Permission>`,
            '</Grant>'
        );
    });

    xml.push('</AccessControlList>',
        '</AccessControlPolicy>'
    );

    return xml.join('');
};

/**
 * checkGrantHeaderValidity - checks whether acl grant header is valid
 * format is x-amz-grant-write:
 *      uri="http://acs.amazonaws.com/groups/s3/LogDelivery",
 *      emailAddress="xyz@amazon.com",
 *      emailAddress="abc@amazon.com"
 *      id=canonicalID
 * @param  {object} headers - request headers
 * @returns {boolean} true if valid, false if not
 */
aclUtils.checkGrantHeaderValidity = function checkGrantHeaderValidity(headers) {
    for (let i = 0; i < possibleGrantHeaders.length; i++) {
        const grantHeader = headers[possibleGrantHeaders[i]];
        if (grantHeader) {
            const grantHeaderArr = grantHeader.split(',');
            for (let j = 0; j < grantHeaderArr.length; j++) {
                const singleGrantArr = grantHeaderArr[j].split('=');
                if (singleGrantArr.length !== 2) {
                    return false;
                }
                const identifier = singleGrantArr[0].trim().toLowerCase();
                const value = singleGrantArr[1].trim();
                if (identifier === 'uri') {
                    if (value !== constants.publicId &&
                        value !== constants.allAuthedUsersId &&
                        value !== constants.logId) {
                        return false;
                    }
                } else if (identifier === 'emailaddress') {
                    if (!regexpEmailAddress.test(value)) {
                        return false;
                    }
                } else if (identifier === 'id') {
                    if (!aclUtils.isValidCanonicalId(value)) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        }
    }
    return true;
};

/**
 * getGrants - Get all acl grants as an object
 * @param {object} acl - acl from metadata
 * @returns {object} grants
 */
function getGrants(acl) {
    return {
        FULL_CONTROL: acl.FULL_CONTROL,
        WRITE: acl.WRITE,
        WRITE_ACP: acl.WRITE_ACP,
        READ: acl.READ,
        READ_ACP: acl.READ_ACP,
    };
}

/**
 * getCanonicalIDs - Get all the unique canonical IDs from object or bucket acl
 * @param {object} acl - acl from metadata
 * @returns {array} canonicalIDs - array of unique canonicalIDs from acl
 */
aclUtils.getCanonicalIDs = function getCanonicalIDs(acl) {
    const aclGrantees = [].concat(
        acl.FULL_CONTROL,
        acl.WRITE,
        acl.WRITE_ACP,
        acl.READ,
        acl.READ_ACP
    );
    const uniqueGrantees = Array.from(new Set(aclGrantees));
    // grantees can be a mix of canonicalIDs and predefined groups in the form
    // of uri, so filter out only canonicalIDs
    return uniqueGrantees.filter(item => item && !grantsByURI.includes(item));
};

/**
 * getUriGrantInfo - Get all the grants from object or bucket acl by uri
 * @param {object} acl - acl from metadata
 * @returns {array} uriGrantInfo - array of grants by uri
 */
aclUtils.getUriGrantInfo = function getUriGrantInfo(acl) {
    const grants = getGrants(acl);
    const uriGrantInfo = [];
    const validGrants = Object.entries(grants)
        .filter(([permission, grantees]) => permission
            && Array.isArray(grantees));
    validGrants.forEach(([permission, grantees]) => {
        grantees.filter(grantee => grantsByURI.includes(grantee))
            .forEach(grantee => {
                uriGrantInfo.push({
                    URI: grantee,
                    permission,
                });
            });
    });
    return uriGrantInfo;
};

/**
 * getIndividualGrants - Get all the grants from object or bucket acl mapped to
 * canonicalID/email
 * @param {object} acl - acl from metadata
 * @param {array} canonicalIDs - list of canonicalIDs from acl
 * @param {array} emails - canonicalID/email dictionary
 * @returns {array} individualGrantInfo - array of grants mapped to
 * canonicalID/email
 */
aclUtils.getIndividualGrants = function getIndividualGrants(acl, canonicalIDs,
    emails) {
    const grants = getGrants(acl);
    const individualGrantInfo = [];
    const validGrants = Object.entries(grants)
        .filter(([permission, grantees]) => permission
            && Array.isArray(grantees));
    validGrants.forEach(([permission, grantees]) => {
        grantees.filter(grantee => canonicalIDs.includes(grantee)
            && emails[grantee])
            .forEach(grantee => {
                individualGrantInfo.push({
                    ID: grantee,
                    displayName: emails[grantee],
                    permission,
                });
            });
    });
    return individualGrantInfo;
};

aclUtils.getHeaders = function getHeaders() {
    return possibleGrantHeaders;
};

module.exports = aclUtils;
