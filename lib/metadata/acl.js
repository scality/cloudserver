const { errors } = require('arsenal');

const getReplicationInfo = require('../api/apiUtils/object/getReplicationInfo');
const aclUtils = require('../utilities/aclUtils');
const { 
    isValidCannedAcl, 
    getCannedAclTemplate, 
    isValidGroup, 
    hasAnyGrantHeaders,
    extractGrantHeaders 
} = require('../utilities/aclLookupTables');
const { pools } = require('../utilities/objectPool');
const metadata = require('../metadata/wrapper');
const vault = require('../auth/vault');
const { config } = require('../Config');

const acl = {
    addACL(bucket, addACLParams, log, cb) {
        log.trace('updating bucket acl in metadata');
        bucket.setFullAcl(addACLParams);
        metadata.updateBucket(bucket.getName(), bucket, log, cb);
    },

    /**
     * returns true if the specified ACL grant is unchanged
     * @param {string} grant name of the grant
     * @param {object} oldAcl old acl config
     * @param {object} newAcl new acl config
     * @returns {bool} is the grant the same
     */
    _aclGrantDidNotChange(grant, oldAcl, newAcl) {
        if (grant === 'Canned') {
            return oldAcl.Canned === newAcl.Canned;
        }
        /**
         * An ACL grant is in form of an array of strings
         * An ACL grant is considered unchanged when both the old and new one
         * contain the same number of elements, and all elements from one
         * grant are incuded in the other grant
         */
        return oldAcl[grant].length === newAcl[grant].length
            && oldAcl[grant].every(value => newAcl[grant].includes(value));
    },

    addObjectACL(bucket, objectKey, objectMD, addACLParams, params, log, cb) {
        log.trace('updating object acl in metadata');
        const isAclUnchanged = Object.keys(objectMD.acl).length === Object.keys(addACLParams).length
            && Object.keys(objectMD.acl).every(grant => this._aclGrantDidNotChange(grant, objectMD.acl, addACLParams));
        if (!isAclUnchanged) {
            /* eslint-disable no-param-reassign */
            objectMD.acl = addACLParams;
            objectMD.originOp = 's3:ObjectAcl:Put';

            // Use storageType to determine if replication update is needed, as it is set only for
            // "cloud" locations. This  ensures that we reset replication when CRR is used, but not
            // when multi-backend replication (i.e. Zenko) is used.
            // TODO: this should be refactored to properly update the replication info, accounting
            // for multiple rules and resetting the status only if needed CLDSRV-646
            const replicationInfo = getReplicationInfo(config, objectKey, bucket, true);
            if (replicationInfo && !replicationInfo.storageType) {
                objectMD.replicationInfo = {
                    ...objectMD.replicationInfo,
                    ...replicationInfo,
                };
            }

            return metadata.putObjectMD(bucket.getName(), objectKey, objectMD, params, log, cb);
        }
        return cb();
    },

    parseAclFromHeaders(params, cb) {
        const headers = params.headers;
        const resourceType = params.resourceType;
        const currentResourceACL = params.acl;
        const log = params.log;
        
        // Early return for canned ACL (most common case) - use pre-compiled lookup
        if (headers['x-amz-acl']) {
            const newCannedACL = headers['x-amz-acl'];
            
            if (isValidCannedAcl(newCannedACL, resourceType)) {
                // Use pre-compiled template instead of creating new object
                const template = getCannedAclTemplate(newCannedACL);
                return cb(null, template);
            }
            return cb(errors.InvalidArgument);
        }

        // Fast pre-check using pre-compiled function
        if (!hasAnyGrantHeaders(headers)) {
            return cb(null, currentResourceACL);
        }

        // Use object pool instead of creating new ACL object
        const resourceACL = pools.aclResource.acquire();

        // Use pre-compiled grant header extraction
        const grantHeaderData = extractGrantHeaders(headers);
        
        const allGrantHeaders = [];
        const usersNeedingLookup = new Set(); // Use Set for faster lookups
        
        // Process grant headers efficiently
        for (const { type, value } of grantHeaderData) {
            const grants = aclUtils.parseGrant(value, type);
            
            // Process grants in single loop instead of multiple filters
            for (const grant of grants) {
                if (grant) {
                    allGrantHeaders.push(grant);
                    if (grant.userIDType.toLowerCase() === 'emailaddress') {
                        usersNeedingLookup.add(grant.identifier);
                    }
                }
            }
        }

        if (allGrantHeaders.length === 0) {
            pools.aclResource.release(resourceACL);
            return cb(null, currentResourceACL);
        }

        // Single pass validation and categorization
        const usersByType = {
            email: [],
            group: [],
            id: []
        };

        for (const grant of allGrantHeaders) {
            const userType = grant.userIDType.toLowerCase();
            if (userType === 'emailaddress') {
                usersByType.email.push(grant);
            } else if (userType === 'uri') {
                // Use pre-compiled group validation
                if (!isValidGroup(grant.identifier)) {
                    pools.aclResource.release(resourceACL);
                    return cb(errors.InvalidArgument);
                }
                usersByType.group.push(grant);
            } else if (userType === 'id') {
                usersByType.id.push(grant);
            }
        }

        // Optimize vault lookup - only if needed
        if (usersByType.email.length > 0) {
            const emailList = Array.from(usersNeedingLookup);
            vault.getCanonicalIds(emailList, log, (err, results) => {
                if (err) {
                    pools.aclResource.release(resourceACL);
                    return cb(err);
                }
                const reconstructedUsers = aclUtils.reconstructUsersIdentifiedByEmail(results, usersByType.email);
                const allUsers = [...reconstructedUsers, ...usersByType.group, ...usersByType.id];
                const revisedACL = aclUtils.sortHeaderGrants(allUsers, resourceACL);
                // Note: Don't release resourceACL here as it's being returned
                return cb(null, revisedACL);
            });
        } else {
            // Skip vault lookup entirely if no email addresses
            const allUsers = [...usersByType.group, ...usersByType.id]; 
            const revisedACL = aclUtils.sortHeaderGrants(allUsers, resourceACL);
            // Note: Don't release resourceACL here as it's being returned
            return cb(null, revisedACL);
        }
        return undefined;
    },
};

module.exports = acl;

