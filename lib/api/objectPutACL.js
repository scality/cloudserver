import { errors } from 'arsenal';
import async from 'async';

import acl from '../metadata/acl';
import aclUtils from '../utilities/aclUtils';
import constants from '../../constants';
import services from '../services';
import vault from '../auth/vault';

/*
   Format of xml request:

   <AccessControlPolicy>
     <Owner>
       <ID>ID</ID>
       <DisplayName>EmailAddress</DisplayName>
     </Owner>
     <AccessControlList>
       <Grant>
         <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:type="CanonicalUser">
           <ID>ID</ID>
           <DisplayName>EmailAddress</DisplayName>
         </Grantee>
         <Permission>Permission</Permission>
       </Grant>
       ...
     </AccessControlList>
   </AccessControlPolicy>
   */

/**
 * Object Put ACL - Create object ACL
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} cb - cb to server
 * @return {undefined}
 */
export default function objectPutACL(authInfo, request, log, cb) {
    log.debug('processing request', { method: 'objectPutACL' });
    const bucketName = request.bucketName;
    const objectKey = request.objectKey;
    const newCannedACL = request.headers['x-amz-acl'];
    const possibleCannedACL =
        ['private', 'public-read', 'public-read-write',
        'authenticated-read', 'bucket-owner-read', 'bucket-owner-full-control'];
    if (newCannedACL && possibleCannedACL.indexOf(newCannedACL) === -1) {
        log.trace('invalid canned acl argument', { cannedAcl: newCannedACL });
        return cb(errors.InvalidArgument);
    }
    if (!aclUtils.checkGrantHeaderValidity(request.headers)) {
        log.trace('invalid acl header');
        return cb(errors.InvalidArgument);
    }
    const possibleGroups = [
        constants.publicId,
        constants.allAuthedUsersId,
        constants.logId,
    ];

    const metadataValParams = {
        authInfo,
        bucketName,
        objectKey,
        requestType: 'objectPutACL',
        log,
    };
    const possibleGrants = ['FULL_CONTROL', 'WRITE_ACP', 'READ', 'READ_ACP'];
    const addACLParams = {
        Canned: '',
        FULL_CONTROL: [],
        WRITE_ACP: [],
        READ: [],
        READ_ACP: [],
    };

    const grantReadHeader =
        aclUtils.parseGrant(request.headers['x-amz-grant-read'], 'READ');
    const grantReadACPHeader =
        aclUtils.parseGrant(request.headers['x-amz-grant-read-acp'],
                         'READ_ACP');
    const grantWriteACPHeader = aclUtils.parseGrant(
        request.headers['x-amz-grant-write-acp'], 'WRITE_ACP');
    const grantFullControlHeader = aclUtils.parseGrant(
        request.headers['x-amz-grant-full-control'], 'FULL_CONTROL');

    return async.waterfall([
        next => services.metadataValidateAuthorization(metadataValParams, next),
        (bucket, objectMD, next) => {
            if (!objectMD) {
                return next(errors.NoSuchKey);
            }
            // If not setting acl through headers, parse body
            let jsonGrants;
            let aclOwnerID;
            if (newCannedACL === undefined
                && grantReadHeader === undefined
                && grantReadACPHeader === undefined
                && grantWriteACPHeader === undefined
                && grantFullControlHeader === undefined) {
                if (request.post) {
                    log.trace('using acls from request body');
                    return aclUtils.parseAclXml(request.post, log,
                        (err, jsonGrants, aclOwnerID) => next(err, bucket,
                            objectMD, jsonGrants, aclOwnerID));
                }
                // If no ACLs sent with request at all
                return next(errors.MalformedXML);
            }
            /**
            * If acl set in headers (including canned acl) pass bucket and
            * undefined to the next function
            */
            log.debug('using acls from request headers');
            return next(null, bucket, objectMD, jsonGrants, aclOwnerID);
        },
        (bucket, objectMD, jsonGrants, aclOwnerID, next) => {
            if (newCannedACL) {
                log.debug('canned acl', { cannedAcl: newCannedACL });
                addACLParams.Canned = newCannedACL;
                return next(null, bucket, objectMD, addACLParams);
            }
            let usersIdentifiedByEmail = [];
            let usersIdentifiedByGroup = [];
            let usersIdentifiedByID = [];

            // If grants set by xml and xml owner ID is incorrect
            if (aclOwnerID && (aclOwnerID !== objectMD['owner-id'])) {
                log.trace('incorrect owner ID provided in ACL', {
                    ACL: request.post,
                    method: 'objectPutACL',
                });
                return next(errors.AccessDenied);
            }

            /**
            * If grants set by xml, loop through the grants
            * and separate grant types so parsed in same manner
            * as header grants
            */
            if (jsonGrants) {
                log.trace('parsing acl grants');
                jsonGrants.forEach(grant => {
                    const grantee = grant.Grantee[0];
                    const granteeType = grantee.$['xsi:type'];
                    const permission = grant.Permission[0];
                    let skip = false;
                    if (possibleGrants.indexOf(permission) < 0) {
                        skip = true;
                    }
                    if (!skip && granteeType === 'AmazonCustomerByEmail') {
                        usersIdentifiedByEmail.push({
                            identifier: grantee.EmailAddress[0],
                            grantType: permission,
                            userIDType: 'emailaddress',
                        });
                    }
                    if (!skip && granteeType === 'CanonicalUser') {
                        usersIdentifiedByID.push({
                            identifier: grantee.ID[0],
                            grantType: permission,
                            userIDType: 'id',
                        });
                    }
                    if (!skip && granteeType === 'Group') {
                        if (possibleGroups.indexOf(grantee.URI[0]) < 0) {
                            log.trace('invalid user group',
                                 { userGroup: grantee.URI[0] });
                            return next(errors.InvalidArgument);
                        }
                        return usersIdentifiedByGroup.push({
                            identifier: grantee.URI[0],
                            grantType: permission,
                            userIDType: 'uri',
                        });
                    }
                    return undefined;
                });
            } else {
                // If no canned ACL and no parsed xml, loop
                // through the access headers
                const allGrantHeaders =
                    [].concat(grantReadHeader,
                    grantReadACPHeader, grantWriteACPHeader,
                    grantFullControlHeader);

                usersIdentifiedByEmail = allGrantHeaders.filter(item =>
                    item && item.userIDType.toLowerCase() === 'emailaddress');
                usersIdentifiedByGroup = allGrantHeaders
                    .filter(itm => itm && itm.userIDType
                    .toLowerCase() === 'uri');
                for (let i = 0; i < usersIdentifiedByGroup.length; i ++) {
                    if (possibleGroups.indexOf(
                            usersIdentifiedByGroup[i].identifier) < 0) {
                        log.trace('invalid user group',
                             { userGroup: usersIdentifiedByGroup[i]
                                 .identifier });
                        return next(errors.InvalidArgument);
                    }
                }
                /** TODO: Consider whether want to verify with Vault
                * whether canonicalID is associated with existing
                * account before adding to ACL */
                usersIdentifiedByID = allGrantHeaders
                    .filter(item => item && item.userIDType
                        .toLowerCase() === 'id');
            }
            const justEmails = usersIdentifiedByEmail
                .map(item => item.identifier);
            // If have to lookup canonicalID's do that asynchronously
            if (justEmails.length > 0) {
                return vault.getCanonicalIds(
                    justEmails, log, (err, results) => {
                        if (err) {
                            log.trace('error looking up canonical ids',
                                { error: err, method: 'getCanonicalIDs' });
                            return next(err);
                        }
                        const reconstructedUsersIdentifiedByEmail = aclUtils
                            .reconstructUsersIdentifiedByEmail(results,
                                usersIdentifiedByEmail);
                        const allUsers = [].concat(
                            reconstructedUsersIdentifiedByEmail,
                            usersIdentifiedByID,
                            usersIdentifiedByGroup);
                        const revisedAddACLParams = aclUtils
                            .sortHeaderGrants(allUsers, addACLParams);
                        return next(null, bucket, objectMD,
                            revisedAddACLParams);
                    });
            }
            const allUsers = [].concat(
                usersIdentifiedByID,
                usersIdentifiedByGroup);
            const revisedAddACLParams =
                aclUtils.sortHeaderGrants(allUsers, addACLParams);
            return next(null, bucket, objectMD, revisedAddACLParams);
        },
        function waterfall4(bucket, objectMD, ACLParams, next) {
            // Add acl's to object metadata
            acl.addObjectACL(bucket, objectKey, objectMD, ACLParams, log, next);
        },
    ], err => {
        if (err) {
            log.trace('error processing request', {
                error: err,
                method: 'objectPutACL',
            });
            return cb(err);
        }
        log.trace('processed request successfully in object put acl api');
        return cb();
    });
}
