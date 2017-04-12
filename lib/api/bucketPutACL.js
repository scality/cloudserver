import { errors } from 'arsenal';
import async from 'async';

import acl from '../metadata/acl';
import aclUtils from '../utilities/aclUtils';
import { cleanUpBucket } from './apiUtils/bucket/bucketCreation';
import collectCorsHeaders from '../utilities/collectCorsHeaders';
import constants from '../../constants';
import { metadataValidateBucket } from '../metadata/metadataUtils';
import vault from '../auth/vault';
import { pushMetric } from '../utapi/utilities';

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
 * Bucket Put ACL - Create bucket ACL
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
export default function bucketPutACL(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketPutACL' });

    const bucketName = request.bucketName;
    const canonicalID = authInfo.getCanonicalID();
    const newCannedACL = request.headers['x-amz-acl'];
    const possibleCannedACL = [
        'private',
        'public-read',
        'public-read-write',
        'authenticated-read',
        'log-delivery-write',
    ];
    if (newCannedACL && possibleCannedACL.indexOf(newCannedACL) === -1) {
        log.trace('invalid canned acl argument', {
            acl: newCannedACL,
            method: 'bucketPutACL',
        });
        return callback(errors.InvalidArgument);
    }
    if (!aclUtils.checkGrantHeaderValidity(request.headers)) {
        log.trace('invalid acl header');
        return callback(errors.InvalidArgument);
    }
    const possibleGroups = [constants.allAuthedUsersId,
        constants.publicId,
        constants.logId,
    ];
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketPutACL',
    };
    const possibleGrants = ['FULL_CONTROL', 'WRITE',
        'WRITE_ACP', 'READ', 'READ_ACP'];
    const addACLParams = {
        Canned: '',
        FULL_CONTROL: [],
        WRITE: [],
        WRITE_ACP: [],
        READ: [],
        READ_ACP: [],
    };

    const grantReadHeader =
        aclUtils.parseGrant(request.headers[
            'x-amz-grant-read'], 'READ');
    const grantWriteHeader =
        aclUtils.parseGrant(request.headers
            ['x-amz-grant-write'], 'WRITE');
    const grantReadACPHeader =
        aclUtils.parseGrant(request.headers
            ['x-amz-grant-read-acp'], 'READ_ACP');
    const grantWriteACPHeader =
        aclUtils.parseGrant(request.headers
            ['x-amz-grant-write-acp'], 'WRITE_ACP');
    const grantFullControlHeader =
        aclUtils.parseGrant(request.headers
            ['x-amz-grant-full-control'], 'FULL_CONTROL');

    return async.waterfall([
        function waterfall1(next) {
            metadataValidateBucket(metadataValParams, log,
            (err, bucket) => {
                if (err) {
                    log.trace('request authorization failed', {
                        error: err,
                        method: 'metadataValidateBucket',
                    });
                    return next(err, bucket);
                }
                return next(null, bucket);
            });
        },
        function waterfall2(bucket, next) {
            // If not setting acl through headers, parse body
            if (newCannedACL === undefined
                    && grantReadHeader === undefined
                    && grantWriteHeader === undefined
                    && grantReadACPHeader === undefined
                    && grantWriteACPHeader === undefined
                    && grantFullControlHeader === undefined) {
                if (request.post) {
                    log.trace('parsing acls from request body');
                    return aclUtils.parseAclXml(request.post, log,
                        (err, jsonGrants) => next(err, bucket, jsonGrants));
                }
                // If no ACLs sent with request at all
                return next(errors.MalformedXML, bucket);
            }
            /**
            * If acl set in headers (including canned acl) pass bucket and
            * undefined to the next function
            */
            log.trace('using acls from request headers');
            return next(null, bucket, undefined);
        },
        function waterfall3(bucket, jsonGrants, next) {
            // If canned ACL just move on and set them
            if (newCannedACL) {
                log.trace('canned acl', { cannedAcl: newCannedACL });
                addACLParams.Canned = newCannedACL;
                return next(null, bucket, addACLParams);
            }
            let usersIdentifiedByEmail = [];
            let usersIdentifiedByGroup = [];
            let usersIdentifiedByID = [];
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
                            return next(errors.InvalidArgument, bucket);
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
                    [].concat(grantReadHeader, grantWriteHeader,
                    grantReadACPHeader, grantWriteACPHeader,
                    grantFullControlHeader);

                usersIdentifiedByEmail = allGrantHeaders.filter(item =>
                    item && item.userIDType.toLowerCase() === 'emailaddress');

                usersIdentifiedByGroup = allGrantHeaders
                    .filter(itm => itm && itm.userIDType
                    .toLowerCase() === 'uri');
                for (let i = 0; i < usersIdentifiedByGroup.length; i ++) {
                    const userGroup = usersIdentifiedByGroup[i].identifier;
                    if (possibleGroups.indexOf(userGroup) < 0) {
                        log.trace('invalid user group', { userGroup,
                            method: 'bucketPutACL' });
                        return next(errors.InvalidArgument, bucket);
                    }
                }
                /** TODO: Consider whether want to verify with Vault
                * whether canonicalID is associated with existing
                * account before adding to ACL */
                usersIdentifiedByID = allGrantHeaders
                    .filter(item => item && item.userIDType
                        .toLowerCase() === 'id');
            }

            // For now, at least make sure ID is 64-char alphanumeric
            // string before adding to ACL (check can be removed if
            // verifying with Vault for associated accounts first)
            for (let i = 0; i < usersIdentifiedByID.length; i++) {
                const id = usersIdentifiedByID[i].identifier;
                if (!aclUtils.isValidCanonicalId(id)) {
                    log.trace('invalid user id argument', {
                        id,
                        method: 'bucketPutACL',
                    });
                    return callback(errors.InvalidArgument, bucket);
                }
            }

            const justEmails = usersIdentifiedByEmail
                .map(item => item.identifier);
            // If have to lookup canonicalID's do that asynchronously
            if (justEmails.length > 0) {
                return vault.getCanonicalIds(justEmails, log,
                    (err, results) => {
                        if (err) {
                            log.trace('error looking up canonical ids', {
                                error: err, method: 'vault.getCanonicalIDs' });
                            return next(err, bucket);
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
                        return next(null, bucket, revisedAddACLParams);
                    });
            }
            const allUsers = [].concat(
                usersIdentifiedByID,
                usersIdentifiedByGroup);
            const revisedAddACLParams =
                aclUtils.sortHeaderGrants(allUsers, addACLParams);
            return next(null, bucket, revisedAddACLParams);
        },
        function waterfall4(bucket, addACLParams, next) {
            if (bucket.hasDeletedFlag() && canonicalID !== bucket.getOwner()) {
                log.trace('deleted flag on bucket');
                return next(errors.NoSuchBucket);
            }
            if (bucket.hasTransientFlag() || bucket.hasDeletedFlag()) {
                log.trace('transient or deleted flag so cleaning up bucket');
                bucket.setFullAcl(addACLParams);
                return cleanUpBucket(bucket, canonicalID, log, err =>
                    next(err, bucket));
            }
            // If no bucket flags, just add acl's to bucket metadata
            return acl.addACL(bucket, addACLParams, log, err =>
                next(err, bucket));
        },
    ], (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.trace('error processing request', { error: err,
                method: 'bucketPutACL' });
        } else {
            pushMetric('putBucketAcl', log, {
                authInfo,
                bucket: bucketName,
            });
        }
        return callback(err, corsHeaders);
    });
}
