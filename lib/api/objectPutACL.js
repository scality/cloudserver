import async from 'async';
import { parseString } from 'xml2js';

import acl from '../metadata/acl';
import constants from '../../constants';
import services from '../services';
import utils from '../utils';

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
        log.warn('invalid canned acl argument', { cannedAcl: newCannedACL });
        return cb('InvalidArgument');
    }
    const possibleGroups = [
        constants.publicId,
        constants.allAuthedUsersId,
        constants.logId,
    ];

    const metadataValParams = {
        authInfo,
        // need the canonicalID too
        bucketName,
        objectKey,
        requestType: 'objectPutACL',
        log,
    };
    const addACLParams = {
        Canned: '',
        FULL_CONTROL: [],
        WRITE_ACP: [],
        READ: [],
        READ_ACP: [],
    };

    const grantReadHeader =
        utils.parseGrant(request.headers['x-amz-grant-read'], 'READ');
    const grantReadACPHeader =
        utils.parseGrant(request.headers['x-amz-grant-read-acp'],
                         'READ_ACP');
    const grantWriteACPHeader = utils.parseGrant(
        request.headers['x-amz-grant-write-acp'], 'WRITE_ACP');
    const grantFullControlHeader = utils.parseGrant(
        request.headers['x-amz-grant-full-control'], 'FULL_CONTROL');

    async.waterfall([
        function waterfall1(next) {
            services.metadataValidateAuthorization(metadataValParams, next);
        },
        function waterfall2(bucket, objectMD, next) {
            // If not setting acl through headers, parse body
            let jsonGrants;
            if (newCannedACL === undefined
                && grantReadHeader === undefined
                && grantReadACPHeader === undefined
                && grantWriteACPHeader === undefined
                && grantFullControlHeader === undefined
                && request.post) {
                log.debug('using acls from request body');
                let xmlToParse = request.post.toString();
                if (typeof xmlToParse === 'object') {
                    xmlToParse = '<AccessControlPolicy xmlns='
                        .concat(xmlToParse['<AccessControlPolicy xmlns']);
                }
                return parseString(xmlToParse, function parseXML(err, result) {
                    if (err) {
                        log.warn('invalid xml', { xml: xmlToParse });
                        return next('MalformedXML');
                    }
                    if (!result.AccessControlPolicy
                        || !result.AccessControlPolicy.AccessControlList
                        || !result.AccessControlPolicy.AccessControlList[0]
                            .Grant) {
                        log.warn('invalid acl', { acl: result });
                        return next('MalformedACLError');
                    }
                    jsonGrants = result
                    .AccessControlPolicy.AccessControlList[0].Grant;
                    log.debug('acl grants', { aclGrants: jsonGrants });
                    return next(null, bucket, objectMD, jsonGrants);
                });
            }
            // If acl set in headers pass bucket and
            // object metadata to the next function
            log.debug('using acls from request headers');
            return next(null, bucket, objectMD, jsonGrants);
        },
        function waterfall3(bucket, objectMD, jsonGrants, next) {
            if (newCannedACL) {
                log.debug('canned acl', { cannedAcl: newCannedACL });
                addACLParams.Canned = newCannedACL;
                return next(null, bucket, objectMD, addACLParams);
            }
            // If grants set by xml, loop through the grants asynchronously
            // and pull canonical ID's as needed.
            // Note that cyberduck sets ACL with xml and automatically will
            // do a FULL_CONTROL grant to the bucket owner.
            // S3cmd uses xml as well but does not automaticlly do
            // a FULL_CONTROL for the bucket owner.
            if (jsonGrants) {
                log.debug('parsing acl grants');
                return async.each(jsonGrants, (grant, moveOnFromEach) => {
                    const grantee = grant.Grantee[0];
                    const granteeType = grantee.$['xsi:type'];
                    const grantPermission = grant.Permission[0];
                    if (granteeType === 'AmazonCustomerByEmail') {
                        acl.getCanonicalID(grantee.EmailAddress[0],
                            log, (err, canonicalID) => {
                                if (err) {
                                    log.warn('error for user with email', {
                                        userEmail: grantee.EmailAddress[0],
                                        error: err,
                                    });
                                    return moveOnFromEach(err);
                                }
                                if (addACLParams[grantPermission]) {
                                    addACLParams[grantPermission]
                                        .push(canonicalID);
                                    log.debug('user granted permission', {
                                        canonicalID,
                                        grantPermission,
                                    });
                                }
                                return moveOnFromEach(null);
                            });
                    } else if (granteeType === 'CanonicalUser') {
                        const canonicalID = grantee.ID[0];
                        // TODO: Consider whether want to verify with Vault
                        // whether canonicalID is associated with existing
                        // account before adding to ACL
                        if (addACLParams[grantPermission]) {
                            addACLParams[grantPermission].push(canonicalID);
                            log.trace('user granted permission', {
                                canonicalID,
                                grantPermission,
                            });
                        }
                        return moveOnFromEach(null);
                    } else if (granteeType === 'Group') {
                        const uri = grantee.URI[0];
                        if (possibleGroups.indexOf(uri) < 0) {
                            log.warn('invalid user group', { uri });
                            return moveOnFromEach('InvalidArgument');
                        }
                        if (addACLParams[grantPermission]) {
                            addACLParams[grantPermission].push(uri);
                            log.debug('permission granted for group', {
                                userGroup: uri,
                                permission: grantPermission,
                            });
                        }
                        return moveOnFromEach(null);
                    } else {
                        log.warn('invalid acl grant', { aclGrant: grant });
                        return moveOnFromEach('MalformedACLError');
                    }
                    // Callback to be called at the end of the loop
                }, (err) => {
                    if (err) {
                        return next(err);
                    }
                    log.debug('parsed acl grants successfully');
                    return next(null, bucket, objectMD, addACLParams);
                });
            }

            // If no canned ACL and no parsed xml, loop
            // through the access headers
            const allGrantHeaders =
                [].concat(
                    grantReadHeader,
                    grantReadACPHeader,
                    grantWriteACPHeader,
                    grantFullControlHeader
                ).filter(item => item !== undefined);

            if (allGrantHeaders.length === 0) {
                return next(null, bucket, objectMD, objectMD.acl);
            }

            const usersIdentifiedByEmail = allGrantHeaders.filter(item =>
                item && item.userIDType.toLowerCase() === 'emailaddress');

            const justEmails = usersIdentifiedByEmail.map(item =>
                item.identifier);
            const usersIdentifiedByGroup = allGrantHeaders.filter(item =>
                item && item.userIDType.toLowerCase() === 'uri');
            for (let i = 0; i < usersIdentifiedByGroup.length; i++) {
                if (possibleGroups.indexOf(
                        usersIdentifiedByGroup[i].identifier) < 0) {
                    return next('InvalidArgument');
                }
            }
            const usersIdentifiedByID = allGrantHeaders.filter(item =>
                item && item.userIDType.toLowerCase() === 'id');
            // If you have to lookup canonicalID's do that asynchronously
            if (justEmails.length > 0) {
                acl.getManyCanonicalIDs(justEmails, log, (err, results) => {
                    if (err) {
                        log.warn('error looking up canonical ids', {
                            error: err,
                        });
                        return next(err);
                    }
                    const reconstructedUsersIdentifiedByEmail =
                    utils.reconstructUsersIdentifiedByEmail(results,
                        usersIdentifiedByEmail);
                    const allUsers = [].concat(
                        reconstructedUsersIdentifiedByEmail,
                        usersIdentifiedByID,
                        usersIdentifiedByGroup)
                        .filter(item => item !== undefined);
                    const revisedAddACLParams =
                    utils.sortHeaderGrants(allUsers, addACLParams);
                    return next(null, bucket, objectMD, revisedAddACLParams);
                });
            } else {
                const revisedAddACLParams =
                    utils.sortHeaderGrants(allGrantHeaders, addACLParams);
                return next(null, bucket, objectMD, revisedAddACLParams);
            }
        },
        function waterfall4(bucket, objectMD, ACLParams, next) {
            // Add acl's to object metadata
            acl.addObjectACL(bucket, objectKey, objectMD, ACLParams, log, next);
        },
    ], function finalfunc(err) {
        if (err) {
            log.warn('error processing request', {
                error: err,
                method: 'objectPutACL',
            });
            return cb(err);
        }
        log.trace('processed request successfully in object put acl api');
        return cb();
    });
}
