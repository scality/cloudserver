import async from 'async';
import { parseString } from 'xml2js';

import acl from '../metadata/acl';
import constants from '../../constants';
import utils from '../utils';
import services from '../services';

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
 * @param  {AuthInfo} Instance of AuthInfo class with requester's info
 * @param  {object} request - http request object
 * @param  {object} log - Werelogs logger
 * @param {function} callback - callback to server
 */
export default function bucketPutACL(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketPutACL' });

    const bucketName = request.bucketName;
    const newCannedACL = request.headers['x-amz-acl'];
    const possibleCannedACL = [
        'private',
        'public-read',
        'public-read-write',
        'authenticated-read',
        'log-delivery-write',
    ];
    if (newCannedACL && possibleCannedACL.indexOf(newCannedACL) === -1) {
        log.warn('invalid canned acl argument', { acl: newCannedACL });
        return callback('InvalidArgument');
    }
    const possibleGroups = [constants.allAuthedUsersId,
        constants.publicId,
        constants.logId,
    ];
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketPutACL',
        log,
    };
    const addACLParams = {
        Canned: '',
        FULL_CONTROL: [],
        WRITE: [],
        WRITE_ACP: [],
        READ: [],
        READ_ACP: [],
    };

    const grantReadHeader =
        utils.parseGrant(request.headers[
            'x-amz-grant-read'], 'READ');
    const grantWriteHeader =
        utils.parseGrant(request.headers
            ['x-amz-grant-write'], 'WRITE');
    const grantReadACPHeader =
        utils.parseGrant(request.headers
            ['x-amz-grant-read-acp'], 'READ_ACP');
    const grantWriteACPHeader =
        utils.parseGrant(request.headers
            ['x-amz-grant-write-acp'], 'WRITE_ACP');
    const grantFullControlHeader =
        utils.parseGrant(request.headers
            ['x-amz-grant-full-control'], 'FULL_CONTROL');


    function parseXml(xmlToParse, next) {
        let toBeParsed = xmlToParse;
        if (typeof xmlToParse === 'object') {
            toBeParsed = '<AccessControlPolicy xmlns='
                .concat(xmlToParse['<AccessControlPolicy xmlns']);
        }
        return parseString(toBeParsed, function parseXML(err, result) {
            if (err) {
                log.warn('invalid xml', { xmlObj: toBeParsed });
                return next('MalformedXML');
            }
            if (!result.AccessControlPolicy
                    || !result.AccessControlPolicy.AccessControlList
                    || !result.AccessControlPolicy
                    .AccessControlList[0].Grant) {
                log.warn('invalid acl', { acl: result });
                return next('MalformedACLError');
            }
            const jsonGrants = result
                .AccessControlPolicy.AccessControlList[0].Grant;
            log.trace('acl grants', { aclGrants: jsonGrants });
            return next(null, jsonGrants);
        });
    }

    async.waterfall([
        function waterfall1(next) {
            // TODO: need to modify to make sure user has
            // write_acp permission on the bucket or is bucket owner
            services.metadataValidateAuthorization(metadataValParams, next);
        },

        function waterfall2(bucket, extraArg, next) {
            // If not setting acl through headers, parse body
            if (newCannedACL === undefined
                    && grantReadHeader === undefined
                    && grantWriteHeader === undefined
                    && grantReadACPHeader === undefined
                    && grantWriteACPHeader === undefined
                    && grantFullControlHeader === undefined) {
                log.trace('using acls from request body');
                if (request.post) {
                    return parseXml(request.post, (err, jsonGrants) => {
                        return next(err, bucket, jsonGrants);
                    });
                }
                next('MalformedXML');
            } else {
                // If acl set in headers pass bucket and
                // undefined to the next function
                log.trace('using acls from request headers');
                return next(null, bucket, undefined);
            }
        },
        function waterfall3(bucket, jsonGrants, next) {
            if (newCannedACL) {
                log.trace('canned acl', { cannedAcl: newCannedACL });
                addACLParams.Canned = newCannedACL;
                return next(null, bucket, addACLParams);
            }
            // If grants set by xml, loop through the grants asynchronously
            // and pull canonical ID's as needed.
            // Note that cyberduck sets ACL with xml and automatically will
            // do a FULL_CONTROL grant to the bucket owner.
            // S3cmd uses xml as well but does not automatically do
            // a FULL_CONTROL for the bucket owner.
            if (jsonGrants) {
                log.trace('parsing acl grants');
                async.each(jsonGrants, (grant, moveOnFromEach) => {
                    const grantee = grant.Grantee[0];
                    const granteeType = grantee.$['xsi:type'];
                    if (granteeType === 'AmazonCustomerByEmail') {
                        acl.getCanonicalID(grantee.EmailAddress[0], log,
                            (err, canonicalID) => {
                                if (err) {
                                    log.warn('error for user with email',
                                        { userEmail: grantee.EmailAddress[0],
                                            error: err });
                                    return moveOnFromEach(err);
                                }
                                addACLParams[grant.Permission[0]]
                                    .push(canonicalID);
                                log.trace('user granted permission',
                                    { canonicalID,
                                        permission: grant.Permission[0] });
                                return moveOnFromEach(null);
                            });
                    } else if (granteeType === 'CanonicalUser') {
                        const canonicalID = grantee.ID[0];
                        // TODO: Consider whether want to verify with Vault
                        // whether canonicalID is associated with existing
                        // account before adding to ACL
                        addACLParams[grant.Permission[0]].push(canonicalID);
                        log.trace('user granted permission', { canonicalID,
                            permission: grant.Permission[0] });
                        return moveOnFromEach(null);
                    } else if (granteeType === 'Group') {
                        const uri = grantee.URI[0];
                        if (possibleGroups.indexOf(uri) < 0) {
                            log.warn('invalid user group', { userGroup: uri });
                            return moveOnFromEach('InvalidArgument');
                        }
                        addACLParams[grant.Permission].push(uri);
                        log.trace('group granted permission', { userGroup: uri,
                            permission: grant.Permission });
                        return moveOnFromEach(null);
                    } else {
                        log.warn('invalid acl grant', { grant });
                        return moveOnFromEach('MalformedACLError');
                    }
                    // Callback to be called at the end of the loop
                }, (err) => {
                    if (err) {
                        return next(err);
                    }
                    log.trace('parsed acl grants successfully');
                    return next(null, bucket, addACLParams);
                });
            }

            // If no canned ACL and no parsed xml, loop
            // through the access headers
            const allGrantHeaders =
                [].concat(grantReadHeader, grantWriteHeader, grantReadACPHeader,
                grantWriteACPHeader, grantFullControlHeader);

            const usersIdentifiedByEmail =
                allGrantHeaders.filter((item) => {
                    if (item && item.userIDType.toLowerCase()
                        === 'emailaddress') {
                        return true;
                    }
                });

            const justEmails = usersIdentifiedByEmail
                .map(item => item.identifier);
            const usersIdentifiedByGroup = allGrantHeaders
                .filter(itm => itm && itm.userIDType.toLowerCase() === 'uri');
            for (let i = 0; i < usersIdentifiedByGroup.length; i ++) {
                if (possibleGroups.indexOf(
                        usersIdentifiedByGroup[i].identifier) < 0) {
                    log.warn('invalid user group',
                         { userGroup: usersIdentifiedByGroup[i].identifier });
                    return next('InvalidArgument');
                }
            }
            const usersIdentifiedByID = allGrantHeaders
                .filter(item => item && item.userIDType.toLowerCase() === 'id');
            // TODO: Consider whether want to verify with Vault
            // whether canonicalID is associated with existing
            // account before adding to ACL


            // If have to lookup canonicalID's do that asynchronously
            if (justEmails.length > 0) {
                acl.getManyCanonicalIDs(
                    justEmails, log, function rebuildGrants(err, results) {
                        if (err) {
                            log.warn('error looking up canonical ids',
                                { error: err, method: 'getManyCanonicalIDs' });
                            return next(err);
                        }
                        const reconstructedUsersIdentifiedByEmail = utils
                            .reconstructUsersIdentifiedByEmail(results,
                                usersIdentifiedByEmail);
                        const allUsers = [].concat(
                            reconstructedUsersIdentifiedByEmail,
                            usersIdentifiedByID,
                            usersIdentifiedByGroup);
                        const revisedAddACLParams = utils
                            .sortHeaderGrants(allUsers, addACLParams);
                        return next(null, bucket, revisedAddACLParams);
                    });
            } else {
                const revisedAddACLParams =
                    utils.sortHeaderGrants(allGrantHeaders, addACLParams);
                return next(null, bucket, revisedAddACLParams);
            }
        },
        function waterfall4(bucket, addACLParams, next) {
            // Add acl's to bucket metadata
            acl.addACL(bucket, addACLParams, log, next);
        }
    ], function finalfunc(err) {
        if (err) {
            log.warn('error processing request',
                { error: err, method: 'bucketPutACL' });
            return callback(err);
        }
        return callback(err, 'ACL Set');
    });
}
