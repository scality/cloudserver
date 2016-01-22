import async from 'async';
import { parseString } from 'xml2js';

import acl from '../metadata/acl';
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
 * @param  {string} accessKey - user's access key
 * @param  {object} metastore - in memory metadata store
 * @param  {object} request - http request object
 * @param  {function} log - Werelogs logger
 * @param {function} callback - callback to server
 */
export default function bucketPutACL(accessKey, metastore, request, log,
    callback) {
    log.debug('Processing the request in Bucket PUT ACL api');

    const bucketName = utils.getResourceNames(request).bucket;
    log.debug(`Bucket Name: ${bucketName}`);
    const newCannedACL = request.lowerCaseHeaders['x-amz-acl'];
    const possibleCannedACL = [
        'private',
        'public-read',
        'public-read-write',
        'authenticated-read',
        'log-delivery-write'
    ];
    if (newCannedACL && possibleCannedACL.indexOf(newCannedACL) === -1) {
        log.error(`Invalid Canned ACL argument: ${newCannedACL}`);
        return callback('InvalidArgument');
    }
    const possibleGroups = [
        'http://acs.amazonaws.com/groups/global/AuthenticatedUsers',
        'http://acs.amazonaws.com/groups/global/AllUsers',
        'http://acs.amazonaws.com/groups/s3/LogDelivery',
    ];
    const metadataValParams = {
        // TODO: Currently assumes accessKey is canonicalID.  Consider
        // sending both accessKey and canonicalID if
        // both useful in authorization.
        accessKey,
        bucketName,
        metastore,
        requestType: 'bucketPutACL',
        log,
    };
    const addACLParams = {
        'Canned': '',
        'FULL_CONTROL': [],
        'WRITE': [],
        'WRITE_ACP': [],
        'READ': [],
        'READ_ACP': [],
    };

    const grantReadHeader =
        utils.parseGrant(request.lowerCaseHeaders[
            'x-amz-grant-read'], 'READ');
    const grantWriteHeader =
        utils.parseGrant(request.lowerCaseHeaders
            ['x-amz-grant-write'], 'WRITE');
    const grantReadACPHeader =
        utils.parseGrant(request.lowerCaseHeaders
            ['x-amz-grant-read-acp'], 'READ_ACP');
    const grantWriteACPHeader =
        utils.parseGrant(request.lowerCaseHeaders
            ['x-amz-grant-write-acp'], 'WRITE_ACP');
    const grantFullControlHeader =
        utils.parseGrant(request.lowerCaseHeaders
            ['x-amz-grant-full-control'], 'FULL_CONTROL');


    function parseXml(xmlToParse, next) {
        let toBeParsed = xmlToParse;
        if (typeof xmlToParse === 'object') {
            toBeParsed = '<AccessControlPolicy xmlns='
                .concat(xmlToParse['<AccessControlPolicy xmlns']);
        }
        return parseString(toBeParsed, function parseXML(err, result) {
            if (err) {
                log.error(`Invalid XML: ${toBeParsed}`);
                return next('MalformedXML');
            }
            if (!result.AccessControlPolicy
                    || !result.AccessControlPolicy.AccessControlList
                    || !result.AccessControlPolicy
                    .AccessControlList[0].Grant) {
                log.error(`Invalid ACL: ${result}`);
                return next('MalformedACLError');
            }
            const jsonGrants = result
                .AccessControlPolicy.AccessControlList[0].Grant;
            log.debug(`ACL grants: ${JSON.stringify(jsonGrants)}`);
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
                        // TODO: Refactor this.
                        // There has to be a better way to do this.
                log.debug('Using ACLs from request body');
                let xmlToParse = '';
                if (request.post) {
                    return parseXml(request.post, (err, jsonGrants) => {
                        return next(err, bucket, jsonGrants);
                    });
                }
                request.on('data', data => xmlToParse += data.toString())
                .on('end', () => {
                    return parseXml(xmlToParse, (err, jsonGrants) => {
                        return next(err, bucket, jsonGrants);
                    });
                });
            } else {
                // If acl set in headers pass bucket and
                // undefined to the next function
                log.debug('Using ACLs from request headers');
                return next(null, bucket, undefined);
            }
        },
        function waterfall3(bucket, jsonGrants, next) {
            if (newCannedACL) {
                log.debug(`Canned ACL: ${newCannedACL}`);
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
                log.debug('Parsing ACL grants');
                async.each(jsonGrants, (grant, moveOnFromEach) => {
                    const grantee = grant.Grantee[0];
                    const granteeType = grantee.$['xsi:type'];
                    if (granteeType === 'AmazonCustomerByEmail') {
                        acl.getCanonicalID(grantee.EmailAddress[0], log,
                            (err, canonicalID) => {
                                if (err) {
                                    log.error(`Error for user with email: ` +
                                        `${grantee.EmailAddress[0]}. ` +
                                        `Returning error ${err}`);
                                    return moveOnFromEach(err);
                                }
                                addACLParams[grant.Permission[0]]
                                    .push(canonicalID);
                                log.debug(`canonicalID of ${canonicalID} ` +
                                    `granted ${grant.Permission[0]}`);
                                return moveOnFromEach(null);
                            });
                    } else if (granteeType === 'CanonicalUser') {
                        const canonicalID = grantee.ID[0];
                        // TODO: Consider whether want to verify with Vault
                        // whether canonicalID is associated with existing
                        // account before adding to ACL
                        addACLParams[grant.Permission[0]].push(canonicalID);
                        log.debug(`canonicalID of ${canonicalID} granted ` +
                            `${grant.Permission[0]}`);
                        return moveOnFromEach(null);
                    } else if (granteeType === 'Group') {
                        const uri = grantee.URI[0];
                        if (possibleGroups.indexOf(uri) < 0) {
                            log.error(`Invalid user group: ${uri}`);
                            return moveOnFromEach('InvalidArgument');
                        }
                        addACLParams[grant.Permission].push(uri);
                        log.debug(`Group ${uri}, granted ${grant.Permission}`);
                        return moveOnFromEach(null);
                    } else {
                        log.error('Invalid ACL: ${JSON.stringify(grant)}');
                        return moveOnFromEach('MalformedACLError');
                    }
                    // Callback to be called at the end of the loop
                }, (err) => {
                    if (err) {
                        return next(err);
                    }
                    log.debug('Parsed ACL grants successfully');
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
                    log.error(`Invalid user group: ` +
                        `${usersIdentifiedByGroup[i].identifier}`);
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
                            log.error(`Error looking up canonical IDS: ${err}`);
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
            log.error(`Error processing request in Bucket PUT ACL api: ${err}`);
            return callback(err);
        }
        log.debug('Processed request successfully in Bucket PUT ACL api');
        return callback(err, 'ACL Set');
    });
}
