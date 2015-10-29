import { parseString } from 'xml2js';
import utils from '../utils';
import services from '../services';
import async from 'async';

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
 * @param {function} callback - callback to server
 */
export default function bucketPutACL(accessKey, metastore, request, callback) {
    const bucketname = utils.getResourceNames(request).bucket;
    const bucketUID = utils.getResourceUID(request.namespace, bucketname);
    const newCannedACL = request.lowerCaseHeaders['x-amz-acl'];
    const possibleCannedACL = [
        'private',
        'public-read',
        'public-read-write',
        'authenticated-read',
        'log-delivery-write'
    ];
    if (newCannedACL && possibleCannedACL.indexOf(newCannedACL) === -1) {
        return callback('InvalidArgument');
    }
    const possibleGroups = [
        'http://acs.amazonaws.com/groups/global/AuthenticatedUsers',
        'http://acs.amazonaws.com/groups/global/AllUsers',
        'http://acs.amazonaws.com/groups/s3/LogDelivery',
    ];
    const metadataValParams = {
        accessKey,
        // need the canonicalID too
        bucketUID,
        metastore,
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
                    && grantFullControlHeader === undefined
                    && request.post) {
                        // TODO: Refactor this.
                        // There has to be a better way to do this.
                const xmlToParse = '<AccessControlPolicy xmlns='.
                            concat(request.post['<AccessControlPolicy xmlns']);
                return parseString(xmlToParse,
                    function parseXML(err, result) {
                        if (err) {
                            return next('MalformedXML');
                        }
                        if (!result.AccessControlPolicy
                                || !result.AccessControlPolicy.AccessControlList
                                || !result.AccessControlPolicy
                                .AccessControlList[0].Grant) {
                            return next('MalformedACLError');
                        }
                        const jsonGrants = result
                            .AccessControlPolicy.AccessControlList[0].Grant;
                        return next(null, bucket, jsonGrants);
                    });
            }
            // If acl set in headers pass bucket and
            // undefined to the next function
            return next(null, bucket, undefined);
        },
        function waterfall3(bucket, jsonGrants, next) {
            if (newCannedACL) {
                addACLParams.Canned = newCannedACL;
                return next(null, bucket, addACLParams);
            }
            // If grants set by xml, loop through the grants asynchronously
            // and pull canonical ID's as needed.
            // Note that cyberduck sets ACL with xml and automatically will
            // do a FULL_CONTROL grant to the bucket owner.
            // S3cmd uses xml as well but does not automaticlly do
            // a FULL_CONTROL for the bucket owner.
            if (jsonGrants) {
                async.each(jsonGrants, (grant, moveOnFromEach) => {
                    if (grant.Grantee[0].$['xsi:type']
                        === 'AmazonCustomerByEmail') {
                        services.getCanonicalID(
                            grant.Grantee[0]
                                .EmailAddress[0], (err, canonicalID) => {
                            if (err) {
                                return moveOnFromEach(err);
                            }
                            addACLParams[grant.Permission[0]].push(canonicalID);
                            return moveOnFromEach(null);
                        });
                    } else if (grant.Grantee[0]
                        .$['xsi:type'] === 'CanonicalUser') {
                        const canonicalID = grant.Grantee[0].ID[0];
                        // TODO: Consider whether want to verify with Vault
                        // whether canonicalID is associated with existing
                        // account before adding to ACL
                        addACLParams[grant.Permission[0]].push(canonicalID);
                        return moveOnFromEach(null);
                    } else if (grant.Grantee[0].$['xsi:type'] === 'Group') {
                        const uri = grant.Grantee[0].URI[0];
                        if (possibleGroups.indexOf(uri) < 0) {
                            return moveOnFromEach('InvalidArgument');
                        }
                        addACLParams[grant.Permission].push(uri);
                        return moveOnFromEach(null);
                    } else {
                        return moveOnFromEach('MalformedACLError');
                    }
                    // Callback to be called at the end of the loop
                }, (err) => {
                    if (err) {
                        return next(err);
                    }
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
                services.getManyCanonicalIDs(
                    justEmails, function rebuildGrants(err, results) {
                        if (err) {
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
            services.addACL(bucket, addACLParams, next);
        }
    ], function finalfunc(err) {
        console.log("metastore", JSON.stringify(metastore, null, '\t'));
        return callback(err, "ACL Set");
    });
}
