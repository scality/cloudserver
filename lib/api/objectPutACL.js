import {parseString} from 'xml2js';
import utils from '../utils.js';
import services from '../services.js';
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
 * Object Put ACL - Create object ACL
 * @param  {string} accessKey - user's access key
 * @param  {object} metastore - in memory metadata store
 * @param  {object} request - http request object
 * @param {function} callback - callback to server
 * @returns {function} callback - returns function with error and result
 */
export default function objectPutACL(accessKey, metastore, request, callback) {
    const bucketname = utils.getResourceNames(request).bucket;
    const objectKey = utils.getResourceNames(request).object;
    const bucketUID = utils.getResourceUID(request.namespace, bucketname);
    const newCannedACL = request.lowerCaseHeaders['x-amz-acl'];
    const possibleCannedACL =
        ['private', 'public-read', 'public-read-write',
        'authenticated-read', 'bucket-owner-read', 'bucket-owner-full-control'];
    if (newCannedACL && possibleCannedACL.indexOf(newCannedACL) === -1) {
        return callback('InvalidArgument');
    }
    const possibleGroups = [
        'http://acs.amazonaws.com/groups/global/AuthenticatedUsers',
        'http://acs.amazonaws.com/groups/global/AllUsers',
        'http://acs.amazonaws.com/groups/s3/LogDelivery'
    ];

    const metadataValParams = {
        accessKey,
        // need the canonicalID too
        bucketUID,
        objectKey,
        metastore,
        requestType: 'objectPutACL',
    };
    const addACLParams = {
        'Canned': '',
        'FULL_CONTROL': [],
        'WRITE_ACP': [],
        'READ': [],
        'READ_ACP': [],
    };

    const grantReadHeader =
        utils.parseGrant(request.lowerCaseHeaders[
            'x-amz-grant-read'], 'READ');
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
                const xmlToParse = '<AccessControlPolicy xmlns='.
                    concat(request.post['<AccessControlPolicy xmlns']);
                return parseString(xmlToParse,
                    function parseXML(err, result) {
                        if (err) {
                            return next('MalformedXML');
                        }
                        if (!result.AccessControlPolicy ||
                                !result.AccessControlPolicy.AccessControlList ||
                                !result.AccessControlPolicy.
                                    AccessControlList[0].Grant) {
                            return next('MalformedACLError');
                        }
                        jsonGrants =
                        result.AccessControlPolicy.AccessControlList[0].Grant;
                        return next(null, bucket, objectMD, jsonGrants);
                    }
                );
            }
            // If acl set in headers pass bucket and
            // object metadata to the next function
            return next(null, bucket, objectMD, jsonGrants);
        },
        function waterfall3(bucket, objectMD, jsonGrants, next) {
            if (newCannedACL) {
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
                return async.each(jsonGrants, (grant, moveOnFromEach) => {
                    const granteeType = grant.Grantee[0].$['xsi:type'];
                    const grantPermission = grant.Permission[0];
                    if (granteeType === 'AmazonCustomerByEmail') {
                        services.getCanonicalID(
                            grant.Grantee[0]
                                .EmailAddress[0], (err, canonicalID) => {
                            if (err) {
                                return moveOnFromEach(err);
                            }
                            if (addACLParams[grantPermission]) {
                                addACLParams[grantPermission].push(canonicalID);
                            }
                            return moveOnFromEach(null);
                        });
                    } else if (granteeType === 'CanonicalUser') {
                        const canonicalID = grant.Grantee[0].ID[0];
                        // TODO: Consider whether want to verify with Vault
                        // whether canonicalID is associated with existing
                        // account before adding to ACL
                        if (addACLParams[grantPermission]) {
                            addACLParams[grantPermission].push(canonicalID);
                        }
                        return moveOnFromEach(null);
                    } else if (granteeType === 'Group') {
                        const uri = grant.Grantee[0].URI[0];
                        if (possibleGroups.indexOf(uri) < 0) {
                            return moveOnFromEach('InvalidArgument');
                        }
                        if (addACLParams[grantPermission]) {
                            addACLParams[grantPermission].push(uri);
                        }
                        return moveOnFromEach(null);
                    } else {
                        return moveOnFromEach('MalformedACLError');
                    }
                    // Callback to be called at the end of the loop
                }, (err) => {
                    if (err) {
                        return next(err);
                    }
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

            const usersIdentifiedByEmail =
                allGrantHeaders.filter((item) => {
                    if (item && item.userIDType.toLowerCase()
                        === 'emailaddress') {
                        return true;
                    }
                });

            const justEmails = usersIdentifiedByEmail.map((item) => {
                return item.identifier;
            });
            const usersIdentifiedByGroup = allGrantHeaders.filter((item) => {
                if (item && item.userIDType.toLowerCase() === 'uri') {
                    return true;
                }
            });
            for (let i = 0; i < usersIdentifiedByGroup.length; i ++) {
                if (possibleGroups.indexOf(
                        usersIdentifiedByGroup[i].identifier) < 0) {
                    return next('InvalidArgument');
                }
            }
            const usersIdentifiedByID = allGrantHeaders.filter((item) => {
                if (item && item.userIDType.toLowerCase() === 'id') {
                    return true;
                }
            });
            // If you have to lookup canonicalID's do that asynchronously
            if (justEmails.length > 0) {
                services.getManyCanonicalIDs(
                    justEmails, (err, results) => {
                        if (err) {
                            return next(err);
                        }
                        const reconstructedUsersIdentifiedByEmail =
                        utils.reconstructUsersIdentifiedByEmail(results,
                            usersIdentifiedByEmail);
                        const allUsers =
                        [].concat(
                            reconstructedUsersIdentifiedByEmail,
                            usersIdentifiedByID,
                            usersIdentifiedByGroup).
                                filter(item => item !== undefined);
                        const revisedAddACLParams =
                        utils.sortHeaderGrants(allUsers, addACLParams);
                        return next(null, bucket, objectMD,
                            revisedAddACLParams);
                    });
            } else {
                const revisedAddACLParams =
                    utils.sortHeaderGrants(allGrantHeaders, addACLParams);
                return next(null, bucket, objectMD, revisedAddACLParams);
            }
        },
        function waterfall4(bucket, objectMD, revisedAddACLParams, next) {
            // Add acl's to object metadata
            services.addObjectACL(bucket, objectKey, objectMD,
                revisedAddACLParams, next);
        }
    ], function finalfunc(err) {
        return callback(err);
    });
}
