import async from 'async';

import acl from '../metadata/acl';
import utils from '../utils';
import services from '../services';

//	Sample XML response:
/*
<AccessControlPolicy>
  <Owner>
    <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
    <DisplayName>CustomersName@amazon.com</DisplayName>
  </Owner>
  <AccessControlList>
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
			xsi:type="CanonicalUser">
        <ID>75aa57f09aa0c8caeab4f8c24e99d10f8
        e7faeebf76c078efc7c6caea54ba06a</ID>
        <DisplayName>CustomersName@amazon.com</DisplayName>
      </Grantee>
      <Permission>FULL_CONTROL</Permission>
    </Grant>
  </AccessControlList>
</AccessControlPolicy>
 */


/**
 * objectGetACL - Return ACL for object
 * @param  {string} accessKey - user's accessKey
 * @param {object} metastore - metadata store
 * @param  {object} request - http request object
 * @param  {function} callback - callback to respond to http request
 * @returns {function} callback - responds back with error and xml result
 *  with either error code or xml response body
 */
export default function objectGetACL(accessKey, metastore, request, callback) {
    const bucketname = utils.getResourceNames(request).bucket;
    const objectKey = utils.getResourceNames(request).object;
    const bucketUID = utils.getResourceUID(request.namespace, bucketname);
    const metadataValParams = {
        accessKey,
        bucketUID,
        objectKey,
        metastore,
        requestType: 'objectGetACL',
    };
    const grantInfo = {
        grants: [],
        ownerInfo: {
            ID: undefined,
            displayName: undefined
        }
    };
    let objectACL;

    async.waterfall([
        function waterfall1(next) {
            services.metadataValidateAuthorization(metadataValParams, next);
        },
        function waterfall2(bucket, objectMD, next) {
            if (!objectMD) {
                return next('NoSuchKey');
            }
            objectACL = objectMD.acl;
            const allSpecificGrants = [].concat(
                objectACL.FULL_CONTROL,
                objectACL.WRITE_ACP,
                objectACL.READ,
                objectACL.READ_ACP
            ).filter(item => item !== undefined);
            // Set the owner info from the info stored on the bucket
            // TODO: Save the bucket owner's canonicalID as the ownerID when
            // creating a bucket
            objectMD['owner-id'] = bucket.owner;
            grantInfo.ownerInfo.ID = objectMD['owner-id'];
            // TODO: When creating a bucket save the creator's email as
            // the owner.displayName so can pull here.
            grantInfo.ownerInfo.displayName = objectMD['owner-display-name'];
            const ownerGrant = {
                ID: objectMD['owner-id'],
                displayName: objectMD['owner-display-name'],
                permission: 'FULL_CONTROL'
            };
            function handleCannedGrant(grantType) {
                const actions = {
                    'private': () => {
                        grantInfo.grants.push(ownerGrant);
                    },
                    'public-read': () => {
                        const publicGrant = {
                            URI:
                            'http://acs.amazonaws.com/groups/global/AllUsers',
                            permission: 'READ'
                        };
                        grantInfo.grants.push(ownerGrant, publicGrant);
                    },
                    'public-read-write': () => {
                        const publicReadGrant = {
                            URI:
                            'http://acs.amazonaws.com/groups/global/AllUsers',
                            permission: 'READ'
                        };
                        const publicWriteGrant = {
                            URI:
                            'http://acs.amazonaws.com/groups/global/AllUsers',
                            permission: 'WRITE'
                        };
                        grantInfo.grants.
                            push(ownerGrant, publicReadGrant, publicWriteGrant);
                    },
                    'authenticated-read': () => {
                        const authGrant = {
                            URI:
                           'http://acs.amazonaws.com/' +
                           'groups/global/AuthenticatedUsers',
                            permission: 'READ'
                        };
                        grantInfo.grants.push(ownerGrant, authGrant);
                    },
                    'bucket-owner-read': () => {
                        const bucketOwnerReadGrant = {
                            ID: bucket.owner,
                            displayName: bucket.ownerDisplayName,
                            permission: 'READ'
                        };
                        grantInfo.grants.push(ownerGrant, bucketOwnerReadGrant);
                    },
                    'bucket-owner-full-control': () => {
                        const bucketOwnerFCGrant = {
                            ID: bucket.owner,
                            displayName: bucket.ownerDisplayName,
                            permission: 'FULL_CONTROL'
                        };
                        grantInfo.grants.push(ownerGrant, bucketOwnerFCGrant);
                    }
                };
                actions[grantType]();
            }

            if (objectACL.Canned !== '') {
                handleCannedGrant(objectACL.Canned);
                // Note: need two arguments to pass on to next function
                return next(null, null);
            }
            if (allSpecificGrants.length > 0) {
                return acl.getManyDisplayNames(allSpecificGrants, next);
            }
            return next(null, null);
        },
        function waterfall3(accountIdentifiers, next) {
            if (accountIdentifiers) {
                accountIdentifiers.forEach((item) => {
                    const permission =
                        utils.getPermissionType(item.canonicalID, objectACL,
                            'object');
                    if (permission) {
                        grantInfo.grants.push({
                            ID: item.canonicalID,
                            displayName: item.displayName,
                            permission: permission
                        });
                    }
                });
                const grantsByURI = [
                    'http://acs.amazonaws.com/groups/global/AllUsers',
                    'http://acs.amazonaws.com/groups/global/AuthenticatedUsers',
                    'http://acs.amazonaws.com/groups/s3/LogDelivery'
                ];
                grantsByURI.forEach((URI) => {
                    const permission =
                        utils.getPermissionType(URI, objectACL, 'object');
                    if (permission) {
                        grantInfo.grants.push({ URI, permission });
                    }
                });
                next();
            }
            next();
        }
    ], function waterfallFinal(err) {
        if (err) {
            return callback(err, null);
        }
        // parse info about accounts and owner info to convert to xml
        const xml = utils.convertToXml(grantInfo, utils.constructGetACLsJson);
        return callback(null, xml);
    });
}
