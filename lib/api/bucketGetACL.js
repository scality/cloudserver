import async from 'async';

import constants from '../../constants';
import acl from '../metadata/acl';
import services from '../services';
import utils from '../utils';

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
 * bucketGetACL - Return ACL's for bucket
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to respond to http request
 *  with either error code or xml response body
 * @return {undefined}
 */
export default function bucketGetACL(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketGetACL' });

    const bucketName = request.bucketName;

    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketGetACL',
        log,
    };
    const grantInfo = {
        grants: [],
        ownerInfo: {
            ID: undefined,
            displayName: undefined,
        },
    };
    let bucketACL;

    async.waterfall([
        function waterfall1(next) {
            services.metadataValidateAuthorization(metadataValParams, next);
        },
        function waterfall2(bucket, extraArgumentFromPreviousFunction, next) {
            bucketACL = bucket.acl;
            const allSpecificGrants = [].concat(
                bucketACL.FULL_CONTROL,
                bucketACL.WRITE,
                bucketACL.WRITE_ACP,
                bucketACL.READ,
                bucketACL.READ_ACP
            );
            grantInfo.ownerInfo.ID = bucket.owner;
            grantInfo.ownerInfo.displayName = bucket.ownerDisplayName;
            const ownerGrant = {
                ID: bucket.owner,
                displayName: bucket.ownerDisplayName,
                permission: 'FULL_CONTROL',
            };
            function handleCannedGrant(grantType) {
                const actions = {
                    'private': () => {
                        grantInfo.grants.push(ownerGrant);
                    },
                    'public-read': () => {
                        const publicGrant = {
                            URI: constants.publicId,
                            permission: 'READ',
                        };
                        grantInfo.grants.push(ownerGrant, publicGrant);
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
                        grantInfo.grants.
                            push(ownerGrant, publicReadGrant, publicWriteGrant);
                    },
                    'authenticated-read': () => {
                        const authGrant = {
                            URI: constants.allAuthedUsersId,
                            permission: 'READ',
                        };
                        grantInfo.grants.push(ownerGrant, authGrant);
                    },
                    'log-delivery-write': () => {
                        const logWriteGrant = {
                            URI: constants.logId,
                            permission: 'WRITE',
                        };
                        const logReadACPGrant = {
                            URI: constants.logId,
                            permission: 'READ_ACP',
                        };
                        grantInfo.grants.push(ownerGrant, logWriteGrant,
                                              logReadACPGrant);
                    },
                };
                actions[grantType]();
            }

            if (bucketACL.Canned !== '') {
                handleCannedGrant(bucketACL.Canned);
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
                accountIdentifiers.forEach(item => {
                    const permission =
                        utils.getPermissionType(item.canonicalID, bucketACL,
                            'bucket');
                    if (permission) {
                        grantInfo.grants.push({
                            ID: item.canonicalID,
                            displayName: item.displayName,
                            permission,
                        });
                    }
                });
                const grantsByURI = [constants.publicId,
                    constants.allAuthedUsersId,
                    constants.logId,
                ];
                grantsByURI.forEach((uri) => {
                    const permission =
                        utils.getPermissionType(uri, bucketACL, 'bucket');
                    if (permission) {
                        grantInfo.grants.push({
                            URI: uri,
                            permission,
                        });
                    }
                });
                return next();
            }
            return next();
        },
    ], function waterfallFinal(err) {
        if (err) {
            log.warn('error processing request',
                { method: 'bucketGetACL', error: err });
            return callback(err, null);
        }
        // parse info about accounts and owner info to convert to xml
        const xml = utils.convertToXml(grantInfo, utils.constructGetACLsJson);
        return callback(null, xml);
    });
}
