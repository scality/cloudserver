import { errors } from 'arsenal';

import aclUtils from '../utilities/aclUtils';
import constants from '../../constants';
import services from '../services';
import utils from '../utils';
import vault from '../auth/vault';

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
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to respond to http request
 * @return {undefined}
 */
export default function objectGetACL(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'objectGetACL' });
    const bucketName = request.bucketName;
    const objectKey = request.objectKey;
    const metadataValParams = {
        authInfo,
        bucketName,
        objectKey,
        requestType: 'objectGetACL',
        log,
    };
    const grantInfo = {
        grants: [],
        ownerInfo: {
            ID: undefined,
            displayName: undefined,
        },
    };
    const grantsByURI = [constants.publicId,
        constants.allAuthedUsersId,
        constants.logId,
    ];

    services.metadataValidateAuthorization(metadataValParams,
        (err, bucket, objectMD) => {
            if (err) {
                log.trace('request authorization failed',
                    { method: 'objectGetACL', error: err });
                return callback(err);
            }
            if (!objectMD) {
                log.trace('error processing request',
                    { method: 'objectGetACL', error: err });
                return callback(errors.NoSuchKey);
            }
            const objectACL = objectMD.acl;
            const allSpecificGrants = [].concat(
                objectACL.FULL_CONTROL,
                objectACL.WRITE_ACP,
                objectACL.READ,
                objectACL.READ_ACP
            ).filter(item => item !== undefined);
            grantInfo.ownerInfo.ID = objectMD['owner-id'];
            grantInfo.ownerInfo.displayName = objectMD['owner-display-name'];
            // Object owner always has full control
            const ownerGrant = {
                ID: objectMD['owner-id'],
                displayName: objectMD['owner-display-name'],
                permission: 'FULL_CONTROL',
            };
            if (objectACL.Canned !== '') {
                /**
                * If bucket owner and object owner are different
                * need to send info about bucket owner from bucket
                * metadata to handleCannedGrant function
                */
                let cannedGrants;
                if (bucket.getOwner() !== objectMD['owner-id']) {
                    cannedGrants = aclUtils.handleCannedGrant(
                    objectACL.Canned, ownerGrant, bucket);
                } else {
                    cannedGrants = aclUtils.handleCannedGrant(
                    objectACL.Canned, ownerGrant);
                }
                grantInfo.grants = grantInfo.grants.concat(cannedGrants);
                const xml = utils.convertToXml(grantInfo,
                    aclUtils.constructGetACLsJson);
                return callback(null, xml);
            }
            /**
            * Build array of all canonicalIDs used in ACLs so duplicates
            * will be retained (e.g. if an account has both read and write
            * privileges, want to display both and not lose the duplicate
            * when receive one dictionary entry back from Vault)
            */
            const canonicalIDs = allSpecificGrants.filter(item =>
                grantsByURI.indexOf(item) < 0);
            // Build array with grants by URI
            const uriGrantInfo = grantsByURI.map(uri => {
                const permission = aclUtils.getPermissionType(uri, objectACL,
                    'object');
                if (permission) {
                    return {
                        URI: uri,
                        permission,
                    };
                }
                return undefined;
            }).filter(item => item !== undefined);

            if (canonicalIDs.length === 0) {
                /**
                * If no acl's set by account canonicalID, just add URI
                * grants (if any) and return
                */
                grantInfo.grants = grantInfo.grants.concat(uriGrantInfo);
                const xml = utils.convertToXml(grantInfo,
                    aclUtils.constructGetACLsJson);
                return callback(null, xml);
            }
            /**
            * If acl's set by account canonicalID,
            * get emails from Vault to serve
            * as display names
            */
            return vault.getEmailAddresses(canonicalIDs, log, (err, emails) => {
                if (err) {
                    log.trace('error processing request',
                        { method: 'objectGetACL', error: err });
                    return callback(err);
                }
                const individualGrants = canonicalIDs.map(canonicalID => {
                /**
                * Emails dict only contains entries that were found
                * in Vault
                */
                    if (emails[canonicalID]) {
                        const permission = aclUtils.getPermissionType(
                            canonicalID, objectACL, 'object');
                        if (permission) {
                            const displayName = emails[canonicalID];
                            return {
                                ID: canonicalID,
                                displayName,
                                permission,
                            };
                        }
                    }
                    return undefined;
                }).filter(item => item !== undefined);
                // Add to grantInfo any individual grants and grants by uri
                grantInfo.grants = grantInfo.grants
                    .concat(individualGrants).concat(uriGrantInfo);
                // parse info about accounts and owner info to convert to xml
                const xml = utils.convertToXml(grantInfo,
                    aclUtils.constructGetACLsJson);
                return callback(null, xml);
            });
        });
}
