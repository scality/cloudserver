import { errors, versioning } from 'arsenal';
import async from 'async';

import aclUtils from '../utilities/aclUtils';
import collectCorsHeaders from '../utilities/collectCorsHeaders';
import constants from '../../constants';
import { pushMetric } from '../utapi/utilities';
import services from '../services';
import vault from '../auth/vault';

const VID = versioning.VersionID;

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
    let versionId = request.query ? request.query.versionId : undefined;
    versionId = versionId || undefined; // to smooth out versionId ''

    if (versionId && versionId !== 'null') {
        try {
            versionId = VID.decrypt(versionId);
        } catch (exception) { // eslint-disable-line
            return callback(errors.InvalidArgument
                .customizeDescription('Invalid version id specified'), null);
        }
    }

    const metadataValParams = {
        authInfo,
        bucketName,
        objectKey,
        versionId: versionId === 'null' ? undefined : versionId,
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

    return async.waterfall([
        callback => services.metadataValidateAuthorization(metadataValParams,
        (err, bucket, objectMD) => {
            if (err) {
                log.trace('request authorization failed',
                    { method: 'objectGetACL', error: err });
                return callback(err, bucket);
            }
            if (!objectMD) {
                const err = versionId ? errors.NoSuchVersion :
                    errors.NoSuchKey;
                log.trace('error processing request',
                    { method: 'objectGetACL', error: err });
                return callback(err, bucket);
            }
            if (versionId === undefined) {
                return callback(null, bucket, objectMD);
            }
            if (versionId !== 'null') {
                return callback(null, bucket, objectMD);
            }
            if (objectMD.isNull || (objectMD && !objectMD.versionId)) {
                return callback(null, bucket, objectMD);
            }
            if (!objectMD.nullVersionId) {
                return callback(errors.NoSuchVersion, bucket);
            }
            metadataValParams.versionId = objectMD.nullVersionId;
            return services.metadataValidateAuthorization(
                metadataValParams, (err, bucket, objectMD) => {
                    if (err) {
                        log.trace('request authorization failed',
                                { method: 'objectGetACL', error: err });
                        return callback(err, bucket);
                    }
                    if (!objectMD) {
                        log.trace('error processing request',
                                { method: 'objectGetACL', error: err });
                        return callback(errors.NoSuchVersion, bucket);
                    }
                    return callback(null, bucket, objectMD);
                });
        }),
        (bucket, objectMD, callback) => {
            // if versioning is enabled or suspended, return version id in
            // response headers
            let resVersionId;
            if (bucket.getVersioningConfiguration()) {
                if (objectMD.isNull || (objectMD && !objectMD.versionId)) {
                    resVersionId = 'null';
                } else {
                    resVersionId = VID.encrypt(objectMD.versionId);
                }
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
                const xml = aclUtils.convertToXml(grantInfo);
                pushMetric('getObjectAcl', log, {
                    authInfo,
                    bucket: bucketName,
                });
                return callback(null, bucket, xml, resVersionId);
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
                const xml = aclUtils.convertToXml(grantInfo);
                pushMetric('getObjectAcl', log, {
                    authInfo,
                    bucket: bucketName,
                });
                return callback(null, bucket, xml, resVersionId);
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
                    return callback(err, bucket);
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
                const xml = aclUtils.convertToXml(grantInfo);
                return callback(null, bucket, xml, resVersionId);
            });
        },
    ], (err, bucket, xml, resVersionId) => {
        const resHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            return callback(err, null, resHeaders);
        }
        pushMetric('getObjectAcl', log, {
            authInfo,
            bucket: bucketName,
        });
        resHeaders['x-amz-version-id'] = resVersionId;
        return callback(null, xml, resHeaders);
    });
}
