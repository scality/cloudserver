const async = require('async');
const { errors } = require('arsenal');

const aclUtils = require('../utilities/aclUtils');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const constants = require('../../constants');
const { pushMetric } = require('../utapi/utilities');
const { decodeVersionId, getVersionIdResHeader }
    = require('./apiUtils/object/versioning');
const vault = require('../auth/vault');
const { metadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const monitoring = require('../utilities/monitoringHandler');

//  Sample XML response:
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
function objectGetACL(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'objectGetACL' });
    const bucketName = request.bucketName;
    const objectKey = request.objectKey;

    const decodedVidResult = decodeVersionId(request.query);
    if (decodedVidResult instanceof Error) {
        log.trace('invalid versionId query', {
            versionId: request.query.versionId,
            error: decodedVidResult,
        });
        return callback(decodedVidResult);
    }
    const versionId = decodedVidResult;

    const metadataValParams = {
        authInfo,
        bucketName,
        objectKey,
        versionId,
        requestType: 'objectGetACL',
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
        function validateBucketAndObj(next) {
            return metadataValidateBucketAndObj(metadataValParams, log,
                (err, bucket, objectMD) => {
                    if (err) {
                        log.trace('request authorization failed',
                        { method: 'objectGetACL', error: err });
                        return next(err);
                    }
                    if (!objectMD) {
                        const err = versionId ? errors.NoSuchVersion :
                            errors.NoSuchKey;
                        log.trace('error processing request',
                        { method: 'objectGetACL', error: err });
                        return next(err, bucket);
                    }
                    if (objectMD.isDeleteMarker) {
                        if (versionId) {
                            log.trace('requested version is delete marker',
                            { method: 'objectGetACL' });
                            return next(errors.MethodNotAllowed);
                        }
                        log.trace('most recent version is delete marker',
                        { method: 'objectGetACL' });
                        return next(errors.NoSuchKey);
                    }
                    return next(null, bucket, objectMD);
                });
        },
        function gatherACLs(bucket, objectMD, next) {
            const verCfg = bucket.getVersioningConfiguration();
            const resVersionId = getVersionIdResHeader(verCfg, objectMD);
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
                return next(null, bucket, xml, resVersionId);
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
                return next(null, bucket, xml, resVersionId);
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
                    return next(err, bucket);
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
                return next(null, bucket, xml, resVersionId);
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
        monitoring.getRequest.inc();
        resHeaders['x-amz-version-id'] = resVersionId;
        return callback(null, xml, resHeaders);
    });
}

module.exports = objectGetACL;
