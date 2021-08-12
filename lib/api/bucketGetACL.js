const aclUtils = require('../utilities/aclUtils');
const { metadataValidateBucket } = require('../metadata/metadataUtils');
const vault = require('../auth/vault');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { pushMetric } = require('../utapi/utilities');
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
 * bucketGetACL - Return ACL's for bucket
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to respond to http request
 *  with either error code or xml response body
 * @return {undefined}
 */
function bucketGetACL(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketGetACL' });

    const bucketName = request.bucketName;

    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketGetACL',
    };
    const grantInfo = {
        grants: [],
        ownerInfo: {
            ID: undefined,
            displayName: undefined,
        },
    };

    metadataValidateBucket(metadataValParams, log, (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.debug('error processing request',
                { method: 'bucketGetACL', error: err });
            monitoring.promMetrics(
                'GET', bucketName, err.code, 'getBucketAcl');
            return callback(err, null, corsHeaders);
        }
        const bucketACL = bucket.getAcl();
        grantInfo.ownerInfo.ID = bucket.getOwner();
        grantInfo.ownerInfo.displayName = bucket.getOwnerDisplayName();
        const ownerGrant = {
            ID: bucket.getOwner(),
            displayName: bucket.getOwnerDisplayName(),
            permission: 'FULL_CONTROL',
        };

        if (bucketACL.Canned !== '') {
            const cannedGrants = aclUtils.handleCannedGrant(
                bucketACL.Canned, ownerGrant);
            grantInfo.grants = grantInfo.grants.concat(cannedGrants);
            const xml = aclUtils.convertToXml(grantInfo);
            pushMetric('getBucketAcl', log, {
                authInfo,
                bucket: bucketName,
            });
            return callback(null, xml, corsHeaders);
        }
        /**
        * Build array of all canonicalIDs used in ACLs so duplicates
        * will be retained (e.g. if an account has both read and write
        * privileges, want to display both and not lose the duplicate
        * when receive one dictionary entry back from Vault)
        */
        const canonicalIDs = aclUtils.getCanonicalIDs(bucketACL);
        // Build array with grants by URI
        const uriGrantInfo = aclUtils.getUriGrantInfo(bucketACL);
        if (canonicalIDs.length === 0) {
            /**
            * If no acl's set by account canonicalID, just add URI
            * grants (if any) and return
            */
            grantInfo.grants = grantInfo.grants.concat(uriGrantInfo);
            const xml = aclUtils.convertToXml(grantInfo);
            pushMetric('getBucketAcl', log, {
                authInfo,
                bucket: bucketName,
            });
            return callback(null, xml, corsHeaders);
        }
        /**
        * If acl's set by account canonicalID, get emails from Vault to serve
        * as display names
        */
        return vault.getEmailAddresses(canonicalIDs, log, (err, emails) => {
            if (err) {
                log.debug('error processing request',
                    { method: 'vault.getEmailAddresses', error: err });
                monitoring.promMetrics(
                    'GET', bucketName, err.code, 'getBucketAcl');
                return callback(err, null, corsHeaders);
            }
            const individualGrants =
                aclUtils.getIndividualGrants(bucketACL, canonicalIDs, emails);
            // Add to grantInfo any individual grants and grants by uri
            grantInfo.grants = grantInfo.grants
                .concat(individualGrants).concat(uriGrantInfo);
            // parse info about accounts and owner info to convert to xml
            const xml = aclUtils.convertToXml(grantInfo);
            pushMetric('getBucketAcl', log, {
                authInfo,
                bucket: bucketName,
            });
            monitoring.promMetrics('GET', bucketName, '200', 'getBucketAcl');
            return callback(null, xml, corsHeaders);
        });
    });
}

module.exports = bucketGetACL;
