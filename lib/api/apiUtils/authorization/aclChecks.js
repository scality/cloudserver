const constants = require('../../../../constants');

function isBucketAuthorized(bucket, requestType, canonicalID) {
    // Check to see if user is authorized to perform a
    // particular action on bucket based on ACLs.
    // TODO: Add IAM checks and bucket policy checks.
    if (bucket.getOwner() === canonicalID) {
        return true;
    }
    if (requestType === 'bucketGetReplication') {
        return canonicalID === 'http://acs.zenko.io/accounts/service/crr';
    }
    if (requestType === 'bucketOwnerAction') {
        // only bucket owner can modify or retrieve this property of a bucket
        return false;
    }
    const bucketAcl = bucket.getAcl();
    if (requestType === 'bucketGet' || requestType === 'bucketHead') {
        if (bucketAcl.Canned === 'public-read'
            || bucketAcl.Canned === 'public-read-write'
            || (bucketAcl.Canned === 'authenticated-read'
                && canonicalID !== constants.publicId)) {
            return true;
        } else if (bucketAcl.FULL_CONTROL.indexOf(canonicalID) > -1
            || bucketAcl.READ.indexOf(canonicalID) > -1) {
            return true;
        }
    }

    if (requestType === 'bucketGetACL') {
        if ((bucketAcl.Canned === 'log-delivery-write'
                && canonicalID === constants.logId)
            || bucketAcl.FULL_CONTROL.indexOf(canonicalID) > -1
            || bucketAcl.READ_ACP.indexOf(canonicalID) > -1) {
            return true;
        }
    }

    if (requestType === 'bucketPutACL') {
        if (bucketAcl.FULL_CONTROL.indexOf(canonicalID) > -1
            || bucketAcl.WRITE_ACP.indexOf(canonicalID) > -1) {
            return true;
        }
    }

    if (requestType === 'bucketDelete' && bucket.getOwner() === canonicalID) {
        return true;
    }

    if (requestType === 'objectDelete' || requestType === 'objectPut') {
        if (bucketAcl.Canned === 'public-read-write'
            || bucketAcl.FULL_CONTROL.indexOf(canonicalID) > -1
            || bucketAcl.WRITE.indexOf(canonicalID) > -1) {
            return true;
        }
    }
    // Note that an account can have the ability to do objectPutACL,
    // objectGetACL, objectHead or objectGet even if the account has no rights
    // to the bucket holding the object.  So, if the request type is
    // objectPutACL, objectGetACL, objectHead or objectGet, the bucket
    // authorization check should just return true so can move on to check
    // rights at the object level.

    return (requestType === 'objectPutACL' || requestType === 'objectGetACL' ||
    requestType === 'objectGet' || requestType === 'objectHead');
}

function isObjAuthorized(bucket, objectMD, requestType, canonicalID) {
    const bucketOwner = bucket.getOwner();
    if (!objectMD) {
        return false;
    }
    if (objectMD['owner-id'] === canonicalID) {
        return true;
    }
    if (canonicalID === 'http://acs.zenko.io/accounts/service/crr' &&
        requestType === 'objectGet') {
        return true;
    }
    // account is authorized if:
    // - requesttype is "bucketOwnerAction" (example: for objectTagging) and
    // - account is the bucket owner
    if (requestType === 'bucketOwnerAction' && bucketOwner === canonicalID) {
        return true;
    }
    if (requestType === 'objectGet' || requestType === 'objectHead') {
        if (objectMD.acl.Canned === 'public-read'
            || objectMD.acl.Canned === 'public-read-write'
            || (objectMD.acl.Canned === 'authenticated-read'
                && canonicalID !== constants.publicId)) {
            return true;
        } else if (objectMD.acl.Canned === 'bucket-owner-read'
                && bucketOwner === canonicalID) {
            return true;
        } else if ((objectMD.acl.Canned === 'bucket-owner-full-control'
                && bucketOwner === canonicalID)
            || objectMD.acl.FULL_CONTROL.indexOf(canonicalID) > -1
            || objectMD.acl.READ.indexOf(canonicalID) > -1) {
            return true;
        }
    }

    // User is already authorized on the bucket for FULL_CONTROL or WRITE or
    // bucket has canned ACL public-read-write
    if (requestType === 'objectPut' || requestType === 'objectDelete') {
        return true;
    }

    if (requestType === 'objectPutACL') {
        if ((objectMD.acl.Canned === 'bucket-owner-full-control'
                && bucketOwner === canonicalID)
            || objectMD.acl.FULL_CONTROL.indexOf(canonicalID) > -1
            || objectMD.acl.WRITE_ACP.indexOf(canonicalID) > -1) {
            return true;
        }
    }

    if (requestType === 'objectGetACL') {
        if ((objectMD.acl.Canned === 'bucket-owner-full-control'
                && bucketOwner === canonicalID)
            || objectMD.acl.FULL_CONTROL.indexOf(canonicalID) > -1
            || objectMD.acl.READ_ACP.indexOf(canonicalID) > -1) {
            return true;
        }
    }
    return false;
}

module.exports = {
    isBucketAuthorized,
    isObjAuthorized,
};
