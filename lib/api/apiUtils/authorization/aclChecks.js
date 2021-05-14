const { allAuthedUsersId, logId, publicId } = require('../../../../constants');

// whitelist buckets to allow public read on objects
const publicReadBuckets = process.env.ALLOW_PUBLIC_READ_BUCKETS ?
    process.env.ALLOW_PUBLIC_READ_BUCKETS.split(',') : [];

function isBucketAuthorized(bucket, requestType, canonicalID) {
    // Check to see if user is authorized to perform a
    // particular action on bucket based on ACLs.
    // TODO: Add IAM checks and bucket policy checks.
    if (bucket.getOwner() === canonicalID) {
        return true;
    } else if (requestType === 'bucketOwnerAction') {
        // only bucket owner can modify or retrieve this property of a bucket
        return false;
    }
    const bucketAcl = bucket.getAcl();
    if (requestType === 'bucketGet' || requestType === 'bucketHead') {
        if (bucketAcl.Canned === 'public-read'
            || bucketAcl.Canned === 'public-read-write'
            || (bucketAcl.Canned === 'authenticated-read'
                && canonicalID !== publicId)) {
            return true;
        } else if (bucketAcl.FULL_CONTROL.indexOf(canonicalID) > -1
            || bucketAcl.READ.indexOf(canonicalID) > -1) {
            return true;
        } else if (bucketAcl.READ.indexOf(publicId) > -1
            || (bucketAcl.READ.indexOf(allAuthedUsersId) > -1
                && canonicalID !== publicId)
            || (bucketAcl.FULL_CONTROL.indexOf(allAuthedUsersId) > -1
                && canonicalID !== publicId)
            || bucketAcl.FULL_CONTROL.indexOf(publicId) > -1) {
            return true;
        }
    }

    if (requestType === 'bucketGetACL') {
        if ((bucketAcl.Canned === 'log-delivery-write'
                && canonicalID === logId)
            || bucketAcl.FULL_CONTROL.indexOf(canonicalID) > -1
            || bucketAcl.READ_ACP.indexOf(canonicalID) > -1) {
            return true;
        } else if (bucketAcl.READ_ACP.indexOf(publicId) > -1
            || (bucketAcl.READ_ACP.indexOf(allAuthedUsersId) > -1
                && canonicalID !== publicId)
            || (bucketAcl.FULL_CONTROL.indexOf(allAuthedUsersId) > -1
                && canonicalID !== publicId)
            || bucketAcl.FULL_CONTROL.indexOf(publicId) > -1) {
            return true;
        }
    }

    if (requestType === 'bucketPutACL') {
        if (bucketAcl.FULL_CONTROL.indexOf(canonicalID) > -1
            || bucketAcl.WRITE_ACP.indexOf(canonicalID) > -1) {
            return true;
        } else if (bucketAcl.WRITE_ACP.indexOf(publicId) > -1
            || (bucketAcl.WRITE_ACP.indexOf(allAuthedUsersId) > -1
                && canonicalID !== publicId)
            || (bucketAcl.FULL_CONTROL.indexOf(allAuthedUsersId) > -1
                && canonicalID !== publicId)
            || bucketAcl.FULL_CONTROL.indexOf(publicId) > -1) {
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

function _isPermissionGranted(permissionList, canonicalID) {
    if (permissionList.indexOf(publicId) > -1) {
        return true;
    }

    if (permissionList.indexOf(allAuthedUsersId) > -1 &&
        canonicalID !== publicId) {
        return true;
    }

    if (permissionList.indexOf(canonicalID) > -1) {
        return true;
    }

    return false;
}

function hasBucketReadAccess(bucket, requestType, canonicalID) {
    const bucketAcl = bucket.getAcl();
    const bucketOwner = bucket.getOwner();

    // User is already authorized on the bucket for FULL_CONTROL or WRITE or
    // bucket has canned ACL public-read-write
    if (requestType === 'objectPut' || requestType === 'objectDelete') {
        return true;
    }

    if (canonicalID === bucketOwner ||
        _isPermissionGranted(bucketAcl.FULL_CONTROL, canonicalID) ||
        _isPermissionGranted(bucketAcl.READ, canonicalID)) {
        return true;
    }

    if (bucketAcl.Canned === 'public-read' ||
        bucketAcl.Canned === 'public-read-write' ||
        (bucketAcl.Canned === 'authenticated-read' &&
         canonicalID !== publicId)) {
        return true;
    }

    return false;
}

function isObjAuthorized(bucket, objectMD, requestType, canonicalID) {
    const bucketOwner = bucket.getOwner();
    if (!objectMD) {
        // if read access is granted, return true to have the api handler
        // respond accordingly to the missing object metadata
        // if read access is not granted, return false for AccessDenied
        return hasBucketReadAccess(bucket, requestType, canonicalID);
    }

    if (objectMD['owner-id'] === canonicalID) {
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
                && canonicalID !== publicId)) {
            return true;
        } else if (objectMD.acl.Canned === 'bucket-owner-read'
                && bucketOwner === canonicalID) {
            return true;
        } else if ((objectMD.acl.Canned === 'bucket-owner-full-control'
                && bucketOwner === canonicalID)
            || objectMD.acl.FULL_CONTROL.indexOf(canonicalID) > -1
            || objectMD.acl.READ.indexOf(canonicalID) > -1) {
            return true;
        } else if (objectMD.acl.READ.indexOf(publicId) > -1
            || (objectMD.acl.READ.indexOf(allAuthedUsersId) > -1
                && canonicalID !== publicId)
            || (objectMD.acl.FULL_CONTROL.indexOf(allAuthedUsersId) > -1
                && canonicalID !== publicId)
            || objectMD.acl.FULL_CONTROL.indexOf(publicId) > -1) {
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
        } else if (objectMD.acl.WRITE_ACP.indexOf(publicId) > -1
            || (objectMD.acl.WRITE_ACP.indexOf(allAuthedUsersId) > -1
                && canonicalID !== publicId)
            || (objectMD.acl.FULL_CONTROL.indexOf(allAuthedUsersId) > -1
                && canonicalID !== publicId)
            || objectMD.acl.FULL_CONTROL.indexOf(publicId) > -1) {
            return true;
        }
    }

    if (requestType === 'objectGetACL') {
        if ((objectMD.acl.Canned === 'bucket-owner-full-control'
                && bucketOwner === canonicalID)
            || objectMD.acl.FULL_CONTROL.indexOf(canonicalID) > -1
            || objectMD.acl.READ_ACP.indexOf(canonicalID) > -1) {
            return true;
        } else if (objectMD.acl.READ_ACP.indexOf(publicId) > -1
            || (objectMD.acl.READ_ACP.indexOf(allAuthedUsersId) > -1
                && canonicalID !== publicId)
            || (objectMD.acl.FULL_CONTROL.indexOf(allAuthedUsersId) > -1
                && canonicalID !== publicId)
            || objectMD.acl.FULL_CONTROL.indexOf(publicId) > -1) {
            return true;
        }
    }

    // allow public reads on buckets that are whitelisted for anonymous reads
    // TODO: remove this after bucket policies are implemented
    const bucketAcl = bucket.getAcl();
    const allowPublicReads = publicReadBuckets.includes(bucket.getName()) &&
        bucketAcl.Canned === 'public-read' &&
        (requestType === 'objectGet' || requestType === 'objectHead');
    if (allowPublicReads) {
        return true;
    }
    return false;
}

module.exports = {
    isBucketAuthorized,
    isObjAuthorized,
};
