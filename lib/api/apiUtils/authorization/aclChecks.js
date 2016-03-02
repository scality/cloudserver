import constants from '../../../../constants';

export function isBucketAuthorized(bucket, requestType, canonicalID) {
    // Check to see if user is authorized to perform a
    // particular action on bucket based on ACLs.
    // TODO: Add IAM checks and bucket policy checks.
    if (bucket.owner === canonicalID) {
        return true;
    }
    if (requestType === 'bucketGet' || requestType === 'bucketHead'
            || requestType === 'objectGet' || requestType === 'objectHead') {
        if (bucket.acl.Canned === 'public-read'
            || bucket.acl.Canned === 'public-read-write'
            || (bucket.acl.Canned === 'authenticated-read'
                && canonicalID !== constants.publicId)) {
            return true;
        } else if (bucket.acl.FULL_CONTROL.indexOf(canonicalID) > -1
            || bucket.acl.READ.indexOf(canonicalID) > -1) {
            return true;
        }
    }

    if (requestType === 'bucketGetACL') {
        if ((bucket.acl.Canned === 'log-delivery-write'
                && canonicalID === constants.logId)
            || bucket.acl.FULL_CONTROL.indexOf(canonicalID) > -1
            || bucket.acl.READ_ACP.indexOf(canonicalID) > -1) {
            return true;
        }
    }

    if (requestType === 'bucketPutACL') {
        if (bucket.acl.FULL_CONTROL.indexOf(canonicalID) > -1
            || bucket.acl.WRITE_ACP.indexOf(canonicalID) > -1) {
            return true;
        }
    }

    if (requestType === 'bucketDelete' && bucket.owner === canonicalID) {
        return true;
    }

    if (requestType === 'objectDelete' || requestType === 'objectPut') {
        if (bucket.acl.Canned === 'public-read-write'
            || bucket.acl.FULL_CONTROL.indexOf(canonicalID) > -1
            || bucket.acl.WRITE.indexOf(canonicalID) > -1) {
            return true;
        }
    }
    // Note that an account can have the ability to do objectPutACL
    // or objectGetACL even if the account has no rights to the bucket
    // holding the object.  So, if the requst type is objectPutACL
    // or objectGetACL, the bucket authorization check should just
    // return true so can move on to check rights at the object level.
    return (requestType === 'objectPutACL' || requestType === 'objectGetACL');
}

export function isObjAuthorized(bucket, objectMD, requestType, canonicalID) {
    if (!objectMD) {
        return false;
    }
    if (objectMD['owner-id'] === canonicalID) {
        return true;
    }
    if (requestType === 'objectGet' || requestType === 'objectHead') {
        if (objectMD.acl.Canned === 'public-read'
            || objectMD.acl.Canned === 'public-read-write'
            || (objectMD.acl.Canned === 'authenticated-read'
                && canonicalID !== constants.publicId)) {
            return true;
        } else if (objectMD.acl.Canned === 'bucket-owner-read'
                && bucket.owner === canonicalID) {
            return true;
        } else if ((objectMD.acl.Canned === 'bucket-owner-full-control'
                && bucket.owner === canonicalID)
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
                && bucket.owner === canonicalID)
            || objectMD.acl.FULL_CONTROL.indexOf(canonicalID) > -1
            || objectMD.acl.WRITE_ACP.indexOf(canonicalID) > -1) {
            return true;
        }
    }

    if (requestType === 'objectGetACL') {
        if (objectMD.acl.Canned === 'bucket-owner-full-control'
            || objectMD.acl.FULL_CONTROL.indexOf(canonicalID) > -1
            || objectMD.acl.READ_ACP.indexOf(canonicalID) > -1) {
            return true;
        }
    }
    return false;
}
