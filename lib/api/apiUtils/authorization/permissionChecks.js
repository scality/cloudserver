const { evaluators, actionMaps } = require('arsenal').policies;
const constants = require('../../../../constants');

const { allAuthedUsersId, bucketOwnerActions, logId, publicId } = constants;

// whitelist buckets to allow public read on objects
const publicReadBuckets = process.env.ALLOW_PUBLIC_READ_BUCKETS ?
    process.env.ALLOW_PUBLIC_READ_BUCKETS.split(',') : [];

function checkBucketAcls(bucket, requestType, canonicalID) {
    if (bucket.getOwner() === canonicalID) {
        return true;
    }

    // any requestType outside of those checked in this function is
    // outside the scope of ACL permissions and will be denied unless
    // allowed by a different permissions granter
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

function checkObjectAcls(bucket, objectMD, requestType, canonicalID) {
    const bucketOwner = bucket.getOwner();
    // acls don't distinguish between users and accounts, so both should be allowed
    if (bucketOwnerActions.includes(requestType)
        && (bucketOwner === canonicalID)) {
        return true;
    }
    if (objectMD['owner-id'] === canonicalID) {
        return true;
    }
    if (!objectMD.acl) {
        return false;
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

function _checkBucketPolicyActions(requestType, actions, log) {
    const mappedAction = actionMaps.actionMapBP[requestType];
    // Deny any action that isn't in list of controlled actions
    if (!mappedAction) {
        return false;
    }
    return evaluators.isActionApplicable(mappedAction, actions, log);
}

function _getAccountId(arn) {
    // account or user arn is of format 'arn:aws:iam::<12-digit-acct-id>:etc...
    return arn.substr(13, 12);
}

function _isAccountId(principal) {
    return (principal.length === 12 && /^\d+$/.test(principal));
}

function _checkPrincipal(requester, principal) {
    if (principal === '*') {
        return true;
    }
    if (principal === requester) {
        return true;
    }
    if (_isAccountId(principal)) {
        return _getAccountId(requester) === principal;
    }
    if (principal.endsWith('root')) {
        return _getAccountId(requester) === _getAccountId(principal);
    }
    return false;
}

function _checkPrincipals(canonicalID, arn, principal) {
    if (principal === '*') {
        return true;
    }
    if (principal.CanonicalUser) {
        if (Array.isArray(principal.CanonicalUser)) {
            return principal.CanonicalUser.some(p => _checkPrincipal(canonicalID, p));
        }
        return _checkPrincipal(canonicalID, principal.CanonicalUser);
    }
    if (principal.AWS) {
        if (Array.isArray(principal.AWS)) {
            return principal.AWS.some(p => _checkPrincipal(arn, p));
        }
        return _checkPrincipal(arn, principal.AWS);
    }
    return false;
}

function checkBucketPolicy(policy, requestType, canonicalID, arn, bucketOwner, log) {
    let permission = 'defaultDeny';
    // if requester is user within bucket owner account, actions should be
    // allowed unless explicitly denied (assumes allowed by IAM policy)
    if (bucketOwner === canonicalID) {
        permission = 'allow';
    }
    let copiedStatement = JSON.parse(JSON.stringify(policy.Statement));
    while (copiedStatement.length > 0) {
        const s = copiedStatement[0];
        const principalMatch = _checkPrincipals(canonicalID, arn, s.Principal);
        const actionMatch = _checkBucketPolicyActions(requestType, s.Action, log);

        if (principalMatch && actionMatch && s.Effect === 'Deny') {
            // explicit deny trumps any allows, so return immediately
            return 'explicitDeny';
        }
        if (principalMatch && actionMatch && s.Effect === 'Allow') {
            permission = 'allow';
        }
        copiedStatement = copiedStatement.splice(1);
    }
    return permission;
}

function isBucketAuthorized(bucket, requestType, canonicalID, authInfo, log) {
    // Check to see if user is authorized to perform a
    // particular action on bucket based on ACLs.
    // TODO: Add IAM checks
    let requesterIsNotUser = true;
    let arn = null;
    if (authInfo) {
        requesterIsNotUser = !authInfo.isRequesterAnIAMUser();
        arn = authInfo.getArn();
    }
    // if the bucket owner is an account, users should not have default access
    if ((bucket.getOwner() === canonicalID) && requesterIsNotUser) {
        return true;
    }
    const aclPermission = checkBucketAcls(bucket, requestType, canonicalID);
    const bucketPolicy = bucket.getBucketPolicy();
    if (!bucketPolicy) {
        return aclPermission;
    }
    const bucketPolicyPermission = checkBucketPolicy(bucketPolicy, requestType,
        canonicalID, arn, bucket.getOwner(), log);
    if (bucketPolicyPermission === 'explicitDeny') {
        return false;
    }
    return (aclPermission || (bucketPolicyPermission === 'allow'));
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

function hasBucketReadAccess(bucket, requestType, canonicalID, authInfo, log) {
    const bucketAcl = bucket.getAcl();
    const bucketOwner = bucket.getOwner();
    const bucketPolicy = bucket.getBucketPolicy();
    let arn = null;
    let requesterIsNotUser = true;

    if (authInfo) {
        requesterIsNotUser = !authInfo.isRequesterAnIAMUser();
        arn = authInfo.getArn();
    }

    // bucket policies over acls if any is applicable
    if (bucketPolicy) {
        // bucketGet covers listObjects and listMultipartUploads, bucket read
        // actions
        const bucketListPermission = checkBucketPolicy(
            bucketPolicy,
            'bucketGet',
            canonicalID,
            arn,
            bucketOwner,
            log
        );

        if (bucketListPermission === 'explicitDeny') {
            return false;
        }

        if (bucketListPermission === 'allow') {
            return true;
        }
        // defaultDeny, fallback onto acls
    }

    if ((canonicalID === bucketOwner && requesterIsNotUser) ||
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

function isObjAuthorized(bucket, objectMD, requestType, canonicalID, authInfo, log) {
    const bucketOwner = bucket.getOwner();
    if (!objectMD) {
        if (requestType === 'objectPut' || requestType === 'objectDelete') {
            return true;
        }

        // if read access is granted, return true to have the api handler
        // respond accordingly to the missing object metadata
        // if read access is not granted, return false for AccessDenied
        return hasBucketReadAccess(bucket, requestType, canonicalID, authInfo, log);
    }
    let requesterIsNotUser = true;
    let arn = null;
    if (authInfo) {
        requesterIsNotUser = !authInfo.isRequesterAnIAMUser();
        arn = authInfo.getArn();
    }
    if (objectMD['owner-id'] === canonicalID && requesterIsNotUser) {
        return true;
    }
    // account is authorized if:
    // - requesttype is included in bucketOwnerActions and
    // - account is the bucket owner
    // - requester is account, not user
    if (bucketOwnerActions.includes(requestType)
        && (bucketOwner === canonicalID)
        && requesterIsNotUser) {
        return true;
    }
    const aclPermission = checkObjectAcls(bucket, objectMD, requestType,
        canonicalID);
    const bucketPolicy = bucket.getBucketPolicy();
    if (!bucketPolicy) {
        return aclPermission;
    }
    const bucketPolicyPermission = checkBucketPolicy(bucketPolicy, requestType,
        canonicalID, arn, bucket.getOwner(), log);
    if (bucketPolicyPermission === 'explicitDeny') {
        return false;
    }
    return (aclPermission || (bucketPolicyPermission === 'allow'));
}

function _checkResource(resource, bucketArn) {
    if (resource === bucketArn) {
        return true;
    }
    if (resource.includes('/')) {
        const rSubs = resource.split('/');
        return rSubs[0] === bucketArn;
    }
    return false;
}

// the resources specified in the bucket policy should contain the bucket name
function validatePolicyResource(bucketName, policy) {
    const bucketArn = `arn:aws:s3:::${bucketName}`;

    return policy.Statement.every(s => {
        if (Array.isArray(s.Resource)) {
            return s.Resource.every(r => _checkResource(r, bucketArn));
        }
        if (typeof s.Resource === 'string') {
            return _checkResource(s.Resource, bucketArn);
        }
        return false;
    });
}

module.exports = {
    isBucketAuthorized,
    isObjAuthorized,
    checkBucketAcls,
    checkObjectAcls,
    validatePolicyResource,
};
