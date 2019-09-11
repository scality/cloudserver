const { evaluators } = require('arsenal').policies;
const constants = require('../../../../constants');

const actionMap = {
    's3:AbortMultipartUpload': 'multipartDelete',
    's3:DeleteBucket': 'bucketDelete',
    's3:DeleteBucketPolicy': 'bucketDeletePolicy',
    's3:DeleteBucketWebsite': 'bucketDeleteWebsite',
    's3:DeleteObject': 'objectDelete',
    's3:DeleteObjectTagging': 'objectDeleteTagging',
    's3:GetBucketAcl': 'bucketGetACL',
    's3:GetBucketCORS': 'bucketGetCors',
    's3:GetBucketLocation': 'bucketGetLocation',
    's3:GetBucketPolicy': 'bucketGetPolicy',
    's3:GetBucketVersioning': 'bucketGetVersioning',
    's3:GetBucketWebsite': 'bucketGetWebsite',
    's3:GetLifecycleConfiguration': 'bucketGetLifecycle',
    's3:GetObject': 'objectGet',
    's3:GetObjectAcl': 'objectGetACL',
    's3:GetObjectTagging': 'objectGetTagging',
    's3:GetReplicationConfiguration': 'bucketGetReplication',
    's3:ListBucket': 'bucketHead',
    's3:ListBucketMultipartUploads': 'listMultipartUploads',
    's3:ListMultipartUploadParts': 'listParts',
    's3:PutBucketAcl': 'bucketPutACL',
    's3:PutBucketCORS': 'bucketPutCors',
    's3:PutBucketPolicy': 'bucketPutPolicy',
    's3:PutBucketVersioning': 'bucketPutVersioning',
    's3:PutBucketWebsite': 'bucketPutWebsite',
    's3:PutLifecycleConfiguration': 'bucketPutLifecycle',
    's3:PutObject': 'objectPut',
    's3:PutObjectAcl': 'objectPutACL',
    's3:PutObjectTagging': 'objectPutTagging',
    's3:PutReplicationConfiguration': 'bucketPutReplication',
};

// whitelist buckets to allow public read on objects
const publicReadBuckets = process.env.ALLOW_PUBLIC_READ_BUCKETS ?
    process.env.ALLOW_PUBLIC_READ_BUCKETS.split(',') : [];

function getServiceAccountProperties(canonicalID) {
    const canonicalIDArray = canonicalID.split('/');
    const serviceName = canonicalIDArray[canonicalIDArray.length - 1];
    return constants.serviceAccountProperties[serviceName];
}

function isServiceAccount(canonicalID) {
    return getServiceAccountProperties(canonicalID) !== undefined;
}

function checkBucketAcls(bucket, requestType, canonicalID) {
    if (constants.bucketOwnerActions.includes(requestType)) {
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

function checkObjectAcls(bucket, objectMD, requestType, canonicalID) {
    if (!objectMD.acl) {
        return false;
    }
    const bucketOwner = bucket.getOwner();

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

function _checkActions(requestType, actions, log) {
    // if requestType isn't in list of controlled actions
    if (!Object.values(actionMap).includes(requestType)) {
        return true;
    }
    const mappedAction = Object.keys(actionMap)
        [Object.values(actionMap).indexOf(requestType)];
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

function checkBucketPolicy(policy, requestType, canonicalID, arn, log) {
    let permission = 'defaultDeny';
    let copiedStatement = JSON.parse(JSON.stringify(policy.Statement));
    while (copiedStatement.length > 0) {
        const s = copiedStatement[0];
        const principalMatch = _checkPrincipals(canonicalID, arn, s.Principal);
        const actionMatch = _checkActions(requestType, s.Action, log);

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

function isBucketAuthorized(bucket, requestType, canonicalID, arn, log) {
    // Check to see if user is authorized to perform a
    // particular action on bucket based on ACLs.
    // TODO: Add IAM checks
    if (bucket.getOwner() === canonicalID || isServiceAccount(canonicalID)) {
        return true;
    }
    const aclPermission = checkBucketAcls(bucket, requestType, canonicalID);
    const bucketPolicy = bucket.getBucketPolicy();
    if (!bucketPolicy) {
        if (constants.bucketOwnerActions.includes(requestType)) {
            return false;
        }
        return aclPermission;
    }
    const bucketPolicyPermission = checkBucketPolicy(bucketPolicy, requestType,
        canonicalID, arn, log);
    if (bucketPolicyPermission === 'explicitDeny') {
        return false;
    }
    return (aclPermission || (bucketPolicyPermission === 'allow'));
}

function isObjAuthorized(bucket, objectMD, requestType, canonicalID, arn, log) {
    const bucketOwner = bucket.getOwner();
    if (!objectMD) {
        return false;
    }
    if (objectMD['owner-id'] === canonicalID) {
        return true;
    }

    if (isServiceAccount(canonicalID)) {
        return true;
    }
    // account is authorized if:
    // - requesttype is included in bucketOwnerActions and
    // - account is the bucket owner
    if (constants.bucketOwnerActions.includes(requestType)
    && bucketOwner === canonicalID) {
        return true;
    }
    const aclPermission = checkObjectAcls(bucket, objectMD, requestType,
        canonicalID);
    const bucketPolicy = bucket.getBucketPolicy();
    if (!bucketPolicy) {
        return aclPermission;
    }
    const bucketPolicyPermission = checkBucketPolicy(bucketPolicy, requestType,
        canonicalID, arn, log);
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
    getServiceAccountProperties,
    isServiceAccount,
    checkBucketAcls,
    checkObjectAcls,
    validatePolicyResource,
};
