const { evaluators, actionMaps, RequestContext } = require('arsenal').policies;
const constants = require('../../../../constants');

const {
    allAuthedUsersId, bucketOwnerActions, logId, publicId,
    assumedRoleArnResourceType, backbeatLifecycleSessionName, arrayOfAllowed,
} = constants;

// whitelist buckets to allow public read on objects
const publicReadBuckets = process.env.ALLOW_PUBLIC_READ_BUCKETS
    ? process.env.ALLOW_PUBLIC_READ_BUCKETS.split(',') : [];

/**
 * Checks the access control for a given bucket based on the request type and user's canonical ID.
 *
 * @param {Bucket} bucket - The bucket to check access control for.
 * @param {string} requestType - The list of s3 actions to check within the API call.
 * @param {string} canonicalID - The canonical ID of the user making the request.
 * @param {string} mainApiCall - The main API call (first item of the requestType).
 *
 * @returns {boolean} - Returns true if the user has the necessary access rights, otherwise false.
 */

function checkBucketAcls(bucket, requestType, canonicalID, mainApiCall) {
    // Same logic applies on the Versioned APIs, so let's simplify it.
    const requestTypeParsed = requestType.endsWith('Version') ?
        requestType.slice(0, 'Version'.length * -1) : requestType;
    if (bucket.getOwner() === canonicalID) {
        return true;
    }
    if (mainApiCall === 'objectGet') {
        if (requestTypeParsed === 'objectGetTagging') {
            return true;
        }
    }
    if (mainApiCall === 'objectPut') {
        if (arrayOfAllowed.includes(requestTypeParsed)) {
            return true;
        }
    }

    const bucketAcl = bucket.getAcl();
    if (requestTypeParsed === 'bucketGet' || requestTypeParsed === 'bucketHead') {
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
    if (requestTypeParsed === 'bucketGetACL') {
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

    if (requestTypeParsed === 'bucketPutACL') {
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

    if (requestTypeParsed === 'objectDelete' || requestTypeParsed === 'objectPut') {
        if (bucketAcl.Canned === 'public-read-write'
            || bucketAcl.FULL_CONTROL.indexOf(canonicalID) > -1
            || bucketAcl.WRITE.indexOf(canonicalID) > -1) {
            return true;
        } else if (bucketAcl.WRITE.indexOf(publicId) > -1
            || (bucketAcl.WRITE.indexOf(allAuthedUsersId) > -1
                && canonicalID !== publicId)
            || (bucketAcl.FULL_CONTROL.indexOf(allAuthedUsersId) > -1
                && canonicalID !== publicId)
            || bucketAcl.FULL_CONTROL.indexOf(publicId) > -1) {
            return true;
        }
    }
    // Note that an account can have the ability to do objectPutACL,
    // objectGetACL, objectHead or objectGet even if the account has no rights
    // to the bucket holding the object.  So, if the request type is
    // objectPutACL, objectGetACL, objectHead or objectGet, the bucket
    // authorization check should just return true so can move on to check
    // rights at the object level.
    return (requestTypeParsed === 'objectPutACL' || requestTypeParsed === 'objectGetACL'
    || requestTypeParsed === 'objectGet' || requestTypeParsed === 'objectHead');
}

function checkObjectAcls(bucket, objectMD, requestType, canonicalID, requesterIsNotUser,
    isUserUnauthenticated, mainApiCall) {
    const bucketOwner = bucket.getOwner();
    // acls don't distinguish between users and accounts, so both should be allowed
    if (bucketOwnerActions.includes(requestType)
        && (bucketOwner === canonicalID)) {
        return true;
    }
    if (objectMD['owner-id'] === canonicalID) {
        return true;
    }

    // Backward compatibility
    if (mainApiCall === 'objectGet') {
        if ((isUserUnauthenticated || (requesterIsNotUser && bucketOwner === objectMD['owner-id']))
            && requestType === 'objectGetTagging') {
            return true;
        }
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
    const allowPublicReads = publicReadBuckets.includes(bucket.getName())
        && bucketAcl.Canned === 'public-read'
        && (requestType === 'objectGet' || requestType === 'objectHead');
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

function _checkBucketPolicyResources(request, resource, log) {
    if (!request || (Array.isArray(resource) && resource.length === 0)) {
        return true;
    }
    // build request context from the request!
    const requestContext = new RequestContext(request.headers, request.query,
        request.bucketName, request.objectKey, null,
        request.connection.encrypted, request.resourceType, 's3');
    return evaluators.isResourceApplicable(requestContext, resource, log);
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

function checkBucketPolicy(policy, requestType, canonicalID, arn, bucketOwner, log, request, actionImplicitDenies) {
    let permission = 'defaultDeny';
    // if requester is user within bucket owner account, actions should be
    // allowed unless explicitly denied (assumes allowed by IAM policy)
    if (bucketOwner === canonicalID && actionImplicitDenies[requestType] === false) {
        permission = 'allow';
    }
    let copiedStatement = JSON.parse(JSON.stringify(policy.Statement));
    while (copiedStatement.length > 0) {
        const s = copiedStatement[0];
        const principalMatch = _checkPrincipals(canonicalID, arn, s.Principal);
        const actionMatch = _checkBucketPolicyActions(requestType, s.Action, log);
        const resourceMatch = _checkBucketPolicyResources(request, s.Resource, log);

        if (principalMatch && actionMatch && resourceMatch && s.Effect === 'Deny') {
            // explicit deny trumps any allows, so return immediately
            return 'explicitDeny';
        }
        if (principalMatch && actionMatch && resourceMatch && s.Effect === 'Allow') {
            permission = 'allow';
        }
        copiedStatement = copiedStatement.splice(1);
    }
    return permission;
}

function processBucketPolicy(requestType, bucket, canonicalID, arn, bucketOwner, log,
    request, aclPermission, results, actionImplicitDenies) {
    const bucketPolicy = bucket.getBucketPolicy();
    let processedResult = results[requestType];
    if (!bucketPolicy) {
        processedResult = actionImplicitDenies[requestType] === false && aclPermission;
    } else {
        const bucketPolicyPermission = checkBucketPolicy(bucketPolicy, requestType, canonicalID, arn,
            bucketOwner, log, request, actionImplicitDenies);

        if (bucketPolicyPermission === 'explicitDeny') {
            processedResult = false;
        } else if (bucketPolicyPermission === 'allow') {
            processedResult = true;
        } else {
            processedResult = actionImplicitDenies[requestType] === false && aclPermission;
        }
    }
    return processedResult;
}

function isBucketAuthorized(bucket, requestTypesInput, canonicalID, authInfo, log, request,
    actionImplicitDeniesInput = {}) {
    const requestTypes = Array.isArray(requestTypesInput) ? requestTypesInput : [requestTypesInput];
    const actionImplicitDenies = !actionImplicitDeniesInput ? {} : actionImplicitDeniesInput;
    const mainApiCall = requestTypes[0];
    const results = {};
    return requestTypes.every(_requestType => {
        // By default, all missing actions are defined as allowed from IAM, to be
        // backward compatible
        actionImplicitDenies[_requestType] = actionImplicitDenies[_requestType] || false;
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
            results[_requestType] = actionImplicitDenies[_requestType] === false;
            return results[_requestType];
        }
        const aclPermission = checkBucketAcls(bucket, _requestType, canonicalID, mainApiCall);
        return processBucketPolicy(_requestType, bucket, canonicalID, arn, bucket.getOwner(), log,
            request, aclPermission, results, actionImplicitDenies);
    });
}

function evaluateBucketPolicyWithIAM(bucket, requestTypesInput, canonicalID, authInfo, actionImplicitDeniesInput = {},
    log, request) {
    const requestTypes = Array.isArray(requestTypesInput) ? requestTypesInput : [requestTypesInput];
    const actionImplicitDenies = !actionImplicitDeniesInput ? {} : actionImplicitDeniesInput;
    const results = {};
    return requestTypes.every(_requestType => {
        // By default, all missing actions are defined as allowed from IAM, to be
        // backward compatible
        actionImplicitDenies[_requestType] = actionImplicitDenies[_requestType] || false;
        let arn = null;
        if (authInfo) {
            arn = authInfo.getArn();
        }
        return processBucketPolicy(_requestType, bucket, canonicalID, arn, bucket.getOwner(), log,
        request, true, results, actionImplicitDenies);
    });
}

function isObjAuthorized(bucket, objectMD, requestTypesInput, canonicalID, authInfo, log, request,
    actionImplicitDeniesInput = {}) {
    const requestTypes = Array.isArray(requestTypesInput) ? requestTypesInput : [requestTypesInput];
    const actionImplicitDenies = !actionImplicitDeniesInput ? {} : actionImplicitDeniesInput;
    const results = {};
    const mainApiCall = requestTypes[0];
    return requestTypes.every(_requestType => {
        // By default, all missing actions are defined as allowed from IAM, to be
        // backward compatible
        actionImplicitDenies[_requestType] = actionImplicitDenies[_requestType] || false;
        const parsedMethodName = _requestType.endsWith('Version')
            ? _requestType.slice(0, -7) : _requestType;
        const bucketOwner = bucket.getOwner();
        if (!objectMD) {
        // User is already authorized on the bucket for FULL_CONTROL or WRITE or
        // bucket has canned ACL public-read-write
            if (parsedMethodName === 'objectPut' || parsedMethodName === 'objectDelete') {
                results[_requestType] = actionImplicitDenies[_requestType] === false;
                return results[_requestType];
            }
            // check bucket has read access
            // 'bucketGet' covers listObjects and listMultipartUploads, bucket read actions
            results[_requestType] = isBucketAuthorized(bucket, 'bucketGet', canonicalID, authInfo, log, request,
                actionImplicitDenies);
            // User is already authorized on the bucket for FULL_CONTROL or WRITE or
            // bucket has canned ACL public-read-write
            if ((parsedMethodName === 'objectPut' || parsedMethodName === 'objectDelete') && results[_requestType] === false) {
                results[_requestType] = actionImplicitDenies[_requestType] === false;
            }
            return results[_requestType];
        }
        let requesterIsNotUser = true;
        let arn = null;
        let isUserUnauthenticated = false;
        if (authInfo) {
            requesterIsNotUser = !authInfo.isRequesterAnIAMUser();
            arn = authInfo.getArn();
            isUserUnauthenticated = arn === undefined;
        }
        if (objectMD['owner-id'] === canonicalID && requesterIsNotUser) {
            results[_requestType] = actionImplicitDenies[_requestType] === false;
            return results[_requestType];
        }
        // account is authorized if:
        // - requesttype is included in bucketOwnerActions and
        // - account is the bucket owner
        // - requester is account, not user
        if (bucketOwnerActions.includes(parsedMethodName)
        && (bucketOwner === canonicalID)
        && requesterIsNotUser) {
            results[_requestType] = actionImplicitDenies[_requestType] === false;
            return results[_requestType];
        }
        const aclPermission = checkObjectAcls(bucket, objectMD, parsedMethodName,
            canonicalID, requesterIsNotUser, isUserUnauthenticated, mainApiCall);
        return processBucketPolicy(_requestType, bucket, canonicalID, arn, bucketOwner,
            log, request, aclPermission, results, actionImplicitDenies);
    });
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

/** isLifecycleSession - check if it is the Lifecycle assumed role session arn.
 * @param {string} arn - Amazon resource name - example:
 * arn:aws:sts::257038443293:assumed-role/rolename/backbeat-lifecycle
 * @return {boolean} true if Lifecycle assumed role session arn, false if not.
 */
function isLifecycleSession(arn) {
    if (!arn) {
        return false;
    }

    const arnSplits = arn.split(':');
    const service = arnSplits[2];

    const resourceNames = arnSplits[arnSplits.length - 1].split('/');

    const resourceType = resourceNames[0];
    const sessionName = resourceNames[resourceNames.length - 1];

    return (service === 'sts'
        && resourceType === assumedRoleArnResourceType
        && sessionName === backbeatLifecycleSessionName);
}

module.exports = {
    isBucketAuthorized,
    isObjAuthorized,
    checkBucketAcls,
    checkObjectAcls,
    validatePolicyResource,
    isLifecycleSession,
    evaluateBucketPolicyWithIAM,
};
