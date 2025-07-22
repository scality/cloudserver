const { evaluators, actionMaps, RequestContext, requestUtils } = require('arsenal').policies;
const { errorInstances } = require('arsenal');
const { parseCIDR, isValid } = require('ipaddr.js');
const constants = require('../../../../constants');
const { config } = require('../../../Config');

const {
    allAuthedUsersId,
    bucketOwnerActions,
    logId,
    publicId,
    arrayOfAllowed,
    assumedRoleArnResourceType,
    backbeatLifecycleSessionName,
    actionsToConsiderAsObjectPut,
} = constants;

// whitelist buckets to allow public read on objects
const publicReadBuckets = process.env.ALLOW_PUBLIC_READ_BUCKETS
    ? process.env.ALLOW_PUBLIC_READ_BUCKETS.split(',') : [];

function getServiceAccountProperties(canonicalID) {
    const canonicalIDArray = canonicalID.split('/');
    const serviceName = canonicalIDArray[canonicalIDArray.length - 1];
    return constants.serviceAccountProperties[serviceName];
}

function isServiceAccount(canonicalID) {
    return getServiceAccountProperties(canonicalID) !== undefined;
}

function isRequesterASessionUser(authInfo) {
    const regexpAssumedRoleArn = /^arn:aws:sts::[0-9]{12}:assumed-role\/.*$/;
    return regexpAssumedRoleArn.test(authInfo.getArn());
}

function isRequesterNonAccountUser(authInfo) {
    return authInfo.isRequesterAnIAMUser() || isRequesterASessionUser(authInfo);
}

// WARNING: enum order matters DO NOT change.
const checkPrincipalResult = Object.freeze({
    KO: 0,
    CROSS_ACCOUNT_OK: 1,
    OK: 2,
});

const checkBucketPolicyResult = Object.freeze({
    DEFAULT_DENY: 0,
    EXPLICIT_DENY: 1,
    ALLOW: 2,
    CROSS_ACCOUNT_ALLOW: 3,
});

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
    let requestTypeParsed = requestType.endsWith('Version') ?
        requestType.slice(0, 'Version'.length * -1) : requestType;
    requestTypeParsed = actionsToConsiderAsObjectPut.includes(requestTypeParsed) ?
        'objectPut' : requestTypeParsed;
    const parsedMainApiCall = actionsToConsiderAsObjectPut.includes(mainApiCall) ?
        'objectPut' : mainApiCall;
    if (bucket.getOwner() === canonicalID) {
        return true;
    }
    if (parsedMainApiCall === 'objectGet') {
        if (requestTypeParsed === 'objectGetTagging') {
            return true;
        }
    }
    if (parsedMainApiCall === 'objectPut') {
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
    const requestTypeParsed = actionsToConsiderAsObjectPut.includes(requestType) ?
        'objectPut' : requestType;
    const parsedMainApiCall = actionsToConsiderAsObjectPut.includes(mainApiCall) ?
        'objectPut' : mainApiCall;
    // acls don't distinguish between users and accounts, so both should be allowed
    if (bucketOwnerActions.includes(requestTypeParsed)
        && (bucketOwner === canonicalID)) {
        return true;
    }
    if (objectMD['owner-id'] === canonicalID) {
        return true;
    }

    // Backward compatibility
    if (parsedMainApiCall === 'objectGet') {
        if ((isUserUnauthenticated || (requesterIsNotUser && bucketOwner === objectMD['owner-id']))
            && requestTypeParsed === 'objectGetTagging') {
            return true;
        }
    }

    if (!objectMD.acl) {
        return false;
    }

    if (requestTypeParsed === 'objectGet' || requestTypeParsed === 'objectHead') {
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
    if (requestTypeParsed === 'objectPut' || requestTypeParsed === 'objectDelete') {
        return true;
    }

    if (requestTypeParsed === 'objectPutACL') {
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

    if (requestTypeParsed === 'objectGetACL') {
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
        && (requestTypeParsed === 'objectGet' || requestTypeParsed === 'objectHead');
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

function _checkBucketPolicyConditions(request, conditions, log) {
    const ip = request ? requestUtils.getClientIp(request, config) : undefined;
    if (!conditions) {
        return true;
    }
    // build request context from the request!
    const requestContext = new RequestContext(request.headers, request.query,
        request.bucketName, request.objectKey, ip,
        request.connection.encrypted, request.resourceType, 's3', null, null,
        null, null, null, null, null, null, null, null, null,
        request.objectLockRetentionDays);
    return evaluators.meetConditions(requestContext, conditions, log);
}

function _getAccountId(arn) {
    // account or user arn is of format 'arn:aws:iam::<12-digit-acct-id>:etc...
    return arn.substr(13, 12);
}

function _isAccountId(principal) {
    return (principal.length === 12 && /^\d+$/.test(principal));
}

/**
 * Checks if the ARN represents a root user account
 * @param {string} arn - The ARN to check
 * @returns {boolean} True if root user, false otherwise
 */
function _isRootUser(arn) {
    if (!arn) {
        return false;
    }

    // Vault returns the following arn when the account makes requests 'arn:aws:iam::123456789012:/accountName/',
    // with an empty resource type ('user/' prefix missing).
    const arns = arn.split(':');
    if (arns.length < 6) {
        return false;
    }

    const resource = arns[arns.length - 1];

    // If we start with '/' is because we have a empty resource type so we know it is a root account.
    if (resource.startsWith('/')) {
        return true;
    }

    return false;
}

/** _evaluateCrossAccount - checks if it is a cross-account request.
 * @param {string} requesterARN - requester ARN
 * @param {string} requesterCanonicalID - requester canonical ID
 * @param {string} bucketOwnerCanonicalID - bucket owner canonical ID
 * @return {checkPrincipalResult} OK if it is not cross-account, CROSS_ACCOUNT_OK otherwise.
 */
function _checkCrossAccount(requesterARN, requesterCanonicalID, bucketOwnerCanonicalID) {
    // Vault returns ARNs like 'arn:aws:iam::123456789012:/accountName/' for root accounts
    // with an empty resource type (missing 'user/' prefix)
    if (!_isRootUser(requesterARN)) {
        return bucketOwnerCanonicalID === requesterCanonicalID ?
            checkPrincipalResult.OK : checkPrincipalResult.CROSS_ACCOUNT_OK;
    }

    return checkPrincipalResult.OK;
}

function _checkPrincipalWildcard(requestARN, requesterCanonicalID, bucketOwnerCanonicalID) {
    if (requestARN === undefined) { // User in unauthenticated (anonymous request)
        return checkPrincipalResult.OK;
    }

    return _checkCrossAccount(requestARN, requesterCanonicalID, bucketOwnerCanonicalID);
}

function _checkPrincipalAWS(principal, requesterARN, requesterCanonicalID, bucketOwnerCanonicalID) {
    if (principal === '*') {
        return _checkPrincipalWildcard(requesterARN, requesterCanonicalID, bucketOwnerCanonicalID);
    }

    if (requesterARN === undefined) { // User in unauthenticated (anonymous request)
        return checkPrincipalResult.KO;
    }

    if (principal === requesterARN) {
        return _checkCrossAccount(requesterARN, requesterCanonicalID, bucketOwnerCanonicalID);
    }

    if (_isAccountId(principal) && principal === _getAccountId(requesterARN)) {
        return _checkCrossAccount(requesterARN, requesterCanonicalID, bucketOwnerCanonicalID);
    }

    if (principal.endsWith(':root') && _getAccountId(principal) === _getAccountId(requesterARN)) {
        return _checkCrossAccount(requesterARN, requesterCanonicalID, bucketOwnerCanonicalID);
    }

    return checkPrincipalResult.KO;
}

function _checkPrincipalCanonicalUser(principal, requesterARN, requesterCanonicalID, bucketOwnerCanonicalID) {
    if (principal === '*') {
        return _checkPrincipalWildcard(requesterARN, requesterCanonicalID, bucketOwnerCanonicalID);
    }

    if (requesterARN === undefined) { // User in unauthenticated (anonymous request)
        return checkPrincipalResult.KO;
    }

    if (principal === requesterCanonicalID) {
        return _checkCrossAccount(requesterARN, requesterCanonicalID, bucketOwnerCanonicalID);
    }

    return checkPrincipalResult.KO;
}

function _findBestPrincipalMatch(principalArray, checkFunc) {
    let bestMatch = checkPrincipalResult.KO;
    if (!principalArray) {
        return bestMatch;
    }

    const principals = Array.isArray(principalArray) ? principalArray : [principalArray];

    for (const p of principals) {
        const result = checkFunc(p);
        if (result === checkPrincipalResult.OK) {
            return checkPrincipalResult.OK; // Highest permission, can exit early
        }
        if (result > bestMatch) {
            bestMatch = result;
        }
    }

    return bestMatch;
}

function _checkPrincipals(canonicalID, arn, principal, bucketOwnerCanonicalID) {
    if (principal === '*') {
        return _checkPrincipalWildcard(arn, canonicalID, bucketOwnerCanonicalID);
    }

    if (principal.CanonicalUser) {
        return _findBestPrincipalMatch(principal.CanonicalUser,
            p => _checkPrincipalCanonicalUser(p, arn, canonicalID, bucketOwnerCanonicalID));
    }

    if (principal.AWS) {
        return _findBestPrincipalMatch(principal.AWS,
            p => _checkPrincipalAWS(p, arn, canonicalID, bucketOwnerCanonicalID));
    }

    return checkPrincipalResult.KO;
}

// checkBucketPolicy Finite State Machine.
// ┌───────────────────────────┐
// │                           ▼
// │                         ┌───────┐
// │               ┌────────►│ ALLOW ├──────────────┐
// │ ┌─────┐       │         └──┬────┘              │
// │ │START│       │            │                   │
// │ └──┬──┘       │            │                   │
// │    │          │            │                   │
// │    ▼          │            ▼                   ▼
// │┌──────────────┤          ┌────┐             ┌─────┐
// ││ DEFAULT_DENY ├─────────►│DENY├────────────►│ END │
// │└──────┬───────┤          └────┘             └─────┘
// │       │       │             ▲                  ▲ ▲
// │       │       │             │                  │ │
// │       │       │             │                  │ │
// │       │       │         ┌───┴───────────┐      │ │
// │       │       └────────►│ CROSS_ACCOUNT ├──────┘ │
// │       │                 └┬──────────────┘        │
// └───────┼──────────────────┘                       │
//         └──────────────────────────────────────────┘
//
function checkBucketPolicy(policy, requestType, canonicalID, arn, bucketOwner, log, request, actionImplicitDenies) {
    let permission = checkBucketPolicyResult.DEFAULT_DENY;
    // if requester is user within bucket owner account, actions should be
    // allowed unless explicitly denied (assumes allowed by IAM policy)
    if (bucketOwner === canonicalID && actionImplicitDenies[requestType] === false) {
        permission = checkBucketPolicyResult.ALLOW;
    }
    let copiedStatement = JSON.parse(JSON.stringify(policy.Statement));
    while (copiedStatement.length > 0) {
        const s = copiedStatement[0];
        const principalMatch = _checkPrincipals(canonicalID, arn, s.Principal, bucketOwner);
        const actionMatch = _checkBucketPolicyActions(requestType, s.Action, log);
        const resourceMatch = _checkBucketPolicyResources(request, s.Resource, log);
        const conditionsMatch = _checkBucketPolicyConditions(request, s.Condition, log);

        const ok = principalMatch === checkPrincipalResult.OK && actionMatch && resourceMatch && conditionsMatch;
        const okCross = principalMatch === checkPrincipalResult.CROSS_ACCOUNT_OK
            && actionMatch && resourceMatch && conditionsMatch;
        switch (permission) {
        case checkBucketPolicyResult.DEFAULT_DENY:
            if ((ok || okCross) && s.Effect === 'Deny') {
                return checkBucketPolicyResult.EXPLICIT_DENY;
            } else if (ok && s.Effect === 'Allow') {
                permission = checkBucketPolicyResult.ALLOW;
            } else if (okCross && s.Effect === 'Allow') {
                permission = checkBucketPolicyResult.CROSS_ACCOUNT_ALLOW;
            }
            break;
        case checkBucketPolicyResult.EXPLICIT_DENY:
            return checkBucketPolicyResult.EXPLICIT_DENY;
        case checkBucketPolicyResult.ALLOW:
            if ((ok || okCross) && s.Effect === 'Deny') {
                return checkBucketPolicyResult.EXPLICIT_DENY;
            }
            break;
        case checkBucketPolicyResult.CROSS_ACCOUNT_ALLOW:
            if ((ok || okCross) && s.Effect === 'Deny') {
                return checkBucketPolicyResult.EXPLICIT_DENY;
            } else if (ok && s.Effect === 'Allow') {
                permission = checkBucketPolicyResult.ALLOW;
            }
            break;
        default: // Needed for the linter, should be unreachable.
            break;
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

        if (bucketPolicyPermission === checkBucketPolicyResult.EXPLICIT_DENY) {
            processedResult = false;
        } else if (bucketPolicyPermission === checkBucketPolicyResult.ALLOW) {
            processedResult = true;
        } else if (bucketPolicyPermission === checkBucketPolicyResult.CROSS_ACCOUNT_ALLOW
            && actionImplicitDenies[requestType] === false) {
            // If the bucket policy is cross account, only return true if Vault also returned an explicit allow.
            processedResult = true;
        } else {
            processedResult = actionImplicitDenies[requestType] === false && aclPermission;
        }
    }
    return processedResult;
}

function isBucketAuthorized(bucket, requestTypesInput, canonicalID, authInfo, log, request,
    actionImplicitDeniesInput = {}, isWebsite = false) {
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
            requesterIsNotUser = !isRequesterNonAccountUser(authInfo);
            arn = authInfo.getArn();
        }
        // if the bucket owner is an account, users should not have default access
        if ((bucket.getOwner() === canonicalID) && requesterIsNotUser || isServiceAccount(canonicalID)) {
            results[_requestType] = actionImplicitDenies[_requestType] === false;
            return results[_requestType];
        }
        const aclPermission = checkBucketAcls(bucket, _requestType, canonicalID, mainApiCall);
        // In case of error bucket access is checked with bucketGet
        // For website, bucket policy only uses objectGet and ignores bucketGet
        // https://docs.aws.amazon.com/AmazonS3/latest/userguide/WebsiteAccessPermissionsReqd.html
        // bucketGet should be used to check acl but switched to objectGet for bucket policy
        if (isWebsite && _requestType === 'bucketGet') {
            // eslint-disable-next-line no-param-reassign
            _requestType = 'objectGet';
            actionImplicitDenies.objectGet = actionImplicitDenies.objectGet || false;
        }
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
    actionImplicitDeniesInput = {}, isWebsite = false) {
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
            // check bucket has read access
            // 'bucketGet' covers listObjects and listMultipartUploads, bucket read actions
            let permission = 'bucketGet';
            if (actionsToConsiderAsObjectPut.includes(_requestType)) {
                permission = 'objectPut';
            }
            results[_requestType] = isBucketAuthorized(bucket, permission, canonicalID, authInfo, log, request,
                actionImplicitDenies, isWebsite);
            // User is already authorized on the bucket for FULL_CONTROL or WRITE or
            // bucket has canned ACL public-read-write
            if ((parsedMethodName === 'objectPut' || parsedMethodName === 'objectDelete')
                && results[_requestType] === false) {
                results[_requestType] = actionImplicitDenies[_requestType] === false;
            }
            return results[_requestType];
        }
        let requesterIsNotUser = true;
        let arn = null;
        let isUserUnauthenticated = false;
        if (authInfo) {
            requesterIsNotUser =  !isRequesterNonAccountUser(authInfo);
            arn = authInfo.getArn();
            isUserUnauthenticated = arn === undefined;
        }
        if (objectMD['owner-id'] === canonicalID && requesterIsNotUser || isServiceAccount(canonicalID)) {
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

function checkIp(value) {
    const errString = 'Invalid IP address in Conditions';

    const values = Array.isArray(value) ? value : [value];

    for (let i = 0; i < values.length; i++) {
        // these preliminary checks are validating the provided
        // ip address against ipaddr.js, the library we use when
        // evaluating IP condition keys. It ensures compatibility,
        // but additional checks are required to enforce the right
        // notation (e.g., xxx.xxx.xxx.xxx/xx for IPv4). Otherwise,
        // we would accept different ip formats, which is not
        // standard in an AWS use case.
        try {
            try {
                parseCIDR(values[i]);
            } catch (err) {
                isValid(values[i]);
            }
        } catch (err) {
            return errString;
        }

        // Apply the existing IP validation logic to each element
        const validateIpRegex = ip => {
            if (constants.ipv4Regex.test(ip)) {
                return ip.split('.').every(part => parseInt(part, 10) <= 255);
            }
            if (constants.ipv6Regex.test(ip)) {
                return ip.split(':').every(part => part.length <= 4);
            }
            return false;
        };

        if (validateIpRegex(values[i]) !== true) {
            return errString;
        }
    }

    // If the function hasn't returned by now, all elements are valid
    return null;
}

// This function checks all bucket policy conditions if the values provided
// are valid for the condition type. If not it returns a relevant Malformed policy error string
function validatePolicyConditions(policy) {
    const validConditions = [
        { conditionKey: 'aws:SourceIp', conditionValueTypeChecker: checkIp },
        { conditionKey: 's3:object-lock-remaining-retention-days' },
    ];
    // keys where value type does not seem to be checked by AWS:
    // - s3:object-lock-remaining-retention-days

    if (!policy.Statement || !Array.isArray(policy.Statement) || policy.Statement.length === 0) {
        return null;
    }

    // there can be multiple statements in the policy, each with a Condition enclosure
    for (let i = 0; i < policy.Statement.length; i++) {
        const s = policy.Statement[i];
        if (s.Condition) {
            const conditionOperators = Object.keys(s.Condition);
            // there can be multiple condition operations in the Condition enclosure
            // eslint-disable-next-line no-restricted-syntax
            for (const conditionOperator of conditionOperators) {
                const conditionKey = Object.keys(s.Condition[conditionOperator])[0];
                const conditionValue = s.Condition[conditionOperator][conditionKey];
                const validCondition = validConditions.find(validCondition =>
                    validCondition.conditionKey === conditionKey
                );
                // AWS returns does not return an error if the condition starts with 'aws:'
                // so we reproduce this behaviour
                if (!validCondition && !conditionKey.startsWith('aws:')) {
                    return errorInstances.MalformedPolicy.customizeDescription('Policy has an invalid condition key');
                }
                if (validCondition && validCondition.conditionValueTypeChecker) {
                    const conditionValueTypeError = validCondition.conditionValueTypeChecker(conditionValue);
                    if (conditionValueTypeError) {
                        return errorInstances.MalformedPolicy.customizeDescription(conditionValueTypeError);
                    }
                }
            }
        }
    }
    return null;
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
    getServiceAccountProperties,
    isServiceAccount,
    isRequesterASessionUser,
    isRequesterNonAccountUser,
    checkBucketAcls,
    checkObjectAcls,
    validatePolicyResource,
    validatePolicyConditions,
    isLifecycleSession,
    evaluateBucketPolicyWithIAM,
    checkBucketPolicy,
    checkBucketPolicyResult,
};
