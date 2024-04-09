const { evaluators, actionMaps, actionNeedQuotaCheck, RequestContext, requestUtils } = require('arsenal').policies;
const { errors } = require('arsenal');
const { parseCIDR, isValid } = require('ipaddr.js');
const constants = require('../../../../constants');
const { config } = require('../../../Config');
const { ScubaClientInstance } = require('../../../scuba/wrapper');

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
        const conditionsMatch = _checkBucketPolicyConditions(request, s.Condition, log);

        if (principalMatch && actionMatch && resourceMatch && conditionsMatch && s.Effect === 'Deny') {
            // explicit deny trumps any allows, so return immediately
            return 'explicitDeny';
        }
        if (principalMatch && actionMatch && resourceMatch && conditionsMatch && s.Effect === 'Allow') {
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

/**
 *
 * @param {BucketInfo} bucket - bucket object
 * @param {Account} account - account object
 * @param {array} apiNames - action names: operations to authorize
 * @param {string} apiMethod - the main API call
 * @param {number} inflight - inflight bytes
 * @param {Logger} log - logger
 * @param {function} callback - callback
 * @returns {boolean} - true if the quota is valid, false otherwise
 */
async function validateQuotas(bucket, account, apiNames, apiMethod, inflight, log, callback) {
    const bucketQuota = bucket.getQuota();
    const accountQuota = account?.quota || 0;
    let bucketQuotaExceeded = false;
    let accountQuotaExceeded = false;

    if ((bucketQuota <= 0 && accountQuota <= 0) || !ScubaClientInstance?.enabled) {
        if (bucketQuota > 0 || accountQuota > 0) {
            log.warn('quota is set for a bucket, but scuba is disabled', {
                bucketName: bucket.getName(),
            });
        }
        return callback();
    }
    const creationDate = new Date(bucket.getCreationDate()).getTime();

    try {
        // A potential optimiation, if inflights are disabled, is to only evaluate
        // the lowest quota.
        // eslint-disable-next-line no-restricted-syntax
        for (const apiName of apiNames) {
            let shouldEvaluateCopyObject = false;
            if (apiName === 'objectGet' && (apiMethod === 'objectCopy' || apiMethod === 'objectPutCopyPart')) {
                shouldEvaluateCopyObject = true;
                // eslint-disable-next-line no-param-reassign
                inflight = Math.abs(inflight);
            }
            if (!shouldEvaluateCopyObject && !actionNeedQuotaCheck[apiName]) {
                continue;
            }
            // eslint-disable-next-line no-await-in-loop
            const bucketMetrics = await ScubaClientInstance.getLatestMetrics('bucket',
                `${bucket.getName()}_${creationDate}`, null, {
                action: apiName,
                inflight,
            });

            if (bucketMetrics.bytesTotal > bucketQuota) {
                log.debug('Bucket quota exceeded', {
                    bucket: bucket.getName(),
                    action: apiName,
                    inflight,
                    quota: bucketQuota,
                    bytesTotal: bucketMetrics.bytesTotal,
                });
                bucketQuotaExceeded = true;
            }

            if (accountQuota > 0 && account?.account) {
                // eslint-disable-next-line no-await-in-loop
                const accountMetrics = await ScubaClientInstance.getLatestMetrics('account',
                    `${account.account}_${creationDate}`, null, {
                    action: apiName,
                    inflight,
                });

                if (accountMetrics.bytesTotal > account.quota) {
                    log.debug('Account quota exceeded', {
                        accountId: account.account,
                        action: apiName,
                        inflight,
                        quota: account.quota,
                        bytesTotal: accountMetrics.bytesTotal,
                    });
                    accountQuotaExceeded = true;
                }
            }
        }

        if (bucketQuotaExceeded || accountQuotaExceeded) {
            if (apiMethod?.endsWith('Delete')) {
                return callback();
            }
            // clean any inflight bytes
            if (inflight > 0) {
                // eslint-disable-next-line no-await-in-loop
                await ScubaClientInstance.getLatestMetrics('bucket',
                    `${bucket.getName()}_${creationDate}`, null, {
                    action: apiMethod,
                    inflight: -inflight,
                });
                if (account?.quota) {
                    // eslint-disable-next-line no-await-in-loop
                    await ScubaClientInstance.getLatestMetrics('account',
                        `${account.account}_${creationDate}`, null, {
                        action: apiMethod,
                        inflight: -inflight,
                    });
                }
            }
            return callback(errors.QuotaExceeded);
        }

        return callback();
    } catch (err) {
        log.warn('Error getting metrics from scuba, allowing the request', {
            error: err.name,
            description: err.message,
        });
        if (bucketQuotaExceeded || accountQuotaExceeded) {
            return callback(errors.QuotaExceeded);
        }
        return callback();
    }
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
                    return errors.MalformedPolicy.customizeDescription('Policy has an invalid condition key');
                }
                if (validCondition && validCondition.conditionValueTypeChecker) {
                    const conditionValueTypeError = validCondition.conditionValueTypeChecker(conditionValue);
                    if (conditionValueTypeError) {
                        return errors.MalformedPolicy.customizeDescription(conditionValueTypeError);
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
    validateQuotas,
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
};
