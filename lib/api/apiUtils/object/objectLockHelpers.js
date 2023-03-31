const { errors, auth, policies } = require('arsenal');
const moment = require('moment');

const { config } = require('../../../Config');
const vault = require('../../../auth/vault');

/**
 * Calculates retain until date for the locked object version
 * @param {object} retention - includes days or years retention period
 * @return {object} the date until the object version remains locked
 */
function calculateRetainUntilDate(retention) {
    const { days, years } = retention;

    if (!days && !years) {
        return undefined;
    }

    const date = moment();
    // Calculate the number of days to retain the lock on the object
    const retainUntilDays = days || years * 365;
    const retainUntilDate
        = date.add(retainUntilDays, 'days');
    return retainUntilDate.toISOString();
}
/**
 * Validates object lock headers
 * @param {object} bucket - bucket metadata
 * @param {object} headers - request headers
 * @param {object} log - the log request
 * @return {object} - object with error if validation fails
 */
function validateHeaders(bucket, headers, log) {
    const bucketObjectLockEnabled = bucket.isObjectLockEnabled();
    const objectLegalHold = headers['x-amz-object-lock-legal-hold'];
    const objectLockDate = headers['x-amz-object-lock-retain-until-date'];
    const objectLockMode = headers['x-amz-object-lock-mode'];
    // If retention headers or legal hold header present but
    // object lock is not enabled on the bucket return error
    if ((objectLockDate || objectLockMode || objectLegalHold)
        && !bucketObjectLockEnabled) {
        log.trace('bucket is missing ObjectLockConfiguration');
        return errors.InvalidRequest.customizeDescription(
            'Bucket is missing ObjectLockConfiguration');
    }
    if ((objectLockMode || objectLockDate) &&
        !(objectLockMode && objectLockDate)) {
        return errors.InvalidArgument.customizeDescription(
            'x-amz-object-lock-retain-until-date and ' +
            'x-amz-object-lock-mode must both be supplied',
        );
    }
    const validModes = new Set(['GOVERNANCE', 'COMPLIANCE']);
    if (objectLockMode && !validModes.has(objectLockMode)) {
        return errors.InvalidArgument.customizeDescription(
            'Unknown wormMode directive');
    }
    const validLegalHolds = new Set(['ON', 'OFF']);
    if (objectLegalHold && !validLegalHolds.has(objectLegalHold)) {
        return errors.InvalidArgument.customizeDescription(
            'Legal hold status must be one of "ON", "OFF"');
    }
    const currentDate = new Date().toISOString();
    if (objectLockMode && objectLockDate <= currentDate) {
        return errors.InvalidArgument.customizeDescription(
            'The retain until date must be in the future!');
    }
    return null;
}

/**
 * Compares new object retention to bucket default retention
 * @param {object} headers - request headers
 * @param {object} defaultRetention - bucket retention configuration
 * @return {object} - final object lock information to set on object
 */
function compareObjectLockInformation(headers, defaultRetention) {
    const objectLockInfoToSave = {};

    if (defaultRetention && defaultRetention.rule) {
        const defaultMode = defaultRetention.rule.mode;
        const defaultTime = calculateRetainUntilDate(defaultRetention.rule);
        if (defaultMode && defaultTime) {
            objectLockInfoToSave.retentionInfo = {
                mode: defaultMode,
                date: defaultTime,
            };
        }
    }

    if (headers) {
        const headerMode = headers['x-amz-object-lock-mode'];
        const headerDate = headers['x-amz-object-lock-retain-until-date'];
        if (headerMode && headerDate) {
            objectLockInfoToSave.retentionInfo = {
                mode: headerMode,
                date: headerDate,
            };
        }
        const headerLegalHold = headers['x-amz-object-lock-legal-hold'];
        if (headerLegalHold) {
            const legalHold = headerLegalHold === 'ON';
            objectLockInfoToSave.legalHold = legalHold;
        }
    }

    return objectLockInfoToSave;
}

/**
 * Sets object retention ond/or legal hold information on object's metadata
 * @param {object} headers - request headers
 * @param {object} md - object metadata
 * @param {(object|null)} defaultRetention - bucket retention configuration if
 * bucket has any configuration set
 * @return {undefined}
 */
function setObjectLockInformation(headers, md, defaultRetention) {
    // Stores retention information if object either has its own retention
    // configuration or default retention configuration from its bucket
    const finalObjectLockInfo =
        compareObjectLockInformation(headers, defaultRetention);
    if (finalObjectLockInfo.retentionInfo) {
        md.setRetentionMode(finalObjectLockInfo.retentionInfo.mode);
        md.setRetentionDate(finalObjectLockInfo.retentionInfo.date);
    }
    if (finalObjectLockInfo.legalHold || finalObjectLockInfo.legalHold === false) {
        md.setLegalHold(finalObjectLockInfo.legalHold);
    }
}

/**
 *  Helper class for check object lock state checks
 */
class ObjectLockInfo {
    /**
     *
     * @param {object} retentionInfo - The object lock retention policy
     * @param {"GOVERNANCE" | "COMPLIANCE" | null} retentionInfo.mode - Retention policy mode.
     * @param {string} retentionInfo.date - Expiration date of retention policy. A string in ISO-8601 format
     * @param {bool} retentionInfo.legalHold - Whether a legal hold is enable for the object
     */
    constructor(retentionInfo) {
        this.mode = retentionInfo.mode || null;
        this.date = retentionInfo.date || null;
        this.legalHold = retentionInfo.legalHold || false;
    }

    /**
     * ObjectLockInfo.isLocked
     * @returns {bool} - Whether the retention policy is active and protecting the object
     */
    isLocked() {
        if (this.legalHold) {
            return true;
        }

        if (!this.mode || !this.date) {
            return false;
        }

        return !this.isExpired();
    }

    /**
     * ObjectLockInfo.isGovernanceMode
     * @returns {bool} - true if retention mode is GOVERNANCE
     */
    isGovernanceMode() {
        return this.mode === 'GOVERNANCE';
    }

    /**
     * ObjectLockInfo.isComplianceMode
     * @returns {bool} - True if retention mode is COMPLIANCE
     */
    isComplianceMode() {
        return this.mode === 'COMPLIANCE';
    }

    /**
     * ObjectLockInfo.isExpired
     * @returns {bool} - True if the retention policy has expired
     */
    isExpired() {
        const now = moment();
        return this.date === null || now.isSameOrAfter(this.date);
    }

    /**
     * ObjectLockInfo.isExtended
     * @param {string} timestamp - Timestamp in ISO-8601 format
     * @returns {bool} - True if the given timestamp is after the policy expiration date or if no expiration date is set
     */
    isExtended(timestamp) {
        return timestamp !== undefined && (this.date === null || moment(timestamp).isSameOrAfter(this.date));
    }

    /**
     * ObjectLockInfo.canModifyObject
     * @param {bool} hasGovernanceBypass - Whether to bypass governance retention policies
     * @returns {bool} - True if the retention policy allows the objects data to be modified (overwritten/deleted)
     */
    canModifyObject(hasGovernanceBypass) {
        // can modify object if object is not locked
        // cannot modify object in any cases if legal hold is enabled
        // if not legal hold, can only modify object if bypass governance when locked
        if (!this.isLocked()) {
            return true;
        }
        return !this.legalHold && this.isGovernanceMode() && !!hasGovernanceBypass;
    }

    /**
     * ObjectLockInfo.canModifyPolicy
     * @param {object} policyChanges - Proposed changes to the retention policy
     * @param {"GOVERNANCE" | "COMPLIANCE" | undefined} policyChanges.mode - Retention policy mode.
     * @param {string} policyChanges.date - Expiration date of retention policy. A string in ISO-8601 format
     * @param {bool} hasGovernanceBypass - Whether to bypass governance retention policies
     * @returns {bool} - True if the changes are allowed to be applied to the retention policy
     */
    canModifyPolicy(policyChanges, hasGovernanceBypass) {
        // If an object does not have a retention policy or it is expired then all changes are allowed
        if (!this.isLocked()) {
            return true;
        }

        // The only allowed change in compliance mode is extending the retention period
        if (this.isComplianceMode()) {
            if (policyChanges.mode === 'COMPLIANCE' && this.isExtended(policyChanges.date)) {
                return true;
            }
        }

        if (this.isGovernanceMode()) {
            // Extensions are always allowed in governance mode
            if (policyChanges.mode === 'GOVERNANCE' && this.isExtended(policyChanges.date)) {
                return true;
            }

            // All other changes in governance mode require a bypass
            if (hasGovernanceBypass) {
                return true;
            }
        }

        return false;
    }
}

/**
 *
 * @param {object} headers - s3 request headers
 * @returns {bool} - True if the headers is present and === "true"
 */
function hasGovernanceBypassHeader(headers) {
    const bypassHeader = headers['x-amz-bypass-governance-retention'] || '';
    return bypassHeader.toLowerCase() === 'true';
}


/**
 * checkUserGovernanceBypass
 *
 *  Checks for the presence of the s3:BypassGovernanceRetention permission for a given user
 *
 * @param {object} request - Incoming s3 request
 * @param {object} authInfo - s3 authentication info
 * @param {object} bucketMD - bucket metadata
 * @param {string} objectKey - object key
 * @param {object} log - Werelogs logger
 * @param {function} cb - callback returns errors.AccessDenied if the authorization fails
 * @returns {undefined} -
 */
function checkUserGovernanceBypass(request, authInfo, bucketMD, objectKey, log, cb) {
    log.trace(
        'object in GOVERNANCE mode and is user, checking for attached policies',
        { method: 'checkUserPolicyGovernanceBypass' },
    );

    const authParams = auth.server.extractParams(request, log, 's3', request.query);
    const ip = policies.requestUtils.getClientIp(request, config);
    const requestContextParams = {
        constantParams: {
            headers: request.headers,
            query: request.query,
            generalResource: bucketMD.getName(),
            specificResource: { key: objectKey },
            requesterIp: ip,
            sslEnabled: request.connection.encrypted,
            apiMethod: 'bypassGovernanceRetention',
            awsService: 's3',
            locationConstraint: bucketMD.getLocationConstraint(),
            requesterInfo: authInfo,
            signatureVersion: authParams.params.data.signatureVersion,
            authType: authParams.params.data.authType,
            signatureAge: authParams.params.data.signatureAge,
        },
    };
    return vault.checkPolicies(requestContextParams,
        authInfo.getArn(), log, (err, authorizationResults) => {
            if (err) {
                return cb(err);
            }
            if (authorizationResults[0].isAllowed !== true) {
                log.trace('authorization check failed for user',
                    {
                        'method': 'checkUserPolicyGovernanceBypass',
                        's3:BypassGovernanceRetention': false,
                    });
                return cb(errors.AccessDenied);
            }
            return cb(null);
        });
}

module.exports = {
    calculateRetainUntilDate,
    compareObjectLockInformation,
    setObjectLockInformation,
    validateHeaders,
    hasGovernanceBypassHeader,
    checkUserGovernanceBypass,
    ObjectLockInfo,
};
