const { errors } = require('arsenal');
const moment = require('moment');
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
            'x-amz-object-lock-mode must both be supplied'
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
 * isObjectLocked - checks whether object is locked or not
 * @param {obect} bucket - bucket metadata
 * @param {object} objectMD - object metadata
 * @param {array} headers - request headers
 * @return {boolean} - indicates whether object is locked or not
 */
function isObjectLocked(bucket, objectMD, headers) {
    if (bucket.isObjectLockEnabled()) {
        const objectLegalHold = objectMD.legalHold;
        if (objectLegalHold) {
            return true;
        }
        const retentionMode = objectMD.retentionMode;
        const retentionDate = objectMD.retentionDate;
        if (!retentionMode || !retentionDate) {
            return false;
        }
        if (retentionMode === 'GOVERNANCE' &&
        headers['x-amz-bypass-governance-retention']) {
            return false;
        }
        const objectDate = moment(retentionDate);
        const now = moment();
        // indicates retain until date has expired
        if (now.isSameOrAfter(objectDate)) {
            return false;
        }
        return true;
    }
    return false;
}


/* objectLockRequiresBypass will return true if the retention info change
 * would require a bypass governance flag to be true.
 * In order for this to be true the action must be valid as well, so going from
 * COMPLIANCE to GOVERNANCE would return false unless it expired.
 */
function objectLockRequiresBypass(objectMD, retentionInfo) {
    const { retentionMode: existingMode, retentionDate: existingDateISO } = objectMD;
    if (!existingMode) {
        return false;
    }

    const existingDate = new Date(existingDateISO);
    const isExpired = existingDate < Date.now();
    const isExtended = new Date(retentionInfo.date) > existingDate;

    if (existingMode === 'GOVERNANCE' && !isExpired) {
        if (retentionInfo.mode === 'GOVERNANCE' && isExtended) {
            return false;
        }
        return true;
    }

    // an invalid retention change or unrelated to bypass
    return false;
}

function validateObjectLockUpdate(objectMD, retentionInfo, bypassGovernance) {
    const { retentionMode: existingMode, retentionDate: existingDateISO } = objectMD;
    if (!existingMode) {
        return null;
    }

    const existingDate = new Date(existingDateISO);
    const isExpired = existingDate < Date.now();
    const isExtended = new Date(retentionInfo.date) > existingDate;

    if (existingMode === 'GOVERNANCE' && !isExpired && !bypassGovernance) {
        if (retentionInfo.mode === 'GOVERNANCE' && isExtended) {
            return null;
        }
        return errors.AccessDenied;
    }

    if (existingMode === 'COMPLIANCE') {
        if (retentionInfo.mode === 'GOVERNANCE' && !isExpired) {
            return errors.AccessDenied;
        }

        if (!isExtended) {
            return errors.AccessDenied;
        }
    }

    return null;
}

module.exports = {
    calculateRetainUntilDate,
    compareObjectLockInformation,
    setObjectLockInformation,
    isObjectLocked,
    validateHeaders,
    validateObjectLockUpdate,
    objectLockRequiresBypass,
};
