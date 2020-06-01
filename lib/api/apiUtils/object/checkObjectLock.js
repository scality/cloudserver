const { errors } = require('arsenal');

function checkObjectLock(bucket, headers) {
    const retentionObj = {};

    if (!bucket.isObjectLockEnabled()) {
        retentionObj.error = errors.InvalidRequest.customizeDescription(
            'Bucket is missing ObjectLockConfiguration');
        return retentionObj;
    }
    const headerMode = headers['x-amz-object-lock-mode'];
    const headerRetainDate = headers['x-amz-object-lock-retain-until-date'];
    const headerLegalHold = headers['x-amz-object-lock-legal-hold-status'];
    if ((headerMode && !headerRetainDate) || (!headerMode && headerRetainDate)) {
        retentionObj.error = errors.InvalidArgument.customizeDescription(
            'x-amz-object-lock-retain-until-date and x-amz-object-lock-mode ' +
            'must both be supplied');
        return retentionObj;
    }
    const validModes = new Set(['GOVERNANCE', 'COMPLIANCE']);
    if (headerMode && !validModes.has(headerMode)) {
        retentionObj.error = errors.InvalidArgument.customizeDescription(
            'Unknown wormMode directive');
        return retentionObj;
    }
    if (headerLegalHold && headerLegalHold !== 'ON') {
        // AWS behavior does not return an error in this case, but proceeds
        // without setting a legal hold. Returning an error will inform the
        // customer
        retentionObj.error = errors.InvalidArgument.customizeDescription(
            'Legal hold value must be "ON". To disable legal hold, omit the ' +
            'header');
        return retentionObj;
    }
    const objectLockConfig = bucket.getObjectLockConfiguration();
    const bucketMode = objectLockConfig.retentionInfo.mode;
    const bucketRetainDate = objectLockConfig.retentionInfo.retainDate;
    const bucketLegalHold = objectLockConfig.legalHold;
    retentionObj.retentionInfo = {};
    retentionObj.retentionInfo.mode = headerMode || bucketMode || '';
    retentionObj.retentionInfo.retainDate =
        headerRetainDate || bucketRetainDate || '';
    retentionObj.legalHold = headerLegalHold || bucketLegalHold || '';
    return retentionObj;
}

module.exports = checkObjectLock;
