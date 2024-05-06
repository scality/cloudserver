const async = require('async');
const { errors } = require('arsenal');
const monitoring = require('../../../utilities/monitoringHandler');
const {
    actionNeedQuotaCheckCopy,
    actionNeedQuotaCheck,
    actionWithDataDeletion,
} = require('arsenal').policies;
const { config } = require('../../../Config');
const QuotaService = require('../../../quotas/quotas');

/**
 * Process the bytes to write based on the request and object metadata
 * @param {string} apiMethod - api method
 * @param {BucketInfo} bucket - bucket info
 * @param {string} versionId - version id of the object
 * @param {number} contentLength - content length of the object
 * @param {object} objMD - object metadata
 * @param {object} destObjMD - destination object metadata
 * @return {number} processed content length
 */
function processBytesToWrite(apiMethod, bucket, versionId, contentLength, objMD, destObjMD = null) {
    let bytes = contentLength;
    if (apiMethod === 'objectRestore') {
        // object is being restored
        bytes = Number.parseInt(objMD['content-length'], 10);
    } else if (!bytes && objMD?.['content-length']) {
        if (apiMethod === 'objectCopy' || apiMethod === 'objectPutCopyPart') {
            if (!destObjMD || bucket.isVersioningEnabled()) {
                // object is being copied
                bytes = Number.parseInt(objMD['content-length'], 10);
            } else if (!bucket.isVersioningEnabled()) {
                // object is being copied and replaces the target
                bytes = Number.parseInt(objMD['content-length'], 10) -
                    Number.parseInt(destObjMD['content-length'], 10);
            }
        } else if (!bucket.isVersioningEnabled() || bucket.isVersioningEnabled() && versionId) {
            // object is being deleted
            bytes = -Number.parseInt(objMD['content-length'], 10);
        }
    } else if (bytes && objMD?.['content-length'] && !bucket.isVersioningEnabled()) {
        // object is being replaced: store the diff, if the bucket is not versioned
        bytes = bytes - Number.parseInt(objMD['content-length'], 10);
    }
    return bytes || 0;
}

/**
 * Checks if a metric is stale based on the provided parameters.
 *
 * @param {Object} metric - The metric object to check.
 * @param {string} resourceType - The type of the resource.
 * @param {string} resourceName - The name of the resource.
 * @param {string} action - The action being performed.
 * @param {number} inflight - The number of inflight requests.
 * @param {Object} log - The logger object.
 * @returns {boolean} Returns true if the metric is stale, false otherwise.
 */
function isMetricStale(metric, resourceType, resourceName, action, inflight, log) {
    if (metric.date && Date.now() - new Date(metric.date).getTime() >
        QuotaService.maxStaleness) {
        log.warn('Stale metrics from the quota service, allowing the request', {
            resourceType,
            resourceName,
            action,
            inflight,
        });
        monitoring.requestWithQuotaMetricsUnavailable.inc();
        return true;
    }
    return false;
}

/**
 *
 * @param {Request} request - request object
 * @param {BucketInfo} bucket - bucket object
 * @param {Account} account - account object
 * @param {array} apiNames - action names: operations to authorize
 * @param {string} apiMethod - the main API call
 * @param {number} inflight - inflight bytes
 * @param {Logger} log - logger
 * @param {function} callback - callback function
 * @returns {boolean} - true if the quota is valid, false otherwise
 */
function validateQuotas(request, bucket, account, apiNames, apiMethod, inflight, log, callback) {
    if (!config.isQuotaEnabled() || !inflight) {
        return callback(null);
    }
    let type;
    let bucketQuotaExceeded = false;
    let accountQuotaExceeded = false;
    let quotaEvaluationDuration;
    const requestStartTime = process.hrtime.bigint();
    const bucketQuota = bucket.getQuota();
    const accountQuota = account?.quota || 0;
    const shouldSendInflights = config.isQuotaInflightEnabled();

    if (bucketQuota && accountQuota) {
        type = 'bucket+account';
    } else if (bucketQuota) {
        type = 'bucket';
    } else {
        type = 'account';
    }

    if (actionWithDataDeletion[apiMethod]) {
        type = 'delete';
    }

    if ((bucketQuota <= 0 && accountQuota <= 0) || !QuotaService?.enabled) {
        if (bucketQuota > 0 || accountQuota > 0) {
            log.warn('quota is set for a bucket, but the quota service is disabled', {
                bucketName: bucket.getName(),
            });
            monitoring.requestWithQuotaMetricsUnavailable.inc();
        }
        return callback(null);
    }
    const creationDate = new Date(bucket.getCreationDate()).getTime();

    return async.forEach(apiNames, (apiName, done) => {
        // Object copy operations first check the target object,
        // meaning the source object, containing the current bytes,
        // is checked second. This logic handles these APIs calls by
        // ensuring the bytes are positives (i.e., not an object
        // replacement).
        if (actionNeedQuotaCheckCopy(apiName, apiMethod)) {
            // eslint-disable-next-line no-param-reassign
            inflight = Math.abs(inflight);
        } else if (!actionNeedQuotaCheck[apiName] && !actionWithDataDeletion[apiName]) {
            return done();
        }
        // When inflights are disabled, the sum of the current utilization metrics
        // and the current bytes are compared with the quota. The current bytes
        // are not sent to the utilization service. When inflights are enabled,
        // the sum of the current utilization metrics only are compared with the
        // quota. They include the current inflight bytes sent in the request.
        const _inflights = shouldSendInflights ? { inflight } : undefined;
        const _inflightForCheck = shouldSendInflights ? 0 : inflight;
        return async.parallel({
            bucketQuota: parallelDone => {
                if (bucketQuota > 0) {
                    return QuotaService.getUtilizationMetrics('bucket',
                        `${bucket.getName()}_${creationDate}`, null, {
                        action: apiName,
                        ..._inflights,
                    }, (err, bucketMetrics) => {
                        if (err) {
                            return parallelDone(err);
                        }
                        if (!isMetricStale(bucketMetrics, 'bucket', bucket.getName(), apiName, inflight, log) &&
                            bucketMetrics.bytesTotal + _inflightForCheck > bucketQuota) {
                            log.debug('Bucket quota exceeded', {
                                bucket: bucket.getName(),
                                action: apiName,
                                inflight,
                                quota: bucketQuota,
                                bytesTotal: bucketMetrics.bytesTotal,
                            });
                            bucketQuotaExceeded = true;
                        }
                        return parallelDone();
                    });
                }
                return parallelDone();
            },
            accountQuota: parallelDone => {
                if (accountQuota > 0 && account?.account) {
                    return QuotaService.getUtilizationMetrics('account',
                        account.account, null, {
                        action: apiName,
                        ..._inflights,
                    }, (err, accountMetrics) => {
                        if (err) {
                            return parallelDone(err);
                        }
                        if (!isMetricStale(accountMetrics, 'account', account.account, apiName, inflight, log) &&
                            accountMetrics.bytesTotal + _inflightForCheck > accountQuota) {
                            log.debug('Account quota exceeded', {
                                accountId: account.account,
                                action: apiName,
                                inflight,
                                quota: accountQuota,
                                bytesTotal: accountMetrics.bytesTotal,
                            });
                            accountQuotaExceeded = true;
                        }
                        return parallelDone();
                    });
                }
                return parallelDone();
            },
        }, err => {
            if (err) {
                return done(err);
            }
            // Inflights are inverted: in case of cleanup, we just re-issue
            // the same API call.
            if (_inflights) {
                _inflights.inflight = -_inflights.inflight;
            }
            const finalizeQuotas = (errorFromAPI, _done) => {
                // Quotas are cleaned only in case of error in the API
                const quotaCleanUpStartTime = process.hrtime.bigint();
                const code = (bucketQuotaExceeded || accountQuotaExceeded) ? 429 : 200;
                if (errorFromAPI) {
                    return async.parallel({
                        deleteBucketInflights: parallelDone => {
                            if (bucketQuota > 0) {
                                return QuotaService.getUtilizationMetrics('bucket',
                                    `${bucket.getName()}_${creationDate}`, null, {
                                    action: apiName,
                                    ..._inflights,
                                }, parallelDone);
                            }
                            return parallelDone();
                        },
                        deleteAccountInflights: parallelDone => {
                            if (accountQuota > 0 && account?.account) {
                                return QuotaService.getUtilizationMetrics('account',
                                    account.account, null, {
                                    action: apiName,
                                    ..._inflights,
                                }, parallelDone);
                            }
                            return parallelDone();
                        },
                    }, err => {
                        if (err) {
                            log.warn('Error deleting inflights from the quota service', {
                                error: err.name,
                                description: err.message,
                            });
                        }
                        const quotaCleanUpDuration = Number(process.hrtime.bigint() - quotaCleanUpStartTime);
                        monitoring.quotaEvaluationDuration.labels({
                            action: apiMethod,
                            type,
                            code,
                        }).observe((quotaEvaluationDuration + quotaCleanUpDuration) / 1e9);
                        return _done();
                    });
                }
                monitoring.quotaEvaluationDuration.labels({
                    action: apiMethod,
                    type,
                    code,
                }).observe(quotaEvaluationDuration / 1e9);
                return _done();
            };
            request.finalizerHooks.push(finalizeQuotas);
            return done();
        });
    }, err => {
        quotaEvaluationDuration = Number(process.hrtime.bigint() - requestStartTime);
        if (err) {
            log.warn('Error getting metrics from the quota service, allowing the request', {
                error: err.name,
                description: err.message,
            });
        }
        if (!actionWithDataDeletion[apiMethod] &&
            (bucketQuotaExceeded || accountQuotaExceeded)) {
            return callback(errors.QuotaExceeded);
        }
        return callback();
    });
}

module.exports = {
    processBytesToWrite,
    isMetricStale,
    validateQuotas,
};
