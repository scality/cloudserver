const { waterfall } = require('async');
const { errors } = require('arsenal');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const metadata = require('../metadata/wrapper');
const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/monitoringHandler');
const { parseString } = require('xml2js');

function validateBucketQuotaProperty(requestBody, next) {
    const quota = requestBody.quota;
    const quotaValue = parseInt(quota, 10);
    if (Number.isNaN(quotaValue)) {
        return next(errors.InvalidArgument.customizeDescription('Quota Value should be a number'));
    }
    if (quotaValue <= 0) {
        return next(errors.InvalidArgument.customizeDescription('Quota value must be a positive number'));
    }
    return next(null, quotaValue);
}

function parseRequestBody(requestBody, next) {
    try {
        const jsonData = JSON.parse(requestBody);
        if (typeof jsonData !== 'object') {
            throw new Error('Invalid JSON');
        }
        return next(null, jsonData);
    } catch (jsonError) {
        return parseString(requestBody, (xmlError, xmlData) => {
            if (xmlError) {
                return next(errors.InvalidArgument.customizeDescription('Request body must be a JSON object'));
            }
            return next(null, xmlData);
        });
    }
}

function bucketUpdateQuota(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketUpdateQuota' });

    const { bucketName } = request;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: request.apiMethods || 'bucketUpdateQuota',
        request,
    };
    let bucket = null;
    return waterfall([
        next => standardMetadataValidateBucket(metadataValParams, request.actionImplicitDenies, log,
            (err, b) => {
                bucket = b;
                return next(err, bucket);
            }),
        (bucket, next) => parseRequestBody(request.post, (err, requestBody) => next(err, bucket, requestBody)),
        (bucket, requestBody, next) => validateBucketQuotaProperty(requestBody, (err, quotaValue) =>
            next(err, bucket, quotaValue)),
        (bucket, quotaValue, next) => {
            bucket.setQuota(quotaValue);
            return metadata.updateBucket(bucket.getName(), bucket, log, next);
        },
    ], (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.debug('error processing request', {
                error: err,
                method: 'bucketUpdateQuota'
            });
            monitoring.promMetrics('PUT', bucketName, err.code,
                'updateBucketQuota');
            return callback(err, err.code, corsHeaders);
        }
        monitoring.promMetrics(
            'PUT', bucketName, '200', 'updateBucketQuota');
        pushMetric('updateBucketQuota', log, {
            authInfo,
            bucket: bucketName,
        });
        return callback(null, corsHeaders);
    });
}

module.exports = bucketUpdateQuota;
