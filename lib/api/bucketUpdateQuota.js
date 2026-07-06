const { waterfall } = require('async');
const { errorInstances } = require('arsenal');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const metadata = require('../metadata/wrapper');
const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/monitoringHandler');
const { parseString } = require('xml2js');
const { config } = require('../Config');

function validateBucketQuotaProperty(requestBody, next) {
    let quota = requestBody.quota;
    if (quota === undefined) {
        quota = requestBody.QuotaConfiguration?.Quota;
    }
    const quotaValue = parseInt(quota, 10);
    if (Number.isNaN(quotaValue)) {
        return next(errorInstances.InvalidArgument.customizeDescription('Quota Value should be a number'));
    }
    if (quotaValue <= 0) {
        return next(errorInstances.InvalidArgument.customizeDescription('Quota value must be a positive number'));
    }
    return next(null, quotaValue);
}

function parseRequestBody(requestBody, contentType, next) {
    switch (contentType) {
        case 'application/xml':
            return parseString(requestBody, { explicitArray: false }, (xmlError, xmlData) => {
                if (xmlError) {
                    return next(errorInstances.InvalidArgument.customizeDescription('Invalid XML format'));
                }
                return next(null, xmlData);
            });
        case 'application/json':
        default:
            try {
                const jsonData = JSON.parse(requestBody);
                if (typeof jsonData !== 'object') {
                    throw new Error('Invalid JSON');
                }
                return next(null, jsonData);
            } catch {
                return next(errorInstances.InvalidArgument.customizeDescription('Request body must be a JSON object'));
            }
    }
}

function seedEmptyBucketCapacity(bucket, log, done) {
    if (!config.isQuotaEnabled()) {
        return done();
    }
    const bucketName = bucket.getName();
    return metadata.listObject(bucketName, { maxKeys: 1, listingType: 'DelimiterVersions' }, log, (err, list) => {
        if (err) {
            log.error('error listing bucket for quota capacity seed', { error: err });
            return done();
        }
        const isEmpty =
            (list.Versions ? list.Versions.length : 0) + (list.DeleteMarkers ? list.DeleteMarkers.length : 0) === 0;
        if (!isEmpty) {
            return done();
        }
        return metadata.initializeBucketCapacity(bucketName, bucket.getCreationDate(), log, seedErr => {
            if (seedErr) {
                log.error('error seeding bucket quota capacity metric', { error: seedErr });
            }
            return done();
        });
    });
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
    return waterfall(
        [
            next =>
                standardMetadataValidateBucket(metadataValParams, request.actionImplicitDenies, log, (err, b) => {
                    bucket = b;
                    return next(err, bucket);
                }),
            (bucket, next) =>
                parseRequestBody(request.post, request.headers['content-type'], (err, requestBody) =>
                    next(err, bucket, requestBody),
                ),
            (bucket, requestBody, next) =>
                validateBucketQuotaProperty(requestBody, (err, quotaValue) => next(err, bucket, quotaValue)),
            (bucket, quotaValue, next) => {
                bucket.setQuota(quotaValue);
                return metadata.updateBucket(bucket.getName(), bucket, log, (err, res) => next(err, bucket, res));
            },
            (bucket, res, next) => seedEmptyBucketCapacity(bucket, log, () => next(null, res)),
        ],
        (err, bucket) => {
            const corsHeaders = collectCorsHeaders(request.headers.origin, request.method, bucket);
            if (err) {
                log.debug('error processing request', {
                    error: err,
                    method: 'bucketUpdateQuota',
                });
                monitoring.promMetrics('PUT', bucketName, err.code, 'updateBucketQuota');
                return callback(err, err.code, corsHeaders);
            }
            monitoring.promMetrics('PUT', bucketName, '200', 'updateBucketQuota');
            pushMetric('updateBucketQuota', log, {
                authInfo,
                bucket: bucketName,
            });
            return callback(null, corsHeaders);
        },
    );
}

module.exports = bucketUpdateQuota;
