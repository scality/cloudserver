const { promisify } = require('util');
const { errorInstances } = require('arsenal');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const metadata = require('../metadata/wrapper');
const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/monitoringHandler');
const { parseString } = require('xml2js');
const { config } = require('../Config');
const constants = require('../../constants');

const listObjects = promisify((...args) => metadata.listObject(...args));
const updateBucketMD = promisify((...args) => metadata.updateBucket(...args));
const initializeBucketCapacity = promisify((...args) => metadata.initializeBucketCapacity(...args));
const validateBucket = promisify(standardMetadataValidateBucket);
const parseStringAsync = promisify(parseString);

function validateBucketQuotaProperty(requestBody) {
    let quota = requestBody.quota;
    if (quota === undefined) {
        quota = requestBody.QuotaConfiguration?.Quota;
    }
    const quotaValue = parseInt(quota, 10);
    if (Number.isNaN(quotaValue)) {
        throw errorInstances.InvalidArgument.customizeDescription('Quota Value should be a number');
    }
    if (quotaValue <= 0) {
        throw errorInstances.InvalidArgument.customizeDescription('Quota value must be a positive number');
    }
    return quotaValue;
}

async function parseRequestBody(requestBody, contentType) {
    if (contentType === 'application/xml') {
        try {
            return await parseStringAsync(requestBody, { explicitArray: false });
        } catch {
            throw errorInstances.InvalidArgument.customizeDescription('Invalid XML format');
        }
    }
    try {
        const jsonData = JSON.parse(requestBody);
        if (typeof jsonData !== 'object') {
            throw new Error('Invalid JSON');
        }
        return jsonData;
    } catch {
        throw errorInstances.InvalidArgument.customizeDescription('Request body must be a JSON object');
    }
}

async function bucketHasInProgressMpus(bucketName, log) {
    const mpuBucketName = `${constants.mpuBucketPrefix}${bucketName}`;
    try {
        // Mirror bucketDelete's check: one overview key per in-progress upload,
        // bounded to a single key. A missing shadow bucket means none.
        const list = await listObjects(mpuBucketName, { prefix: 'overview', maxKeys: 1 }, log);
        return (list.Contents?.length ?? 0) > 0;
    } catch (err) {
        if (err.is?.NoSuchBucket) {
            return false;
        }
        throw err;
    }
}

async function seedEmptyBucketCapacity(bucket, log) {
    if (!config.isQuotaEnabled()) {
        return;
    }
    const bucketName = bucket.getName();
    try {
        const list = await listObjects(bucketName, { maxKeys: 1, listingType: 'DelimiterVersions' }, log);
        const hasObjects = (list.Versions?.length ?? 0) + (list.DeleteMarkers?.length ?? 0) > 0;
        if (hasObjects) {
            return;
        }
        // A DelimiterVersions listing does not see in-progress MPU parts (shadow
        // bucket), which hold real storage and are counted by count-items; seeding
        // zero for a bucket with only uncommitted uploads would under-enforce.
        if (await bucketHasInProgressMpus(bucketName, log)) {
            return;
        }
        await initializeBucketCapacity(bucketName, bucket.getCreationDate(), log);
    } catch (err) {
        // Best-effort: a missing metric self-heals at the next count-items run,
        // so a seeding failure must never fail the quota update.
        log.warn('error seeding bucket quota capacity metric', { error: err });
    }
}

async function bucketUpdateQuota(authInfo, request, log, callback) {
    if (callback) {
        return bucketUpdateQuota(authInfo, request, log).then(
            corsHeaders => callback(null, corsHeaders),
            err => callback(err, err.code, err.additionalResHeaders),
        );
    }

    log.debug('processing request', { method: 'bucketUpdateQuota' });
    const { bucketName } = request;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: request.apiMethods || 'bucketUpdateQuota',
        request,
    };

    let bucket;
    try {
        bucket = await validateBucket(metadataValParams, request.actionImplicitDenies, log);
        const requestBody = await parseRequestBody(request.post, request.headers['content-type']);
        const quotaValue = validateBucketQuotaProperty(requestBody);
        // Seed before enabling the quota so the metric exists before quotas are enforced. A PUT racing
        // between the emptiness check and this call leaves a stale zero that self-heals at the next count-items run.
        await seedEmptyBucketCapacity(bucket, log);
        bucket.setQuota(quotaValue);
        await updateBucketMD(bucket.getName(), bucket, log);
    } catch (err) {
        const corsHeaders = collectCorsHeaders(request.headers.origin, request.method, bucket);
        log.debug('error processing request', {
            error: err,
            method: 'bucketUpdateQuota',
        });
        monitoring.promMetrics('PUT', bucketName, err.code, 'updateBucketQuota');
        err.additionalResHeaders = err.additionalResHeaders || corsHeaders;
        throw err;
    }

    const corsHeaders = collectCorsHeaders(request.headers.origin, request.method, bucket);
    monitoring.promMetrics('PUT', bucketName, '200', 'updateBucketQuota');
    pushMetric('updateBucketQuota', log, {
        authInfo,
        bucket: bucketName,
    });
    return corsHeaders;
}

module.exports = bucketUpdateQuota;
