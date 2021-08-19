const { waterfall } = require('async');
const uuid = require('uuid/v4');
const LifecycleConfiguration =
    require('arsenal').models.LifecycleConfiguration;

const parseXML = require('../utilities/parseXML');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const metadata = require('../metadata/wrapper');
const { metadataValidateBucket } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');

/**
 * Bucket Put Lifecycle - Create or update bucket lifecycle configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */

function bucketPutLifecycle(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketPutLifecycle' });

    const bucketName = request.bucketName;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketPutLifecycle',
        request,
    };
    return waterfall([
        next => parseXML(request.post, log, next),
        (parsedXml, next) => {
            const lcConfigClass = new LifecycleConfiguration(parsedXml);
            // if there was an error getting lifecycle configuration,
            // returned configObj will contain 'error' key
            process.nextTick(() => {
                const configObj = lcConfigClass.getLifecycleConfiguration();
                if (configObj.error) {
                    return next(configObj.error);
                }
                return next(null, configObj);
            });
        },
        (lcConfig, next) => metadataValidateBucket(metadataValParams, log,
            (err, bucket) => {
                if (err) {
                    return next(err, bucket);
                }
                return next(null, bucket, lcConfig);
            }),
        (bucket, lcConfig, next) => {
            if (!bucket.getUid()) {
                bucket.setUid(uuid());
            }
            bucket.setLifecycleConfiguration(lcConfig);
            metadata.updateBucket(bucket.getName(), bucket, log, err =>
                next(err, bucket));
        },
    ], (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.trace('error processing request', { error: err,
                method: 'bucketPutLifecycle' });
            return callback(err, corsHeaders);
        }
        pushMetric('putBucketLifecycle', log, {
            authInfo,
            bucket: bucketName,
        });
        return callback(null, corsHeaders);
    });
}

module.exports = bucketPutLifecycle;
