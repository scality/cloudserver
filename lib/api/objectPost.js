const async = require('async');
const { errors, versioning } = require('arsenal');

const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const createAndStoreObject = require('./apiUtils/object/createAndStoreObject');
const { standardMetadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const { config } = require('../Config');
const { setExpirationHeaders } = require('./apiUtils/object/expirationHeaders');
const monitoring = require('../utilities/metrics');
const writeContinue = require('../utilities/writeContinue');
const { overheadField } = require('../../constants');


const versionIdUtils = versioning.VersionID;


/**
 * POST Object in the requested bucket. Steps include:
 * validating metadata for authorization, bucket and object existence etc.
 * store object data in datastore upon successful authorization
 * store object location returned by datastore and
 * object's (custom) headers in metadata
 * return the result in final callback
 *
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {request} request - request object given by router,
 *                            includes normalized headers
 * @param {object | undefined } streamingV4Params - if v4 auth,
 * object containing accessKey, signatureFromRequest, region, scopeDate,
 * timestamp, and credentialScope
 * (to be used for streaming v4 auth if applicable)
 * @param {object} log - the log request
 * @param {Function} callback - final callback to call with the result
 * @return {undefined}
 */
function objectPost(authInfo, request, streamingV4Params, log, callback) {
    const {
        headers,
        method,
        formData,
        bucketName,
    } = request;
    const requestType = request.apiMethods || 'objectPost';
    const valParams = {
        authInfo,
        bucketName,
        objectKey: formData.key,
        requestType,
        request,
    };
    const canonicalID = authInfo.getCanonicalID();

    log.trace('owner canonicalID to send to data', { canonicalID });
    return standardMetadataValidateBucketAndObj(valParams, request.actionImplicitDenies, log,
        (err, bucket, objMD) => {
            const responseHeaders = collectCorsHeaders(headers.origin,
                method, bucket);

            // TODO RING-45960 remove accessdenied skip
            if (err && !err.AccessDenied) {
                log.trace('error processing request', {
                    error: err,
                    method: 'metadataValidateBucketAndObj',
                });
                monitoring.promMetrics('POST', request.bucketName, err.code, 'postObject');
                return callback(err, responseHeaders);
            }
            if (bucket.hasDeletedFlag() && canonicalID !== bucket.getOwner()) {
                log.trace('deleted flag on bucket and request ' +
                    'from non-owner account');
                monitoring.promMetrics('POST', request.bucketName, 404, 'postObject');
                return callback(errors.NoSuchBucket);
            }

            return async.waterfall([
                function objectCreateAndStore(next) {
                    writeContinue(request, request._response);
                    return createAndStoreObject(request.bucketName,
                        bucket, request.formData.key, objMD, authInfo, canonicalID, null,
                        request, false, streamingV4Params, overheadField, log, next);
                },
            ], (err, storingResult) => {
                if (err) {
                    monitoring.promMetrics('POST', request.bucketName, err.code,
                        'postObject');
                    return callback(err, responseHeaders);
                }
                setExpirationHeaders(responseHeaders, {
                    lifecycleConfig: bucket.getLifecycleConfiguration(),
                    objectParams: {
                        key: request.key,
                        date: storingResult.lastModified,
                        tags: storingResult.tags,
                    },
                });
                if (storingResult) {
                    // ETag's hex should always be enclosed in quotes
                    responseHeaders.Key = request.formData.key;
                    responseHeaders.location = `/${bucketName}/${request.formData.key}`;
                    responseHeaders.Bucket = bucketName;
                    responseHeaders.ETag = `"${storingResult.contentMD5}"`;
                }
                const vcfg = bucket.getVersioningConfiguration();
                const isVersionedObj = vcfg && vcfg.Status === 'Enabled';
                if (isVersionedObj) {
                    if (storingResult && storingResult.versionId) {
                        responseHeaders['x-amz-version-id'] =
                            versionIdUtils.encode(storingResult.versionId,
                                config.versionIdEncodingType);
                    }
                }

                return callback(null, responseHeaders);
            });
        });
}

module.exports = objectPost;
