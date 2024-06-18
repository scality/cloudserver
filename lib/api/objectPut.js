const async = require('async');
const { errors, versioning } = require('arsenal');
const opentelemetry = require('@opentelemetry/api');

const aclUtils = require('../utilities/aclUtils');
const { cleanUpBucket } = require('./apiUtils/bucket/bucketCreation');
const { getObjectSSEConfiguration } = require('./apiUtils/bucket/bucketEncryption');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const createAndStoreObject = require('./apiUtils/object/createAndStoreObject');
const { checkQueryVersionId } = require('./apiUtils/object/versioning');
const { standardMetadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const { validateHeaders } = require('./apiUtils/object/objectLockHelpers');
const { hasNonPrintables } = require('../utilities/stringChecks');
const kms = require('../kms/wrapper');
const { config } = require('../Config');
const { setExpirationHeaders } = require('./apiUtils/object/expirationHeaders');
const monitoring = require('../utilities/metrics');
const validateChecksumHeaders = require('./apiUtils/object/validateChecksumHeaders');

const writeContinue = require('../utilities/writeContinue');
const { overheadField } = require('../../constants');

const versionIdUtils = versioning.VersionID;

/**
 * PUT Object in the requested bucket. Steps include:
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
 * @param {object} authorizationResults - authorization results from
 * @param {object} oTel - OpenTelemetry methods
 * @return {undefined}
 */
function objectPut(authInfo, request, streamingV4Params, log, callback, authorizationResults, oTel) {
    const {
        cloudserverApiSpan,
        activeSpan,
        activeTracerContext,
        tracer,
    } = oTel;
    activeSpan.addEvent('Entered objectPut()');
    const cloudserverApiSpanContext = opentelemetry.trace.setSpan(
        activeTracerContext,
        cloudserverApiSpan,
    );
    return tracer.startActiveSpan('PutObject API:: Storing object in S3 after bucket policy checks', undefined, cloudserverApiSpanContext, objectPutSpan => {
        const objectPutSpanContext = opentelemetry.trace.setSpan(
            activeTracerContext,
            objectPutSpan,
        );
        objectPutSpan.setAttributes({
            'code.function': 'objectPut()',
            'code.filename': 'lib/api/objectPut.js',
            'code.lineno': 45,
        });
        log.debug('processing request', { method: 'objectPut' });
        const {
            bucketName,
            headers,
            method,
            objectKey,
            parsedContentLength,
            query,
        } = request;
        if (!aclUtils.checkGrantHeaderValidity(headers)) {
            activeSpan.recordException(errors.InvalidArgument);
            objectPutSpan.end();
            log.trace('invalid acl header');
            monitoring.promMetrics('PUT', request.bucketName, 400,
                'putObject');
            return callback(errors.InvalidArgument);
        }
        const queryContainsVersionId = checkQueryVersionId(query);
        if (queryContainsVersionId instanceof Error) {
            activeSpan.recordException(queryContainsVersionId);
            objectPutSpan.end();
            return callback(queryContainsVersionId);
        }
        const invalidSSEError = errors.InvalidArgument.customizeDescription(
            'The encryption method specified is not supported');
        const requestType = request.apiMethods || 'objectPut';
        const valParams = { authInfo, bucketName, objectKey, requestType, request };
        const canonicalID = authInfo.getCanonicalID();

        if (hasNonPrintables(objectKey)) {
            activeSpan.recordException(errors.InvalidInput);
            objectPutSpan.end();
            return callback(errors.InvalidInput.customizeDescription(
                'object keys cannot contain non-printable characters',
            ));
        }

        const checksumHeaderErr = validateChecksumHeaders(headers);
        if (checksumHeaderErr) {
            activeSpan.recordException(checksumHeaderErr);
            objectPutSpan.end();
            return callback(checksumHeaderErr);
        }

        log.trace('owner canonicalID to send to data', { canonicalID });
        const mdSpan = tracer.startSpan('metadataValidateBucketAndObj', undefined, objectPutSpanContext);
        const mdSpanContext = opentelemetry.trace.setSpan(
            objectPutSpanContext,
            mdSpan,
        );
        return standardMetadataValidateBucketAndObj(valParams, request.actionImplicitDenies, log,
        (err, bucket, objMD) => {
            mdSpan.end();
            activeSpan.addEvent('Metadata validation complete');
            activeSpan.addEvent('collecting Cors headers');
            const responseHeaders = collectCorsHeaders(headers.origin,
                method, bucket);
            activeSpan.addEvent('Cors headers collected');
            if (err) {
                log.trace('error processing request', {
                    error: err,
                    method: 'metadataValidateBucketAndObj',
                });
                monitoring.promMetrics('PUT', bucketName, err.code, 'putObject');
                activeSpan.recordException(err);
                objectPutSpan.end();
                return callback(err, responseHeaders);
            }
            activeSpan.addEvent('User passed bucket policy validation');
            if (bucket.hasDeletedFlag() && canonicalID !== bucket.getOwner()) {
                log.trace('deleted flag on bucket and request ' +
                    'from non-owner account');
                monitoring.promMetrics('PUT', bucketName, 404, 'putObject');
                activeSpan.recordException(errors.NoSuchBucket);
                objectPutSpan.end();
                return callback(errors.NoSuchBucket);
            }

            return async.waterfall([
                function handleTransientOrDeleteBuckets(next) {
                    if (bucket.hasTransientFlag() || bucket.hasDeletedFlag()) {
                        activeSpan.addEvent('Bucket is in transient or deleted state');
                        return tracer.startActiveSpan('Bucket check for transient or deleted state, cleaning bucket', undefined, objectPutSpanContext, currentSpan => {
                            currentSpan.setAttributes({
                                'code.function': 'objectPut()',
                                'code.filename': 'lib/api/objectPut.js',
                                'code.lineno': 168,
                            });
                            return cleanUpBucket(bucket, log, err => {
                                activeSpan.addEvent('Bucket cleanup complete');
                                currentSpan.end();
                                if (err) {
                                    activeSpan.recordException(err);
                                    objectPutSpan.end();
                                    return next(err);
                                }
                                return next();
                            });
                        });
                    }
                    return next(null);
                },
                next => tracer.startActiveSpan('Bucket check for Server side configuration - SSE', undefined, objectPutSpanContext, currentSpan => {
                    currentSpan.setAttributes({
                        'code.function': 'objectPut()',
                        'code.filename': 'lib/api/objectPut.js',
                        'code.lineno': 178,
                    });
                    return next(null, currentSpan);
                }),
                function getSSEConfig(currentSpan, next) {
                    return getObjectSSEConfiguration(headers, bucket, log,
                        (err, sseConfig) => {
                            if (err) {
                                log.error('error getting server side encryption config', { err });
                                activeSpan.recordException(invalidSSEError);
                                currentSpan.end();
                                objectPutSpan.end();
                                return next(invalidSSEError);
                            }
                            return next(null, sseConfig, currentSpan);
                        }
                    );
                },
                (sseConfig, currentSpan, next) => {
                    activeSpan.addEvent('Got server side encryption config');
                    currentSpan.end();
                    return next(null, sseConfig);
                },
                (sseConfig, next) => tracer.startActiveSpan('Create Cipher Bundle from server side encryption config', undefined, objectPutSpanContext, currentSpan => {
                    currentSpan.setAttributes({
                        'code.function': 'objectPut()',
                        'code.filename': 'lib/api/objectPut.js',
                        'code.lineno': 205,
                    });
                    return next(null, sseConfig, currentSpan);
                }),
                function createCipherBundle(serverSideEncryptionConfig, currentSpan, next) {
                    if (serverSideEncryptionConfig) {
                        activeSpan.addEvent('KMS cipher bundle create');
                        currentSpan.end();
                        objectPutSpan.end();
                        return kms.createCipherBundle(
                            serverSideEncryptionConfig, log, next);
                    }
                    activeSpan.addEvent('No server side encryption config');
                    return next(null, currentSpan);
                },
                (currentSpan, next) => {
                    activeSpan.addEvent('Got server side encryption config');
                    currentSpan.end();
                    return next();
                },
                (next) => tracer.startActiveSpan('Create and store object', undefined, objectPutSpanContext, currentSpan => {
                    currentSpan.setAttributes({
                        'code.function': 'objectPut()',
                        'code.filename': 'lib/api/objectPut.js',
                        'code.lineno': 229,
                    });
                    return next(null, null, currentSpan);
                }),
                function objectCreateAndStore(cipherBundle, currentSpan, next) {
                    const currentSpanContext = opentelemetry.trace.setSpan(
                        objectPutSpanContext,
                        currentSpan,
                    );
                    activeSpan.addEvent('Object create and store operation started');
                    const objectLockValidationError
                        = validateHeaders(bucket, headers, log);
                    if (objectLockValidationError) {
                        activeSpan.recordException(objectLockValidationError);
                        currentSpan.end();
                        objectPutSpan.end();
                        return next(objectLockValidationError);
                    }
                    writeContinue(request, request._response);
                    return createAndStoreObject(bucketName,
                    bucket, objectKey, objMD, authInfo, canonicalID, cipherBundle,
                    request, false, streamingV4Params, overheadField, log, (err, storingResult) => {
                        if (err) {
                            activeSpan.recordException(err);
                            currentSpan.end();
                            objectPutSpan.end();
                            return next(err);
                        }
                        return next(null, storingResult, currentSpan);
                    }, { activeSpan, activeTracerContext: currentSpanContext, tracer });
                },
            ], (err, storingResult, currentSpan) => {
                currentSpan.end();
                if (err) {
                    monitoring.promMetrics('PUT', bucketName, err.code,
                        'putObject');
                    activeSpan.recordException(err);
                    objectPutSpan.end();
                    return callback(err, responseHeaders);
                }
                activeSpan.addEvent('Completed object create and store operation');
                // ingestSize assumes that these custom headers indicate
                // an ingestion PUT which is a metadata only operation.
                // Since these headers can be modified client side, they
                // should be used with caution if needed for precise
                // metrics.
                const ingestSize = (request.headers['x-amz-meta-mdonly']
                    && !Number.isNaN(request.headers['x-amz-meta-size']))
                    ? Number.parseInt(request.headers['x-amz-meta-size'], 10) : null;
                const newByteLength = parsedContentLength;

                setExpirationHeaders(responseHeaders, {
                    lifecycleConfig: bucket.getLifecycleConfiguration(),
                    objectParams: {
                        key: objectKey,
                        date: storingResult.lastModified,
                        tags: storingResult.tags,
                    },
                });

                // Utapi expects null or a number for oldByteLength:
                // * null - new object
                // * 0 or > 0 - existing object with content-length 0 or > 0
                // objMD here is the master version that we would
                // have overwritten if there was an existing version or object
                //
                // TODO: Handle utapi metrics for null version overwrites.
                const oldByteLength = objMD && objMD['content-length']
                    !== undefined ? objMD['content-length'] : null;
                if (storingResult) {
                    // ETag's hex should always be enclosed in quotes
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

                // Only pre-existing non-versioned objects get 0 all others use 1
                const numberOfObjects = !isVersionedObj && oldByteLength !== null ? 0 : 1;

                // only the bucket owner's metrics should be updated, regardless of
                // who the requester is
                pushMetric('putObject', log, {
                    authInfo,
                    canonicalID: bucket.getOwner(),
                    bucket: bucketName,
                    keys: [objectKey],
                    newByteLength,
                    oldByteLength: isVersionedObj ? null : oldByteLength,
                    versionId: isVersionedObj && storingResult ? storingResult.versionId : undefined,
                    location: bucket.getLocationConstraint(),
                    numberOfObjects,
                });
                activeSpan.setAttributes({
                    'aws.s3.upload_id': storingResult ? storingResult.uploadId : undefined,
                });
                monitoring.promMetrics('PUT', bucketName, '200',
                    'putObject', newByteLength, oldByteLength, isVersionedObj,
                    null, ingestSize);
                activeSpan.addEvent('Leaving objectPut()');
                objectPutSpan.end();
                return callback(null, responseHeaders);
            });
        }, { activeSpan, activeTracerContext: mdSpanContext, tracer });
    });
}

module.exports = objectPut;
