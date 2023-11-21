const async = require('async');
const { errors, versioning } = require('arsenal');

const constants = require('../../constants');
const aclUtils = require('../utilities/aclUtils');
const { cleanUpBucket } = require('./apiUtils/bucket/bucketCreation');
const { getObjectSSEConfiguration } = require('./apiUtils/bucket/bucketEncryption');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const createAndStoreObject = require('./apiUtils/object/createAndStoreObject');
const { checkQueryVersionId, decodeVID } = require('./apiUtils/object/versioning');
const { standardMetadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const { validateHeaders } = require('./apiUtils/object/objectLockHelpers');
const { hasNonPrintables } = require('../utilities/stringChecks');
const kms = require('../kms/wrapper');
const monitoring = require('../utilities/monitoringHandler');
const { validatePutVersionId } = require('./apiUtils/object/coldStorage');
const { setExpirationHeaders } = require('./apiUtils/object/expirationHeaders');
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
 * @return {undefined}
 */
function objectPut(authInfo, request, streamingV4Params, log, callback) {
    log.debug('processing request', { method: 'objectPut' });

    const putVersionId = request.headers['x-scal-s3-version-id'];
    const isPutVersion = putVersionId || putVersionId === '';

    let versionId;

    if (putVersionId) {
        const decodedVidResult = decodeVID(putVersionId);
        if (decodedVidResult instanceof Error) {
            log.trace('invalid x-scal-s3-version-id header', {
                versionId: putVersionId,
                error: decodedVidResult,
            });
            return process.nextTick(() => callback(decodedVidResult));
        }
        versionId = decodedVidResult;
    }

    const {
        bucketName,
        headers,
        method,
        objectKey,
        parsedContentLength,
        query,
    } = request;
    if (headers['x-amz-storage-class'] &&
        !constants.validStorageClasses.includes(headers['x-amz-storage-class'])) {
        log.trace('invalid storage-class header');
        monitoring.promMetrics('PUT', request.bucketName,
            errors.InvalidStorageClass.code, 'putObject');
        return callback(errors.InvalidStorageClass);
    }
    if (!aclUtils.checkGrantHeaderValidity(headers)) {
        log.trace('invalid acl header');
        monitoring.promMetrics('PUT', request.bucketName, 400,
            'putObject');
        return callback(errors.InvalidArgument);
    }
    const queryContainsVersionId = checkQueryVersionId(query);
    if (queryContainsVersionId instanceof Error) {
        return callback(queryContainsVersionId);
    }
    const size = request.parsedContentLength;
    if (Number.parseInt(size, 10) > constants.maximumAllowedUploadSize) {
        log.debug('Upload size exceeds maximum allowed for a single PUT',
            { size });
        return callback(errors.EntityTooLarge);
    }

    const invalidSSEError = errors.InvalidArgument.customizeDescription(
        'The encryption method specified is not supported');
    const requestType = request.apiMethods || 'objectPut';
    const valParams = { authInfo, bucketName, objectKey, versionId,
        requestType, request };
    const canonicalID = authInfo.getCanonicalID();

    if (hasNonPrintables(objectKey)) {
        return callback(errors.InvalidInput.customizeDescription(
            'object keys cannot contain non-printable characters',
        ));
    }

    const checksumHeaderErr = validateChecksumHeaders(headers);
    if (checksumHeaderErr) {
        return callback(checksumHeaderErr);
    }

    log.trace('owner canonicalID to send to data', { canonicalID });
    return standardMetadataValidateBucketAndObj(valParams, request.actionImplicitDenies, log,
    (err, bucket, objMD) => {
        const responseHeaders = collectCorsHeaders(headers.origin,
            method, bucket);
        if (err) {
            log.trace('error processing request', {
                error: err,
                method: 'metadataValidateBucketAndObj',
            });
            monitoring.promMetrics('PUT', bucketName, err.code, 'putObject');
            return callback(err, responseHeaders);
        }
        if (bucket.hasDeletedFlag() && canonicalID !== bucket.getOwner()) {
            log.trace('deleted flag on bucket and request ' +
                'from non-owner account');
            monitoring.promMetrics('PUT', bucketName, 404, 'putObject');
            return callback(errors.NoSuchBucket);
        }

        if (isPutVersion) {
            const error = validatePutVersionId(objMD, putVersionId, log);
            if (error) {
                return callback(error);
            }
        }

        return async.waterfall([
            function handleTransientOrDeleteBuckets(next) {
                if (bucket.hasTransientFlag() || bucket.hasDeletedFlag()) {
                    return cleanUpBucket(bucket, canonicalID, log, next);
                }
                return next();
            },
            function getSSEConfig(next) {
                return getObjectSSEConfiguration(headers, bucket, log,
                    (err, sseConfig) => {
                        if (err) {
                            log.error('error getting server side encryption config', { err });
                            return next(invalidSSEError);
                        }
                        return next(null, sseConfig);
                    }
                );
            },
            function createCipherBundle(serverSideEncryptionConfig, next) {
                if (serverSideEncryptionConfig) {
                    return kms.createCipherBundle(
                        serverSideEncryptionConfig, log, next);
                }
                return next(null, null);
            },
            function objectCreateAndStore(cipherBundle, next) {
                const objectLockValidationError
                    = validateHeaders(bucket, headers, log);
                if (objectLockValidationError) {
                    return next(objectLockValidationError);
                }
                writeContinue(request, request._response);
                return createAndStoreObject(bucketName,
                bucket, objectKey, objMD, authInfo, canonicalID, cipherBundle,
                request, false, streamingV4Params, overheadField, log, 's3:ObjectCreated:Put', next);
            },
        ], (err, storingResult) => {
            if (err) {
                monitoring.promMetrics('PUT', bucketName, err.code,
                    'putObject');
                return callback(err, responseHeaders);
            }
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
                        versionIdUtils.encode(storingResult.versionId);
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
            monitoring.promMetrics('PUT', bucketName, '200',
                'putObject', newByteLength, oldByteLength, isVersionedObj,
                null, ingestSize);
            return callback(null, responseHeaders);
        });
    });
}

module.exports = objectPut;
