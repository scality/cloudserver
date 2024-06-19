const async = require('async');
const { errors, versioning } = require('arsenal');
const { PassThrough } = require('stream');

const { getObjectSSEConfiguration } = require('./apiUtils/bucket/bucketEncryption');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const createAndStoreObject = require('./apiUtils/object/createAndStoreObject');
const { standardMetadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const { validateHeaders } = require('./apiUtils/object/objectLockHelpers');
const kms = require('../kms/wrapper');
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
 * @param {object} fileInfo - object containing file stream and filename
 * @param {Function} callback - final callback to call with the result
 * @return {undefined}
 */
function objectPost(authInfo, request, streamingV4Params, log, callback) {
    const {
        headers,
        method,
    } = request;
    let parsedContentLength = 0;

    const passThroughStream = new PassThrough();

    // TODO CLDSRV-527 add acl header check
    // if (!aclUtils.checkGrantHeaderValidity(headers)) {
    //     log.trace('invalid acl header');
    //     monitoring.promMetrics('PUT', request.bucketName, 400,
    //         'putObject');
    //     return callback(errors.InvalidArgument);
    // }
    // TODO CLDSRV-527 add check for versionId
    // const queryContainsVersionId = checkQueryVersionId(query);
    // if (queryContainsVersionId instanceof Error) {
    //     return callback(queryContainsVersionId);
    // }
    const invalidSSEError = errors.InvalidArgument.customizeDescription(
        'The encryption method specified is not supported');
    const requestType = request.apiMethods || 'objectPost';

    const valParams = { authInfo, bucketName: request.formData.bucket, objectKey: request.formData.key, requestType, request };

    const canonicalID = authInfo.getCanonicalID();

    // TODO CLDSRV-527 add check for non-printable characters?
    // if (hasNonPrintables(objectKey)) {
    //     return callback(errors.InvalidInput.customizeDescription(
    //         'object keys cannot contain non-printable characters',
    //     ));
    // }

    // TODO CLDSRV-527 add checksum header check
    // const checksumHeaderErr = validateChecksumHeaders(headers);
    // if (checksumHeaderErr) {
    //     return callback(checksumHeaderErr);
    // }

    log.trace('owner canonicalID to send to data', { canonicalID });

    return standardMetadataValidateBucketAndObj(valParams, request.actionImplicitDenies, log,
        (err, bucket, objMD) => {
            const responseHeaders = collectCorsHeaders(headers.origin,
                method, bucket);

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
                function countPOSTFileSize(next) {
                    request.file.on('data', (chunk) => {
                        const boundaryBuffer = Buffer.from(`${request.fileEventData.boundaryBuffer}--`);
                        const boundaryIndex = chunk.indexOf(boundaryBuffer);

                        if (boundaryIndex !== -1) {
                            // If the boundary is found, slice the chunk to exclude the boundary
                            chunk = chunk.slice(0, boundaryIndex);
                        }

                        parsedContentLength += chunk.length;
                        passThroughStream.write(chunk);
                    });

                    request.file.on('end', () => {
                        // Here totalBytes will have the total size of the file
                        passThroughStream.end();
                        request.file = passThroughStream;
                        request.parsedContentLength = parsedContentLength;
                        return next();
                    });
                    return undefined;
                },
                // TODO CLDSRV-527 add this back?
                // function handleTransientOrDeleteBuckets(next) {
                //     if (bucket.hasTransientFlag() || bucket.hasDeletedFlag()) {
                //         return cleanUpBucket(bucket, canonicalID, log, next);
                //     }
                //     return next();
                // },
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
                    return createAndStoreObject(request.bucketName,
                        bucket, request.formData.key, objMD, authInfo, canonicalID, cipherBundle,
                        request, false, streamingV4Params, overheadField, log, next);
                },
            ], (err, storingResult) => {
                if (err) {
                    monitoring.promMetrics('POST', request.bucketName, err.code,
                        'postObject');
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
                        key: request.key,
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
                pushMetric('postObject', log, {
                    authInfo,
                    canonicalID: bucket.getOwner(),
                    bucket: request.bucketName,
                    keys: [request.key],
                    newByteLength,
                    oldByteLength: isVersionedObj ? null : oldByteLength,
                    versionId: isVersionedObj && storingResult ? storingResult.versionId : undefined,
                    location: bucket.getLocationConstraint(),
                    numberOfObjects,
                });
                monitoring.promMetrics('POST', request.bucketName, '204',
                    'postObject', newByteLength, oldByteLength, isVersionedObj,
                    null, ingestSize);
                return callback(null, responseHeaders);
            });
        });
}

module.exports = objectPost;
