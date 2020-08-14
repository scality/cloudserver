const async = require('async');
const { errors, versioning } = require('arsenal');

const aclUtils = require('../utilities/aclUtils');
const { cleanUpBucket } = require('./apiUtils/bucket/bucketCreation');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const createAndStoreObject = require('./apiUtils/object/createAndStoreObject');
const { checkQueryVersionId } = require('./apiUtils/object/versioning');
const { metadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const { validateHeaders } = require('./apiUtils/object/objectLockHelpers');
const kms = require('../kms/wrapper');
const checkObjectEncryption = require('./apiUtils/object/checkEncryption');

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
    const {
        bucketName,
        headers,
        method,
        objectKey,
        parsedContentLength,
        query,
    } = request;
    if (!aclUtils.checkGrantHeaderValidity(headers)) {
        log.trace('invalid acl header');
        return callback(errors.InvalidArgument);
    }
    const queryContainsVersionId = checkQueryVersionId(query);
    if (queryContainsVersionId instanceof Error) {
        return callback(queryContainsVersionId);
    }
    const invalidSSEError = errors.InvalidArgument.customizeDescription(
        'The encryption method specified is not supported');
    const requestType = 'objectPut';
    const valParams = { authInfo, bucketName, objectKey, requestType };
    const canonicalID = authInfo.getCanonicalID();
    log.trace('owner canonicalID to send to data', { canonicalID });

    return metadataValidateBucketAndObj(valParams, log,
    (err, bucket, objMD) => {
        const responseHeaders = collectCorsHeaders(headers.origin,
            method, bucket);
        if (err) {
            log.trace('error processing request', {
                error: err,
                method: 'metadataValidateBucketAndObj',
            });
            return callback(err, responseHeaders);
        }
        if (bucket.hasDeletedFlag() && canonicalID !== bucket.getOwner()) {
            log.trace('deleted flag on bucket and request ' +
                'from non-owner account');
            return callback(errors.NoSuchBucket);
        }
        return async.waterfall([
            function handleTransientOrDeleteBuckets(next) {
                if (bucket.hasTransientFlag() || bucket.hasDeletedFlag()) {
                    return cleanUpBucket(bucket, canonicalID, log, next);
                }
                return next();
            },
            function createCipherBundle(next) {
                const serverSideEncryption = bucket.getServerSideEncryption();
                if (!checkObjectEncryption.isValidSSES3(request,
                    serverSideEncryption)) {
                    return next(invalidSSEError);
                }
                if (serverSideEncryption) {
                    return kms.createCipherBundle(
                            serverSideEncryption, log, next);
                }
                return next(null, null);
            },
            function objectCreateAndStore(cipherBundle, next) {
                const objectLockValidationError
                    = validateHeaders(bucket, headers, log);
                if (objectLockValidationError) {
                    return next(objectLockValidationError);
                }
                return createAndStoreObject(bucketName,
                bucket, objectKey, objMD, authInfo, canonicalID, cipherBundle,
                request, false, streamingV4Params, log, next);
            },
        ], (err, storingResult) => {
            if (err) {
                return callback(err, responseHeaders);
            }
            const newByteLength = parsedContentLength;

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
            return callback(null, responseHeaders);
        });
    });
}

module.exports = objectPut;
