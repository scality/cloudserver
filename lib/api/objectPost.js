const async = require('async');
const { errors, versioning } = require('arsenal');
const { PassThrough } = require('stream');

const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const createAndStoreObject = require('./apiUtils/object/createAndStoreObject');
const { standardMetadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const { config } = require('../Config');
const { setExpirationHeaders } = require('./apiUtils/object/expirationHeaders');
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
    } = request;
    let parsedContentLength = 0;
    const passThroughStream = new PassThrough();
    const requestType = request.apiMethods || 'objectPost';
    const valParams = {
        authInfo,
        bucketName: request.formData.bucket,
        objectKey: request.formData.key,
        requestType,
        request,
    };
    const canonicalID = authInfo.getCanonicalID();


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
                return callback(err, responseHeaders);
            }
            if (bucket.hasDeletedFlag() && canonicalID !== bucket.getOwner()) {
                log.trace('deleted flag on bucket and request ' +
                    'from non-owner account');
                return callback(errors.NoSuchBucket);
            }

            return async.waterfall([
                function countPOSTFileSize(next) {
                    if (!request.fileEventData || !request.fileEventData.file) {
                        return next();
                    }
                    request.fileEventData.file.on('data', (chunk) => {
                        parsedContentLength += chunk.length;
                        passThroughStream.write(chunk);
                    });

                    request.fileEventData.file.on('end', () => {
                        passThroughStream.end();
                        // Setting the file in the request avoids the need to make changes to createAndStoreObject's
                        // parameters and thus all it's subsequent calls. This is necessary as the stream used to create
                        // the object is that of the request directly; something we must work around
                        // to use the file data produced from the multipart form data.
                        /* eslint-disable no-param-reassign */
                        request.fileEventData.file = passThroughStream;
                        /* eslint-disable no-param-reassign */
                        // Here parsedContentLength will have the total size of the file
                        // This is used when calculating the size of the object in createAndStoreObject
                        request.parsedContentLength = parsedContentLength;
                        return next();
                    });
                    return undefined;
                },
                function objectCreateAndStore(next) {
                    writeContinue(request, request._response);
                    return createAndStoreObject(request.bucketName,
                        bucket, request.formData.key, objMD, authInfo, canonicalID, null,
                        request, false, streamingV4Params, overheadField, log, next);
                },
            ], (err, storingResult) => {
                if (err) {
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
