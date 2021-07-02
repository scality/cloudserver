const { v4: uuidv4 } = require('uuid');
const { errors, s3middleware } = require('arsenal');
const async = require('async');
const getMetaHeaders = s3middleware.userMetadata.getMetaHeaders;
const convertToXml = s3middleware.convertToXml;
const { pushMetric } = require('../utapi/utilities');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { cleanUpBucket } = require('./apiUtils/bucket/bucketCreation');
const constants = require('../../constants');
const services = require('../services');
const { metadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const locationConstraintCheck
    = require('./apiUtils/object/locationConstraintCheck');
const validateWebsiteHeader = require('./apiUtils/object/websiteServing')
    .validateWebsiteHeader;
<<<<<<< HEAD
const monitoring = require('../utilities/monitoringHandler');
const { data } = require('../data/wrapper');
const applyZenkoUserMD = require('./apiUtils/object/applyZenkoUserMD');
const checkObjectEncryption = require('./apiUtils/object/checkEncryption');
=======
const { config } = require('../Config');
const multipleBackendGateway = require('../data/multipleBackendGateway');
>>>>>>> origin/feature/S3C-4508_per_object_encryption_headers
const { validateHeaders, compareObjectLockInformation } =
    require('./apiUtils/object/objectLockHelpers');
const { getObjectSSEConfiguration } = require('./apiUtils/bucket/bucketEncryption');

/*
Sample xml response:
<?xml version='1.0' encoding='UTF-8'?>
<InitiateMultipartUploadResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>
  <Bucket>example-bucket</Bucket>
  <Key>example-object</Key>
  <UploadId>VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS
  1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>
</InitiateMultipartUploadResult>
*/

/**
 * Initiate multipart upload returning xml that includes the UploadId
 * for the multipart upload
 * @param  {AuthInfo} authInfo - Instance of AuthInfo class with requester's
 *                               info
 * @param  {request} request - request object given by router,
 * includes normalized headers
 * @param  {object} log - the log request
 * @param  {function} callback - final callback to call with the result
 * @return {undefined} calls callback from router
 * with err and result as arguments
 */
function initiateMultipartUpload(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'initiateMultipartUpload' });
    const bucketName = request.bucketName;
    const objectKey = request.objectKey;
    // Note that we are using the string set forth in constants.js
    // to split components in the storage
    // of each MPU.  AWS does not restrict characters in object keys so
    // there is the possiblity that the chosen splitter will occur in the object
    // name itself. To prevent this, we are restricting the creation of a
    // multipart upload object with a key containing the splitter.
    const websiteRedirectHeader =
        request.headers['x-amz-website-redirect-location'];
    if (!validateWebsiteHeader(websiteRedirectHeader)) {
        const err = errors.InvalidRedirectLocation;
        log.debug('invalid x-amz-website-redirect-location' +
            `value ${websiteRedirectHeader}`, { error: err });
        return callback(err);
    }
    const metaHeaders = getMetaHeaders(request.headers);
    if (metaHeaders instanceof Error) {
        log.debug('user metadata validation failed', {
            error: metaHeaders,
            method: 'createAndStoreObject',
        });
        return process.nextTick(() => callback(metaHeaders));
    }
    // if the request occurs within a Zenko deployment, we place a user-metadata
    // field on the object
    applyZenkoUserMD(metaHeaders);

    // TODO: Add this as a utility function for all object put requests
    // but after authentication so that string to sign is not impacted
    // This is GH Issue#89
    const storageClassOptions =
        ['standard', 'standard_ia', 'reduced_redundancy'];
    let storageClass = 'STANDARD';
    if (storageClassOptions.indexOf(request
        .headers['x-amz-storage-class']) > -1) {
        storageClass = request.headers['x-amz-storage-class']
            .toUpperCase();
    }
    const metadataValParams = {
        objectKey,
        authInfo,
        bucketName,
        // Required permissions for this action are same as objectPut
        requestType: 'objectPut',
    };
    const accountCanonicalID = authInfo.getCanonicalID();
    let initiatorID = accountCanonicalID;
    let initiatorDisplayName = authInfo.getAccountDisplayName();
    if (authInfo.isRequesterAnIAMUser()) {
        initiatorID = authInfo.getArn();
        initiatorDisplayName = authInfo.getIAMdisplayName();
    }

    const metadataStoreParams = {
        objectKey,
        storageClass,
        metaHeaders,
        eventualStorageBucket: bucketName,
        headers: request.headers,
        // The ownerID should be the account canonicalID.
        ownerID: accountCanonicalID,
        ownerDisplayName: authInfo.getAccountDisplayName(),
        // If initiator is an IAM user, the initiatorID is the ARN.
        // Otherwise, it is the same as the ownerID
        // (the account canonicalID)
        initiatorID,
        // If initiator is an IAM user, the initiatorDisplayName is the
        // IAM user's displayname.
        // Otherwise, it is the same as the ownerDisplayName.
        initiatorDisplayName,
        splitter: constants.splitter,
    };
    const tagging = request.headers['x-amz-tagging'];
    if (tagging) {
        metadataStoreParams.tagging = tagging;
    }

    function _getMPUBucket(destinationBucket, log, corsHeaders,
    uploadId, cipherBundle, locConstraint, callback) {
        const xmlParams = {
            bucketName,
            objectKey,
            uploadId,
        };
        const xml = convertToXml('initiateMultipartUpload', xmlParams);
        metadataStoreParams.uploadId = uploadId;

        services.getMPUBucket(destinationBucket, bucketName, log,
            (err, MPUbucket) => {
                if (err) {
                    log.trace('error getting MPUbucket', {
                        error: err,
                    });
                    return callback(err);
                }
                // BACKWARD: Remove to remove the old splitter
                if (MPUbucket.getMdBucketModelVersion() < 2) {
                    metadataStoreParams.splitter = constants.oldSplitter;
                }
                return services.metadataStoreMPObject(MPUbucket.getName(),
                    cipherBundle, metadataStoreParams,
                    log, err => {
                        if (err) {
                            log.trace('error storing multipart object', {
                                error: err,
                            });
                            monitoring.promMetrics('PUT', bucketName, err.code,
                                'initiateMultipartUpload');
                            return callback(err, null, corsHeaders);
                        }
                        log.addDefaultFields({ uploadId });
                        log.trace('successfully initiated mpu');
                        pushMetric('initiateMultipartUpload', log, {
                            authInfo,
                            bucket: bucketName,
                            keys: [objectKey],
                            location: locConstraint,
                        });
                        monitoring.promMetrics('PUT', bucketName, '200',
                            'initiateMultipartUpload');
                        return callback(null, xml, corsHeaders);
                    });
            });
    }

    function _storetheMPObject(destinationBucket, corsHeaders, serverSideEncryption) {
        let cipherBundle = null;
        if (serverSideEncryption) {
            cipherBundle = {
                algorithm: serverSideEncryption.algorithm,
                masterKeyId: serverSideEncryption.masterKeyId,
            };
        }
        const backendInfoObj = locationConstraintCheck(request, null,
            destinationBucket, log);
        if (backendInfoObj.err) {
            return process.nextTick(() => {
                callback(backendInfoObj.err);
            });
        }
        const locConstraint = backendInfoObj.controllingLC;
        metadataStoreParams.controllingLocationConstraint = locConstraint;
        metadataStoreParams.dataStoreName = locConstraint;

        if (request.headers) {
            const objectLockValError =
                validateHeaders(destinationBucket, request.headers, log);
            if (objectLockValError) {
                return callback(objectLockValError);
            }
        }
        const defaultRetention = destinationBucket.getObjectLockConfiguration();
        const finalObjectLockInfo =
            compareObjectLockInformation(request.headers, defaultRetention);

        if (finalObjectLockInfo.retentionInfo) {
            metadataStoreParams.retentionMode =
                finalObjectLockInfo.retentionInfo.mode;
            metadataStoreParams.retentionDate =
                finalObjectLockInfo.retentionInfo.date;
        }
        if (finalObjectLockInfo.legalHold) {
            metadataStoreParams.legalHold = finalObjectLockInfo.legalHold;
        }

        let uploadId;
        const mpuInfo = {
            objectKey,
            metaHeaders,
            bucketName,
            locConstraint,
            destinationBucket,
            tagging,
        };
        return data.initiateMPU(mpuInfo, websiteRedirectHeader, log,
        (err, dataBackendResObj, isVersionedObj) => {
            // will return as true and a custom error if external backend does
            // not support versioned objects
            if (isVersionedObj) {
                monitoring.promMetrics('PUT', bucketName, 501,
                    'initiateMultipartUpload');
                return callback(err);
            }
            if (err) {
                monitoring.promMetrics('PUT', bucketName, err.code,
                    'initiateMultipartUpload');
                return callback(err);
            }
            // if mpu not handled externally, dataBackendResObj will be null
            if (dataBackendResObj) {
                uploadId = dataBackendResObj.UploadId;
            } else {
                // Generate uniqueID without dashes so routing not messed up
                uploadId = uuidv4().replace(/-/g, '');
            }
            return _getMPUBucket(destinationBucket, log, corsHeaders,
                uploadId, cipherBundle, locConstraint, callback);
        });
    }

    async.waterfall([
        next => metadataValidateBucketAndObj(metadataValParams, log,
            (error, destinationBucket) => {
                const corsHeaders = collectCorsHeaders(request.headers.origin,
                    request.method, destinationBucket);
                if (error) {
                    log.debug('error processing request', {
                        error,
                        method: 'metadataValidateBucketAndObj',
                    });
                    monitoring.promMetrics('PUT', bucketName, err.code, 'initiateMultipartUpload');
                    return next(error, corsHeaders);
                }
                return next(null, corsHeaders, destinationBucket);
            }),
        (corsHeaders, destinationBucket, next) => {
            if (destinationBucket.hasDeletedFlag() && accountCanonicalID !== destinationBucket.getOwner()) {
                log.trace('deleted flag on bucket and request from non-owner account');
                monitoring.promMetrics('PUT', bucketName, 404, 'initiateMultipartUpload');
                return next(errors.NoSuchBucket, corsHeaders);
            }
            if (destinationBucket.hasTransientFlag() || destinationBucket.hasDeletedFlag()) {
                log.trace('transient or deleted flag so cleaning up bucket');
                return cleanUpBucket(
                        destinationBucket,
                        accountCanonicalID,
                        log,
                        error => {
                            if (error) {
                                log.debug('error cleaning up bucket with flag',
                                    {
                                        error,
                                        transientFlag: destinationBucket.hasTransientFlag(),
                                        deletedFlag: destinationBucket.hasDeletedFlag(),
                                    });
                                monitoring.promMetrics('PUT', bucketName, 500, 'initiateMultipartUpload');
                                // To avoid confusing user with error
                                // from cleaning up
                                // bucket return InternalError
                                return next(errors.InternalError, corsHeaders);
                            }
                            return next(null, corsHeaders, destinationBucket);
                        });
            }
            return next(null, corsHeaders, destinationBucket);
        },
        (corsHeaders, destinationBucket, next) =>
            getObjectSSEConfiguration(
                request.headers,
                destinationBucket,
                log,
                (error, objectSSEConfig) => {
                    if (error) {
                        log.error('error fetching server-side encryption config', {
                            error,
                            method: 'getObjectSSEConfiguration',
                        });
                        return next(error, corsHeaders);
                    }
                    return next(null, corsHeaders, destinationBucket, objectSSEConfig);
                }
            ),
    ],
        (error, corsHeaders, destinationBucket, objectSSEConfig) => {
            if (error) {
                return callback(error, null, corsHeaders);
            }
            return _storetheMPObject(destinationBucket, corsHeaders, objectSSEConfig);
        }
    );
    return undefined;
}

module.exports = initiateMultipartUpload;
