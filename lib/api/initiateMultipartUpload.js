const UUID = require('node-uuid');
const { errors, s3middleware } = require('arsenal');
const getMetaHeaders = s3middleware.userMetadata.getMetaHeaders;
const escapeForXml = s3middleware.escapeForXml;
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
const { config } = require('../Config');
const multipleBackendGateway = require('../data/multipleBackendGateway');

const externalVersioningErrorMessage = 'We do not currently support putting ' +
'a versioned object to a location-constraint of type AWS or Azure.';

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
 * _convertToXml - Convert the `xmlParams` object created in
 * `initiateMultipartUpload()` to an XML DOM string
 * @param {object} xmlParams - The object created in
 * `initiateMultipartUpload()` to convert into an XML DOM string
 * @return {string} xml.join('') - The XML DOM string
 */
const _convertToXml = xmlParams => {
    const xml = [];
    xml.push('<?xml version="1.0" encoding="UTF-8"?>',
             '<InitiateMultipartUploadResult ' +
                'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
             `<Bucket>${xmlParams.bucketName}</Bucket>`,
             `<Key>${escapeForXml(xmlParams.objectKey)}</Key>`,
             `<UploadId>${xmlParams.uploadId}</UploadId>`,
             '</InitiateMultipartUploadResult>'
    );

    return xml.join('');
};

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

    function _getMPUBucket(destinationBucket, log, corsHeaders,
    uploadId, cipherBundle, callback) {
        const xmlParams = {
            bucketName,
            objectKey,
            uploadId,
        };
        const xml = _convertToXml(xmlParams);
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
                            return callback(err, null, corsHeaders);
                        }
                        log.addDefaultFields({ uploadId });
                        log.trace('successfully initiated mpu');
                        pushMetric('initiateMultipartUpload', log, {
                            authInfo,
                            bucket: bucketName,
                            keys: [objectKey],
                        });
                        return callback(null, xml, corsHeaders);
                    });
            });
    }

    function _storetheMPObject(destinationBucket, corsHeaders) {
        const serverSideEncryption =
            destinationBucket.getServerSideEncryption();
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

        let uploadId;
        if (config.backends.data === 'multiple') {
            return multipleBackendGateway.createMPU(objectKey, metaHeaders,
            destinationBucket, websiteRedirectHeader, locConstraint, log,
            (err, dataBackendResObj) => {
                if (err) {
                    return callback(err);
                }
              // NOTE: remove the following when we will support putting a
              // versioned object to a location-constraint of type AWS or Azure.
                if (locConstraint &&
                  config.locationConstraints[locConstraint] &&
                  config.locationConstraints[locConstraint].type &&
                  constants.externalBackends[config
                      .locationConstraints[locConstraint].type]
                ) {
                    const vcfg = destinationBucket.getVersioningConfiguration();
                    const isVersionedObj = vcfg && vcfg.Status === 'Enabled';
                    if (isVersionedObj) {
                        log.debug(externalVersioningErrorMessage,
                            { method: 'multipleBackendGateway',
                                error: errors.NotImplemented });
                        return callback(errors.NotImplemented
                        .customizeDescription(externalVersioningErrorMessage));
                    }
                }
                if (dataBackendResObj) {
                    // dataBackendResObj will be returned in data backend
                    // handles mpu
                    uploadId = dataBackendResObj.UploadId;
                } else {
                    uploadId = UUID.v4().replace(/-/g, '');
                }
                return _getMPUBucket(destinationBucket, log, corsHeaders,
                uploadId, cipherBundle, callback);
            });
        }
        // Generate uniqueID without dashes so that routing not messed up
        uploadId = UUID.v4().replace(/-/g, '');

        return _getMPUBucket(destinationBucket, log, corsHeaders,
        uploadId, cipherBundle, callback);
    }

    metadataValidateBucketAndObj(metadataValParams, log,
        (err, destinationBucket) => {
            const corsHeaders = collectCorsHeaders(request.headers.origin,
                request.method, destinationBucket);
            if (err) {
                log.debug('error processing request', {
                    error: err,
                    method: 'metadataValidateBucketAndObj',
                });
                return callback(err, null, corsHeaders);
            }
            if (destinationBucket.hasDeletedFlag() &&
                accountCanonicalID !== destinationBucket.getOwner()) {
                log.trace('deleted flag on bucket and request ' +
                    'from non-owner account');
                return callback(errors.NoSuchBucket);
            }
            if (destinationBucket.hasTransientFlag() ||
                destinationBucket.hasDeletedFlag()) {
                log.trace('transient or deleted flag so cleaning ' +
                    'up bucket');
                return cleanUpBucket(destinationBucket,
                        accountCanonicalID, log, err => {
                            if (err) {
                                log.debug('error cleaning up bucket ' +
                                    'with flag',
                                    { error: err,
                                        transientFlag:
                                    destinationBucket.hasTransientFlag(),
                                        deletedFlag:
                                    destinationBucket.hasDeletedFlag(),
                                    });
                                // To avoid confusing user with error
                                // from cleaning up
                                // bucket return InternalError
                                return callback(errors.InternalError,
                                    null, corsHeaders);
                            }
                            return _storetheMPObject(destinationBucket,
                                corsHeaders);
                        });
            }
            return _storetheMPObject(destinationBucket, corsHeaders);
        });
    return undefined;
}

module.exports = initiateMultipartUpload;
