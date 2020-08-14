const async = require('async');

const { errors, versioning, s3middleware } = require('arsenal');
const getMetaHeaders = s3middleware.userMetadata.getMetaHeaders;
const validateHeaders = s3middleware.validateConditionalHeaders;

const constants = require('../../constants');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const locationConstraintCheck
    = require('./apiUtils/object/locationConstraintCheck');
const { checkQueryVersionId, versioningPreprocessing }
    = require('./apiUtils/object/versioning');
const getReplicationInfo = require('./apiUtils/object/getReplicationInfo');
const data = require('../data/wrapper');
const logger = require('../utilities/logger');
const services = require('../services');
const { pushMetric } = require('../utapi/utilities');
const removeAWSChunked = require('./apiUtils/object/removeAWSChunked');
const { metadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const validateWebsiteHeader = require('./apiUtils/object/websiteServing')
    .validateWebsiteHeader;
const { config } = require('../Config');
const checkObjectEncryption = require('./apiUtils/object/checkEncryption');

const versionIdUtils = versioning.VersionID;
const locationHeader = constants.objectLocationConstraintHeader;
const versioningNotImplBackends = constants.versioningNotImplBackends;
const externalVersioningErrorMessage = 'We do not currently support putting ' +
'a versioned object to a location-constraint of type AWS or Azure.';

/**
 * Preps metadata to be saved (based on copy or replace request header)
 * @param {object} request - request
 * @param {object} sourceObjMD - object md of source object
 * @param {object} headers - request headers
 * @param {boolean} sourceIsDestination - whether or not source is same as
 * destination
 * @param {AuthInfo} authInfo - authInfo from Vault
 * @param {string} objectKey - destination key name
 * @param {object} sourceBucketMD - bucket metadata of source bucket
 * @param {object} destBucketMD - bucket metadata of bucket being copied to
 * @param {string} sourceVersionId - versionId of source object for copy
 * @param {object} log - logger object
 * @return {object}
 * - (storeMetadataParams
 * - sourceLocationConstraintName {string} - location type of the source
 * - OR error
 */
function _prepMetadata(request, sourceObjMD, headers, sourceIsDestination,
    authInfo, objectKey, sourceBucketMD, destBucketMD, sourceVersionId, log) {
    let whichMetadata = headers['x-amz-metadata-directive'];
    // Default is COPY
    whichMetadata = whichMetadata === undefined ? 'COPY' : whichMetadata;
    if (whichMetadata !== 'COPY' && whichMetadata !== 'REPLACE') {
        return { error: errors.InvalidArgument };
    }
    let whichTagging = headers['x-amz-tagging-directive'];
    // Default is COPY
    whichTagging = whichTagging === undefined ? 'COPY' : whichTagging;
    if (whichTagging !== 'COPY' && whichTagging !== 'REPLACE') {
        return { error: errors.InvalidArgument
          .customizeDescription('Unknown tagging directive') };
    }
    const overrideMetadata = {};
    if (headers['x-amz-server-side-encryption']) {
        overrideMetadata['x-amz-server-side-encryption'] =
            headers['x-amz-server-side-encryption'];
    }
    if (headers['x-amz-storage-class']) {
        overrideMetadata['x-amz-storage-class'] =
            headers['x-amz-storage-class'];
    }
    if (headers['x-amz-website-redirect-location']) {
        overrideMetadata['x-amz-website-redirect-location'] =
            headers['x-amz-website-redirect-location'];
    }
    const retentionHeaders = headers['x-amz-object-lock-mode']
        && headers['x-amz-object-lock-retain-until-date'];
    const legalHoldHeader = headers['x-amz-object-lock-legal-hold'];
    if ((retentionHeaders || legalHoldHeader)
        && !destBucketMD.isObjectLockEnabled()) {
        return { error: errors.InvalidRequest.customizeDescription(
            'Bucket is missing ObjectLockConfiguration') };
    }
    // Cannot copy from same source and destination if no MD
    // changed and no source version id
    if (sourceIsDestination && whichMetadata === 'COPY' &&
        Object.keys(overrideMetadata).length === 0 && !sourceVersionId) {
        return { error: errors.InvalidRequest.customizeDescription('This copy' +
            ' request is illegal because it is trying to copy an ' +
            'object to itself without changing the object\'s metadata, ' +
            'storage class, website redirect location or encryption ' +
            'attributes.') };
    }
    // If COPY, pull all x-amz-meta keys/values from source object
    // Otherwise, pull all x-amz-meta keys/values from request headers
    const userMetadata = whichMetadata === 'COPY' ?
        getMetaHeaders(sourceObjMD) :
        getMetaHeaders(headers);
    if (userMetadata instanceof Error) {
        log.debug('user metadata validation failed', {
            error: userMetadata,
            method: 'objectCopy',
        });
        return { error: userMetadata };
    }
    // If metadataDirective is:
    // - 'COPY' and source object has a location constraint in its metadata
    // we use the bucket destination location constraint
    if (whichMetadata === 'COPY'
        && userMetadata[locationHeader]
        && destBucketMD.getLocationConstraint()) {
        userMetadata[locationHeader] = destBucketMD.getLocationConstraint();
    }
    const backendInfoObjSource = locationConstraintCheck(request,
        sourceObjMD, sourceBucketMD, log);
    if (backendInfoObjSource.err) {
        return { error: backendInfoObjSource.err };
    }
    const sourceLocationConstraintName = backendInfoObjSource.controllingLC;

    const backendInfoObjDest = locationConstraintCheck(request,
        userMetadata, destBucketMD, log);
    if (backendInfoObjDest.err) {
        return { error: backendInfoObjSource.err };
    }
    const destLocationConstraintName = backendInfoObjDest.controllingLC;

    // If location constraint header is not included, locations match
    const locationMatch =
    sourceLocationConstraintName === destLocationConstraintName;

    // If tagging directive is REPLACE but you don't specify any
    // tags in the request, the destination object will
    // not have any tags.
    // If tagging directive is COPY but the source object does not have tags,
    // the destination object will not have any tags.
    let tagging;
    let taggingCopy;
    if (whichTagging === 'COPY') {
        taggingCopy = sourceObjMD.tags || {};
    } else {
        tagging = headers['x-amz-tagging'] || '';
    }

    // If COPY, pull the necessary headers from source object
    // Otherwise, pull them from request headers
    const headersToStoreSource = whichMetadata === 'COPY' ?
        sourceObjMD : headers;

    const storeMetadataParams = {
        objectKey,
        log,
        headers,
        authInfo,
        metaHeaders: userMetadata,
        size: sourceObjMD['content-length'],
        contentType: headersToStoreSource['content-type'],
        contentMD5: sourceObjMD['content-md5'],
        cacheControl: headersToStoreSource['cache-control'],
        contentDisposition: headersToStoreSource['content-disposition'],
        contentEncoding:
            removeAWSChunked(headersToStoreSource['content-encoding']),
        dataStoreName: destLocationConstraintName,
        expires: headersToStoreSource.expires,
        overrideMetadata,
        lastModifiedDate: new Date().toJSON(),
        tagging,
        taggingCopy,
        replicationInfo: getReplicationInfo(objectKey, destBucketMD, false,
            sourceObjMD['content-length']),
        locationMatch,
        originOp: 's3:ObjectCreated:Copy',
    };

    const defaultRetentionConfig = destBucketMD.getObjectLockConfiguration();
    if (defaultRetentionConfig && !legalHoldHeader) {
        storeMetadataParams.defaultRetention = defaultRetentionConfig;
    }

    // In case whichMetadata === 'REPLACE' but contentType is undefined in copy
    // request headers, make sure to keep the original header instead
    if (!storeMetadataParams.contentType) {
        storeMetadataParams.contentType = sourceObjMD['content-type'];
    }
    return { storeMetadataParams, sourceLocationConstraintName,
      backendInfoDest: backendInfoObjDest.backendInfo };
}

/**
 * PUT Object Copy in the requested bucket.
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with
 * requester's info
 * @param {request} request - request object given by router,
 *                            includes normalized headers
 * @param {string} sourceBucket - name of source bucket for object copy
 * @param {string} sourceObject - name of source object for object copy
 * @param {string} sourceVersionId - versionId of source object for copy
 * @param {object} log - the log request
 * @param {function} callback - final callback to call with the result
 * @return {undefined}
 */
function objectCopy(authInfo, request, sourceBucket,
    sourceObject, sourceVersionId, log, callback) {
    log.debug('processing request', { method: 'objectCopy' });
    const destBucketName = request.bucketName;
    const destObjectKey = request.objectKey;
    const sourceIsDestination =
        destBucketName === sourceBucket && destObjectKey === sourceObject;
    const valGetParams = {
        authInfo,
        bucketName: sourceBucket,
        objectKey: sourceObject,
        versionId: sourceVersionId,
        requestType: 'objectGet',
    };
    const valPutParams = {
        authInfo,
        bucketName: destBucketName,
        objectKey: destObjectKey,
        requestType: 'objectPut',
    };
    const dataStoreContext = {
        bucketName: destBucketName,
        owner: authInfo.getCanonicalID(),
        namespace: request.namespace,
        objectKey: destObjectKey,
    };
    const websiteRedirectHeader =
        request.headers['x-amz-website-redirect-location'];
    const invalidSSEError
        = errors.InvalidArgument.customizeDescription('The encryption method '
        + 'specified is not supported');

    if (!validateWebsiteHeader(websiteRedirectHeader)) {
        const err = errors.InvalidRedirectLocation;
        log.debug('invalid x-amz-website-redirect-location' +
            `value ${websiteRedirectHeader}`, { error: err });
        return callback(err);
    }
    const queryContainsVersionId = checkQueryVersionId(request.query);
    if (queryContainsVersionId instanceof Error) {
        return callback(queryContainsVersionId);
    }

    return async.waterfall([
        function checkDestAuth(next) {
            return metadataValidateBucketAndObj(valPutParams, log,
                (err, destBucketMD, destObjMD) => {
                    if (err) {
                        log.debug('error validating put part of request',
                        { error: err });
                        return next(err, destBucketMD);
                    }
                    const flag = destBucketMD.hasDeletedFlag()
                        || destBucketMD.hasTransientFlag();
                    if (flag) {
                        log.trace('deleted flag or transient flag ' +
                        'on destination bucket', { flag });
                        return next(errors.NoSuchBucket);
                    }
                    return next(null, destBucketMD, destObjMD);
                });
        },
        function checkSourceAuthorization(destBucketMD, destObjMD, next) {
            return metadataValidateBucketAndObj(valGetParams, log,
                (err, sourceBucketMD, sourceObjMD) => {
                    if (err) {
                        log.debug('error validating get part of request',
                        { error: err });
                        return next(err, null, destBucketMD);
                    }
                    if (!sourceObjMD) {
                        const err = sourceVersionId ? errors.NoSuchVersion :
                            errors.NoSuchKey;
                        log.debug('no source object', { sourceObject });
                        return next(err, null, destBucketMD);
                    }
                    if (sourceObjMD.isDeleteMarker) {
                        log.debug('delete marker on source object',
                        { sourceObject });
                        if (sourceVersionId) {
                            const err = errors.InvalidRequest
                            .customizeDescription('The source of a copy ' +
                            'request may not specifically refer to a delete' +
                            'marker by version id.');
                            return next(err, destBucketMD);
                        }
                        // if user specifies a key in a versioned source bucket
                        // without specifying a version, and the object has
                        // a delete marker, return NoSuchKey
                        return next(errors.NoSuchKey, destBucketMD);
                    }
                    const headerValResult =
                        validateHeaders(request.headers,
                        sourceObjMD['last-modified'],
                        sourceObjMD['content-md5']);
                    if (headerValResult.error) {
                        return next(errors.PreconditionFailed, destBucketMD);
                    }
                    const { storeMetadataParams, error: metadataError,
                    sourceLocationConstraintName, backendInfoDest } =
                        _prepMetadata(request, sourceObjMD, request.headers,
                            sourceIsDestination, authInfo, destObjectKey,
                            sourceBucketMD, destBucketMD, sourceVersionId, log);
                    if (metadataError) {
                        return next(metadataError, destBucketMD);
                    }
                    if (storeMetadataParams.metaHeaders) {
                        dataStoreContext.metaHeaders =
                          storeMetadataParams.metaHeaders;
                    }
                    let dataLocator;
                    // If 0 byte object just set dataLocator to empty array
                    if (!sourceObjMD.location) {
                        dataLocator = [];
                    } else {
                        // To provide for backwards compatibility before
                        // md-model-version 2, need to handle cases where
                        // objMD.location is just a string
                        dataLocator = Array.isArray(sourceObjMD.location) ?
                        sourceObjMD.location : [{ key: sourceObjMD.location }];
                    }

                    if (sourceObjMD['x-amz-server-side-encryption']) {
                        for (let i = 0; i < dataLocator.length; i++) {
                            dataLocator[i].masterKeyId = sourceObjMD[
                                'x-amz-server-side-encryption-aws-kms-key-id'];
                            dataLocator[i].algorithm =
                                sourceObjMD['x-amz-server-side-encryption'];
                        }
                    }
                    return next(null, storeMetadataParams, dataLocator,
                        sourceBucketMD, destBucketMD, destObjMD,
                        sourceLocationConstraintName, backendInfoDest);
                });
        },
        function goGetData(storeMetadataParams, dataLocator, sourceBucketMD,
            destBucketMD, destObjMD, sourceLocationConstraintName,
            backendInfoDest, next) {
            const serverSideEncryption = destBucketMD.getServerSideEncryption();
            const vcfg = destBucketMD.getVersioningConfiguration();
            const isVersionedObj = vcfg && vcfg.Status === 'Enabled';
            const destLocationConstraintName =
                storeMetadataParams.dataStoreName;
            if (!checkObjectEncryption.isValidSSES3(request,
                serverSideEncryption)) {
                return callback(invalidSSEError);
            }
            // skip if source and dest and location constraint the same and
            // versioning is not enabled
            // still send along serverSideEncryption info so algo
            // and masterKeyId stored properly in metadata
            if (sourceIsDestination && storeMetadataParams.locationMatch
                && !isVersionedObj) {
                return next(null, storeMetadataParams, dataLocator, destObjMD,
                    serverSideEncryption, destBucketMD);
            }

            // also skip if 0 byte object, unless location constraint is an
            // external backend and differs from source, in which case put
            // metadata to backend
            let destLocationConstraintType;
            if (config.backends.data === 'multiple') {
                destLocationConstraintType =
                config.getLocationConstraintType(destLocationConstraintName);
            }
            if (destLocationConstraintType &&
              versioningNotImplBackends[destLocationConstraintType]
                && isVersionedObj) {
                log.debug(externalVersioningErrorMessage,
                    { method: 'multipleBackendGateway',
                        error: errors.NotImplemented });
                return next(errors.NotImplemented.customizeDescription(
                  externalVersioningErrorMessage), destBucketMD);
            }
            if (dataLocator.length === 0) {
                if (!storeMetadataParams.locationMatch &&
                destLocationConstraintType &&
                constants.externalBackends[destLocationConstraintType]) {
                    return data.put(null, null, storeMetadataParams.size,
                        dataStoreContext, backendInfoDest,
                        log, (error, objectRetrievalInfo) => {
                            if (error) {
                                return next(error, destBucketMD);
                            }
                            const putResult = {
                                key: objectRetrievalInfo.key,
                                dataStoreName: objectRetrievalInfo.
                                    dataStoreName,
                                dataStoreType: objectRetrievalInfo.
                                    dataStoreType,
                                size: storeMetadataParams.size,
                            };
                            const putResultArr = [putResult];
                            return next(null, storeMetadataParams, putResultArr,
                                destObjMD, serverSideEncryption, destBucketMD);
                        });
                }
                return next(null, storeMetadataParams, dataLocator, destObjMD,
                    serverSideEncryption, destBucketMD);
            }
            return data.copyObject(request, sourceLocationConstraintName,
              storeMetadataParams, dataLocator, dataStoreContext,
              backendInfoDest, sourceBucketMD, destBucketMD, log,
            (err, results) => {
                if (err) {
                    return next(err, destBucketMD);
                }
                return next(null, storeMetadataParams, results,
                    destObjMD, serverSideEncryption, destBucketMD);
            });
        },
        function getVersioningInfo(storeMetadataParams, destDataGetInfoArr,
            destObjMD, serverSideEncryption, destBucketMD, next) {
            return versioningPreprocessing(destBucketName,
                destBucketMD, destObjectKey, destObjMD, log,
                (err, options) => {
                    if (err) {
                        log.debug('error processing versioning info',
                        { error: err });
                        return next(err, null, destBucketMD);
                    }
                    // eslint-disable-next-line
                    storeMetadataParams.versionId = options.versionId;
                    // eslint-disable-next-line
                    storeMetadataParams.versioning = options.versioning;
                    // eslint-disable-next-line
                    storeMetadataParams.isNull = options.isNull;
                    // eslint-disable-next-line
                    storeMetadataParams.nullVersionId = options.nullVersionId;
                    const dataToDelete = options.dataToDelete;
                    return next(null, storeMetadataParams, destDataGetInfoArr,
                        destObjMD, serverSideEncryption, destBucketMD,
                        dataToDelete);
                });
        },
        function storeNewMetadata(storeMetadataParams, destDataGetInfoArr,
            destObjMD, serverSideEncryption, destBucketMD, dataToDelete, next) {
            return services.metadataStoreObject(destBucketName,
                destDataGetInfoArr, serverSideEncryption,
                storeMetadataParams, (err, result) => {
                    if (err) {
                        log.debug('error storing new metadata', { error: err });
                        return next(err, null, destBucketMD);
                    }
                    const sourceObjSize = storeMetadataParams.size;
                    const destObjPrevSize = (destObjMD &&
                        destObjMD['content-length'] !== undefined) ?
                        destObjMD['content-length'] : null;
                    return next(null, dataToDelete, result, destBucketMD,
                        storeMetadataParams, serverSideEncryption,
                        sourceObjSize, destObjPrevSize);
                });
        },
        function deleteExistingData(dataToDelete, storingNewMdResult,
            destBucketMD, storeMetadataParams, serverSideEncryption,
            sourceObjSize, destObjPrevSize, next) {
            // Clean up any potential orphans in data if object
            // put is an overwrite of already existing
            // object with same name, so long as the source is not
            // the same as the destination
            if (!sourceIsDestination && dataToDelete) {
                const newDataStoreName = storeMetadataParams.dataStoreName;
                const delLog = logger.newRequestLoggerFromSerializedUids(
                    log.getSerializedUids());
                return data.batchDelete(dataToDelete, request.method,
                    newDataStoreName, delLog, err => {
                        if (err) {
                            // if error, log the error and move on as it is not
                            // relevant to the client as the client's
                            // object already succeeded putting data, metadata
                            log.error('error deleting existing data',
                                { error: err });
                        }
                        next(null,
                        storingNewMdResult, destBucketMD, storeMetadataParams,
                        serverSideEncryption, sourceObjSize, destObjPrevSize);
                    });
            }
            return next(null,
                storingNewMdResult, destBucketMD, storeMetadataParams,
                serverSideEncryption, sourceObjSize, destObjPrevSize);
        },
    ], (err, storingNewMdResult, destBucketMD, storeMetadataParams,
        serverSideEncryption, sourceObjSize, destObjPrevSize) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, destBucketMD);

        if (err) {
            return callback(err, null, corsHeaders);
        }
        const xml = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<CopyObjectResult>',
            '<LastModified>', new Date(storeMetadataParams.lastModifiedDate)
                .toISOString(), '</LastModified>',
            '<ETag>&quot;', storeMetadataParams.contentMD5, '&quot;</ETag>',
            '</CopyObjectResult>',
        ].join('');
        const additionalHeaders = corsHeaders || {};
        if (serverSideEncryption) {
            additionalHeaders['x-amz-server-side-encryption'] =
                serverSideEncryption.algorithm;
            if (serverSideEncryption.algorithm === 'aws:kms') {
                additionalHeaders[
                'x-amz-server-side-encryption-aws-kms-key-id'] =
                    serverSideEncryption.masterKeyId;
            }
        }
        if (sourceVersionId) {
            additionalHeaders['x-amz-copy-source-version-id'] =
                versionIdUtils.encode(sourceVersionId);
        }
        const isVersioned = storingNewMdResult && storingNewMdResult.versionId;
        if (isVersioned) {
            additionalHeaders['x-amz-version-id'] =
                versionIdUtils.encode(storingNewMdResult.versionId);
        }

        // Only pre-existing non-versioned objects get 0 all others use 1
        const numberOfObjects = !isVersionedObj && destObjPrevSize !== null ? 0 : 1;

        pushMetric('copyObject', log, {
            authInfo,
            canonicalID: destBucketMD.getOwner(),
            bucket: destBucketName,
            keys: [destObjectKey],
            newByteLength: sourceObjSize,
            oldByteLength: isVersioned ? null : destObjPrevSize,
            location: storeMetadataParams.dataStoreName,
            versionId: isVersioned ? storingNewMdResult.versionId : undefined,
            numberOfObjects,
        });
        // Add expiration header if lifecycle enabled
        return callback(null, xml, additionalHeaders);
    });
}

module.exports = objectCopy;
