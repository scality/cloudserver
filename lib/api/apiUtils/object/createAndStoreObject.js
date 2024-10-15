const async = require('async');
const { errors, s3middleware } = require('arsenal');
const getMetaHeaders = s3middleware.userMetadata.getMetaHeaders;

const constants = require('../../../../constants');
const { data } = require('../../../data/wrapper');
const services = require('../../../services');
const { dataStore } = require('./storeObject');
const locationConstraintCheck = require('./locationConstraintCheck');
const { versioningPreprocessing, overwritingVersioning } = require('./versioning');
const removeAWSChunked = require('./removeAWSChunked');
const getReplicationInfo = require('./getReplicationInfo');
const { config } = require('../../../Config');
const validateWebsiteHeader = require('./websiteServing')
    .validateWebsiteHeader;
const applyZenkoUserMD = require('./applyZenkoUserMD');
const { VersionID } = require('arsenal/build/lib/versioning');
const { externalBackends, versioningNotImplBackends } = constants;

const externalVersioningErrorMessage = 'We do not currently support putting ' +
'a versioned object to a location-constraint of type Azure or GCP.';

function _storeInMDandDeleteData(bucketName, dataGetInfo, cipherBundle,
    metadataStoreParams, dataToDelete, log, requestMethod, callback) {
    services.metadataStoreObject(bucketName, dataGetInfo,
        cipherBundle, metadataStoreParams, (err, result) => {
            if (err) {
                return callback(err);
            }
            if (dataToDelete) {
                const newDataStoreName = Array.isArray(dataGetInfo) ?
                    dataGetInfo[0].dataStoreName : null;
                return data.batchDelete(dataToDelete, requestMethod,
                    newDataStoreName, log, err => callback(err, result));
            }
            return callback(null, result);
        });
}

/** createAndStoreObject - store data, store metadata, and delete old data
 * and old metadata as necessary
 * @param {string} bucketName - name of bucket
 * @param {BucketInfo} bucketMD - BucketInfo instance
 * @param {string} objectKey - name of object
 * @param {object} objMD - object metadata
 * @param {AuthInfo} authInfo - AuthInfo instance with requester's info
 * @param {string} canonicalID - user's canonical ID
 * @param {object} cipherBundle - cipher bundle that encrypts the data
 * @param {Request} request - http request object
 * @param {boolean} [isDeleteMarker] - whether creating a delete marker
 * @param {(object|null)} streamingV4Params - if v4 auth, object containing
 * accessKey, signatureFromRequest, region, scopeDate, timestamp, and
 * credentialScope (to be used for streaming v4 auth if applicable)
 * @param {(object|null)} overheadField - fields to be included in metadata overhead
 * @param {RequestLogger} log - logger instance
 * @param {string} originOp - Origin operation
 * @param {function} callback - callback function
 * @return {undefined} and call callback with (err, result) -
 * result.contentMD5 - content md5 of new object or version
 * result.versionId - unencrypted versionId returned by metadata
 */
function createAndStoreObject(bucketName, bucketMD, objectKey, objMD, authInfo,
        canonicalID, cipherBundle, request, isDeleteMarker, streamingV4Params,
        overheadField, log, originOp, callback) {
    const putVersionId = request.headers['x-scal-s3-version-id'];
    const isPutVersion = putVersionId || putVersionId === '';

    const size = isDeleteMarker ? 0 : request.parsedContentLength;
    // although the request method may actually be 'DELETE' if creating a
    // delete marker, for our purposes we consider this to be a 'PUT'
    // operation
    const requestMethod = 'PUT';
    const websiteRedirectHeader =
        request.headers['x-amz-website-redirect-location'];
    if (!validateWebsiteHeader(websiteRedirectHeader)) {
        const err = errors.InvalidRedirectLocation;
        log.debug('invalid x-amz-website-redirect-location' +
            `value ${websiteRedirectHeader}`, { error: err });
        return callback(err);
    }

    const metaHeaders = isDeleteMarker ? [] : getMetaHeaders(request.headers);
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

    log.trace('meta headers', { metaHeaders, method: 'objectPut' });
    const objectKeyContext = {
        bucketName,
        owner: canonicalID,
        namespace: request.namespace,
        objectKey,
        metaHeaders,
        tagging: request.headers['x-amz-tagging'],
        isDeleteMarker,
    };
    // If the request was made with a pre-signed url, the x-amz-acl 'header'
    // might be in the query string rather than the actual headers so include
    // it here
    const headers = request.headers;
    if (request.query && request.query['x-amz-acl']) {
        headers['x-amz-acl'] = request.query['x-amz-acl'];
    }
    const metadataStoreParams = {
        objectKey,
        authInfo,
        metaHeaders,
        size,
        headers,
        isDeleteMarker,
        replicationInfo: getReplicationInfo(
            objectKey, bucketMD, false, size, null, null, authInfo),
        overheadField,
        log,
    };

    // For Azure BlobStorage API compatability
    // If an object already exists copy/repair creation-time
    // creation-time must remain static after an object is created
    //  --> EVEN FOR VERSIONS <--
    if (objMD) {
        if (objMD['creation-time']) {
            metadataStoreParams.creationTime = objMD['creation-time'];
        } else {
            // If creation-time is not set (for old objects)
            // fall back to the last modified and store it back to the db
            metadataStoreParams.creationTime = objMD['last-modified'];
        }
    }

    if (!isDeleteMarker) {
        metadataStoreParams.contentType = request.headers['content-type'];
        metadataStoreParams.cacheControl = request.headers['cache-control'];
        metadataStoreParams.contentDisposition =
            request.headers['content-disposition'];
        metadataStoreParams.contentEncoding =
            removeAWSChunked(request.headers['content-encoding']);
        metadataStoreParams.expires = request.headers.expires;
        metadataStoreParams.tagging = request.headers['x-amz-tagging'];
        metadataStoreParams.originOp = originOp;
        const defaultObjectLockConfiguration
            = bucketMD.getObjectLockConfiguration();
        if (defaultObjectLockConfiguration) {
            metadataStoreParams.defaultRetention
                = defaultObjectLockConfiguration;
        }
    }

    // if creating new delete marker and there is an existing object, copy
    // the object's location constraint metaheader to determine backend info
    if (isDeleteMarker && objMD) {
        // eslint-disable-next-line no-param-reassign
        request.headers[constants.objectLocationConstraintHeader] =
            objMD[constants.objectLocationConstraintHeader];
        metadataStoreParams.originOp = originOp;
    }

    const backendInfoObj =
        locationConstraintCheck(request, null, bucketMD, log);
    if (backendInfoObj.err) {
        return process.nextTick(() => {
            callback(backendInfoObj.err);
        });
    }

    const backendInfo = backendInfoObj.backendInfo;
    const location = backendInfo.getControllingLocationConstraint();
    const locationType = backendInfoObj.defaultedToDataBackend ? location :
        config.getLocationConstraintType(location);
    metadataStoreParams.dataStoreName = location;

    if (versioningNotImplBackends[locationType]) {
        const vcfg = bucketMD.getVersioningConfiguration();
        const isVersionedObj = vcfg && vcfg.Status === 'Enabled';

        if (isVersionedObj) {
            log.debug(externalVersioningErrorMessage,
              { method: 'createAndStoreObject', error: errors.NotImplemented });
            return process.nextTick(() => {
                callback(errors.NotImplemented.customizeDescription(
                  externalVersioningErrorMessage));
            });
        }
    }

    if (objMD && objMD.uploadId) {
        metadataStoreParams.oldReplayId = objMD.uploadId;
    }

    if (isPutVersion && location === bucketMD.getLocationConstraint() && bucketMD.isIngestionBucket()) {
        // When restoring to OOB bucket, we cannot force the versionId of the object written to the
        // backend, and it is thus not match the versionId of the ingested object. Thus we add extra
        // user metadata to allow OOB to allow ingestion processor to "match" the (new) restored
        // object with the existing ingested object.
        objectKeyContext.metaHeaders = {
            ...objectKeyContext.metaHeaders,
            'x-amz-meta-scal-version-id': putVersionId,
            'x-amz-meta-scal-restore-info': objMD['x-scal-restore-info'],
        };
    }

    /* eslint-disable camelcase */
    const dontSkipBackend = externalBackends;
    /* eslint-enable camelcase */

    const mdOnlyHeader = request.headers['x-amz-meta-mdonly'];
    const mdOnlySize = request.headers['x-amz-meta-size'];

    return async.waterfall([
        function storeData(next) {
            if (size === 0) {
                if (!dontSkipBackend[locationType]) {
                    metadataStoreParams.contentMD5 = constants.emptyFileMd5;
                    return next(null, null, null);
                }
                // Handle mdOnlyHeader as a metadata only operation. If
                // the object in question is actually 0 byte or has a body size
                // then handle normally.
                if (mdOnlyHeader === 'true' && mdOnlySize > 0) {
                    log.debug('metadata only operation x-amz-meta-mdonly');
                    const md5 = request.headers['x-amz-meta-md5chksum']
                        ? new Buffer(request.headers['x-amz-meta-md5chksum'],
                        'base64').toString('hex') : null;
                    const numParts = request.headers['x-amz-meta-md5numparts'];
                    let _md5;
                    if (numParts === undefined) {
                        _md5 = md5;
                    } else {
                        _md5 = `${md5}-${numParts}`;
                    }
                    const versionId = request.headers['x-amz-meta-version-id'];
                    const dataGetInfo = {
                        key: objectKey,
                        dataStoreName: location,
                        dataStoreType: locationType,
                        dataStoreVersionId: versionId,
                        dataStoreMD5: _md5,
                    };
                    return next(null, dataGetInfo, _md5);
                }
            }
            return dataStore(objectKeyContext, cipherBundle, request, size,
                    streamingV4Params, backendInfo, log, next);
        },
        function processDataResult(dataGetInfo, calculatedHash, next) {
            if (dataGetInfo === null || dataGetInfo === undefined) {
                return next(null, null);
            }
            // So that data retrieval information for MPU's and
            // regular puts are stored in the same data structure,
            // place the retrieval info here into a single element array
            const { key, dataStoreName, dataStoreType, dataStoreETag,
                dataStoreVersionId } = dataGetInfo;
            const prefixedDataStoreETag = dataStoreETag
                      ? `1:${dataStoreETag}`
                      : `1:${calculatedHash}`;
            const dataGetInfoArr = [{ key, size, start: 0, dataStoreName,
                dataStoreType, dataStoreETag: prefixedDataStoreETag,
                dataStoreVersionId }];
            if (cipherBundle) {
                dataGetInfoArr[0].cryptoScheme = cipherBundle.cryptoScheme;
                dataGetInfoArr[0].cipheredDataKey =
                    cipherBundle.cipheredDataKey;
            }
            if (mdOnlyHeader === 'true') {
                metadataStoreParams.size = mdOnlySize;
                dataGetInfoArr[0].size = mdOnlySize;
            }
            metadataStoreParams.contentMD5 = calculatedHash;
            return next(null, dataGetInfoArr);
        },
        function getVersioningInfo(infoArr, next) {
            // if x-scal-s3-version-id header is specified, we overwrite the object/version metadata.
            if (isPutVersion) {
                const options = overwritingVersioning(objMD, metadataStoreParams);
                return process.nextTick(() => next(null, options, infoArr));
            }
            if (!bucketMD.isVersioningEnabled() && objMD?.archive?.archiveInfo) {
                // Ensure we trigger a "delete" event in the oplog for the previously archived object
                metadataStoreParams.needOplogUpdate = 's3:ReplaceArchivedObject';
            }
            return versioningPreprocessing(bucketName, bucketMD,
                metadataStoreParams.objectKey, objMD, log, (err, options) => {
                    if (err) {
                        // TODO: check AWS error when user requested a specific
                        // version before any versions have been put
                        const logLvl = err.is.BadRequest ?
                            'debug' : 'error';
                        log[logLvl]('error getting versioning info', {
                            error: err,
                            method: 'versioningPreprocessing',
                        });
                    }
                    return next(err, options, infoArr);
                });
        },
        function storeMDAndDeleteData(options, infoArr, next) {
            const location = infoArr[0].dataStoreName;
            if (location === bucketMD.getLocationConstraint() && bucketMD.isIngestionBucket()) {
                // If the object is being written to the "ingested" storage location, keep the same
                // versionId for consistency and to avoid creating an extra version when it gets
                // ingested
                metadataStoreParams.versionId = VersionID.decode(infoArr[0].dataStoreVersionId);
            } else {
                metadataStoreParams.versionId = options.versionId;
            }
            metadataStoreParams.versioning = options.versioning;
            metadataStoreParams.isNull = options.isNull;
            metadataStoreParams.deleteNullKey = options.deleteNullKey;
            if (options.extraMD) {
                Object.assign(metadataStoreParams, options.extraMD);
            }
            return _storeInMDandDeleteData(bucketName, infoArr,
                cipherBundle, metadataStoreParams,
                options.dataToDelete, log, requestMethod, next);
        },
    ], callback);
}

module.exports = createAndStoreObject;
