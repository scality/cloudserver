const async = require('async');
const { errors, s3middleware } = require('arsenal');
const getMetaHeaders = s3middleware.userMetadata.getMetaHeaders;

const constants = require('../../../../constants');
const { data } = require('../../../data/wrapper');
const services = require('../../../services');
const { dataStore } = require('./storeObject');
const locationConstraintCheck = require('./locationConstraintCheck');
const { versioningPreprocessing } = require('./versioning');
const removeAWSChunked = require('./removeAWSChunked');
const getReplicationInfo = require('./getReplicationInfo');
const { config } = require('../../../Config');
const validateWebsiteHeader = require('./websiteServing')
    .validateWebsiteHeader;
const {
    externalBackends, versioningNotImplBackends, zenkoIDHeader,
} = constants;

const externalVersioningErrorMessage = 'We do not currently support putting ' +
'a versioned object to a location-constraint of type Azure.';

/**
 * Retro-propagation is where S3C ingestion will re-ingest an object whose
 * request originated from Zenko.
 * To avoid this, Zenko requests which create objects/versions will be tagged
 * with a user-metadata header defined in constants.zenkoIDHeader. When
 * ingesting objects into Zenko, we can determine if this object has already
 * been created in Zenko.
 * Delete marker requests cannot specify user-metadata fields, so we instead
 * rely on checking the "user-agent" to see the origin of a request.
 * If delete marker, and user-agent came from a Zenko client, we add the
 * user-metadata field to the object metadata.
 * @param {Object} metaHeaders - user metadata object
 * @param {http.ClientRequest} request - client request with user-agent header
 * @param {Boolean} isDeleteMarker - delete marker indicator
 * @return {undefined}
 */
function _checkAndApplyZenkoMD(metaHeaders, request, isDeleteMarker) {
    const userAgent = request.headers['user-agent'];

    if (isDeleteMarker && userAgent && userAgent.includes('Zenko')) {
        // eslint-disable-next-line no-param-reassign
        metaHeaders[zenkoIDHeader] = 'zenko';
    }
}

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
 * @param {function} callback - callback function
 * @return {undefined} and call callback with (err, result) -
 * result.contentMD5 - content md5 of new object or version
 * result.versionId - unencrypted versionId returned by metadata
 */
function createAndStoreObject(bucketName, bucketMD, objectKey, objMD, authInfo,
        canonicalID, cipherBundle, request, isDeleteMarker, streamingV4Params,
        overheadField, log, callback) {
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
    // if receiving a request from Zenko for a delete marker, we place a
    // user-metadata field on the object
    _checkAndApplyZenkoMD(metaHeaders, request, isDeleteMarker);

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
        replicationInfo: getReplicationInfo(objectKey, bucketMD, false, size, null, null, authInfo, isDeleteMarker),
        overheadField,
        log,
    };

    if (!isDeleteMarker) {
        metadataStoreParams.contentType = request.headers['content-type'];
        metadataStoreParams.cacheControl = request.headers['cache-control'];
        metadataStoreParams.contentDisposition =
            request.headers['content-disposition'];
        metadataStoreParams.contentEncoding =
            removeAWSChunked(request.headers['content-encoding']);
        metadataStoreParams.expires = request.headers.expires;
        metadataStoreParams.tagging = request.headers['x-amz-tagging'];
        metadataStoreParams.originOp = 's3:ObjectCreated:Put';
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
        metadataStoreParams.originOp = 's3:ObjectRemoved:DeleteMarkerCreated';
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

    /* eslint-disable camelcase */
    const dontSkipBackend = externalBackends;
    /* eslint-enable camelcase */

    return async.waterfall([
        function storeData(next) {
            if (size === 0 && !dontSkipBackend[locationType]) {
                metadataStoreParams.contentMD5 = constants.emptyFileMd5;
                return next(null, null, null);
            }
            // Object Post receives a file stream.
            //  This is to be used to store data instead of the request stream itself.

            let stream;

            if (request.apiMethod === 'objectPost') {
                stream = request.fileEventData ? request.fileEventData.file : undefined;
            } else {
                stream = request;
            }

            return dataStore(objectKeyContext, cipherBundle, stream, size, streamingV4Params, backendInfo, log, next);
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
            metadataStoreParams.contentMD5 = calculatedHash;
            return next(null, dataGetInfoArr);
        },
        function getVersioningInfo(infoArr, next) {
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
            metadataStoreParams.versionId = options.versionId;
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
