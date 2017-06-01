const async = require('async');
const { errors } = require('arsenal');

const constants = require('../../../../constants');
const data = require('../../../data/wrapper');
const services = require('../../../services');
const logger = require('../../../utilities/logger');
const utils = require('../../../utils');
const { dataStore } = require('./storeObject');
const locationConstraintCheck = require('./locationConstraintCheck');
const { versioningPreprocessing } = require('./versioning');
const removeAWSChunked = require('./removeAWSChunked');
const { decodeVersionId } = require('./versioning');
const { config } = require('../../../Config');

function _storeInMDandDeleteData(bucketName, dataGetInfo, cipherBundle,
    metadataStoreParams, dataToDelete, deleteLog, requestMethod, callback) {
    services.metadataStoreObject(bucketName, dataGetInfo,
        cipherBundle, metadataStoreParams, (err, result) => {
            if (err) {
                return callback(err);
            }
            if (dataToDelete) {
                const newDataStoreName = Array.isArray(dataGetInfo) ?
                    dataGetInfo[0].dataStoreName : null;
                data.batchDelete(dataToDelete, requestMethod,
                newDataStoreName, deleteLog);
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
 * @param {RequestLogger} log - logger instance
 * @param {function} callback - callback function
 * @return {undefined} and call callback with (err, result) -
 * result.contentMD5 - content md5 of new object or version
 * result.versionId - unencrypted versionId returned by metadata
 */
function createAndStoreObject(bucketName, bucketMD, objectKey, objMD, authInfo,
        canonicalID, cipherBundle, request, isDeleteMarker, streamingV4Params,
        log, callback) {
    const size = isDeleteMarker ? 0 : request.parsedContentLength;

    const websiteRedirectHeader =
        request.headers['x-amz-website-redirect-location'];
    if (!utils.validateWebsiteHeader(websiteRedirectHeader)) {
        const err = errors.InvalidRedirectLocation;
        log.debug('invalid x-amz-website-redirect-location' +
            `value ${websiteRedirectHeader}`, { error: err });
        return callback(err);
    }

    const metaHeaders = isDeleteMarker ? [] :
        utils.getMetaHeaders(request.headers);
    if (metaHeaders instanceof Error) {
        log.debug('user metadata validation failed', {
            error: metaHeaders,
            method: 'createAndStoreObject',
        });
        return process.nextTick(() => callback(metaHeaders));
    }
    log.trace('meta headers', { metaHeaders, method: 'objectPut' });
    const objectKeyContext = {
        bucketName,
        owner: canonicalID,
        namespace: request.namespace,
        objectKey,
        metaHeaders,
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
    }

    const decodedVidResult = decodeVersionId(request.query);
    if (decodedVidResult instanceof Error) {
        log.trace('invalid versionId query', {
            versionId: request.query.versionId,
            error: decodedVidResult,
        });
        return callback(decodedVidResult);
    }
    const reqVersionId = decodedVidResult;

    const backendInfoObj =
        locationConstraintCheck(request, null, bucketMD, log);
    if (backendInfoObj.err) {
        return process.nextTick(() => {
            callback(backendInfoObj.err);
        });
    }
    const backendInfo = backendInfoObj.backendInfo;
    const location = backendInfo.getControllingLocationConstraint();
    const locationType = config.locationConstraints[location].type;
    /* eslint-disable camelcase */
    const dontSkipBackend = { aws_s3: true };
    /* eslint-enable camelcase */

    const requestLogger =
        logger.newRequestLoggerFromSerializedUids(log.getSerializedUids());
    return async.waterfall([
        function storeData(next) {
            if (size === 0 && !dontSkipBackend[locationType]) {
                metadataStoreParams.contentMD5 = constants.emptyFileMd5;
                return next(null, null, null);
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
            const { key, dataStoreName, dataStoreType, dataStoreETag } =
                dataGetInfo;
            const dataGetInfoArr = [{ key, size, start: 0, dataStoreName,
                dataStoreType, dataStoreETag }];
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
                metadataStoreParams.objectKey, objMD, reqVersionId, log,
                (err, options) => {
                    if (err) {
                        // TODO: check AWS error when user requested a specific
                        // version before any versions have been put
                        const logLvl = err === errors.BadRequest ?
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
            metadataStoreParams.nullVersionId = options.nullVersionId;
            return _storeInMDandDeleteData(bucketName, infoArr,
                cipherBundle, metadataStoreParams,
                options.dataToDelete, requestLogger, request.method, next);
        },
    ], callback);
}

module.exports = createAndStoreObject;
