import { errors } from 'arsenal';
import async from 'async';

import constants from '../../../../constants';
import data from '../../../data/wrapper';
import metadata from '../../../metadata/wrapper';
import services from '../../../services';
import { logger } from '../../../utilities/logger';
import utils from '../../../utils';
import { dataStore } from './storeObject';
import locationConstraintCheck from './locationConstraintCheck';
import { versioningPreprocessing } from './versioning';
import removeAWSChunked from './removeAWSChunked';
import { decodeVersionId } from './versioning';

function _storeInMDandDeleteData(bucketName, dataGetInfo, cipherBundle,
    metadataStoreParams, dataToDelete, deleteLog, callback) {
    services.metadataStoreObject(bucketName, dataGetInfo,
        cipherBundle, metadataStoreParams, (err, result) => {
            if (err) {
                return callback(err);
            }
            if (dataToDelete) {
                data.batchDelete(dataToDelete, deleteLog);
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
export default
function createAndStoreObject(bucketName, bucketMD, objectKey, objMD, authInfo,
        canonicalID, cipherBundle, request, isDeleteMarker, streamingV4Params,
        log, callback) {
    const size = request.parsedContentLength;

    const websiteRedirectHeader =
        request.headers['x-amz-website-redirect-location'];
    if (!utils.validateWebsiteHeader(websiteRedirectHeader)) {
        const err = errors.InvalidRedirectLocation;
        log.debug('invalid x-amz-website-redirect-location' +
            `value ${websiteRedirectHeader}`, { error: err });
        return callback(err);
    }

    const metaHeaders = utils.getMetaHeaders(request.headers);
    log.trace('meta headers', { metaHeaders, method: 'objectPut' });
    const objectKeyContext = {
        bucketName,
        owner: canonicalID,
        namespace: request.namespace,
        objectKey,
    };
    // If the request was made with a pre-signed url, the x-amz-acl 'header'
    // might be in the query string rather than the actual headers so include
    // it here
    const headers = request.headers;
    if (request.query && request.query['x-amz-acl']) {
        headers['x-amz-acl'] = request.query['x-amz-acl'];
    }
    const metadataStoreParams = {
        objectKey, authInfo, metaHeaders, size,
        contentType: request.headers['content-type'],
        cacheControl: request.headers['cache-control'],
        contentDisposition: request.headers['content-disposition'],
        contentEncoding: removeAWSChunked(request.headers['content-encoding']),
        expires: request.headers.expires,
        headers,
        log,
        isDeleteMarker,
    };
    let dataToDelete = undefined;
    if (objMD && objMD.location) {
        dataToDelete = Array.isArray(objMD.location) ?
            objMD.location : [objMD.location];
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

    const requestLogger =
        logger.newRequestLoggerFromSerializedUids(log.getSerializedUids());
    return async.waterfall([
        function storeData(next) {
            if (size === 0) {
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
            const { key, dataStoreName } = dataGetInfo;
            const dataGetInfoArr = [{ key, size, start: 0, dataStoreName }];
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
        function createNullVerDeleteArray(options, infoArr, next) {
            if (!options.deleteNullVersionData) {
                return next(null, options, infoArr);
            }
            // When options.deleteNullVersionData is true, need to get
            // location info of null version for deletion. Only applies when
            // there is pre-existing null version that is not the latest version
            // and versioning is suspended.
            const params = { versionId: options.nullVersionId };
            return metadata.getObjectMD(bucketName, objectKey,
                params, log, (err, nullObjMD) => {
                    if (err) {
                        log.debug('err from metadata getting null version', {
                            error: err,
                            method: 'createAndStoreObject',
                        });
                        return next(err);
                    }
                    if (nullObjMD.location) {
                        dataToDelete = Array.isArray(nullObjMD.location) ?
                            nullObjMD.location : [nullObjMD.location];
                    }
                    return next(null, options, infoArr);
                });
        },
        function storeMDAndDeleteData(options, infoArr, next) {
            metadataStoreParams.versionId = options.versionId;
            metadataStoreParams.versioning = options.versioning;
            metadataStoreParams.isNull = options.isNull;
            metadataStoreParams.nullVersionId = options.nullVersionId;
            return _storeInMDandDeleteData(bucketName, infoArr,
                cipherBundle, metadataStoreParams,
                options.deleteData ? dataToDelete : undefined,
                requestLogger, next);
        },
    ], callback);
}
