import async from 'async';
import { errors } from 'arsenal';

import constants from '../../constants';
import data from '../data/wrapper';
import kms from '../kms/wrapper';
import metadata from '../metadata/wrapper';
import RelayMD5Sum from '../utilities/RelayMD5Sum';
import { logger } from '../utilities/logger';
import services from '../services';
import setUpCopyLocator from './apiUtils/object/setUpCopyLocator';
import validateHeaders from '../utilities/validateHeaders';


/**
 * PUT Part Copy during a multipart upload.
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with
 * requester's info
 * @param {request} request - request object given by router,
 *                            includes normalized headers
 * @param {string} sourceBucket - name of source bucket for object copy
 * @param {string} sourceObject - name of source object for object copy
 * @param {object} log - the request logger
 * @param {function} callback - final callback to call with the result
 * @return {undefined}
 */
export default
function objectPutCopyPart(authInfo, request, sourceBucket,
    sourceObject, log, callback) {
    log.debug('processing request', { method: 'objectPutCopyPart' });
    const destBucketName = request.bucketName;
    const destObjectKey = request.objectKey;
    const valGetParams = {
        authInfo,
        bucketName: sourceBucket,
        objectKey: sourceObject,
        requestType: 'objectGet',
        log,
    };


    const partNumber = Number.parseInt(request.query.partNumber, 10);
    // AWS caps partNumbers at 10,000
    if (partNumber > 10000 || !Number.isInteger(partNumber) || partNumber < 1) {
        return callback(errors.InvalidArgument);
    }
    // We pad the partNumbers so that the parts will be sorted
    // in numerical order
    const paddedPartNumber = `000000${partNumber}`.substr(-5);
    // Note that keys in the query object retain their case, so
    // request.query.uploadId must be called with that exact
    // capitalization
    const uploadId = request.query.uploadId;

    const valPutParams = {
        authInfo,
        bucketName: destBucketName,
        objectKey: destObjectKey,
        requestType: 'objectPut',
        log,
    };

    // For validating the request at the MPU, the params are the same
    // as validating for the destination bucket except additionally need
    // the uploadId and splitter.
    // Also, requestType is 'putPart or complete'
    const valMPUParams = Object.assign({
        uploadId,
        splitter: constants.splitter,
    }, valPutParams);
    valMPUParams.requestType = 'putPart or complete';

    const dataStoreContext = {
        objectKey: destObjectKey,
        bucketName: destBucketName,
        owner: authInfo.getCanonicalID(),
        namespace: request.namespace,
    };

    return async.waterfall([
        function checkSourceAuthorization(next) {
            return services.metadataValidateAuthorization(valGetParams,
                (err, sourceBucketMD, sourceObjMD) => {
                    if (err) {
                        log.debug('error validating get part of request',
                        { error: err });
                        return next(err);
                    }
                    if (!sourceObjMD) {
                        log.debug('no source object', { sourceObject });
                        return next(errors.NoSuchKey);
                    }
                    const headerValResult =
                        validateHeaders(sourceObjMD, request.headers);
                    if (headerValResult.error) {
                        return next(errors.PreconditionFailed);
                    }
                    const copyLocator = setUpCopyLocator(sourceObjMD,
                        request.headers['x-amz-copy-source-range'], log);
                    if (copyLocator.error) {
                        return next(copyLocator.error);
                    }
                    return next(null, copyLocator.dataLocator,
                        copyLocator.copyObjectSize);
                });
        },
        function checkDestAuth(dataLocator, copyObjectSize, next) {
            return services.metadataValidateAuthorization(valPutParams,
                (err, destBucketMD) => {
                    if (err) {
                        log.debug('error validating authorization for ' +
                        'destination bucket',
                        { error: err });
                        return next(err);
                    }
                    const flag = destBucketMD.hasDeletedFlag()
                        || destBucketMD.hasTransientFlag();
                    if (flag) {
                        log.trace('deleted flag or transient flag ' +
                        'on destination bucket', { flag });
                        return next(errors.NoSuchBucket);
                    }
                    return next(null, dataLocator, destBucketMD,
                        copyObjectSize);
                });
        },
        function checkMPUBucketAuth(dataLocator, destBucketMD,
            copyObjectSize, next) {
            return services.metadataValidateMultipart(valMPUParams,
                (err, mpuBucket) => {
                    if (err) {
                        log.trace('error authorizing based on mpu bucket',
                        { error: err });
                        return next(err);
                    }
                    return next(null, dataLocator,
                        destBucketMD, mpuBucket, copyObjectSize);
                });
        },
        function goGetData(dataLocator, destBucketMD,
            mpuBucket, copyObjectSize, next) {
            const serverSideEncryption = destBucketMD.getServerSideEncryption();

            // skip if 0 byte object
            if (dataLocator.length === 0) {
                return next(null, [], constants.emptyFileMd5,
                    copyObjectSize, mpuBucket,
                    serverSideEncryption);
            }
            // totalHash will be sent through the RelayMD5Sum transform streams
            // to collect the md5 from multiple streams
            let totalHash;
            const locations = [];
             // dataLocator is an array.  need to get and put all parts
             // in order so can get the ETag of full object
            return async.forEachOfSeries(dataLocator,
                // eslint-disable-next-line prefer-arrow-callback
                function copyPart(part, index, cb) {
                    return data.get(part, log, (err, stream) => {
                        if (err) {
                            log.debug('error getting object part',
                            { error: err });
                            return cb(err);
                        }
                        const hashedStream =
                            new RelayMD5Sum(totalHash, updatedHash => {
                                totalHash = updatedHash;
                            });
                        stream.pipe(hashedStream);
                        const numberPartSize =
                            Number.parseInt(part.size, 10);
                        if (serverSideEncryption) {
                            return kms.createCipherBundle(
                                serverSideEncryption,
                                log, (err, cipherBundle) => {
                                    if (err) {
                                        log.debug('error getting cipherBundle',
                                        { error: err });
                                        return cb(errors.InternalError);
                                    }
                                    return data.put(cipherBundle, hashedStream,
                                        numberPartSize, dataStoreContext, log,
                                        (error, partRetrievalInfo) => {
                                            if (error) {
                                                log.debug('error putting ' +
                                                'encrypted part', { error });
                                                return cb(error);
                                            }
                                            const partResult = {
                                                key: partRetrievalInfo.key,
                                                dataStoreName: partRetrievalInfo
                                                    .dataStoreName,
                                                // Do not include part start
                                                // here since will change in
                                                // final MPU object
                                                size: part.size,
                                                sseCryptoScheme: cipherBundle
                                                    .cryptoScheme,
                                                sseCipheredDataKey: cipherBundle
                                                    .cipheredDataKey,
                                                sseAlgorithm: cipherBundle
                                                    .algorithm,
                                                sseMasterKeyId: cipherBundle
                                                    .masterKeyId,
                                            };
                                            locations.push(partResult);
                                            return cb();
                                        });
                                });
                        }
                        // Copied object is not encrypted so just put it
                        // without a cipherBundle
                        return data.put(null, hashedStream, numberPartSize,
                        dataStoreContext, log, (error, partRetrievalInfo) => {
                            if (error) {
                                log.debug('error putting object part',
                                { error });
                                return cb(error);
                            }
                            const partResult = {
                                key: partRetrievalInfo.key,
                                dataStoreName: partRetrievalInfo.dataStoreName,
                                size: part.size,
                            };
                            locations.push(partResult);
                            return cb();
                        });
                    });
                }, err => {
                    if (err) {
                        log.debug('error transferring data from source',
                        { error: err, method: 'goGetData' });
                        return next(err);
                    }
                    // Digest the final combination of all of the part streams
                    totalHash = totalHash.digest('hex');
                    return next(null, locations, totalHash,
                        copyObjectSize, mpuBucket,
                        serverSideEncryption);
                });
        },
        function getExistingPartInfo(locations, totalHash,
            copyObjectSize, mpuBucket, serverSideEncryption, next) {
            const partKey =
                `${uploadId}${constants.splitter}${paddedPartNumber}`;
            metadata.getObjectMD(mpuBucket.getName(), partKey, log,
                (err, result) => {
                    // If there is nothing being overwritten just move on
                    if (err && !err.NoSuchKey) {
                        log.debug('error getting current part (if any)',
                        { error: err });
                        return next(err);
                    }
                    let oldLocations;
                    if (result) {
                        oldLocations = result.partLocations;
                        // Pull locations to clean up any potential orphans
                        // in data if object put is an overwrite of
                        // already existing object with same key and part number
                        oldLocations = Array.isArray(oldLocations) ?
                            oldLocations : [oldLocations];
                    }
                    return next(null, locations, totalHash,
                        copyObjectSize, mpuBucket, serverSideEncryption,
                        oldLocations);
                });
        },
        function storeNewPartMetadata(locations, totalHash,
            copyObjectSize, mpuBucket, serverSideEncryption,
            oldLocations, next) {
            const lastModified = new Date().toJSON();
            const metaStoreParams = {
                partNumber: paddedPartNumber,
                contentMD5: totalHash,
                size: copyObjectSize,
                uploadId,
                splitter: constants.splitter,
                lastModified,
            };
            return services.metadataStorePart(mpuBucket.getName(),
                locations, metaStoreParams, log, err => {
                    if (err) {
                        log.debug('error storing new metadata',
                        { error: err, method: 'storeNewPartMetadata' });
                        return next(err);
                    }
                    // Clean up the old data now that new metadata (with new
                    // data locations) has been stored
                    if (oldLocations) {
                        data.batchDelete(oldLocations,
                            logger.newRequestLoggerFromSerializedUids(
                                log.getSerializedUids()));
                    }
                    return next(null, totalHash, lastModified,
                        serverSideEncryption);
                });
        },
    ], (err, totalHash, lastModified, serverSideEncryption) => {
        if (err) {
            log.trace('error from copy part waterfall',
            { error: err });
            return callback(err);
        }
        const xml = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<CopyPartResult>',
            '<LastModified>', new Date(lastModified)
                .toUTCString(), '</LastModified>',
            '<ETag>&quot;', totalHash, '&quot;</ETag>',
            '</CopyPartResult>',
        ].join('');
        // TODO: Add version headers for response
        // (if source is a version).
        const additionalHeaders = {};
        if (serverSideEncryption) {
            additionalHeaders['x-amz-server-side-encryption'] =
                serverSideEncryption.algorithm;
            if (serverSideEncryption.algorithm === 'aws:kms') {
                additionalHeaders
                ['x-amz-server-side-encryption-aws-kms-key-id'] =
                    serverSideEncryption.masterKeyId;
            }
        }
        return callback(null, xml, additionalHeaders);
    });
}
