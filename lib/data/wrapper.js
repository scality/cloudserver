const async = require('async');
const { errors, s3middleware } = require('arsenal');
const PassThrough = require('stream').PassThrough;

const DataFileInterface = require('./file/backend');
const inMemory = require('./in_memory/backend').backend;
const multipleBackendGateway = require('./multipleBackendGateway');
const utils = require('./external/utils');
const { config } = require('../Config');
const MD5Sum = s3middleware.MD5Sum;
const NullStream = s3middleware.NullStream;
const assert = require('assert');
const kms = require('../kms/wrapper');
const externalBackends = require('../../constants').externalBackends;
const constants = require('../../constants');
const { BackendInfo } = require('../api/apiUtils/object/BackendInfo');
const RelayMD5Sum = require('../utilities/RelayMD5Sum');
const skipError = new Error('skip');

let CdmiData;
try {
    CdmiData = require('cdmiclient').CdmiData;
} catch (err) {
    CdmiData = null;
}

let client;
let implName;

if (config.backends.data === 'mem') {
    client = inMemory;
    implName = 'mem';
} else if (config.backends.data === 'file') {
    client = new DataFileInterface();
    implName = 'file';
} else if (config.backends.data === 'multiple') {
    client = multipleBackendGateway;
    implName = 'multipleBackends';
} else if (config.backends.data === 'cdmi') {
    if (!CdmiData) {
        throw new Error('Unauthorized backend');
    }

    client = new CdmiData({
        path: config.cdmi.path,
        host: config.cdmi.host,
        port: config.cdmi.port,
        readonly: config.cdmi.readonly,
    });
    implName = 'cdmi';
}

/**
 * _retryDelete - Attempt to delete key again if it failed previously
 * @param { string | object } objectGetInfo - either string location of object
 *      to delete or object containing info of object to delete
 * @param {object} log - Werelogs request logger
 * @param {number} count - keeps count of number of times function has been run
 * @param {function} cb - callback
 * @returns undefined and calls callback
 */
const MAX_RETRY = 2;

// This check is done because on a put, complete mpu or copy request to
// Azure/AWS, if the object already exists on that backend, the existing object
// should not be deleted, which is the functionality for all other backends
function _shouldSkipDelete(locations, requestMethod, newObjDataStoreName) {
    const skipMethods = { PUT: true, POST: true };
    if (!Array.isArray(locations) || !locations[0] ||
        !locations[0].dataStoreType) {
        return false;
    }
    const isSkipBackend = externalBackends[locations[0].dataStoreType];
    const isMatchingBackends =
        locations[0].dataStoreName === newObjDataStoreName;
    const isSkipMethod = skipMethods[requestMethod];
    return (isSkipBackend && isMatchingBackends && isSkipMethod);
}

function _retryDelete(objectGetInfo, log, count, cb) {
    if (count > MAX_RETRY) {
        return cb(errors.InternalError);
    }
    return client.delete(objectGetInfo, log.getSerializedUids(), err => {
        if (err) {
            log.error('delete error from datastore',
                      { error: err, implName, moreRetries: 'yes' });
            return _retryDelete(objectGetInfo, log, count + 1, cb);
        }
        return cb();
    });
}

function _put(cipherBundle, value, valueSize,
              keyContext, backendInfo, log, cb) {
    assert.strictEqual(typeof valueSize, 'number');
    log.debug('sending put to datastore', { implName, keyContext,
        method: 'put' });
    let hashedStream = null;
    if (value) {
        hashedStream = new MD5Sum();
        value.pipe(hashedStream);
    }

    if (implName === 'multipleBackends') {
        // Need to send backendInfo to client.put and
        // client.put will provide dataRetrievalInfo so no
        // need to construct here
        /* eslint-disable no-param-reassign */
        keyContext.cipherBundle = cipherBundle;
        return client.put(hashedStream,
               valueSize, keyContext, backendInfo, log.getSerializedUids(),
               (err, dataRetrievalInfo) => {
                   if (err) {
                       log.error('put error from datastore',
                                 { error: err, implName });
                       return cb(errors.ServiceUnavailable);
                   }
                   return cb(null, dataRetrievalInfo, hashedStream);
               });
    }
    /* eslint-enable no-param-reassign */

    let writeStream = hashedStream;
    if (cipherBundle && cipherBundle.cipher) {
        writeStream = cipherBundle.cipher;
        hashedStream.pipe(writeStream);
    }

    return client.put(writeStream, valueSize, keyContext,
                      log.getSerializedUids(), (err, key) => {
                          if (err) {
                              log.error('put error from datastore',
                                        { error: err, implName });
                              return cb(errors.InternalError);
                          }
                          const dataRetrievalInfo = {
                              key,
                              dataStoreName: implName,
                          };
                          return cb(null, dataRetrievalInfo, hashedStream);
                      });
}

const data = {
    put: (cipherBundle, value, valueSize, keyContext, backendInfo, log, cb) => {
        _put(cipherBundle, value, valueSize, keyContext, backendInfo, log,
             (err, dataRetrievalInfo, hashedStream) => {
                 if (err) {
                     return cb(err);
                 }
                 if (hashedStream) {
                     if (hashedStream.completedHash) {
                         return cb(null, dataRetrievalInfo, hashedStream);
                     }
                     hashedStream.on('hashed', () => {
                         hashedStream.removeAllListeners('hashed');
                         return cb(null, dataRetrievalInfo, hashedStream);
                     });
                     return undefined;
                 }
                 return cb(null, dataRetrievalInfo);
             });
    },

    head: (objectGetInfo, log, cb) => {
        if (implName !== 'multipleBackends') {
            // no-op if not multipleBackend implementation;
            // head is used during get just to check external backend data state
            return process.nextTick(cb);
        }
        return client.head(objectGetInfo, log.getSerializedUids(), cb);
    },

    get: (objectGetInfo, response, log, cb) => {
        const isMdModelVersion2 = typeof(objectGetInfo) === 'string';
        const isRequiredStringKey = constants.clientsRequireStringKey[implName];
        const key = isMdModelVersion2 ? objectGetInfo : objectGetInfo.key;
        const clientGetInfo = isRequiredStringKey ? key : objectGetInfo;
        const range = objectGetInfo.range;

        // If the key is explicitly set to null, the part to
        // be read doesn't really exist and is only made of zeroes.
        // This functionality is used by Scality-NFSD.
        // Otherwise, the key is always defined
        assert(key === null || key !== undefined);
        if (key === null) {
            cb(null, new NullStream(objectGetInfo.size, range));
            return;
        }
        log.debug('sending get to datastore', { implName,
            key, range, method: 'get' });
        // We need to use response as a writable stream for AZURE GET
        if (!isMdModelVersion2 && !isRequiredStringKey && response) {
            clientGetInfo.response = response;
        }
        client.get(clientGetInfo, range, log.getSerializedUids(),
            (err, stream) => {
                if (err) {
                    log.error('get error from datastore',
                              { error: err, implName });
                    return cb(errors.ServiceUnavailable);
                }
                if (objectGetInfo.cipheredDataKey) {
                    const serverSideEncryption = {
                        cryptoScheme: objectGetInfo.cryptoScheme,
                        masterKeyId: objectGetInfo.masterKeyId,
                        cipheredDataKey: Buffer.from(
                            objectGetInfo.cipheredDataKey, 'base64'),
                    };
                    const offset = objectGetInfo.range ?
                        objectGetInfo.range[0] : 0;
                    return kms.createDecipherBundle(
                        serverSideEncryption, offset, log,
                        (err, decipherBundle) => {
                            if (err) {
                                log.error('cannot get decipher bundle ' +
                                    'from kms', {
                                        method: 'data.wrapper.data.get',
                                    });
                                return cb(err);
                            }
                            stream.pipe(decipherBundle.decipher);
                            return cb(null, decipherBundle.decipher);
                        });
                }
                return cb(null, stream);
            });
    },

    delete: (objectGetInfo, log, cb) => {
        const callback = cb || log.end;
        const isMdModelVersion2 = typeof(objectGetInfo) === 'string';
        const isRequiredStringKey = constants.clientsRequireStringKey[implName];
        const key = isMdModelVersion2 ? objectGetInfo : objectGetInfo.key;
        const clientGetInfo = isRequiredStringKey ? key : objectGetInfo;

        log.trace('sending delete to datastore', {
            implName, key, method: 'delete' });
        // If the key is explicitly set to null, the part to
        // be deleted doesn't really exist.
        // This functionality is used by Scality-NFSD.
        // Otherwise, the key is always defined
        assert(key === null || key !== undefined);
        if (key === null) {
            callback(null);
            return;
        }
        _retryDelete(clientGetInfo, log, 0, err => {
            if (err) {
                log.error('delete error from datastore',
                    { error: err, key: objectGetInfo.key, moreRetries: 'no' });
            }
            return callback(err);
        });
    },
    // It would be preferable to have an sproxyd batch delete route to
    // replace this
    batchDelete: (locations, requestMethod, newObjDataStoreName, log) => {
        // TODO: The method of persistence of sproxy delete key will
        // be finalized; refer Issue #312 for the discussion. In the
        // meantime, we at least log the location of the data we are
        // about to delete before attempting its deletion.
        if (_shouldSkipDelete(locations, requestMethod, newObjDataStoreName)) {
            return;
        }
        log.trace('initiating batch delete', {
            keys: locations,
            implName,
            method: 'batchDelete',
        });
        async.eachLimit(locations, 5, (loc, next) => {
            process.nextTick(() => data.delete(loc, log, next));
        },
        err => {
            if (err) {
                log.error('batch delete failed', { error: err });
            } else {
                log.trace('batch delete successfully completed');
            }
            log.end();
        });
    },

    switch: newClient => {
        client = newClient;
        return client;
    },

    checkHealth: (log, cb, flightCheckOnStartUp) => {
        if (!client.healthcheck) {
            const defResp = {};
            defResp[implName] = { code: 200, message: 'OK' };
            return cb(null, defResp);
        }
        return client.healthcheck(flightCheckOnStartUp, log, (err, result) => {
            let respBody = {};
            if (err) {
                log.error(`error from ${implName}`, { error: err });
                respBody[implName] = {
                    error: err,
                };
                // error returned as null so async parallel doesn't return
                // before all backends are checked
                return cb(null, respBody);
            }
            if (implName === 'multipleBackends') {
                respBody = result;
                return cb(null, respBody);
            }
            respBody[implName] = {
                code: result.statusCode,
                message: result.statusMessage,
            };
            return cb(null, respBody);
        });
    },

    getDiskUsage: (log, cb) => {
        if (!client.getDiskUsage) {
            log.debug('returning empty disk usage as fallback', { implName });
            return cb(null, {});
        }
        return client.getDiskUsage(log.getSerializedUids(), cb);
    },


   /**
    * _putForCopy - put used for copying object
    * @param {object} cipherBundle - cipher bundle that encrypt the data
    * @param {object} stream - stream containing the data
    * @param {object} part - element of dataLocator array
    * @param {object} dataStoreContext - information of the
    * destination object
    * dataStoreContext.bucketName: destination bucket name,
    * dataStoreContext.owner: owner,
    * dataStoreContext.namespace: request namespace,
    * dataStoreContext.objectKey: destination object key name,
    * @param {BackendInfo} destBackendInfo - Instance of BackendInfo:
    * Represents the info necessary to evaluate which data backend to use
    * on a data put call.
    * @param {object} log - Werelogs request logger
    * @param {function} cb - callback
    * @returns {function} cb - callback
    */
    _putForCopy: (cipherBundle, stream, part, dataStoreContext,
    destBackendInfo, log, cb) => data.put(cipherBundle, stream,
        part.size, dataStoreContext,
        destBackendInfo, log,
        (error, partRetrievalInfo) => {
            if (error) {
                return cb(error);
            }
            const partResult = {
                key: partRetrievalInfo.key,
                dataStoreName: partRetrievalInfo
                    .dataStoreName,
                dataStoreType: partRetrievalInfo
                    .dataStoreType,
                start: part.start,
                size: part.size,
            };
            if (cipherBundle) {
                partResult.cryptoScheme = cipherBundle.cryptoScheme;
                partResult.cipheredDataKey = cipherBundle.cipheredDataKey;
            }
            if (part.dataStoreETag) {
                partResult.dataStoreETag = part.dataStoreETag;
            }
            if (partRetrievalInfo.dataStoreVersionId) {
                partResult.dataStoreVersionId =
                partRetrievalInfo.dataStoreVersionId;
            }
            return cb(null, partResult);
        }),

    /**
     * _dataCopyPut - put used for copying object with and without
     * encryption
     * @param {string} serverSideEncryption - Server side encryption
     * @param {object} stream - stream containing the data
     * @param {object} part - element of dataLocator array
     * @param {object} dataStoreContext - information of the
     * destination object
     * dataStoreContext.bucketName: destination bucket name,
     * dataStoreContext.owner: owner,
     * dataStoreContext.namespace: request namespace,
     * dataStoreContext.objectKey: destination object key name,
     * @param {BackendInfo} destBackendInfo - Instance of BackendInfo:
     * Represents the info necessary to evaluate which data backend to use
     * on a data put call.
     * @param {object} log - Werelogs request logger
     * @param {function} cb - callback
     * @returns {function} cb - callback
     */
    _dataCopyPut: (serverSideEncryption, stream, part, dataStoreContext,
    destBackendInfo, log, cb) => {
        if (serverSideEncryption) {
            return kms.createCipherBundle(
            serverSideEncryption,
            log, (err, cipherBundle) => {
                if (err) {
                    log.debug('error getting cipherBundle');
                    return cb(errors.InternalError);
                }
                return data._putForCopy(cipherBundle, stream, part,
                  dataStoreContext, destBackendInfo, log, cb);
            });
        }
        // Copied object is not encrypted so just put it
        // without a cipherBundle
        return data._putForCopy(null, stream, part, dataStoreContext,
          destBackendInfo, log, cb);
    },

    /**
     * copyObject - copy object
     * @param {object} request - request object
     * @param {string} sourceLocationConstraintName -
     * source locationContraint name (awsbackend, azurebackend, ...)
     * @param {object} storeMetadataParams - metadata information of the
     * source object
     * @param {array} dataLocator - source object metadata location(s)
     * NOTE: for Azure and AWS data backend this array only has one item
     * @param {object} dataStoreContext - information of the
     * destination object
     * dataStoreContext.bucketName: destination bucket name,
     * dataStoreContext.owner: owner,
     * dataStoreContext.namespace: request namespace,
     * dataStoreContext.objectKey: destination object key name,
     * @param {BackendInfo} destBackendInfo - Instance of BackendInfo:
     * Represents the info necessary to evaluate which data backend to use
     * on a data put call.
     * @param {object} sourceBucketMD - metadata of the source bucket
     * @param {object} destBucketMD - metadata of the destination bucket
     * @param {object} log - Werelogs request logger
     * @param {function} cb - callback
     * @returns {function} cb - callback
     */
    copyObject: (request,
      sourceLocationConstraintName, storeMetadataParams, dataLocator,
      dataStoreContext, destBackendInfo, sourceBucketMD, destBucketMD, log,
      cb) => {
        const serverSideEncryption = destBucketMD.getServerSideEncryption();
        if (config.backends.data === 'multiple' &&
        utils.externalBackendCopy(sourceLocationConstraintName,
        storeMetadataParams.dataStoreName, sourceBucketMD, destBucketMD)) {
            const destLocationConstraintName =
              storeMetadataParams.dataStoreName;
            const objectGetInfo = dataLocator[0];
            const externalSourceKey = objectGetInfo.key;
            return client.copyObject(request, destLocationConstraintName,
            externalSourceKey, sourceLocationConstraintName, log,
            (error, objectRetrievalInfo) => {
                if (error) {
                    return cb(error);
                }
                const putResult = {
                    key: objectRetrievalInfo.key,
                    dataStoreName: objectRetrievalInfo.
                        dataStoreName,
                    dataStoreType: objectRetrievalInfo.
                        dataStoreType,
                    dataStoreVersionId:
                        objectRetrievalInfo.dataStoreVersionId,
                    size: storeMetadataParams.size,
                    dataStoreETag: objectGetInfo.dataStoreETag,
                    start: objectGetInfo.start,
                };
                const putResultArr = [putResult];
                return cb(null, putResultArr);
            });
        }

        // dataLocator is an array.  need to get and put all parts
        // For now, copy 1 part at a time. Could increase the second
        // argument here to increase the number of parts
        // copied at once.
        return async.mapLimit(dataLocator, 1,
            // eslint-disable-next-line prefer-arrow-callback
            function copyPart(part, copyCb) {
                if (part.dataStoreType === 'azure') {
                    const passThrough = new PassThrough();
                    return async.parallel([
                        parallelCb => data.get(part, passThrough, log, err =>
                          parallelCb(err)),
                        parallelCb => data._dataCopyPut(serverSideEncryption,
                            passThrough,
                            part, dataStoreContext, destBackendInfo, log,
                            parallelCb),
                    ], (err, res) => {
                        if (err) {
                            return copyCb(err);
                        }
                        return copyCb(null, res[1]);
                    });
                }
                return data.get(part, null, log, (err, stream) => {
                    if (err) {
                        return copyCb(err);
                    }
                    return data._dataCopyPut(serverSideEncryption, stream,
                    part, dataStoreContext, destBackendInfo, log, copyCb);
                });
            }, (err, results) => {
                if (err) {
                    log.debug('error transferring data from source',
                    { error: err });
                    return cb(err);
                }
                return cb(null, results);
            });
    },


    _dataCopyPutPart: (request,
      serverSideEncryption, stream, part,
      dataStoreContext, destBackendInfo, locations, log, cb) => {
        const numberPartSize =
          Number.parseInt(part.size, 10);
        const partNumber = Number.parseInt(request.query.partNumber, 10);
        const uploadId = request.query.uploadId;
        const destObjectKey = request.objectKey;
        const destBucketName = request.bucketName;
        const destLocationConstraintName = destBackendInfo
        .getControllingLocationConstraint();
        if (externalBackends[config
            .locationConstraints[destLocationConstraintName]
            .type]) {
            return multipleBackendGateway.uploadPart(null, null,
            stream, numberPartSize,
            destLocationConstraintName, destObjectKey, uploadId,
            partNumber, destBucketName, log,
            (err, partInfo) => {
                if (err) {
                    log.error('error putting ' +
                    'part to AWS', {
                        error: err,
                        method:
                        'objectPutCopyPart::' +
                        'multipleBackendGateway.' +
                        'uploadPart',
                    });
                    return cb(errors.ServiceUnavailable);
                }
                // skip to end of waterfall
                // because don't need to store
                // part metadata
                if (partInfo &&
                    partInfo.dataStoreType === 'aws_s3') {
                    // if data backend handles MPU, skip to end
                    // of waterfall
                    const partResult = {
                        dataStoreETag: partInfo.dataStoreETag,
                    };
                    locations.push(partResult);
                    return cb(skipError, partInfo.dataStoreETag);
                } else if (
                  partInfo &&
                  partInfo.dataStoreType === 'azure') {
                    const partResult = {
                        key: partInfo.key,
                        dataStoreName: partInfo.dataStoreName,
                        dataStoreETag: partInfo.dataStoreETag,
                        size: numberPartSize,
                        numberSubParts:
                          partInfo.numberSubParts,
                    };
                    locations.push(partResult);
                    return cb();
                }
                return cb(skipError);
            });
        }
        if (serverSideEncryption) {
            return kms.createCipherBundle(
                serverSideEncryption,
                log, (err, cipherBundle) => {
                    if (err) {
                        log.debug('error getting cipherBundle',
                        { error: err });
                        return cb(errors.InternalError);
                    }
                    return data.put(cipherBundle, stream,
                        numberPartSize, dataStoreContext,
                        destBackendInfo, log,
                        (error, partRetrievalInfo,
                        hashedStream) => {
                            if (error) {
                                log.debug('error putting ' +
                                'encrypted part', { error });
                                return cb(error);
                            }
                            const partResult = {
                                key: partRetrievalInfo.key,
                                dataStoreName: partRetrievalInfo
                                    .dataStoreName,
                                dataStoreETag: hashedStream
                                    .completedHash,
                                // Do not include part start
                                // here since will change in
                                // final MPU object
                                size: numberPartSize,
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
        return data.put(null, stream, numberPartSize,
        dataStoreContext, destBackendInfo,
        log, (error, partRetrievalInfo, hashedStream) => {
            if (error) {
                log.debug('error putting object part',
                { error });
                return cb(error);
            }
            const partResult = {
                key: partRetrievalInfo.key,
                dataStoreName: partRetrievalInfo.dataStoreName,
                dataStoreETag: hashedStream.completedHash,
                size: numberPartSize,
            };
            locations.push(partResult);
            return cb();
        });
    },

    /**
     * uploadPartCopy - put copy part
     * @param {object} request - request object
     * @param {object} log - Werelogs request logger
     * @param {object} destBucketMD - destination bucket metadata
     * @param {string} sourceLocationConstraintName -
     * source locationContraint name (awsbackend, azurebackend, ...)
     * @param {string} destLocationConstraintName -
     * location of the destination MPU object (awsbackend, azurebackend, ...)
     * @param {array} dataLocator - source object metadata location(s)
     * NOTE: for Azure and AWS data backend this array
     * @param {object} dataStoreContext - information of the
     * destination object
     * dataStoreContext.bucketName: destination bucket name,
     * dataStoreContext.owner: owner,
     * dataStoreContext.namespace: request namespace,
     * dataStoreContext.objectKey: destination object key name,
     * dataStoreContext.uploadId: uploadId
     * dataStoreContext.partNumber: request.query.partNumber
     * @param {function} callback - callback
     * @returns {function} cb - callback
     */
    uploadPartCopy: (request, log, destBucketMD, sourceLocationConstraintName,
      destLocationConstraintName, dataLocator, dataStoreContext,
      callback) => {
        const serverSideEncryption = destBucketMD.getServerSideEncryption();
        const lastModified = new Date().toJSON();

        // skip if 0 byte object
        if (dataLocator.length === 0) {
            return process.nextTick(() => {
                callback(null, constants.emptyFileMd5,
                    lastModified, serverSideEncryption, []);
            });
        }

        const locationTypeMatchAWS =
        config.backends.data === 'multiple' &&
        config.getLocationConstraintType(sourceLocationConstraintName) ===
        config.getLocationConstraintType(destLocationConstraintName) &&
        config.getLocationConstraintType(sourceLocationConstraintName) ===
        'aws_s3';

        // NOTE: using multipleBackendGateway.uploadPartCopy only if copying
        // from AWS to AWS

        if (locationTypeMatchAWS && dataLocator.length === 1) {
            const awsSourceKey = dataLocator[0].key;
            return multipleBackendGateway.uploadPartCopy(request,
            destLocationConstraintName, awsSourceKey,
            sourceLocationConstraintName, log, (error, eTag) => {
                if (error) {
                    return callback(error);
                }
                return callback(skipError, eTag,
                    lastModified, serverSideEncryption);
            });
        }

        const backendInfo = new BackendInfo(destLocationConstraintName);

        // totalHash will be sent through the RelayMD5Sum transform streams
        // to collect the md5 from multiple streams
        let totalHash;
        const locations = [];
         // dataLocator is an array.  need to get and put all parts
         // in order so can get the ETag of full object
        return async.forEachOfSeries(dataLocator,
            // eslint-disable-next-line prefer-arrow-callback
            function copyPart(part, index, cb) {
                if (part.dataStoreType === 'azure') {
                    const passThrough = new PassThrough();
                    return async.parallel([
                        next => data.get(part, passThrough, log, err => {
                            if (err) {
                                log.error('error getting data part ' +
                                'from Azure', {
                                    error: err,
                                    method:
                                    'objectPutCopyPart::' +
                                    'multipleBackendGateway.' +
                                    'copyPart',
                                });
                                return next(err);
                            }
                            return next();
                        }),
                        next => data._dataCopyPutPart(request,
                          serverSideEncryption, passThrough, part,
                          dataStoreContext, backendInfo, locations, log, next),
                    ], err => {
                        if (err) {
                            return cb(err);
                        }
                        return cb();
                    });
                }
                return data.get(part, null, log, (err, stream) => {
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

                    // destLocationConstraintName is location of the
                    // destination MPU object
                    return data._dataCopyPutPart(request,
                      serverSideEncryption, hashedStream, part,
                      dataStoreContext, backendInfo, locations, log, cb);
                });
            }, err => {
                // Digest the final combination of all of the part streams
                if (totalHash) {
                    totalHash = totalHash.digest('hex');
                } else {
                    totalHash = locations[0].dataStoreETag;
                }
                if (err) {
                    if (err === skipError) {
                        return callback(skipError, totalHash,
                            lastModified, serverSideEncryption);
                    }
                    log.debug('error transferring data from source',
                    { error: err, method: 'goGetData' });
                    return callback(err);
                }
                return callback(null, totalHash,
                    lastModified, serverSideEncryption, locations);
            });
    },
};

module.exports = data;
