const crypto = require('crypto');

const async = require('async');
const { parseString } = require('xml2js');
const { auth, errors, versioning, s3middleware, policies } = require('arsenal');

const escapeForXml = s3middleware.escapeForXml;
const { pushMetric } = require('../utapi/utilities');
const bucketShield = require('./apiUtils/bucket/bucketShield');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const metadata = require('../metadata/wrapper');
const services = require('../services');
const vault = require('../auth/vault');
const { isBucketAuthorized, evaluateBucketPolicyWithIAM } =
    require('./apiUtils/authorization/permissionChecks');
const { preprocessingVersioningDelete }
    = require('./apiUtils/object/versioning');
const createAndStoreObject = require('./apiUtils/object/createAndStoreObject');
const monitoring = require('../utilities/metrics');
const metadataUtils = require('../metadata/metadataUtils');
const { config } = require('../Config');
const { hasGovernanceBypassHeader, checkUserGovernanceBypass, ObjectLockInfo }
    = require('./apiUtils/object/objectLockHelpers');
const requestUtils = policies.requestUtils;
const { data } = require('../data/wrapper');
const logger = require('../utilities/logger');
const { _bucketRequiresOplogUpdate } = require('./apiUtils/object/deleteObject');
const { overheadField } = require('../../constants');

const versionIdUtils = versioning.VersionID;


/*
   Format of xml request:
   <Delete>
       <Quiet>true</Quiet>
       <Object>
            <Key>Key</Key>
            <VersionId>VersionId</VersionId>
       </Object>
       <Object>
            <Key>Key</Key>
       </Object>
       ...
   </Delete>
   */


  /*
  Format of xml response:
  <?xml version="1.0" encoding="UTF-8"?>
  <DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Deleted>
      <Key>sample1.txt</Key>
    </Deleted>
    <Error>
      <Key>sample2.txt</Key>
      <Code>AccessDenied</Code>
      <Message>Access Denied</Message>
    </Error>
  </DeleteResult>
   */

/**
* formats xml for response
* @param {boolean} quietSetting - true if xml should just include error list
* and false if should include deleted list and error list
* @param {object []} errorResults - list of error result objects with each
* object containing -- entry: { key, versionId }, error: arsenal error
* @param {object []} deleted - list of object deleted, an object has the format
*     object: { entry, isDeleteMarker, isDeletingDeleteMarker }
*     object.entry : above
*     object.newDeleteMarker: if deletion resulted in delete marker
*     object.isDeletingDeleteMarker: if a delete marker was deleted
* @return {string} xml string
*/
function _formatXML(quietSetting, errorResults, deleted) {
    let errorXML = [];
    errorResults.forEach(errorObj => {
        errorXML.push(
        '<Error>',
        '<Key>', escapeForXml(errorObj.entry.key), '</Key>',
        '<Code>', escapeForXml(errorObj.error.message), '</Code>');
        if (errorObj.entry.versionId) {
            const version = errorObj.entry.versionId === 'null' ?
                'null' : escapeForXml(errorObj.entry.versionId);
            errorXML.push('<VersionId>', version, '</VersionId>');
        }
        errorXML.push(
        '<Message>',
        escapeForXml(errorObj.error.description),
        '</Message>',
        '</Error>'
        );
    });
    errorXML = errorXML.join('');
    const xml = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<DeleteResult ',
        'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
        // placeholder in array for deleted list if verbose response
        '',
        errorXML,
        '</DeleteResult>',
    ];
    if (quietSetting) {
        // just return errors
        return xml.join('');
    }
    const deletedXML = [];
    deleted.forEach(version => {
        const isDeleteMarker = version.isDeleteMarker;
        const deleteMarkerVersionId = version.deleteMarkerVersionId;
        // if deletion resulted in new delete marker or deleting a delete marker
        deletedXML.push(
            '<Deleted>',
            '<Key>',
            escapeForXml(version.entry.key),
            '</Key>'
        );
        if (version.entry.versionId) {
            deletedXML.push(
                '<VersionId>',
                escapeForXml(version.entry.versionId),
                '</VersionId>'
            );
        }
        if (isDeleteMarker) {
            deletedXML.push(
                '<DeleteMarker>',
                isDeleteMarker,
                '</DeleteMarker>',
                '<DeleteMarkerVersionId>',
                deleteMarkerVersionId,
                '</DeleteMarkerVersionId>'
            );
        }
        deletedXML.push('</Deleted>');
    });
    xml[3] = deletedXML.join('');
    return xml.join('');
}

function _parseXml(xmlToParse, next) {
    return parseString(xmlToParse, (err, result) => {
        if (err || !result || !result.Delete || !result.Delete.Object) {
            return next(errors.MalformedXML);
        }
        const json = result.Delete;
        // not quiet is the default if nothing specified
        const quietSetting = json.Quiet && json.Quiet[0] === 'true';
        // format of json is
        // {"Object":[
        //     {"Key":["test1"],"VersionId":["vid"]},
        //     {"Key":["test2"]}
        // ]}
        const objects = [];
        for (let i = 0; i < json.Object.length; i++) {
            const item = json.Object[i];
            if (!item.Key) {
                return next(errors.MalformedXML);
            }
            const object = { key: item.Key[0] };
            if (item.VersionId) {
                object.versionId = item.VersionId[0];
            }
            objects.push(object);
        }
        return next(null, quietSetting, objects);
    });
}

/**
 * decodeObjectVersion - decode object version to be deleted
 * @param {object} entry - entry from data model
 * @param {function} next - callback to call with error or decoded version
 * @return {undefined}
 **/
function decodeObjectVersion(entry) {
    let decodedVersionId;
    if (entry.versionId) {
        decodedVersionId = entry.versionId === 'null' ?
            'null' : versionIdUtils.decode(entry.versionId);
    }
    if (decodedVersionId instanceof Error) {
        return [errors.NoSuchVersion];
    }
    return [null, decodedVersionId];
}

/**
 * Initialization function for the MultiObjectDelete API that will, based on the
 * current metadata backend, assess if metadata READ batching is supported. If
 * yes, the initialization step will call the metadataGetObjects function from
 * the MetadataWrapper.
 * @param {string} bucketName - bucket name
 * @param {string []} inPlay - list of object keys still in play
 * @param {object} log - logger object
 * @param {function} callback - callback to call with error or list of objects
 * @return {undefined}
 */
function initializeMultiObjectDeleteWithBatchingSupport(bucketName, inPlay, log, callback) {
    if (config.multiObjectDeleteEnableOptimizations === false) {
        return callback(null, {});
    }
    // If the backend supports batching, we want to optimize the API latency by
    // first getting all the objects metadata, stored in memory, for later use
    // in the API. This approach does not change the API architecture, but
    // transplants an additional piece of code that can greatly improve the API
    // latency when the database supports batching.
    const objectKeys = Object.values(inPlay).map(entry => {
        const [err, versionId] = decodeObjectVersion(entry, bucketName);
        if (err) {
            return null;
        }
        return {
            versionId,
            inPlay: entry,
        };
    });
    return metadataUtils.metadataGetObjects(bucketName, objectKeys, log, (err, cache) => {
        // This optional step is read-only, so any error can be safely ignored
        if (err) {
            return callback(null, {});
        }
        return callback(null, cache);
    });
}

/**
* gets object metadata and deletes object
* @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
* @param {string} canonicalID - canonicalId of requester
* @param {object} request - http request
* @param {string} bucketName - bucketName
* @param {BucketInfo} bucket - bucket
* @param {boolean} quietSetting - true if xml should just include error list
* and false if should include deleted list and error list
* @param {object []} errorResults - list of error result objects with each
* object containing -- key: objectName, error: arsenal error
* @param {string []} inPlay - list of object keys still in play
* @param {object} log - logger object
* @param {function} next - callback to next step in waterfall
* @return {undefined}
* @callback called with (err, quietSetting, errorResults, numOfObjects,
* successfullyDeleted, totalContentLengthDeleted)
*/
function getObjMetadataAndDelete(authInfo, canonicalID, request,
        bucketName, bucket, quietSetting, errorResults, inPlay, log, next) {
    const successfullyDeleted = [];
    let totalContentLengthDeleted = 0;
    let numOfObjectsRemoved = 0;
    const skipError = new Error('skip');
    const objectLockedError = new Error('object locked');
    let deleteFromStorage = [];

    return async.waterfall([
        callback => initializeMultiObjectDeleteWithBatchingSupport(bucketName, inPlay, log, callback),
        (cache, callback) => async.forEachLimit(inPlay, config.multiObjectDeleteConcurrency, (entry, moveOn) => {
            async.waterfall([
                callback => callback(...decodeObjectVersion(entry, bucketName)),
                // for obj deletes, no need to check acl's at object level
                // (authority is at the bucket level for obj deletes)
                (versionId, callback) => metadataUtils.metadataGetObject(bucketName, entry.key,
                    versionId, cache, log, (err, objMD) => callback(err, objMD, versionId)),
                (objMD, versionId, callback) => {
                    if (!objMD) {
                        const verCfg = bucket.getVersioningConfiguration();
                        // To adhere to AWS behavior, create a delete marker
                        // if trying to delete an object that does not exist
                        // when versioning has been configured
                        if (verCfg && !entry.versionId) {
                            log.debug('trying to delete specific version ' +
                                'that does not exist');
                            return callback(null, objMD, versionId);
                        }
                        // otherwise if particular key does not exist, AWS
                        // returns success for key so add to successfullyDeleted
                        // list and move on
                        successfullyDeleted.push({ entry });
                        return callback(skipError);
                    }
                    if (versionId && objMD.location &&
                        Array.isArray(objMD.location) && objMD.location[0]) {
                        // we need this information for data deletes to AWS
                        // eslint-disable-next-line no-param-reassign
                        objMD.location[0].deleteVersion = true;
                    }
                    return callback(null, objMD, versionId);
                },
                (objMD, versionId, callback) => {
                    // AWS only returns an object lock error if a version id
                    // is specified, else continue to create a delete marker
                    if (!versionId || !bucket.isObjectLockEnabled()) {
                        return callback(null, null, objMD, versionId);
                    }
                    const hasGovernanceBypass = hasGovernanceBypassHeader(request.headers);
                    if (hasGovernanceBypass && authInfo.isRequesterAnIAMUser()) {
                        return checkUserGovernanceBypass(request, authInfo, bucket, entry.key, log, error => {
                            if (error && error.is.AccessDenied) {
                                log.debug('user does not have BypassGovernanceRetention and object is locked',
                                    { error });
                                return callback(objectLockedError);
                            }
                            if (error) {
                                return callback(error);
                            }
                            return callback(null, hasGovernanceBypass, objMD, versionId);
                        });
                    }
                    return callback(null, hasGovernanceBypass, objMD, versionId);
                },
                (hasGovernanceBypass, objMD, versionId, callback) => {
                    // AWS only returns an object lock error if a version id
                    // is specified, else continue to create a delete marker
                    if (!versionId || !bucket.isObjectLockEnabled()) {
                        return callback(null, objMD, versionId);
                    }
                    const objLockInfo = new ObjectLockInfo({
                        mode: objMD.retentionMode,
                        date: objMD.retentionDate,
                        legalHold: objMD.legalHold || false,
                    });

                    // If the object can not be deleted raise an error
                    if (!objLockInfo.canModifyObject(hasGovernanceBypass)) {
                        log.debug('trying to delete locked object');
                        return callback(objectLockedError);
                    }

                    return callback(null, objMD, versionId);
                },
                (objMD, versionId, callback) => {
                    const options = preprocessingVersioningDelete(
                        bucketName, bucket, objMD, versionId, config.nullVersionCompatMode);
                    const deleteInfo = {};
                    if (options && options.deleteData) {
                        options.overheadField = overheadField;
                        deleteInfo.deleted = true;
                        if (!_bucketRequiresOplogUpdate(bucket)) {
                            options.doesNotNeedOpogUpdate = true;
                        }
                        if (objMD.uploadId) {
                            // eslint-disable-next-line
                            options.replayId = objMD.uploadId;
                        }
                        return services.deleteObject(bucketName, objMD,
                            entry.key, options, config.multiObjectDeleteEnableOptimizations, log, (err, toDelete) => {
                                if (err) {
                                    return callback(err);
                                }
                                if (toDelete) {
                                    deleteFromStorage = deleteFromStorage.concat(toDelete);
                                }
                                return callback(null, objMD, deleteInfo);
                            });
                    }
                    deleteInfo.newDeleteMarker = true;
                    // This call will create a delete-marker
                    return createAndStoreObject(bucketName, bucket, entry.key,
                        objMD, authInfo, canonicalID, null, request,
                        deleteInfo.newDeleteMarker, null, overheadField, log, (err, result) =>
                        callback(err, objMD, deleteInfo, result.versionId));
                },
            ], (err, objMD, deleteInfo, versionId) => {
                if (err === skipError) {
                    return moveOn();
                } else if (err === objectLockedError) {
                    errorResults.push({ entry, error: errors.AccessDenied, objectLocked: true });
                    return moveOn();
                } else if (err) {
                    log.error('error deleting object', { error: err, entry });
                    errorResults.push({ entry, error: err });
                    return moveOn();
                }
                if (deleteInfo.deleted && objMD['content-length']) {
                    numOfObjectsRemoved++;
                    totalContentLengthDeleted += objMD['content-length'];
                }
                let isDeleteMarker;
                let deleteMarkerVersionId;
                // - If trying to delete an object that does not exist (if a new
                // delete marker was created)
                // - Or if an object exists but no version was specified
                // return DeleteMarkerVersionId equals the versionID of the marker
                // you just generated and DeleteMarker tag equals true
                if (deleteInfo.newDeleteMarker) {
                    isDeleteMarker = true;
                    deleteMarkerVersionId = versionIdUtils.encode(
                        versionId, config.versionIdEncodingType);
                    // In this case we are putting a new object (i.e., the delete
                    // marker), so we decrement the numOfObjectsRemoved value.
                    numOfObjectsRemoved--;
                    // If trying to delete a delete marker, DeleteMarkerVersionId equals
                    // deleteMarker's versionID and DeleteMarker equals true
                } else if (objMD && objMD.isDeleteMarker) {
                    isDeleteMarker = true;
                    deleteMarkerVersionId = entry.versionId;
                }
                successfullyDeleted.push({
                    entry, isDeleteMarker,
                    deleteMarkerVersionId,
                });
                return moveOn();
            });
        },
            // end of forEach func
            err => {
                // Batch delete all objects
                const onDone = () => callback(err, quietSetting, errorResults, numOfObjectsRemoved,
                    successfullyDeleted, totalContentLengthDeleted, bucket);

                if (err && deleteFromStorage.length === 0) {
                    log.trace('no objects to delete from data backend');
                    return onDone();
                }
                // If error but we have objects in the list, delete them to ensure
                // consistent state.
                log.trace('deleting objects from data backend');

                // Split the array into chunks
                const chunks = [];
                while (deleteFromStorage.length > 0) {
                    chunks.push(deleteFromStorage.splice(0, config.multiObjectDeleteConcurrency));
                }

                return async.each(chunks, (chunk, done) => data.batchDelete(chunk, null, null,
                    logger.newRequestLoggerFromSerializedUids(log.getSerializedUids()), done),
                    err => {
                        if (err) {
                            log.error('error deleting objects from data backend', { error: err });
                            return onDone(err);
                        }
                        return onDone();
                    });
            }),
    ], (err, ...results) => {
        // if general error from metadata return error
        if (err) {
            monitoring.promMetrics('DELETE', bucketName, err.code,
                'multiObjectDelete');
            return next(err);
        }
        return next(null, ...results);
    });
}

/**
 * multiObjectDelete - Delete multiple objects
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http.IncomingMessage as modified by
 * lib/utils and routes/routePOST.js
 * @param {object} request.headers - request headers
 * @param {object} request.query - query from request
 * @param {string} request.post - concatenation of request body
 * @param {string} request.bucketName - parsed bucketName
 * @param {string} request.socket.remoteAddress - requester IP
 * @param {boolean} request.connection.encrypted - whether request was encrypted
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function multiObjectDelete(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'multiObjectDelete' });
    if (!request.post) {
        monitoring.promMetrics('DELETE', request.bucketName, 400,
            'multiObjectDelete');
        return callback(errors.MissingRequestBodyError);
    }
    const md5 = crypto.createHash('md5')
        .update(request.post, 'utf8').digest('base64');
    if (md5 !== request.headers['content-md5']) {
        monitoring.promMetrics('DELETE', request.bucketName, 400,
            'multiObjectDelete');
        return callback(errors.BadDigest);
    }

    const bucketName = request.bucketName;
    const canonicalID = authInfo.getCanonicalID();

    return async.waterfall([
        function parseXML(next) {
            return _parseXml(request.post,
                (err, quietSetting, objects) => {
                    if (err || objects.length < 1 || objects.length > 1000) {
                        return next(errors.MalformedXML);
                    }
                    return next(null, quietSetting, objects);
                });
        },
        function checkBucketMetadata(quietSetting, objects, next) {
            const errorResults = [];
            return metadata.getBucket(bucketName, log, (err, bucketMD) => {
                if (err) {
                    log.trace('error retrieving bucket metadata',
                        { error: err });
                    return next(err);
                }
                // check whether bucket has transient or deleted flag
                if (bucketShield(bucketMD, 'objectDelete')) {
                    return next(errors.NoSuchBucket);
                }
                if (!isBucketAuthorized(bucketMD, 'objectDelete', canonicalID, authInfo, log, request,
                    request.actionImplicitDenies)) {
                    log.trace("access denied due to bucket acl's");
                    // if access denied at the bucket level, no access for
                    // any of the objects so all results will be error results
                    objects.forEach(entry => {
                        errorResults.push({
                            entry,
                            error: errors.AccessDenied,
                        });
                    });
                    // by sending an empty array as the objects array
                    // async.forEachLimit below will not actually
                    // make any calls to metadata or data but will continue on
                    // to the next step to build xml
                    return next(null, quietSetting, errorResults, [], bucketMD);
                }
                return next(null, quietSetting, errorResults, objects, bucketMD);
            });
        },
        function checkPolicies(quietSetting, errorResults, objects, bucketMD, next) {
            // track keys that are still on track to be deleted
            const inPlay = [];
            // if request from account, no need to check policies
            // all objects are inPlay so send array of object keys
            // as inPlay argument
            if (!authInfo.isRequesterAnIAMUser()) {
                return next(null, quietSetting, errorResults, objects, bucketMD);
            }

            // TODO: once arsenal's extractParams is separated from doAuth
            // function, refactor so only extract once and send
            // params on to this api
            const authParams = auth.server.extractParams(request, log,
                's3', request.query);
            const ip = requestUtils.getClientIp(request, config);
            const requestContextParams = {
                constantParams: {
                    headers: request.headers,
                    query: request.query,
                    generalResource: request.bucketName,
                    requesterIp: ip,
                    sslEnabled: request.connection.encrypted,
                    apiMethod: 'objectDelete',
                    awsService: 's3',
                    locationConstraint: null,
                    requesterInfo: authInfo,
                    signatureVersion: authParams.params.data.authType,
                    authType: authParams.params.data.signatureVersion,
                    signatureAge: authParams.params.data.signatureAge,
                },
                parameterize: {
                    // eslint-disable-next-line
                    specificResource: objects.map(entry => {
                        return {
                            key: entry.key,
                            versionId: entry.versionId,
                        };
                    }),
                },
            };
            return vault.checkPolicies(requestContextParams, authInfo.getArn(),
                log, (err, authorizationResults) => {
                    // there were no policies so received a blanket AccessDenied
                    if (err && err.is.AccessDenied) {
                        objects.forEach(entry => {
                            errorResults.push({
                                entry,
                                error: errors.AccessDenied });
                        });
                        // send empty array for inPlay
                        return next(null, quietSetting, errorResults, [], bucketMD);
                    }
                    if (err) {
                        log.trace('error checking policies', {
                            error: err,
                            method: 'multiObjectDelete.checkPolicies',
                        });
                        return next(err);
                    }
                    if (objects.length !== authorizationResults.length) {
                        log.error('vault did not return correct number of ' +
                        'authorization results', {
                            authorizationResultsLength:
                                authorizationResults.length,
                            objectsLength: objects.length,
                        });
                        return next(errors.InternalError);
                    }
                     // Convert authorization results into an easier to handle format
                     const actionImplicitDenies = authorizationResults.reduce((acc, curr, idx) => {
                        const apiMethod = requestContextParams[idx].apiMethod;
                        acc[apiMethod] = curr.isImplicit;
                        return acc;
                     }, {});
                    for (let i = 0; i < authorizationResults.length; i++) {
                        const result = authorizationResults[i];
                        // result is { isAllowed: true,
                        // arn: arn:aws:s3:::bucket/object,
                        // versionId: sampleversionId } unless not allowed
                        // in which case no isAllowed key will be present
                        const slashIndex = result.arn.indexOf('/');
                        if (slashIndex === -1) {
                            log.error('wrong arn format from vault');
                            return next(errors.InternalError);
                        }
                        const entry = {
                            key: result.arn.slice(slashIndex + 1),
                            versionId: result.versionId,
                        };
                        // Deny immediately if there is an explicit deny
                        if (!result.isImplicit && !result.isAllowed) {
                            errorResults.push({
                                entry,
                                error: errors.AccessDenied,
                            });
                            continue;
                        }

                        // Evaluate against the bucket policies
                        const areAllActionsAllowed = evaluateBucketPolicyWithIAM(
                            bucketMD,
                            Object.keys(actionImplicitDenies),
                            canonicalID,
                            authInfo,
                            actionImplicitDenies,
                            log,
                            request);

                        if (areAllActionsAllowed) {
                            inPlay.push(entry);
                        } else {
                            errorResults.push({
                                entry,
                                error: errors.AccessDenied,
                            });
                        }
                    }
                    return next(null, quietSetting, errorResults, inPlay, bucketMD);
                });
        },
        function getObjMetadataAndDeleteStep(quietSetting, errorResults, inPlay,
            bucket, next) {
            return getObjMetadataAndDelete(authInfo, canonicalID, request,
                    bucketName, bucket, quietSetting, errorResults, inPlay,
                    log, next);
        },
    ], (err, quietSetting, errorResults, numOfObjectsRemoved,
        successfullyDeleted, totalContentLengthDeleted, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            monitoring.promMetrics('DELETE', bucketName, err.code,
                'multiObjectDelete');
            return callback(err, null, corsHeaders);
        }
        const xml = _formatXML(quietSetting, errorResults,
            successfullyDeleted);
        const deletedKeys = successfullyDeleted.map(item => item.key);
        const removedDeleteMarkers = successfullyDeleted
            .filter(item => item.isDeleteMarker && item.entry && item.entry.versionId)
            .length;
        pushMetric('multiObjectDelete', log, {
            authInfo,
            canonicalID: bucket ? bucket.getOwner() : '',
            bucket: bucketName,
            keys: deletedKeys,
            byteLength: Number.parseInt(totalContentLengthDeleted, 10),
            numberOfObjects: numOfObjectsRemoved,
            removedDeleteMarkers,
            isDelete: true,
        });
        monitoring.promMetrics('DELETE', bucketName, '200',
            'multiObjectDelete',
            Number.parseInt(totalContentLengthDeleted, 10), null, null,
            numOfObjectsRemoved);
        return callback(null, xml, corsHeaders);
    });
}

module.exports = {
    getObjMetadataAndDelete,
    multiObjectDelete,
    decodeObjectVersion,
    initializeMultiObjectDeleteWithBatchingSupport,
};
