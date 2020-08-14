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
const { isBucketAuthorized } =
    require('./apiUtils/authorization/permissionChecks');
const { preprocessingVersioningDelete }
    = require('./apiUtils/object/versioning');
const createAndStoreObject = require('./apiUtils/object/createAndStoreObject');
const { metadataGetObject } = require('../metadata/metadataUtils');
const { config } = require('../Config');
const { isObjectLocked } = require('./apiUtils/object/objectLockHelpers');
const requestUtils = policies.requestUtils;

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
    xml[2] = deletedXML.join('');
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

    // doing 5 requests at a time. note that the data wrapper
    // will do 5 parallel requests to data backend to delete parts
    return async.forEachLimit(inPlay, 5, (entry, moveOn) => {
        async.waterfall([
            callback => {
                let decodedVersionId;
                if (entry.versionId) {
                    decodedVersionId = entry.versionId === 'null' ?
                        'null' : versionIdUtils.decode(entry.versionId);
                }
                if (decodedVersionId instanceof Error) {
                    return callback(errors.NoSuchVersion);
                }
                return callback(null, decodedVersionId);
            },
            // for obj deletes, no need to check acl's at object level
            // (authority is at the bucket level for obj deletes)
            (versionId, callback) => metadataGetObject(bucketName, entry.key,
                versionId, log, (err, objMD) => {
                    // if general error from metadata return error
                    if (err && !err.NoSuchKey) {
                        return callback(err);
                    }
                    if (err && err.NoSuchKey) {
                        const verCfg = bucket.getVersioningConfiguration();
                        // To adhere to AWS behavior, create a delete marker
                        // if trying to delete an object that does not exist
                        // when versioning has been configured
                        if (verCfg && !entry.versionId) {
                            log.debug('trying to delete specific version ' +
                            ' that does not exist');
                            return callback(null, objMD, versionId);
                        }
                        // otherwise if particular key does not exist, AWS
                        // returns success for key so add to successfullyDeleted
                        // list and move on
                        successfullyDeleted.push({ entry });
                        return callback(skipError);
                    }
                    if (versionId && isObjectLocked(bucket, objMD, request.headers)) {
                        log.debug('trying to delete locked object');
                        return callback(objectLockedError);
                    }
                    return callback(null, objMD, versionId);
                }),
            (objMD, versionId, callback) =>
                preprocessingVersioningDelete(bucketName, bucket, objMD,
                versionId, log, (err, options) => callback(err, options,
                    objMD)),
            (options, objMD, callback) => {
                const deleteInfo = {};
                if (options && options.deleteData) {
                    deleteInfo.deleted = true;
                    return services.deleteObject(bucketName, objMD,
                        entry.key, options, log, err =>
                        callback(err, objMD, deleteInfo));
                }
                deleteInfo.newDeleteMarker = true;
                // This call will create a delete-marker
                return createAndStoreObject(bucketName, bucket, entry.key,
                    objMD, authInfo, canonicalID, null, request,
                    deleteInfo.newDeleteMarker, null, log, (err, result) =>
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
                deleteMarkerVersionId = versionIdUtils.encode(versionId);
                // In this case we are putting a new object (i.e., the delete
                // marker), so we decrement the numOfObjectsRemoved value.
                numOfObjectsRemoved--;
            // If trying to delete a delete marker, DeleteMarkerVersionId equals
            // deleteMarker's versionID and DeleteMarker equals true
            } else if (objMD && objMD.isDeleteMarker) {
                isDeleteMarker = true;
                deleteMarkerVersionId = entry.versionId;
            }
            successfullyDeleted.push({ entry, isDeleteMarker,
                deleteMarkerVersionId });
            return moveOn();
        });
    },
    // end of forEach func
    err => {
        log.trace('finished deleting objects', { numOfObjectsRemoved });
        return next(err, quietSetting, errorResults, numOfObjectsRemoved,
            successfullyDeleted, totalContentLengthDeleted, bucket);
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
        return callback(errors.MissingRequestBodyError);
    }
    const md5 = crypto.createHash('md5')
        .update(request.post, 'utf8').digest('base64');
    if (md5 !== request.headers['content-md5']) {
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
        function checkPolicies(quietSetting, objects, next) {
            // track keys that are still on track to be deleted
            const inPlay = [];
            const errorResults = [];
            // if request from account, no need to check policies
            // all objects are inPlay so send array of object keys
            // as inPlay argument
            if (!authInfo.isRequesterAnIAMUser()) {
                return next(null, quietSetting, errorResults, objects);
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
                    if (err && err.AccessDenied) {
                        objects.forEach(entry => {
                            errorResults.push({
                                entry,
                                error: errors.AccessDenied });
                        });
                        // send empty array for inPlay
                        return next(null, quietSetting, errorResults, []);
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
                        if (result.isAllowed) {
                            inPlay.push(entry);
                        } else {
                            errorResults.push({
                                entry,
                                error: errors.AccessDenied,
                            });
                        }
                    }
                    return next(null, quietSetting, errorResults, inPlay);
                });
        },
        function checkBucketMetadata(quietSetting, errorResults, inPlay, next) {
            // if no objects in play, no need to check ACLs / get metadata,
            // just move on if there is no Origin header
            if (inPlay.length === 0 && !request.headers.origin) {
                return next(null, quietSetting, errorResults, inPlay,
                    undefined);
            }
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
                // if no objects in play, no need to check ACLs
                if (inPlay.length === 0) {
                    return next(null, quietSetting, errorResults, inPlay,
                        bucketMD);
                }
                if (!isBucketAuthorized(bucketMD, 'objectDelete',
                    canonicalID, authInfo, log)) {
                    log.trace("access denied due to bucket acl's");
                    // if access denied at the bucket level, no access for
                    // any of the objects so all results will be error results
                    inPlay.forEach(entry => {
                        errorResults.push({
                            entry,
                            error: errors.AccessDenied,
                        });
                    });
                    // by sending an empty array as the inPlay array
                    // async.forEachLimit below will not actually
                    // make any calls to metadata or data but will continue on
                    // to the next step to build xml
                    return next(null, quietSetting, errorResults, [], bucketMD);
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
            return callback(err, null, corsHeaders);
        }
        const xml = _formatXML(quietSetting, errorResults,
            successfullyDeleted);
        const deletedKeys = successfullyDeleted.map(item => item.key);
        pushMetric('multiObjectDelete', log, {
            authInfo,
            canonicalID: bucket ? bucket.getOwner() : '',
            bucket: bucketName,
            keys: deletedKeys,
            byteLength: Number.parseInt(totalContentLengthDeleted, 10),
            numberOfObjects: numOfObjectsRemoved,
            isDelete: true,
        });
        return callback(null, xml, corsHeaders);
    });
}

module.exports = {
    getObjMetadataAndDelete,
    multiObjectDelete,
};
