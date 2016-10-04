import async from 'async';
import crypto from 'crypto';
import { parseString } from 'xml2js';

import { auth, errors, policies } from 'arsenal';

import escapeForXML from '../utilities/escapeForXML';
import bucketShield from './apiUtils/bucket/bucketShield';
import metadata from '../metadata/wrapper';
import services from '../services';
import { isBucketAuthorized } from './apiUtils/authorization/aclChecks';

const RequestContext = policies.RequestContext;


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
* object containing -- key: objectName, error: arsenal error
* @param {string []} deleted - list of object keys deleted
* @return {string} xml string
*/
function _formatXML(quietSetting, errorResults, deleted) {
    let errorXML = [];
    errorResults.forEach(errorObj => {
        errorXML.push(
        '<Error>',
        '<Key>', escapeForXML(errorObj.key), '</Key>',
        '<Code>', errorObj.error.message, '</Code>',
        '<Message>', errorObj.error.description, '</Message>',
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
    deleted.forEach(objKey => {
        deletedXML.push(
            '<Deleted>',
            '<Key>', escapeForXML(objKey), '</Key>',
            '</Deleted>'
        );
    });
    xml[2] = deletedXML.join('');
    return xml.join('');
}

function _parseXml(xmlToParse, next) {
    return parseString(xmlToParse, (err, result) => {
        if (err || !result || !result.Delete) {
            return next(errors.MalformedXML);
        }
        const json = result.Delete;
        // not quiet is the default if nothing specified
        const quietSetting = json.Quiet && json.Quiet[0] === 'true';
        // format of json is {"Object":[{"Key":["test1"]},{"Key":["test2"]}]}
        const objects = json.Object.map(item => item.Key[0]);
        return next(null, quietSetting, objects);
    });
}

/**
 * multiObjectDelete - Delete multiple objects
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
export default
function multiObjectDelete(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'multiObjectDelete' });
    if (!request.post) {
        return callback(errors.MissingRequestBodyError);
    }
    const md5 = crypto.createHash('md5').update(request.post).digest('base64');
    if (md5 !== request.headers['content-md5']) {
        return callback(errors.BadDigest);
    }

    const bucketName = request.bucketName;
    const canonicalID = authInfo.getCanonicalID();
    // track the error results for any keys with an error response
    const errorResults = [];
    // track keys that have been successfully deleted
    const deleted = [];
    // track keys that are still on track to be deleted
    let inPlay = [];
    let totalContentLengthDeleted = 0;

    return async.waterfall([
        function parseXML(next) {
            return _parseXml(request.post, (err, quietSetting, objects) => {
                if (err) {
                    return next(err);
                }
                if (objects.length < 1 || objects.length > 1000) {
                    return next(errors.MalformedXML);
                }
                return next(null, quietSetting, objects);
            });
        },
        function checkPolicies(quietSetting, objects, next) {
            // if request from account, no need to check policies
            if (!authInfo.isRequesterAnIAMUser()) {
                inPlay = objects.slice();
                return next(null, quietSetting);
            }
            // create a requstContext for each object with the action of
            // objectDelete
            const requestContexts = objects.map(object =>
                new RequestContext(request.headers,
                request.query, request.bucketName, object,
                request.socket.remoteAddress, request.connection.encrypted,
                'objectDelete', 's3')
            );
            // TODO: consider creating a vault route for just authorization
            // so do not have to authenticate again
            return auth.server.doAuth(request, log, (err, userInfo,
                authorizationResults) => {
                if (err) {
                    log.trace('authorization error', { error: err });
                    return next(err);
                }
                if (objects.length !== authorizationResults.length) {
                    log.trace('vault did not return correct number of ' +
                    'authorization results');
                    return next(errors.InternalError);
                }
                for (let i = 0; i < authorizationResults.length; i++) {
                    const result = authorizationResults[i];
                    // result is { isAllowed: true,
                    // arn: arn:aws:s3:::bucket/object} unless not allowed
                    // in which case no isAllowed key will be present
                    const slashIndex = result.arn.indexOf('/');
                    if (slashIndex === undefined) {
                        log.trace('wrong arn format from vault');
                        return next(errors.InternalError);
                    }
                    const key = result.arn.slice(slashIndex + 1);
                    if (result.isAllowed) {
                        inPlay.push(key);
                    } else {
                        errorResults.push({
                            key,
                            error: errors.AccessDenied,
                        });
                    }
                }
                return next(null, quietSetting);
            }, 's3', requestContexts);
        },
        function checkBucketMetadata(quietSetting, next) {
            return metadata.getBucket(bucketName, log, (err, bucketMD) => {
                if (err) {
                    log.trace('error retrieving bucket metadata');
                    return next(err);
                }
                // check whether bucket has transient or deleted flag
                if (bucketShield(bucketMD, 'objectDelete')) {
                    return next(errors.NoSuchBucket);
                }
                if (!isBucketAuthorized(bucketMD, 'objectDelete',
                    canonicalID)) {
                    log.trace('access denied due to bucket acl\'s');
                    // if access denied at the bucket level, no access for
                    // any of the objects so all results will be error results
                    inPlay.forEach(key => {
                        errorResults.push({
                            key,
                            error: errors.AccessDenied,
                        });
                    });
                    // by setting inPlay length to 0 it empties out the
                    // array so async.forEachLimit below will not actually
                    // make any calls to metadata or data but will continue on
                    // to the next step to build xml
                    inPlay.length = 0;
                }
                return next(null, quietSetting);
            });
        },
        function getObjMetadataAndDelete(quietSetting, next) {
            // for obj deletes, no need to check acl's at object level
            // (authority is at the bucket level for obj deletes)

            // doing 5 requests at a time. note that services.deleteObject
            // will do 5 parallel requests to data backend to delete parts
            return async.forEachLimit(inPlay, 5, (key, moveOn) => {
                metadata.getObjectMD(bucketName, key, log, (err, objMD) => {
                    // if general error from metadata return error
                    if (err && !err.NoSuchKey) {
                        return next(err);
                    }
                    // if particular key does not exist, AWS returns success
                    // for key so add to deleted list and move on
                    if (err && err.NoSuchKey) {
                        deleted.push(key);
                        return moveOn();
                    }
                    return services.deleteObject(bucketName, objMD, key, log,
                        err => {
                            if (err) {
                                const obj = {};
                                obj[key] = err;
                                errorResults.push({
                                    key,
                                    error: err,
                                });
                            }
                            if (objMD['content-length']) {
                                totalContentLengthDeleted +=
                                    objMD['content-length'];
                            }
                            deleted.push(key);
                            return moveOn();
                        });
                });
            },
            // end of forEach func
            err => {
                const numOfObjects = deleted.length;
                log.trace('finished deleting objects', { numOfObjects });
                return next(err, quietSetting, numOfObjects);
            });
        },
    ], (err, quietSetting, numOfObjects) => {
        if (err) {
            return callback(err);
        }
        const xml = _formatXML(quietSetting, errorResults, deleted);
        return callback(null, xml, totalContentLengthDeleted, numOfObjects);
    });
}
