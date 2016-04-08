import assert from 'assert';

import async from 'async';
import { errors } from 'arsenal';

import BucketInfo from './metadata/BucketInfo';
import acl from './metadata/acl';
import constants from '../constants';
import data from './data/wrapper';
import { isBucketAuthorized, isObjAuthorized } from
    './api/apiUtils/authorization/aclChecks';
import metadata from './metadata/wrapper';

const splitter = constants.splitter;
const usersBucket = constants.usersBucket;

function _deleteUserBucket(bucketName, canonicalID, log, cb) {
    log.trace('deleting user bucket');
    const keyForUserBucket = `${canonicalID}${splitter}${bucketName}`;
    metadata.deleteObjectMD(usersBucket, keyForUserBucket, log, error => {
        if (error && error !== errors.NoSuchKey) {
            log.error('from metadata while deleting user bucket', { error });
        }
        log.trace('deleted bucket from user bucket');
        return cb(error);
    });
}

export default {
    getService(authInfo, request, log, cb) {
        const canonicalID = authInfo.getCanonicalID();
        const prefix = `${canonicalID}${splitter}`;
        // Note that since maxKeys on a listObject request is 10,000,
        // this request will retrieve up to 10,000 bucket names for a user.
        metadata.listObject(usersBucket, prefix, null, null, null, log,
            (err, listResponse) => {
                // If MD responds with NoSuchBucket, this means the
                // hidden usersBucket has not yet been created for
                // the domain.  If this is the case, it means
                // that no buckets in this domain have been created so
                // it follows that this particular user has no buckets.
                // So, the get service listing should not have any
                // buckets to list. By returning an empty array, the
                // getService API will just respond with the user info
                // without listing any buckets.
                if (err && err.NoSuchBucket) {
                    log.trace('no buckets found');
                    return cb(null, []);
                }
                if (err) {
                    log.error('error from metadata', { error: err });
                    return cb(err);
                }
                return cb(null, listResponse.Contents);
            });
    },

    /**
     * Checks whether resource exists and the user is authorized
     * @param {object} params - custom built object containing
     * resource name, type, authInfo etc.
     * @param {function} cb - callback containing error,
     * bucket and object references for the next task
     * @return {function} calls callback with arguments:
     * error, bucket, and objectMetada(if any)
     */
    metadataValidateAuthorization(params, cb) {
        const { authInfo, bucketName, objectKey, requestType, log } = params;
        const canonicalID = authInfo.getCanonicalID();
        assert.strictEqual(typeof bucketName, 'string');
        assert.strictEqual(typeof canonicalID, 'string');
        log.trace('performing metadata validation checks');

        if (objectKey === undefined) {
            return metadata.getBucket(bucketName, log, (err, bucketAttrs) => {
                if (err) {
                    log.debug('metadata getbucket failed', { error: err });
                    return cb(err);
                }
                const bucket = new BucketInfo(bucketAttrs.name,
                    bucketAttrs.owner, bucketAttrs.ownerDisplayName,
                    bucketAttrs.creationDate, bucketAttrs.acl);
                if (!isBucketAuthorized(bucket, requestType, canonicalID)) {
                    log.debug('access denied for user on bucket', {
                        requestType,
                    });
                    return cb(errors.AccessDenied);
                }
                log.trace('found bucket in metadata');
                return cb(null, bucket, null);
            });
        }
        return metadata.getBucketAndObjectMD(bucketName, objectKey, log,
        (err, data) => {
            if (err) {
                log.debug('metadata get failed', { error: err });
                return cb(err);
            }

            const bucketAttrs = data.bucket ? JSON.parse(data.bucket) :
                undefined;
            const obj = data.obj ? JSON.parse(data.obj) : undefined;
            if (!bucketAttrs) {
                log.debug('bucketAttrs is undefined', {
                    bucket: bucketName,
                    method: 'services.metadataValidateAuthorization',
                });
                return cb(errors.NoSuchBucket);
            }
            const bucket = new BucketInfo(bucketAttrs.name,
                bucketAttrs.owner, bucketAttrs.ownerDisplayName,
                bucketAttrs.creationDate, bucketAttrs.acl);
            if (!isBucketAuthorized(bucket, requestType, canonicalID)) {
                log.debug('access denied for user on bucket', { requestType });
                return cb(errors.AccessDenied);
            }
            if (!obj) {
                // NEED to pass on three arguments here for the objectPut
                // async waterfall to work
                log.trace('Bucket found', { bucketName });
                return cb(null, bucket, null);
            }
            // TODO: Add bucket policy and IAM checks
            if (!isObjAuthorized(bucket, obj, requestType, canonicalID)) {
                log.debug('access denied for user on object', { requestType });
                return cb(errors.AccessDenied);
            }
            log.trace('found bucket and object in metadata');
            return cb(null, bucket, obj);
        });
    },

    /**
     * Stores object and responds back with location and storage type
     * @param {object} objectMetadata - object's metadata (or if multipart
     * upload, then it is the multipart metadata)
     * @param {object} objectContext - object's keyContext for sproxyd Key
     * computation (put API)
     * @param {object} request - request linked to the stream containing
     *                           the data
     * @param {RequestLogger} log - the current request logger
     * @param {function} cb - callback containing result for the next task
     * @return {undefined}
     */
    dataStore(objectMetadata, objectContext, request, log, cb) {
        assert.strictEqual(arguments.length, 5);
        data.put(request, objectContext, log, (err, dataRetrievalInfo) => {
            if (err) {
                log.error('error in datastore', { error: err });
                return cb(err);
            }
            if (dataRetrievalInfo) {
                log.trace('dataStore: backend stored key',
                    { dataRetrievalInfo });
                // Note if this is the upload of a part, objectMetadata is
                // actually the multipart upload metadata
                return cb(null, objectMetadata, dataRetrievalInfo);
            }
            log.fatal('data put returned neither an error nor a key',
                      { method: 'dataStore' });
            return cb(errors.InternalError);
        });
    },

    /**
     * Stores object location, custom headers, version etc.
     * @param {object} bucketName - bucket in which metadata is stored
     * @param {object} objectMetadata - object's metadata
     * @param {array} dataGetInfo - array of objects with information to
     * retrieve data or null if 0 bytes object
     * @param {object} params - custom built object containing resource details.
     * @param {function} cb - callback containing result for the next task
     * @return {function} executes callback with err or ETag as arguments
     */
    metadataStoreObject(bucketName, objectMetadata, dataGetInfo, params, cb) {
        const { objectKey, authInfo, size, contentMD5, metaHeaders,
            contentType, multipart, headers, log } = params;
        log.trace('storing object in metadata');

        assert.strictEqual(typeof bucketName, 'string');
        let omVal;
        if (objectMetadata) {
            omVal = objectMetadata;
        } else {
            omVal = {};
            // Version 2 changes the format of the location property
            omVal['md-model-version'] = 2;
            omVal.Date = new Date().toJSON();

            // AWS docs state that the user that creates
            // a resource is the owner.
            // Assumed here that original creator remains
            // owner even if later Puts to object.
            // If an IAM user uploads a resource,
            // the owner should be the parent account.
            // http://docs.aws.amazon.com/AmazonS3/
            // latest/dev/access-control-overview.html
            omVal['owner-display-name'] =
                authInfo.getAccountDisplayName();
            // This should be object creator's canonical ID.
            omVal['owner-id'] = authInfo.getCanonicalID();
        }
        omVal['content-length'] = size;
        omVal['content-type'] = contentType;
        // confirm date format
        omVal['last-modified'] = new Date().toJSON();
        omVal['content-md5'] = contentMD5;
        // Need to complete values
        omVal['x-amz-server-side-encryption'] = '';
        omVal['x-amz-server-version-id'] = '';
        // TODO: Handle this as a utility function for all object puts
        // similar to normalizing request but after checkAuth so
        // string to sign is not impacted.  This is GH Issue#89.
        omVal['x-amz-storage-class'] = 'STANDARD';
        omVal['x-amz-website-redirect-location'] = '';
        omVal['x-amz-server-side-encryption-aws-kms-key-id'] = '';
        omVal['x-amz-server-side-encryption-customer-algorithm'] = '';
        omVal.location = dataGetInfo;
        // simple/no version. will expand once object versioning is introduced
        omVal['x-amz-version-id'] = 'null';
        omVal.acl = {
            Canned: 'private',
            FULL_CONTROL: [],
            WRITE_ACP: [],
            READ: [],
            READ_ACP: [],
        };

        // Store user provided metadata.  TODO: limit size.
        // For multipart upload this also serves to transfer
        // over metadata originally sent with the initiation
        // of the multipart upload (for instance, ACL's).
        // Do NOT move this to before
        // the assignments of metadata above since this loop
        // will reassign some of the above values with the ones
        // from the intiation of the multipart upload
        // (for instance, storage class)
        Object.keys(metaHeaders).forEach((val) => {
            omVal[val] = metaHeaders[val];
        });
        log.trace('object metadata', { omVal });

        // If this is not the completion of a multipart upload
        // parse the headers to get the ACL's if any
        if (!multipart) {
            const parseAclParams = {
                headers,
                resourceType: 'object',
                acl: omVal.acl,
                log,
            };
            log.trace('parsing acl from headers');
            acl.parseAclFromHeaders(parseAclParams, (err, parsedACL) => {
                if (err) {
                    log.warn('error parsing acl', { error: err });
                    return cb(err);
                }
                omVal.acl = parsedACL;
                metadata.putObjectMD(bucketName, objectKey, omVal, log, err => {
                    if (err) {
                        log.error('error from metadata', { error: err });
                        return cb(err);
                    }
                    log.trace('object successfully stored in metadata');
                    return cb(err, contentMD5);
                });
            });
        } else {
            metadata.putObjectMD(bucketName, objectKey, omVal, log, err => {
                if (err) {
                    log.error('error from metadata', { error: err });
                    return cb(err);
                }
                log.trace('object successfully stored in metadata');
                return cb(err, contentMD5);
            });
        }
    },

    /**
     * Creates bucket
     * @param {AuthInfo} authInfo - Instance of AuthInfo class with
     *                              requester's info
     * @param {string} bucketName - name of bucket
     * @param {object} headers - request headers
     * @param {string} locationConstraint - locationConstraint provided in
     *                                      request body xml (if provided)
     * @param {function} log - Werelogs logger
     * @param {function} cb - callback to bucketPut
     * @return {undefined}
     */
    createBucket(authInfo, bucketName, headers, locationConstraint, log, cb) {
        log.trace('Creating bucket');
        assert.strictEqual(typeof bucketName, 'string');
        assert.strictEqual(arguments.length, 6);
        const owner = authInfo.getCanonicalID();
        const ownerDisplayName = authInfo.getAccountDisplayName();
        const creationDate = new Date().toJSON();
        const bucket = new BucketInfo(bucketName, owner, ownerDisplayName,
                                      creationDate);
        if (locationConstraint !== null) {
            bucket.setLocationConstraint(locationConstraint);
        }
        const parseAclParams = {
            headers,
            resourceType: 'bucket',
            acl: bucket.acl,
            log,
        };
        acl.parseAclFromHeaders(parseAclParams, (error, parsedACL) => {
            if (error) {
                log.debug('error parsing acl from headers', { error });
                return cb(error);
            }
            bucket.setFullAcl(parsedACL);
            return metadata.createBucket(bucketName, bucket, log, error => {
                if (error) {
                    log.error('error from metadata', { error });
                    return cb(error);
                }
                const key = `${owner}${splitter}${bucketName}`;
                const omVal = { creationDate: new Date().toJSON() };
                return metadata.putObjectMD(usersBucket, key, omVal, log,
                err => {
                    if (err && err.NoSuchBucket) {
                        log.trace('users bucket does not exist, creating ' +
                                  'users bucket');
                        const creationDate = new Date().toJSON();
                        const freshBucket = new BucketInfo(usersBucket, 'admin',
                                                        'admin', creationDate);
                        return metadata.createBucket(usersBucket, freshBucket,
                            log, err => {
                                // Note: In the event that two
                                // users' requests try to create the
                                // usersBucket at the same time, this will
                                // prevent one of the users from getting a
                                // BucketAlreadyExists error with respect
                                // to the usersBucket.
                                if (err && err !== errors.BucketAlreadyExists) {
                                    log.error('error from metadata', {
                                        error: err,
                                    });
                                    return cb(err);
                                }
                                log.trace('Users bucket created');
                                return metadata.putObjectMD(usersBucket, key,
                                    omVal, log, cb);
                            });
                    }
                    return cb(err);
                });
            });
        });
    },

    /**
     * Deletes objects from a bucket
     * @param {string} bucketName - bucket in which objectMD is stored
     * @param {object} objectMD - object's metadata
     * @param {object} responseMDHeaders - contains user meta headers
     *                                     to be passed to response
     * @param {string} objectKey - object key name
     * @param {Log} log - logger instance
     * @param {function} cb - callback from async.waterfall in objectGet
     * @return {undefined}
     */
    deleteObject(bucketName, objectMD, responseMDHeaders, objectKey, log, cb) {
        log.trace('deleting object from bucket');
        assert.strictEqual(typeof bucketName, 'string');
        assert.strictEqual(typeof objectMD, 'object');
        if (objectMD['x-amz-version-id'] === 'null') {
            log.trace('object identified as non-versioned');
            // non-versioned buckets
            log.trace('deleteObject: deleting non-versioned object');
            if (objectMD.location === null) {
                return metadata.deleteObjectMD(bucketName, objectKey, log, cb);
            } else if (!Array.isArray(objectMD.location)) {
                return data.delete(objectMD.location, log, err => {
                    if (err) {
                        log.error('error deleting from dataStore', {
                            error: err,
                        });
                        return cb(err);
                    }
                    log.trace('deleteobject: data delete ok');
                    metadata.deleteObjectMD(bucketName, objectKey, log, cb);
                });
            }
            const errs = [];
            async.eachLimit(objectMD.location, 5, (loc, ok) => {
                data.delete(loc, log, err => {
                    if (err) {
                        errs.push(err);
                        log.error('error deleting from dataStore', {
                            error: err,
                        });
                    }
                    log.trace('deleteobject: data delete ok');
                    ok();
                });
            }, () => {
                // Arrays are only used internally for MPU objects
                if (errs[0]) {
                    return cb(errs[0]);
                }
                metadata.deleteObjectMD(bucketName, objectKey, log, cb);
            });
        } else {
            // versioning
            log.warn('deleteObject: versioning not implemented');
            cb(errors.NotImplemented);
        }
    },

    /**
     * Delete bucket from namespace
     * @param {string} bucketName - bucket in which objectMetadata is stored
     * @param {string} canonicalID - account canonicalID of requester
     * @param {object} log - Werelogs logger
     * @param {function} cb - callback from async.waterfall in objectGet
     * @return {function} executes cb with error or success message as arguments
     */
    deleteBucket(bucketName, canonicalID, log, cb) {
        log.trace('deleting bucket from metadata');
        assert.strictEqual(typeof bucketName, 'string');
        assert.strictEqual(typeof canonicalID, 'string');
        // Check to see if there are any objects in the bucket
        // by doing a listing with 1 maxKey
        metadata.listObject(bucketName, null, null, null, 1, log,
            (err, objectsListRes) => {
                if (err) {
                    log.error('error from metadata', { error: err });
                    return cb(err);
                }
                if (objectsListRes.Contents.length) {
                    log.warn('bucket delete failed',
                        { error: errors.BucketNotEmpty });
                    return cb(errors.BucketNotEmpty);
                }
                metadata.deleteBucket(bucketName, log, (err) => {
                    log.trace('deleting bucket from metadata');
                    if (err) {
                        log.error('error from metadata', { error: err });
                        return cb(err);
                    }
                    log.trace('deleted bucket from metadata');
                    log.trace("deleting bucket from user's bucket");
                    _deleteUserBucket(bucketName, canonicalID, log, cb);
                });
            });
    },

    /**
     * Validates request headers included 'if-modified-since',
     * 'if-unmodified-since', 'if-match' or 'if-none-match'
     * headers.  If so, return appropriate response based
     * on last-modified date of object or ETag.
     * Also pulls user's meta headers from metadata and
     * passes them along to be added to response.
     * @param {object} objectMD - object's metadata
     * @param {object} metadataCheckParams - contains lowercased
     * headers from request object
     * @param {function} cb - callback from async.waterfall in objectGet
     * @return {function} executes cb error, bucket, objectMetada
     * and responseMetaHeaders as arguments
     */
    validateHeaders(objectMD, metadataCheckParams, cb) {
        assert.strictEqual(arguments.length, 3);
        if (!objectMD) {
            return cb(errors.NoSuchKey);
        }

        const headers = metadataCheckParams.headers;
        const lastModified = new Date(objectMD['last-modified']).getTime();
        const contentMD5 = objectMD['content-md5'];
        let ifModifiedSinceTime = headers['if-modified-since'];
        let ifUnmodifiedSinceTime = headers['if-unmodified-since'];
        const ifETagMatch = headers['if-match'];
        const ifETagNoneMatch = headers['if-none-match'];
        if (ifModifiedSinceTime) {
            ifModifiedSinceTime = new Date(ifModifiedSinceTime);
            ifModifiedSinceTime = ifModifiedSinceTime.getTime();
            if (isNaN(ifModifiedSinceTime)) {
                return cb(errors.InvalidArgument);
            }
            if (lastModified < ifModifiedSinceTime) {
                return cb(errors.NotModified);
            }
        }
        if (ifUnmodifiedSinceTime) {
            ifUnmodifiedSinceTime = new Date(ifUnmodifiedSinceTime);
            ifUnmodifiedSinceTime = ifUnmodifiedSinceTime.getTime();
            if (isNaN(ifUnmodifiedSinceTime)) {
                return cb(errors.InvalidArgument);
            }
            if (lastModified > ifUnmodifiedSinceTime) {
                return cb(errors.PreconditionFailed);
            }
        }
        if (ifETagMatch) {
            if (ifETagMatch !== contentMD5) {
                return cb(errors.PreconditionFailed);
            }
        }
        if (ifETagNoneMatch) {
            if (ifETagNoneMatch === contentMD5) {
                return cb(errors.NotModified);
            }
        }
        // Add user meta headers from objectMD
        const responseMetaHeaders = {};
        Object.keys(objectMD).filter(val => val.substr(0, 11) === 'x-amz-meta-')
            .forEach(id => { responseMetaHeaders[id] = objectMD[id]; });

        // TODO: Add additional response headers --
        // i.e. x-amz-storage-class and x-amz-server-side-encryption
        responseMetaHeaders['Content-Length'] = objectMD['content-length'];
        // Note: ETag must have a capital "E" and capital "T" for cosbench
        // to work.
        responseMetaHeaders.ETag = `"${objectMD['content-md5']}"`;
        responseMetaHeaders['Last-Modified'] =
            new Date(objectMD['last-modified']).toUTCString();
        if (objectMD['content-type']) {
            responseMetaHeaders['Content-Type'] = objectMD['content-type'];
        }
        return cb(null, objectMD, responseMetaHeaders);
    },

    /**
     * Gets list of objects in bucket
     * @param {object} bucketName - bucket in which objectMetadata is stored
     * @param {object} listingParams - params object passing on
     * needed items from request object
     * @param {object} log - request logger instance
     * @param {function} cb - callback to bucketGet.js
     * @return {undefined}
     * JSON response from metastore
     */
    getObjectListing(bucketName, listingParams, log, cb) {
        assert.strictEqual(typeof bucketName, 'string');
        const { delimiter, marker, prefix } = listingParams;
        const maxKeys = Number(listingParams.maxKeys);
        log.trace('performing metadata get object listing',
            { listingParams, maxKeys });
        metadata.listObject(bucketName, prefix, marker, delimiter, maxKeys, log,
            (err, listResponse) => {
                if (err) {
                    log.warn('error from metadata', { error: err });
                    return cb(err);
                }
                return cb(null, listResponse);
            });
    },

    metadataStoreMPObject(bucketName, params, log, cb) {
        assert.strictEqual(typeof bucketName, 'string');
        // TODO: Determine splitter that will not appear in
        // any of these items.  This is GH Issue#218
        // 1) ObjectKey can contain any characters so when initiating
        // the MPU, we restricted the ability to create an object containing
        // the splitter.
        // 2) UploadId's are UUID version 4
        const longMPUIdentifier =
            `overview${splitter}${params.objectKey}` +
            `${splitter}${params.uploadId}`;
        const multipartObjectMD = {};
        multipartObjectMD.id = params.uploadId;
        multipartObjectMD.eventualStorageBucket = params.eventualStorageBucket;
        multipartObjectMD.initiated = new Date().toJSON();
        // Note: opting to store the initiator and owner
        // info here (including display names)
        // rather than just saving the canonicalID and
        // calling the display name when get a view request.
        // Since multi-part upload will likely not be open
        // for that long, seems unnecessary
        // to be concerned about a change in the display
        // name while the multi part upload is open.
        multipartObjectMD['owner-display-name'] = params.ownerDisplayName;
        multipartObjectMD['owner-id'] = params.ownerID;
        multipartObjectMD.initiator = {
            DisplayName: params.initiatorDisplayName,
            ID: params.initiatorID,
        };
        multipartObjectMD.key = params.objectKey;
        multipartObjectMD.uploadId = params.uploadId;
        multipartObjectMD['cache-control'] = params.headers['cache-control'];
        multipartObjectMD['content-disposition'] =
            params.headers['content-disposition'];
        multipartObjectMD['content-encoding'] =
            params.headers['content-encoding'];
        multipartObjectMD['content-type'] =
            params.headers['content-type'];
        multipartObjectMD.expires =
            params.headers.expires;
        multipartObjectMD['x-amz-storage-class'] = params.storageClass;
        multipartObjectMD['x-amz-website​-redirect-location'] =
            params.headers['x-amz-website-redirect-location'];
        Object.keys(params.metaHeaders).forEach(val => {
            multipartObjectMD[val] = params.metaHeaders[val];
        });

        // TODO: Add encryption values from headers if sent with request

        const parseAclParams = {
            headers: params.headers,
            resourceType: 'object',
            acl: {
                Canned: 'private',
                FULL_CONTROL: [],
                WRITE_ACP: [],
                READ: [],
                READ_ACP: [],
            },
            log,
        };
        acl.parseAclFromHeaders(parseAclParams, (err, parsedACL) => {
            if (err) {
                return cb(err);
            }
            multipartObjectMD.acl = parsedACL;
            metadata.putObjectMD(bucketName, longMPUIdentifier,
                multipartObjectMD, log, (err) => {
                    if (err) {
                        return cb(err);
                    }
                    return cb();
                });
        });
    },


    /**
     * Checks whether bucket exists, multipart upload
     * has been initatied and the user is authorized
     * @param {object} params - custom built object containing
     * bucket name, uploadId, authInfo etc.
     * @param {function} cb - callback containing error and
     * bucket reference for the next task
     * @return {function} calls callback with arguments:
     * error, bucket and the multipart upload metadata
     */
    metadataValidateMultipart(params, cb) {
        const { bucketName, uploadId, authInfo,
            objectKey, requestType, log } = params;

        assert.strictEqual(typeof bucketName, 'string');
        // This checks whether the mpu bucket exists.
        // If the MPU was initiated, the mpu bucket should exist.
        const mpuBucketName = `${constants.mpuBucketPrefix}${bucketName}`;
        metadata.getBucket(mpuBucketName, log, (err, mpuBucket) => {
            if (err) {
                return cb(errors.NoSuchUpload);
            }
            const searchArgs = {
                prefix: `overview${splitter}${objectKey}${splitter}${uploadId}`,
                marker: undefined,
                delimiter: undefined,
                maxKeys: 1,
            };

            metadata.listObject(mpuBucketName, searchArgs.prefix,
                searchArgs.marker, searchArgs.delimiter, searchArgs.maxKeys,
                log, (err, response) => {
                    if (err) {
                        return cb(err);
                    }
                    if (response.Contents.length !== 1) {
                        return cb(errors.NoSuchUpload);
                    }

                    // Take the saved overview and convert it into an array
                    // so can pull info from name
                    let mpuOverview =
                        response.Contents[0].key.split(splitter);

                    // Add other mpu info to the mpuOverview Array
                    const storedValue = response.Contents[0].value;
                    const eventualStorageBucket = storedValue
                        .eventualStorageBucket;
                    const initiatorID = storedValue.Initiator.ID;
                    const initiatorDisplayName = storedValue
                        .Initiator.DisplayName;
                    const ownerID = storedValue.Owner.ID;
                    const ownerDisplayName = storedValue.Owner.DisplayName;
                    const storageClass = storedValue.StorageClass;
                    const initiated = storedValue.Inititated;
                    mpuOverview = mpuOverview.concat([eventualStorageBucket,
                        initiatorID, initiatorDisplayName,
                        ownerID, ownerDisplayName,
                        storageClass, initiated]);
                    // If access was provided by the destination bucket's
                    // bucket policies, go ahead.
                    if (requestType === 'bucketPolicyGoAhead') {
                        return cb(null, mpuBucket, mpuOverview);
                    }

                    const requesterID = authInfo.isRequesterAnIAMUser() ?
                        authInfo.getArn() : authInfo.getCanonicalID();
                    const isRequesterInitiator =
                        initiatorID === requesterID;
                    const isRequesterParentAccountOfInitiator =
                        ownerID === authInfo.getCanonicalID();
                    if (requestType === 'putPart or complete') {
                        // Only the initiator of the multipart
                        // upload can upload a part or complete the mpu
                        if (!isRequesterInitiator) {
                            return cb(errors.AccessDenied);
                        }
                    }
                    if (requestType === 'deleteMPU'
                        || requestType === 'listParts') {
                        // In order for account/user to be
                        // authorized must either be the
                        // bucket owner or intitator of
                        // the multipart upload request
                        // (or parent account of initiator).
                        // In addition if the bucket policy
                        // designates someone else with
                        // s3:AbortMultipartUpload or
                        // s3:ListMultipartUploadPartsrights,
                        // as applicable, that account/user will have the right.
                        // If got to this step, it means there is
                        // no bucket policy on this.
                        if (mpuBucket.getOwner() !== authInfo.getCanonicalID()
                        && !isRequesterInitiator
                        && !isRequesterParentAccountOfInitiator) {
                            return cb(errors.AccessDenied);
                        }
                    }
                    return cb(null, mpuBucket, mpuOverview);
                });
        });
    },

    /**
     * Stores metadata about a part of a multipart upload
     * @param {string} mpuBucketName - name of the special mpu bucket
     * @param {object} partLocations - data retrieval info for part
     * @param {string} partLocations.key - key in datastore for part
     * @param {string} partLocations.dataStoreName - name of dataStore
     * @param {object} metaStoreParams - custom built object
     * @param {object} log - request logger instance
     * @param {function} cb - callback to send error or move to next task
     * @return {undefined}
     */
    metadataStorePart(mpuBucketName, partLocations, metaStoreParams, log, cb) {
        assert.strictEqual(typeof mpuBucketName, 'string');
        const { partNumber, contentMD5, size, uploadId } = metaStoreParams;
        const lastModified = new Date().toJSON();
        // TODO: Determine splitter that will not appear in
        // any of these items.  This is GH Issue#218
        // 1) UploadId's are UUID version 4
        // 2) Part Number will be a stringified number between 1 and 10000
        const partKey = `${uploadId}${splitter}${partNumber}`;
        const omVal = {
            // Version 2 changes the format of partLocations
            'md-model-version': 2,
            partLocations,
            'key': partKey,
            'last-modified': lastModified,
            'content-md5': contentMD5,
            'content-length': size,
        };
        metadata.putObjectMD(mpuBucketName, partKey, omVal, log, err => {
            if (err) {
                return cb(err);
            }
            return cb(null);
        });
    },

    /**
    * Gets list of open multipart uploads in bucket
    * @param {object} MPUbucketName - bucket in which objectMetadata is stored
    * @param {object} listingParams - params object passing on
    *                                 needed items from request object
    * @param {object} log - Werelogs logger
    * @param {function} cb - callback to listMultipartUploads.js
    * @return {undefined}
    */
    getMultipartUploadListing(MPUbucketName, listingParams, log, cb) {
        assert.strictEqual(typeof MPUbucketName, 'string');

        metadata.getBucket(MPUbucketName, log, (err, bucket) => {
            if (bucket === undefined) {
                return cb(null, {
                    IsTruncated: false,
                    NextMarker: undefined,
                    MaxKeys: 0,
                    Uploads: [],
                    CommonPrefixes: [],
                });
            }
            metadata.listMultipartUploads(MPUbucketName, listingParams, log,
                cb);
        });
    },

    /**
     * Gets the special multipart upload bucket associated with
     * the user's account or creates it if it does not exist
     * @param {Bucket} destinationBucket - bucket the mpu will end up in
     * @param {string} bucketName - name of the destination bucket
     * @param {object} log - Werelogs logger
     * @param {function} cb - callback that returns multipart
     *                        upload bucket or error if any
     * @return {undefined}
     */
    getMPUBucket(destinationBucket, bucketName, log, cb) {
        assert.strictEqual(typeof bucketName, 'string');
        const MPUBucketName = `${constants.mpuBucketPrefix}${bucketName}`;
        metadata.getBucket(MPUBucketName, log, (err, bucket) => {
            if (err && err.NoSuchBucket) {
                const creationDate = new Date().toJSON();
                const mpuBucket = new BucketInfo(MPUBucketName,
                    destinationBucket.getOwner(),
                    destinationBucket.getOwnerDisplayName(), creationDate);
                // Note that unlike during the creation of a normal bucket,
                // we do NOT add this bucket to the lists of a user's buckets.
                // By not adding this bucket to the lists of a user's buckets,
                // a getService request should not return a reference to this
                // bucket.  This is the desired behavior since this should be
                // a hidden bucket.
                return metadata.createBucket(MPUBucketName, mpuBucket, log,
                    err => {
                        if (err) {
                            return cb(err);
                        }
                        return cb(null, mpuBucket);
                    });
            }
            return cb(null, bucket);
        });
    },

    getMPUparts(mpuBucketName, uploadId, log, cb) {
        assert.strictEqual(typeof mpuBucketName, 'string');
        const searchArgs = {
            prefix: `${uploadId}`,
            marker: undefined,
            delimiter: undefined,
            maxKeys: 10000,
        };
        metadata.listObject(mpuBucketName, searchArgs.prefix, searchArgs.marker,
            searchArgs.delimiter, searchArgs.maxKeys, log, cb);
    },

    getSomeMPUparts(params, cb) {
        const { uploadId, mpuBucketName, maxParts, partNumberMarker, log } =
            params;
        assert.strictEqual(typeof mpuBucketName, 'string');
        const searchArgs = {
            prefix: uploadId,
            marker: `${uploadId}${splitter}${partNumberMarker}`,
            delimiter: undefined,
            maxKeys: maxParts,
        };
        metadata.listObject(mpuBucketName, searchArgs.prefix, searchArgs.marker,
            searchArgs.delimiter, searchArgs.maxKeys, log, cb);
    },

    batchDeleteObjectMetadata(mpuBucketName, keysToDelete, log, cb) {
        // If have efficient way to batch delete metadata, should so this
        // all at once in production implementation
        assert.strictEqual(typeof mpuBucketName, 'string');
        async.eachLimit(keysToDelete, 5, function action(key, callback) {
            metadata.deleteObjectMD(mpuBucketName, key, log, callback);
        },
        function finalCallback(err) {
            return cb(err);
        });
    },
};
