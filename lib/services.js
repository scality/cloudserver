import assert from 'assert';
import { Readable } from 'stream';

import async from 'async';

import Bucket from './metadata/in_memory/Bucket';
import constants from '../constants';
import acl from './metadata/acl';
import data from './data/wrapper';
import metadata from './metadata/wrapper';

const splitter = constants.splitter;
const usersBucket = constants.usersBucket;

function _deleteUserBucket(bucketName, accessKey, log, cb) {
    log.debug('Deleting user bucket...');
    const keyForUserBucket = `${accessKey}${splitter}${bucketName}`;
    metadata.deleteObjectMD(usersBucket, keyForUserBucket, log, (err) => {
        if (err) {
            log.error(`Error from Metadata while deleting user bucket ${err}`);
        }
        log.debug('Deleted bucket ${bucketName} from user bucket');
        return cb(err);
    });
}

function isBucketAuthorized(bucket, requestType, accessKey) {
    // Check to see if user is authorized to perform a
    // particular action on bucket based on ACLs.
    // This assumes accessKey is the canonicalID.
    // TODO: ensure that accessKey being provided here is
    // canonicalID.
    // TODO: Add IAM checks and bucket policy checks.
    if (requestType === 'bucketGet' || requestType === 'bucketHead'
            || requestType === 'objectGet' || requestType === 'objectHead') {
        if (bucket.owner === accessKey) {
            return true;
        } else if (bucket.acl.Canned === 'public-read'
            || bucket.acl.Canned === 'public-read-write'
            || (bucket.acl.Canned === 'authenticated-read'
                && accessKey
                    !== 'http://acs.amazonaws.com/groups/global/AllUsers')) {
            return true;
        } else if (bucket.acl.FULL_CONTROL.indexOf(accessKey) > -1
            || bucket.acl.READ.indexOf(accessKey) > -1) {
            return true;
        }
    }

    if (requestType === 'bucketGetACL') {
        if (bucket.owner === accessKey
            || (bucket.acl.Canned === 'log-delivery-write'
                && accessKey
                === 'http://acs.amazonaws.com/groups/s3/LogDelivery')
            || bucket.acl.FULL_CONTROL.indexOf(accessKey) > -1
            || bucket.acl.READ_ACP.indexOf(accessKey) > -1) {
            return true;
        }
    }

    if (requestType === 'bucketPutACL') {
        if (bucket.owner === accessKey
            || bucket.acl.FULL_CONTROL.indexOf(accessKey) > -1
            || bucket.acl.WRITE_ACP.indexOf(accessKey) > -1) {
            return true;
        }
    }

    if (requestType === 'bucketDelete' && bucket.owner === accessKey) {
        return true;
    }

    if (requestType === 'objectDelete' || requestType === 'objectPut') {
        if (bucket.owner === accessKey
            || bucket.acl.Canned === 'public-read-write'
            || bucket.acl.FULL_CONTROL.indexOf(accessKey) > -1
            || bucket.acl.WRITE.indexOf(accessKey) > -1) {
            return true;
        }
    }
    return (requestType === 'objectPutACL' || requestType === 'objectGetACL');
}

function isObjAuthorized(bucket, objectMD, requestType, accessKey) {
    if (!objectMD) {
        return false;
    }
    if (requestType === 'objectGet' || requestType === 'objectHead') {
        if (objectMD['owner-id'] === accessKey) {
            return true;
        } else if (objectMD.acl.Canned === 'public-read'
            || objectMD.acl.Canned === 'public-read-write'
            || (objectMD.acl.Canned === 'authenticated-read'
                && accessKey !== 'http://acs.amazonaws.com' +
                '/groups/global/AllUsers')) {
            return true;
        } else if (objectMD.acl.Canned === 'bucket-owner-read'
                && bucket.owner === accessKey) {
            return true;
        } else if ((objectMD.acl.Canned === 'bucket-owner-full-control'
                && bucket.owner === accessKey)
            || objectMD.acl.FULL_CONTROL.indexOf(accessKey) > -1
            || objectMD.acl.READ.indexOf(accessKey) > -1) {
            return true;
        }
    }

    if (requestType === 'objectPut' || requestType === 'objectDelete') {
        if (objectMD['owner-id'] === accessKey
            || objectMD.acl.Canned === 'public-read-write'
            || (objectMD.acl.Canned === 'bucket-owner-full-control'
                && bucket.owner === accessKey)
            || objectMD.acl.FULL_CONTROL.indexOf(accessKey) > -1
            || objectMD.acl.WRITE.indexOf(accessKey) > -1) {
            return true;
        }
    }

    if (requestType === 'objectPutACL') {
        if (objectMD['owner-id'] === accessKey
            || (objectMD.acl.Canned === 'bucket-owner-full-control'
                && bucket.owner === accessKey)
            || objectMD.acl.FULL_CONTROL.indexOf(accessKey) > -1
            || objectMD.acl.WRITE_ACP.indexOf(accessKey) > -1) {
            return true;
        }
    }

    if (requestType === 'objectGetACL') {
        if (objectMD['owner-id'] === accessKey
            || objectMD.acl.Canned === 'bucket-owner-full-control'
            || objectMD.acl.FULL_CONTROL.indexOf(accessKey) > -1
            || objectMD.acl.READ_ACP.indexOf(accessKey) > -1) {
            return true;
        }
    }
    return false;
}

export default {
    getService(accessKey, request, log, cb) {
        const prefix = `${accessKey}${splitter}`;
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
                if (err === 'NoSuchBucket') {
                    log.debug('No buckets found');
                    return cb(null, []);
                }
                if (err) {
                    log.error(`Error from metadata: ${err}`);
                    return cb(err);
                }
                return cb(null, listResponse.Contents);
            });
    },

    /**
     * Checks whether resource exists and the user is authorized
     * @param {object} params - custom built object containing
     * resource name, type, access key etc.
     * @param {function} cb - callback containing error,
     * bucket and object references for the next task
     * @return {function} calls callback with arguments:
     * error, bucket, and objectMetada(if any)
     */
    metadataValidateAuthorization(params, cb) {
        const { accessKey, bucketName, objectKey, requestType, log } = params;
        assert.strictEqual(typeof bucketName, 'string');
        log.debug('Performing metadata validation checks');
        metadata.getBucket(bucketName, log, (err, bucket) => {
            if (err) {
                log.error(`Metadata get bucket failed: ${err}`);
                return cb(err);
            }
            if (!isBucketAuthorized(bucket, requestType, accessKey)) {
                log.error(`User does not have authorization to perform ` +
                     `${requestType} for this bucket.`);
                return cb('AccessDenied');
            }
            if (objectKey === undefined) {
                // NEED to pass on three arguments here for the objectPut async
                // waterfall to work
                log.trace('Found bucket in metadata');
                return cb(null, bucket, null);
            }

            metadata.getObjectMD(bucketName, objectKey, log, (err, objMD) => {
                log.trace('Performing metadata get object');

                // A key not already existing is not necessarily an error
                // that should be returned (for instance with a put object,
                // the put should continue)
                if (err && (err === 'NoSuchKey' || err === 'ObjNotFound')) {
                    // NEED to pass on three arguments here for the objectPut
                    // async waterfall to work
                    log.debug(`Bucket found: ${bucketName}`);
                    return cb(null, bucket, null);
                }
                // If there is an error other than the key not existing
                // return the error
                if (err) {
                    log.error(`Error from metadata get object: ${err}`);
                    return cb(err);
                }
                // TODO: Add bucket policy and IAM checks
                if (!isObjAuthorized(bucket, objMD, requestType, accessKey)) {
                    log.error(`User does not have authorization to perform ` +
                         `${requestType} on this object.`);
                    return cb('AccessDenied');
                }
                return cb(null, bucket, objMD);
            });
        });
    },

    /**
     * Stores object and responds back with location and storage type
     * @param {object} objectMetadata - object's metadata (or if multipart
     * upload, then it is the multipart metadata)
     * @param {object} objectContext - object's keyContext for sproxyd Key
     * computation (put API)
     * @param {object} value - the data to be stored
     * @param {RequestLogger} log - the current request logger
     * @param {function} cb - callback containing result for the next task
     * @return {function} executes cb with either
     * error or bucket as arguments
     */
    dataStore(objectMetadata, objectContext, value, log, cb) {
        assert.strictEqual(arguments.length, 5);
        data.put(value, objectContext, log, (err, keys) => {
            if (err) {
                log.error(`dataStore: ${err}`);
                return cb(err);
            }
            if (keys) {
                log.debug(`dataStore: backend stored keys ${keys.join(',')}`);
                // Note if this is the upload of a part, objectMetadata is
                // actually the multipart upload metadata
                return cb(null, objectMetadata, keys);
            }
            log.fatal(`dataStore: backend data put returned neither an error ` +
                      `nor a set of keys`);
            return cb('InternalError');
        });
    },

    /**
     * Stores object location, custom headers, version etc.
     * @param {object} bucket - bucket in which metadata is stored
     * @param {object} objectMetadata - object's metadata
     * @param {string[]} keys - object locations in datastore
     * @param {object} params - custom built object containing resource details.
     * @param {function} cb - callback containing result for the next task
     * @return {function} executes callback with err or ETag as arguments
     */
    metadataStoreObject(bucketName, objectMetadata, keys, params, cb) {
        const { objectKey, accessKey, size, contentMD5, metaHeaders,
            contentType, multipart, headers, log } = params;
        log.debug('Storing object in metadata');

        assert.strictEqual(typeof bucketName, 'string');
        // If a non multipart upload object is uploaded, change the
        // format of the location reference to an array with one item
        const dataArray = keys instanceof Array ? keys : [ keys ];
        let omVal;
        if (objectMetadata) {
            omVal = objectMetadata;
        } else {
            omVal = {};
            omVal.Date = new Date().toJSON();

            // AWS docs state that the user that creates
            //  a resource is the owner.
            // Assumed here that original creator remains
            // owner even if later Puts to object.
            // If an IAM user uploads a resource,
            // the owner should be the parent.
            // TODO: Need to update this to handle IAM users.
            // http://docs.aws.amazon.com/AmazonS3/
            // latest/dev/access-control-overview.html
            // TODO: display name should be different from accessKey.
            omVal['owner-display-name'] = accessKey;
            // TODO: This should be object creator's canonical ID.
            omVal['owner-id'] = accessKey;
        }
        omVal['content-length'] = size;
        omVal['content-type'] = contentType;
        // confirm date format
        omVal['last-modified'] = new Date().toJSON();
        omVal['content-md5'] = contentMD5;
        // Need to complete values
        omVal['x-amz-server-side-encryption'] = "";
        omVal['x-amz-server-version-id'] = "";
        // TODO: Handle this as a utility function for all object puts
        // similar to normalizing request but after checkAuth so
        // string to sign is not impacted.  This is GH Issue#89.
        omVal['x-amz-storage-class'] = 'STANDARD';
        omVal['x-amz-website-redirect-location'] = "";
        omVal['x-amz-server-side-encryption-aws-kms-key-id'] = "";
        omVal['x-amz-server-side-encryption-customer-algorithm'] = "";
        omVal.location = dataArray;
        // simple/no version. will expand once object versioning is introduced
        omVal['x-amz-version-id'] = 'null';
        omVal.policy = {};
        omVal.acl = {
            'Canned': 'private',
            'FULL_CONTROL': [],
            'WRITE_ACP': [],
            'READ': [],
            'READ_ACP': [],
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

        log.debug(`Object metadata: ${JSON.stringify(omVal)}`);

        // If this is not the completion of a multipart upload
        // parse the headers to get the ACL's if any
        if (!multipart) {
            const parseAclParams = {
                headers,
                resourceType: 'object',
                acl: omVal.acl,
                log,
            };
            log.debug(`Parsing ACL from Headers`);
            acl.parseAclFromHeaders(parseAclParams, (err, parsedACL) => {
                if (err) {
                    log.error(`Error parsing ACL: ${err}`);
                    return cb(err);
                }
                omVal.acl = parsedACL;
                metadata.putObjectMD(bucketName, objectKey, omVal, log, err => {
                    if (err) {
                        log.error(`Error from metadata: ${err}`);
                        return cb(err);
                    }
                    log.debug('Object successfully stored in metadata');
                    return cb(err, contentMD5);
                });
            });
        } else {
            metadata.putObjectMD(bucketName, objectKey, omVal, log, err => {
                if (err) {
                    log.error(`Error from metadata: ${err}`);
                    return cb(err);
                }
                log.debug('Object successfully stored in metadata');
                return cb(err, contentMD5);
            });
        }
    },

    /**
     * Checks whether user is authorized to create a bucket and whether
     * the bucket already exists.
     * If user is authorized and bucket does not already exist,
     * bucket is created and saved in metastore
     * along with metadata provided with request.
     * @param {string} accessKey - user's access key
     * @param {string} bucketName - name of bucket
     * @param {object} headers - request headers
     * @param {string} locationConstraint - locationConstraint
     * provided in request body xml (if provided)
     * @param {object} metastore - global metastore
     * @param {function} log - Werelogs logger
     * @param {function} callback - callback to bucketPut
     * @return {function} calls callback with error or result as arguments
     */
    createBucket(accessKey, bucketName, headers, locationConstraint,
            metastore, log, callback) {
        log.debug('Creating bucket');
        assert.strictEqual(typeof bucketName, 'string');
        assert.strictEqual(arguments.length, 7);
        async.waterfall([
            function waterfall2(next) {
                const bucket = new Bucket(bucketName, accessKey);

                if (locationConstraint !== undefined) {
                    bucket.locationConstraint = locationConstraint;
                }
                const parseAclParams = {
                    headers,
                    resourceType: 'bucket',
                    acl: bucket.acl,
                    log,
                };
                acl.parseAclFromHeaders(parseAclParams, (err, parsedACL) => {
                    if (err) {
                        log.error(`Error parsing ACL from headers: ${err}`);
                        return next(err);
                    }
                    bucket.acl = parsedACL;
                    return next(null, bucket);
                });
            },
            function waterfall3(bucket, next) {
                log.debug(`Creating bucket: ${bucketName} in Metadata`);
                metadata.createBucket(bucketName, bucket, log, (err) => {
                    if (err) {
                        log.error(`Error from Metadata: ${err}`);
                        return next(err);
                    }
                    log.debug(`Created bucket: ${bucketName} in Metadata`);
                    return next(null, bucketName);
                });
            },
            function addToUsersBucket(bucketName, next) {
                log.debug(`Adding bucket: ${bucketName} to user's bucket`);
                const key = `${accessKey}${splitter}${bucketName}`;
                const omVal = {};
                omVal.creationDate = new Date().toJSON();
                metadata.putObjectMD(usersBucket, key, omVal, log, err => {
                    if (err === 'NoSuchBucket') {
                        log.debug('Users bucket does not exist, creating ' +
                            'users bucket');
                        const freshBucket = new Bucket(usersBucket, 'admin');
                        return metadata.createBucket(usersBucket, freshBucket,
                            log, err => {
                                // Note: In the event that two
                                // users' requests try to create the
                                // usersBucket at the same time, this will
                                // prevent one of the users from getting a
                                // BucketAlreadyExists error with respect
                                // to the usersBucket.
                                if (err && err !== 'BucketAlreadyExists') {
                                    log.error(`Error from Metadata: ${err}`);
                                    return next(err);
                                }
                                log.debug('Users bucket created');
                                return metadata.putObjectMD(usersBucket, key,
                                    omVal, log, err => {
                                        if (err) {
                                            log.error(`Error from Metadata: ` +
                                                `${err}`);
                                            return next(err);
                                        }
                                        log.debug('Added bucket to users ' +
                                            'bucket');
                                        log.debug(`Bucket created`);
                                        return next(null, 'Bucket created');
                                    });
                            });
                    } else if (err) {
                        log.error(`Error from Metadata: ${err}`);
                        return next(err);
                    }
                    log.debug(`Bucket created`);
                    return next(null, 'Bucket created');
                });
            }
        ], function waterfallFinal(err, result) {
            callback(err, result);
        });
    },

    /**
     * Gets object from datastore
     * @param {object} bucket - bucket in which objectMetadata is stored
     * @param {object} objectMetadata - object's metadata
     * @param {object} responseMetaHeaders - contains user meta headers
     * to be passed to response
     * @param {function} cb - callback from async.waterfall in objectGet
     * @return {function} executes cb with error, result and responseMetaHeaders
     * as arguments
     */
    getFromDatastore(objectMetadata, responseMetaHeaders, log, cb) {
        assert.strictEqual(arguments.length, 4);
        // TODO: Handle range requests
        const locations = objectMetadata.location;
        const readStream = new Readable;

        // Call the data store asynchronously in order to get
        // the chunks from each part of the multipart upload
        data.get(locations, log, (err, chunks) => {
            if (err) {
                log.error(`getFromDatastore: ${err}`);
                return cb(err);
            }
            chunks.forEach(chunk => {
                if (chunk instanceof Array) {
                    log.debug(`getFromDatastore: pushing chunk group to read ` +
                              `stream`);
                    chunk.forEach(c => readStream.push(c));
                } else {
                    log.debug(`getFromDatastore: pushing chunk to read stream`);
                    readStream.push(chunk);
                }
            });
            log.debug(`getFromDatastore: finished pushing data to read stream`);
            readStream.push(null);
            return cb(null, readStream, responseMetaHeaders);
        });
    },

    /**
     * Deletes objects from a bucket
     * @param {object} bucket - bucket in which objectMD is stored
     * @param {object} objectMD - object's metadata
     * @param {object} responseMDHeaders - contains user meta headers
     * to be passed to response
     * @param {string} objectKey - object key name
     * @param {function} cb - callback from async.waterfall in objectGet
     * @return {function} executes cb with error,
     * result message and responseMDHeaders as arguments
     */
    deleteObject(bucketName, objectMD, responseMDHeaders, objectKey, log, cb) {
        log.debug('Deleting object from bucket');
        assert.strictEqual(typeof bucketName, 'string');
        assert.strictEqual(typeof objectMD, 'object');
        if (objectMD['x-amz-version-id'] === 'null') {
            log.debug('Object identified as non-versioned');
            // non-versioned buckets
            log.debug('deleteObject: deleting non-versioned object');
            data.delete(objectMD.location, log, err => {
                if (err) {
                    log.error(`deleteObject: ${err} key=${objectKey}`);
                    return cb(err);
                }
                log.debug(`deleteObject: data delete ok key=${objectKey}`);
                metadata.deleteObjectMD(bucketName, objectKey, log, cb);
            });
        } else {
            // versioning
            log.error('deleteObject: versioning not implemented');
            cb('NotImplemented');
        }
    },

    /**
     * Delete bucket from namespace
     * @param {object} bucket - bucket in which objectMetadata is stored
     * @param {object} metastore - metadata store
     * @param {string} accessKey - user's access key
     * @param {function} log - Werelogs logger
     * @param {function} cb - callback from async.waterfall in objectGet
     * @return {function} executes cb with error or success message as arguments
     */
    deleteBucket(bucketName, metastore, accessKey, log, cb) {
        log.debug('Deleting bucket from metadata');
        assert.strictEqual(typeof bucketName, 'string');
        metadata.listObject(bucketName, null, null, null, null, log,
            (err, objectsListRes) => {
                if (err) {
                    log.error(`Error from Metadata: ${err}`);
                    return cb(err);
                }
                if (objectsListRes.Contents.length) {
                    log.error('Bucket DELETE failed: BucketNotEmpty');
                    return cb('BucketNotEmpty');
                }
                metadata.deleteBucket(bucketName, log, (err) => {
                    log.debug('Deleting bucket from metadata');
                    if (err) {
                        log.error(`Error from Metadata: ${err}`);
                        return cb(err);
                    }
                    log.debug('Deleted bucket from metadata');
                    log.debug('Deleting bucket from user\'s bucket');
                    _deleteUserBucket(bucketName, accessKey, log, cb);
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
            return cb('NoSuchKey');
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
                return cb('InvalidArgument');
            }
            if (lastModified < ifModifiedSinceTime) {
                return cb('NotModified');
            }
        }
        if (ifUnmodifiedSinceTime) {
            ifUnmodifiedSinceTime = new Date(ifUnmodifiedSinceTime);
            ifUnmodifiedSinceTime = ifUnmodifiedSinceTime.getTime();
            if (isNaN(ifUnmodifiedSinceTime)) {
                return cb('InvalidArgument');
            }
            if (lastModified > ifUnmodifiedSinceTime) {
                return cb('PreconditionFailed');
            }
        }
        if (ifETagMatch) {
            if (ifETagMatch !== contentMD5) {
                return cb('PreconditionFailed');
            }
        }
        if (ifETagNoneMatch) {
            if (ifETagNoneMatch === contentMD5) {
                return cb('NotModified');
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
     * @param {function} cb - callback to bucketGet.js
     * @returns {function} callback with either error or
     * JSON response from metastore
     */
    getObjectListing(bucketName, listingParams, log, cb) {
        assert.strictEqual(typeof bucketName, 'string');
        const { delimiter, marker, prefix } = listingParams;
        const maxKeys = Number(listingParams.maxKeys);
        log.debug(`Performing Metadata Get Object Listing with params: ` +
            `${JSON.stringify(listingParams)} and maxKeys: ${maxKeys}`);
        metadata.listObject(bucketName, prefix, marker, delimiter, maxKeys, log,
            (err, listResponse) => {
                if (err) {
                    log.error(`Error from metadata: ${err}`);
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
            'DisplayName': params.initiatorDisplayName,
            'ID': params.initiatorID,
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
        Object.keys(params.metaHeaders).forEach((val) => {
            multipartObjectMD[val] = params.metaHeaders[val];
        });

        // TODO: Add encryption values from headers if sent with request

        const parseAclParams = {
            headers: params.headers,
            resourceType: 'object',
            acl: {
                'Canned': 'private',
                'FULL_CONTROL': [],
                'WRITE_ACP': [],
                'READ': [],
                'READ_ACP': [],
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
     * bucket name, uploadId, access key etc.
     * @param {function} cb - callback containing error and
     * bucket reference for the next task
     * @return {function} calls callback with arguments:
     * error, bucket and the multipart upload metadata
     */
    metadataValidateMultipart(params, cb) {
        const { bucketName, uploadId, accessKey,
            objectKey, requestType, log } = params;

        assert.strictEqual(typeof bucketName, 'string');
        // This checks whether the mpu bucket exists.
        // If the MPU was initiated, the mpu bucket should exist.
        const mpuBucketName = `mpuShadowBucket${bucketName}`;
        metadata.getBucket(mpuBucketName, log, (err, mpuBucket) => {
            if (err) {
                return cb('NoSuchUpload');
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
                        return cb('NoSuchUpload');
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

                    // TODO: initiator.id is the initiator's
                    // canonical id or IAM user's
                    // ARN.  So, accessKey here should
                    // be canonicalID or
                    // ARN (if applicable).
                    // If the initiator has an ARN and the current accessKey
                    // is a canonicalID, must also check to see
                    // if the canonicalID is the parent account of the ARN
                    // (could handle this by saving the canonical ID along with
                    // the arn as initiator.id in an array).
                    // GH Issue#75
                    const isInitiator =
                        initiatorID === accessKey ? true : false;
                    if (requestType === 'putPart or complete') {
                        // Only the initiator of the multipart
                        // upload can upload a part
                        if (!isInitiator) {
                            return cb('AccessDenied');
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
                        if (mpuBucket.owner !== accessKey && !isInitiator) {
                            return cb('AccessDenied');
                        }
                    }
                    return cb(null, mpuBucket, mpuOverview);
                });
        });
    },

    /**
     * Stores metadata about a part of a multipart upload
     * @param {object} mpuBucket - special mpu bucket
     * @param {array} dataLocations - locations of part in data store
     * @param {metaStoreParams } metaStoreParams - custom built object
     * @param {function} cb - callback to send error or move to next
     * task
     * @return {function} calls callback with either error or null
     */
    metadataStorePart(mpuBucketName, partLocations, metaStoreParams, log, cb) {
        assert.strictEqual(typeof mpuBucketName, 'string');
        const { partNumber, contentMD5, size, uploadId } = metaStoreParams;
        const lastModified = new Date().toJSON();
        // TODO: Determine splitter that will not appear in
        // any of these items.  This is GH Issue#218
        // 1) UploadId's are UUID version 4
        // 2) Part Number will be a stringified number between 1 and 10000
        const partKey =
            `${uploadId}${splitter}${partNumber}`;
        const omVal = {
            key: partKey,
            partLocations,
            'last-modified': lastModified,
            'content-md5': contentMD5,
            'content-length': size,
        };
        metadata.putObjectMD(mpuBucketName, partKey, omVal, log, (err) => {
            if (err) {
                return cb(err);
            }
            return cb(null);
        });
    },

    /**
    * Gets list of open multipart uploads in bucket
    * @param {object} bucket - bucket in which objectMetadata is stored
    * @param {object} listingParams - params object passing on
    * needed items from request object
    * @param {function} cb - callback to listMultipartUploads.js
    * @returns {function} callback with either error or
    * JSON response from metastore
    */
    getMultipartUploadListing(metastore, MPUbucketName, listingParams, log,
        cb) {
        assert.strictEqual(typeof MPUbucketName, 'string');
        metadata.getBucket(MPUbucketName, log, (err, bucket) => {
            if (bucket === undefined) {
                return cb(null, {
                    IsTruncated: false,
                    NextMarker: undefined,
                    MaxKeys: 0,
                });
            }
            bucket.getMultipartUploadListing(listingParams, (err, list) => {
                if (err) {
                    return cb(err);
                }
                return cb(null, list);
            });
        });
    },

    /**
     * Gets the special multipart upload bucket associated with
     * the user's account or creates it if it does not exist
     * @param {Bucket} destinationBucket - bucket the mpu will end up in
     * @param {string} bucketUID - unique identifier of the bucket
     * @param {function} cb - callback that returns multipart
     * upload bucket or error if any
     */
    getMPUBucket(destinationBucket, metastore, bucketName, log, cb) {
        assert.strictEqual(typeof bucketName, 'string');
        // Note that AWS does not allow '...' in bucket names.
        // http://docs.aws.amazon.com/AmazonS3/
        // latest/dev/BucketRestrictions.html
        // So, it will not be possible that a user will on its own take
        // any bucket name starting with 'mpuShadowBucket'
        const MPUBucketName = `mpuShadowBucket${bucketName}`;
        metadata.getBucket(MPUBucketName, log, (err, bucket) => {
            if (err === 'NoSuchBucket') {
                const mpuBucket = new Bucket(MPUBucketName,
                                          destinationBucket.owner);
                // Note that unlike during the creation of a normal bucket,
                // we do NOT add this bucket to the lists of a user's buckets.
                // By not adding this bucket to the lists of a user's buckets,
                // a getService request should not return a reference to this
                // bucket.  This is the desired behavior since this should be
                // a hidden bucket.
                return metadata.createBucket(MPUBucketName, mpuBucket, log,
                    (err) => {
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
        let results = [];
        let weAreNotDone;
        let marker;
        async.doWhilst(
            function searching(moveOn) {
                const searchArgs = {
                    prefix: `${uploadId}`,
                    marker,
                    delimiter: undefined,
                    maxKeys: 1000,
                };

                metadata.listObject(mpuBucketName, searchArgs.prefix,
                    searchArgs.marker, searchArgs.delimiter,
                    searchArgs.maxKeys, log, (err, response) => {
                        if (err) {
                            log.error(`Error from metadata: ${err}`);
                            return moveOn(err);
                        }
                        results = results.concat(response.Contents);
                        weAreNotDone = response.isTruncated;
                        marker = response.NextMarker;
                        moveOn(null, results);
                    });
            },
            function test() { return weAreNotDone; },
            function allDone(err) {
                return cb(err, results);
            });
    },

    getSomeMPUparts(params, cb) {
        const { uploadId, mpuBucketName, maxParts, partNumberMarker, log} =
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
        async.each(keysToDelete, function action(key, callback) {
            metadata.deleteObjectMD(mpuBucketName, key, log, callback);
        },
        function finalCallback(err) {
            return cb(err);
        });
    },

    checkBucketPolicies(params, cb) {
        // TODO: Check bucket policies to see if user is granted
        // permission or forbidden permission to take given action.
        // If denied, return cb('AccessDenied')
        // If permitted, return cb(null, 'accessGranted')
        // This is GH Issue#76
        return cb(null, 'accessGranted');
    }
};
