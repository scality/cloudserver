import assert from 'assert';
import { Readable } from 'stream';

import async from 'async';

import Bucket from './metadata/in_memory/Bucket';
import Config from '../lib/Config';
import acl from './metadata/acl';
import data from './data/wrapper';
import metadata from './metadata/wrapper';

const splitter = new Config().splitter;

function _deleteUserBucket(metastore, bucketName, accessKey, cb) {
    const userBuckets = metastore.users[accessKey].buckets;
    for (let i = 0; i < userBuckets.length; i++) {
        if (userBuckets[i].name === bucketName) {
            userBuckets.splice(i, 1);
            break;
        }
    }
    cb();
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
    getService(accessKey, metastore, request, cb) {
        const userBuckets = metastore.users[accessKey].buckets;
        if (userBuckets === undefined) {
            return cb('InternalError');
        }
        return cb(null, userBuckets);
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
        const { accessKey, bucketName, objectKey, requestType } = params;
        assert.strictEqual(typeof bucketName, 'string');
        metadata.getBucket(bucketName, function bucketCheck(err, bucket) {
            if (err) {
                return cb(err);
            }
            if (!isBucketAuthorized(bucket, requestType, accessKey)) {
                return cb('AccessDenied');
            }
            if (objectKey === undefined) {
                // NEED to pass on three arguments here for the objectPut async
                // waterfall to work
                return cb(null, bucket, null);
            }

            metadata.getObjectMD(bucketName, objectKey, (err, objMD) => {
                // A key not already existing is not necessarily an error
                // that should be returned (for instance with a put object,
                // the put should continue)
                if (err && (err === 'NoSuchKey' || err === 'ObjNotFound')) {
                    // NEED to pass on three arguments here for the objectPut
                    // async waterfall to work
                    return cb(null, bucket, null);
                }
                // If there is an error other than the key not existing
                // return the error
                if (err) {
                    return cb(err);
                }
                // TODO: Add bucket policy and IAM checks
                if (!isObjAuthorized(bucket, objMD, requestType, accessKey)) {
                    return cb('AccessDenied');
                }
                return cb(null, bucket, objMD);
            });
        });
    },

    /**
     * Stores object and responds back with location and storage type
     * @param {object} bucket - bucket in which metadata is stored
     * @param {object} objectMetadata - object's metadata (or if multipart
     * upload, then it is the multipart metadata)
     * @param {object} value - the data to be stored
     * @param {function} cb - callback containing result for the next task
     * @return {function} executes cb with either
     * error or bucket as arguments
     */
    dataStore(objectMetadata, value, cb) {
        // Note: In a multipart upload if a user uploads the
        // same part number twice, the second write should
        // overwrite the first write. By using the partUID as the key,
        // this functionality should be accomplished here (i.e., using
        // the same part number will result in the same partUID and then
        // putting in the datastore will overwrite the key with that partUID).
        assert.strictEqual(arguments.length, 3);
        data.put(value, (err, keys) => {
            if (err) {
                cb(err);
            }
            if (keys) {
            // Note if this is the upload of a part, objectMetadata is
            // actually the multipart upload metadata
                return cb(null, objectMetadata, keys);
            }
            return cb(null);
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
            contentType, multipart, headers } = params;
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

        // If this is not the completion of a multipart upload
        // parse the headers to get the ACL's if any
        if (!multipart) {
            const parseAclParams = {
                headers,
                resourceType: 'object',
                acl: omVal.acl
            };
            acl.parseAclFromHeaders(parseAclParams, (err, parsedACL) => {
                if (err) {
                    return cb(err);
                }
                omVal.acl = parsedACL;
                metadata.putObjectMD(bucketName, objectKey, omVal, err => {
                    return cb(err, contentMD5);
                });
            });
        } else {
            metadata.putObjectMD(bucketName, objectKey, omVal, err => {
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
     * @param {function} callback - callback to bucketPut
     * @return {function} calls callback with error or result as arguments
     */
    createBucket(accessKey, bucketName, headers, locationConstraint,
            metastore, callback) {
        assert.strictEqual(typeof bucketName, 'string');
        assert.strictEqual(arguments.length, 6);
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
                };
                acl.parseAclFromHeaders(parseAclParams, (err, parsedACL) => {
                    if (err) {
                        return next(err);
                    }
                    bucket.acl = parsedACL;
                    return next(null, bucket);
                });
            },
            function waterfall3(bucket, next) {
                metadata.createBucket(bucketName, bucket, (err) => {
                    if (err) {
                        return next(err);
                    }
                    metastore.users[accessKey].buckets.push({
                        name: bucketName,
                        creationDate: bucket.creationDate,
                    });
                    next(null, 'Bucket created');
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
    getFromDatastore(objectMetadata, responseMetaHeaders, cb) {
        assert.strictEqual(arguments.length, 3);
        // TODO: Handle range requests
        const locations = objectMetadata.location;
        const readStream = new Readable;

        // Call the data store asynchronously in order to get
        // the chunks from each part of the multipart upload
        data.get(locations, (err, chunks) => {
            if (err) {
                return cb(err);
            }
            chunks.forEach(chunk => {
                if (chunk instanceof Array) {
                    chunk.forEach(c => readStream.push(c));
                } else {
                    readStream.push(chunk);
                }
            });
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
    deleteObject(bucketName, objectMD, responseMDHeaders, objectKey, cb) {
        assert.strictEqual(typeof bucketName, 'string');
        assert.strictEqual(typeof objectMD, 'object');
        if (objectMD['x-amz-version-id'] === 'null') {
            // non-versioned buckets
            data.delete(objectMD.location, err => {
                if (err) {
                    return cb(err);
                }
                metadata.deleteObjectMD(bucketName, objectKey, cb);
            });
        } else {
            // versioning
            cb('NotImplemented');
        }
    },

    /**
     * Delete bucket from namespace
     * @param {object} bucket - bucket in which objectMetadata is stored
     * @param {object} metastore - metadata store
     * @param {string} accessKey - user's access key
     * @param {function} cb - callback from async.waterfall in objectGet
     * @return {function} executes cb with error or success message as arguments
     */
    deleteBucket(bucketName, metastore, accessKey, cb) {
        assert.strictEqual(typeof bucketName, 'string');
        metadata.listObject(bucketName, null, null, null, null,
            function buckDeleter(err, objectsListRes) {
                if (objectsListRes.Contents.length) {
                    return cb('BucketNotEmpty');
                }
                metadata.deleteBucket(bucketName, (err) => {
                    if (err) {
                        return cb(err);
                    }
                    _deleteUserBucket(metastore, bucketName, accessKey, cb);
                });
            });
    },

    /**
     * Checks whether request headers included 'if-modified-since',
     * 'if-unmodified-since', 'if-match' or 'if-none-match'
     * headers.  If so, return appropriate response based
     * on last-modified date of object or ETag.
     * Also pulls user's meta headers from metadata and
     * passes them along to be added to response.
     * @param {object} bucket - bucket in which objectMetadata is stored
     * @param {object} objectMD - object's metadata
     * @param {object} metadataCheckParams - contains lowercased
     * headers from request object
     * @param {function} cb - callback from async.waterfall in objectGet
     * @return {function} executes cb error, bucket, objectMetada
     * and responseMetaHeaders as arguments
     */
    metadataChecks(objectMD, metadataCheckParams, cb) {
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
    getObjectListing(bucketName, listingParams, cb) {
        assert.strictEqual(typeof bucketName, 'string');
        const { delimiter, marker, prefix } = listingParams;
        const maxKeys = Number(listingParams.maxKeys);
        metadata.listObject(bucketName, prefix, marker, delimiter, maxKeys,
            (err, listResponse) => {
                if (err) {
                    return cb(err);
                }
                return cb(null, listResponse);
            });
    },

    metadataStoreMPObject(bucketName, params, cb) {
        assert.strictEqual(typeof bucketName, 'string');
        const initiated = new Date().toJSON();
        // Create a string identifier that contains a series of characters
        // which can be used to split the string later
        // to pull its components -- '...!*!'
        // This string of characters ('...!*!') should not
        // otherwise be found in the
        // components of the string because:
        // 1) ObjectKey can contain any characters so when initiating
        // the MPU, we restricted the ability to create an object containing
        // '...!*!'
        // 2) UploadId's are are UUID version 4
        // so should not contain '...'
        // 3) Bucket names may not contain '...' per AWS guidelines
        // 4) CanonicalId's are long strings of numbers and letters
        // so should not contain '...'
        // 5) Per AWS, names of users must be alphanumeric,
        // including the following common characters: plus (+), equal (=),
        // comma (,), period (.), at (@), underscore (_), and hyphen (-).
        // http://docs.aws.amazon.com/IAM/latest/
        // UserGuide/reference_iam-limits.html
        // So, name should not include any of '!*!'
        // 6) storageClass will either be 'standard', 'standard_ia'
        // or 'reduced_redundancy' so will not contain '...'
        // 7) Initiated will be an ISO date string so will not contain '...'
        const longMPUIdentifier =
            `overview${splitter}${params.objectKey}` +
            `${splitter}${params.uploadId}` +
            `${splitter}${params.eventualStorageBucket}` +
            `${splitter}${params.initiatorID}` +
            `${splitter}${params.initiatorDisplayName}` +
            `${splitter}${params.ownerID}` +
            `${splitter}${params.ownerDisplayName}` +
            `${splitter}${params.storageClass}` +
            `${splitter}${initiated}`;
        const multipartObjectMD = {};
        multipartObjectMD.id = params.uploadId;
        // Note: opting to store the initiator and owner
        // info here (including display names)
        // rather than just saving the canonicalID and
        // calling the display name when get a view request.
        // Since multi-part upload will likely not be open
        // for that long, seems unnecessary
        // to be concerned about a change in the display
        // name while the multi part upload is open.
        multipartObjectMD.owner = {
            'displayName': params.ownerDisplayName,
            'id': params.ownerID,
        };
        multipartObjectMD.initiator = {
            'displayName': params.initiatorDisplayName,
            'id': params.initiatorID,
        };
        multipartObjectMD.key = params.objectKey;
        multipartObjectMD.initiated = initiated;
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
        };
        acl.parseAclFromHeaders(parseAclParams, (err, parsedACL) => {
            if (err) {
                return cb(err);
            }
            multipartObjectMD.acl = parsedACL;
            metadata.putObjectMD(bucketName, longMPUIdentifier,
                multipartObjectMD, (err) => {
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
            objectKey, requestType } = params;

        assert.strictEqual(typeof bucketName, 'string');
        // This checks whether the mpu bucket exists.
        // If the MPU was initiated, the mpu bucket should exist.
        const mpuBucketName = `mpu...${bucketName}`;
        metadata.getBucket(mpuBucketName, (err, mpuBucket) => {
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
                function handleSearchResponse(err, response) {
                    if (err) {
                        return cb(err);
                    }
                    if (response.Contents.length !== 1) {
                        return cb('NoSuchUpload');
                    }

                    // Take the saved overview and convert it into an array
                    // so can pull info from name
                    const mpuOverview =
                        response.Contents[0].key.split(splitter);

                    // Having first comma ignores the overview string at array
                    // position 0
                    // Disable eslint since the linter does
                    // not like pulling these
                    // items and not using them in this file.
                    // But the array is used
                    // in other files.
                    /*eslint-disable */
                    const [ , objectKey,
                            uploadId,
                            eventualStorageBucket,
                            initiatorID,
                            initiatorDisplayName,
                            ownerID,
                            ownerDisplayName,
                            storageClass,
                            initiated ] = mpuOverview;

                    /*eslint-enable */

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
     * @param {string} newLocation - location of part in data store
     * @param {metaStoreParams } metaStoreParams - custom built object
     * @param {function} cb - callback to send error or move to next
     * task
     * @return {function} calls callback with either error or null
     */
    metadataStorePart(mpuBucketName, newLocation, metaStoreParams, cb) {
        assert.strictEqual(typeof mpuBucketName, 'string');
        const { partNumber, contentMD5, size, uploadId } = metaStoreParams;
        const lastModified = new Date().toJSON();
        // Create a string identifier that contains a series of characters
        // which can be used to split the string later
        // to pull its components -- '...!*!'
        // This string of characters ('...!*!') should not
        // otherwise be found in the
        // components of the string because:
        // 1) UploadId's are UUID version 4
        // so should not contain '...'
        // 2) Part Number will be a stringified number between 1 and 10000
        // 3) Last Modified will be an ISO date string so will not contain '...'
        // 4) ContentMD5 will be a string of a hexadecimal number
        // 5) Size will be a string of numbers
        // 6) New Location will be a string of a hexadecimal number created by
        // taking a md5 hash of the namespace, uploadId and part number
        const partKey =
            `${uploadId}${splitter}` +
            `${partNumber}${splitter}` +
            `${lastModified}${splitter}` +
            `${contentMD5}${splitter}` +
            `${size}${splitter}` +
            `${newLocation}`;
        const value = {};
        metadata.putObjectMD(mpuBucketName, partKey, value, (err) => {
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
    getMultipartUploadListing(metastore, MPUbucketName, listingParams, cb) {
        assert.strictEqual(typeof MPUbucketName, 'string');
        metadata.getBucket(MPUbucketName, function getListing(err, bucket) {
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
    getMPUBucket(destinationBucket, metastore, bucketName, cb) {
        assert.strictEqual(typeof bucketName, 'string');
        // Note that AWS does not allow '...' in bucket names.
        // http://docs.aws.amazon.com/AmazonS3/
        // latest/dev/BucketRestrictions.html
        // So, it will not be possible that a user will on its own take
        // any bucket name starting with 'mpu...'
        const MPUBucketName = `mpu...${bucketName}`;
        metadata.getBucket(MPUBucketName, (err, bucket) => {
            if (err === 'NoSuchBucket') {
                const mpuBucket = new Bucket(MPUBucketName,
                                          destinationBucket.owner);
                // Note that unlike during the creation of a normal bucket,
                // we do NOT add this bucket to the lists of a user's buckets.
                // By not adding this bucket to the lists of a user's buckets,
                // a getService request should not return a reference to this
                // bucket.  This is the desired behavior since this should be
                // a hidden bucket.
                return metadata.createBucket(MPUBucketName,
                    mpuBucket, (err) => {
                        if (err) {
                            return cb(err);
                        }
                        return cb(null, mpuBucket);
                    });
            }
            return cb(null, bucket);
        });
    },

    getMPUparts(mpuBucketName, uploadId, cb) {
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
                    searchArgs.maxKeys,
                    function handleSearchResponse(err, response) {
                        if (err) {
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
        const { uploadId, mpuBucketName, maxParts, partNumberMarker} = params;
        // Note: Must add 1 to the partNumberMarker because the marker
        // will be compared against the long objectKeys which have characters
        // following the partNumber.  For instance,
        // the marker uploadId...!*!1 will
        // be compared against key uploadId...!*!1...!*! and the key
        // will appear to follow the marker even though that is not the
        // desired result.
        assert.strictEqual(typeof mpuBucketName, 'string');
        const searchArgs = {
            prefix: uploadId,
            marker: `${uploadId}${splitter}${partNumberMarker + 1}`,
            delimiter: undefined,
            maxKeys: maxParts,
        };
        metadata.listObject(mpuBucketName, searchArgs.prefix, searchArgs.marker,
            searchArgs.delimiter, searchArgs.maxKeys,
            function handleSearchResponse(err, response) {
                cb(err, response);
            });
    },

    batchDeleteObjectMetadata(mpuBucketName, keysToDelete, cb) {
        // If have efficient way to batch delete metadata, should so this
        // all at once in production implementation
        assert.strictEqual(typeof mpuBucketName, 'string');
        async.each(keysToDelete, function action(key, callback) {
            metadata.deleteObjectMD(mpuBucketName, key, callback);
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
