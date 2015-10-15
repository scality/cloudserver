"use strict";

const async = require('async');
const bucketLists = require("./testdata/vault.json").bucketLists;
const Bucket = require('./bucket_mem.js');

let services = {};



/**
 * Called by services.dataStore to actually put the key and value in the datastore
 * @param {object} ds - in memory datastore
 * @param {string} key - object key
 * @param {string} value - object value (request.post)
 * @param {function} callback - callback to services.dataStore
 */

services.putDataStore = function (ds, key, value, callback) {
    // In memory implementation
    ds[key] = value;
    let location = key;
    callback(null, location);
};

services.getService = function (accessKey, metastore, request, cb) {
    let userBuckets = metastore.users[accessKey].buckets;
    if (userBuckets === undefined) {
        return cb('InternalError');
    }

    return cb(null, userBuckets);
};

/**
 * Checks whether resource exists and the user is authorized
 * @param {object} params - custom built object containing resource name, type, access key etc.
 * @param {function} cb - callback containing error, bucket and object references for the next task
 */
services.metadataValidateAuthorization = function (params, cb) {
    let store = params.metastore;
    let bucketUID = params.bucketUID;
    let objectKey = params.objectKey;

    if (store.buckets[bucketUID] === undefined) {
        return cb("NoSuchBucket");
    }

    let bucket = store.buckets[bucketUID];

    // For now, user is authorized if they are bucket owner.  In future implementation
    // will parse xml permissions more particularly.
    if (bucket.owner !== params.accessKey) {
        return cb("AccessDenied");
    }

    if (objectKey === undefined) {
        // NEED to pass on three arguments here for the objectPut async waterfall to work
        return cb(null, bucket, null);
    }

    // TODO: If objectKey is defined, should check whether user has applicable authorization for object.
    // If not, return AccessDenied error.

    bucket.GETObject(objectKey, function (objectMetadataNotFound, objectMetadata, objectUID) {
        if (objectMetadataNotFound) {
            // NEED to pass on three arguments here for the objectPut async waterfall to work
            return cb(null, bucket, null);
        }
        return cb(null, bucket, objectMetadata);
    });
};



/**
 * Stores object and responds back with location and storage type
 * @param {object} params - custom built object containing resource name, resource body, type, access key etc.
 * @param {function} cb - callback containing result for the next task
 */
services.dataStore = function (bucket, objectMetadata, ds, params, cb) {
    if (params.headers['content-md5']) {
        if (params.headers['content-md5'] !== params.contentMD5) {
            cb('InvalidDigest');
        }
    }


    let key = params.objectUID;
    this.putDataStore(ds, key, params.value, function (err, newLocation) {
        if (err) {
            cb(err);
        }
        if (newLocation) {
            return cb(null, bucket, objectMetadata, newLocation);
        }
        return cb(null, bucket);
    });

};

/**
 * Stores object location, custom headers, version etc.
 * @param {object} params - custom built object containing resource details.
 * @param {function} cb - callback containing result for the next task
 */

services.metadataStoreObject = function (bucket, objectMetadata, newLocation, metastore, params, cb) {
    let omVal;
    if (objectMetadata) {
        // Save then current location to have prior location?
        omVal = objectMetadata;
    } else {
        omVal = {};
        omVal['Date'] = new Date();
        omVal['objectUID'] = params.objectUID;

        // AWS docs state that the user that creates a resource is the owner.
        // Assumed here that original creator remains owner even if later Puts to object.
        // If an IAM user uploads a resource, the owner should be the parent.
        // TODO: Need to update this to handle IAM users.
        // http://docs.aws.amazon.com/AmazonS3/latest/dev/access-control-overview.html
        // TODO: display name should be different from accessKey.
        omVal['owner-display-name'] = params.accessKey;
        // TODO: This should be object creator's canonical ID.
        omVal['owner-id'] = 'canonicalIDtoCome';
    }

    omVal['content-length'] = params.headers['content-length'];
    omVal['content-type'] = params.headers['content-type'];
    // confirm date format
    omVal['last-modified'] = new Date();
    omVal['content-md5'] = params.contentMD5;
    // Need to complete values
    omVal['x-amz-server-side-encryption'] = "";
    omVal['x-amz-server-version-id'] = "";
    // Hard-coded storage class as Standard.  Could have config option.
    omVal['x-amz-storage-class'] = "Standard";
    omVal['x-amz-website-redirect-location'] = "";
    omVal['x-amz-server-side-encryption-aws-kms-key-id'] = "";
    omVal['x-amz-server-side-encryption-customer-algorithm'] = "";
    omVal['location'] = newLocation;
    // simple/no version. will expand once object versioning is introduced
    omVal['x-amz-version-id'] = null;

    // Store user provided metadata.  TODO: limit size.
    for (let k in params['metaHeaders']) {
        omVal[k] = params['metaHeaders'][k];
    }

    bucket.PUTObject(params.objectKey, omVal, function (err) {
        if (err) {
            return cb(err);
        }
        return cb(null, params.contentMD5);
    });

};


/**
 * Checks whether user is authorized to create a bucket and whether the bucket already exists.
 * If user is authorized and bucket does not already exist, bucket is created and saved in metastore
 * along with metadata provided with request.
 * @param {string} accessKey - user's access key
 * @param {string} bucketUID - unique identifier for bucket
 * @param {object} headers - request headers
 * @param {string} locationConstraint - locationConstraint provided in request body xml (if provided)
 * @param {object} metastore - global metastore
 * @param {function} callback - callback to bucketPut
 */

services.createBucket = function (accessKey, bucketname, bucketUID, headers, locationConstraint, metastore, callback) {

    async.waterfall([
        function (next) {
                // TODO Check user policies to see if user is authorized to create a bucket
                if (metastore.buckets[bucketUID] !== undefined) {
                // TODO Check whether user already owns the bucket, if so return "BucketAlreadyOwnedByYou"
                // If not owned by user, return "BucketAlreadyExists"
                    next("BucketAlreadyExists");
                }
                next();
            },
        function (next) {
                let bucket = new Bucket();
                bucket.owner = accessKey;
                bucket.name = bucketname;

                if (locationConstraint !== undefined) {
                    bucket.locationConstraint = locationConstraint;
                }

                if (headers['x-amz-acl']) {
                    bucket.acl = headers['x-amz-acl'];
                }

                if (headers['x-amz-grant-read']) {
                    bucket.acl['x-amz-grant-read'] = headers['x-amz-grant-read'];
                }

                if (headers['x-amz-grant-write']) {
                    bucket.acl['x-amz-grant-write'] = headers['x-amz-grant-write'];
                }

                if (headers['x-amz-grant-read-acp']) {
                    bucket.acl['x-amz-grant-read-acp'] = headers['x-amz-grant-read-acp'];
                }

                if (headers['x-amz-grant-write-acp']) {
                    bucket.acl['x-amz-grant-write-acp'] = headers['x-amz-grant-write-acp'];
                }

                if (headers['x-amz-grant-full-control']) {
                    bucket.acl['x-amz-grant-full-control'] = headers['x-amz-grant-full-control'];
                }

                metastore.buckets[bucketUID] = bucket;
                metastore.users[accessKey].buckets.push({
                    name: bucket.name,
                    creationDate: bucket.creationDate,
                    uid: bucketUID
                });
                next(null, "Bucket created");
            }
    ], function (err, result) {
        callback(err, result);
    });
};

/**
 * Gets object from datastore
 * @param {object} bucket - bucket in which objectMetadata is stored
 * @param {object} objectMetadata - object's metadata
 * @param {object} responseMetaHeaders - contains user meta headers to be passed to response
 * @param {object} ds - datastore
 * @param {function} cb - callback from async.waterfall in objectGet
 */

services.getFromDatastore = function (bucket, objectMetadata, responseMetaHeaders, ds, cb) {
        // TODO: Handle range requests
        let location = objectMetadata['location'];
        let result = ds[location];
        if (!result) {
            return cb("InternalError");
        }
        cb(null, result, responseMetaHeaders);
    };

/**
 * Deletes objects from a bucket
 * @param {object} bucket - bucket in which objectMetadata is stored
 * @param {object} objectMetadata - object's metadata
 * @param {object} responseMetaHeaders - contains user meta headers to be passed to response
 * @param {string} object unique identifier
 * @param {function} cb - callback from async.waterfall in objectGet
 */
services.deleteObjectFromBucket = function (bucket, objectMetadata, responseMetaHeaders, datastore, objectUID, cb) {
        if (objectMetadata['x-amz-delete-marker']) {
            responseMetaHeaders['x-amz-delete-marker'] = true;

            bucket.DELETEObject(objectUID, function (err) {
                if (err) {
                    return cb(err, null, responseMetaHeaders);
                }
                return cb(null, 'Object deleted permanently', responseMetaHeaders);
            });
        } else if (objectMetadata['x-amz-version-id'] !== undefined) {
            // goes here if bucket has a version_id or the version_id is null (non-versioned bucket)
            objectMetadata['x-amz-delete-marker'] = true;

            let location = objectMetadata['location'];
            delete datastore[location];
            return cb(null, 'Object deleted permanently', responseMetaHeaders);

            /**/
        } else {
            // if version_id is undefined( not mentioned in the request), then mark object deleted
            // you will get here only if there are versioned buckets and version_id is not mentioned in the request
            objectMetadata['x-amz-delete-marker'] = true;
            return cb(null, 'Object marked as deleted', responseMetaHeaders);
        }
    };

/**
 * Delete bucket from namespace
 * @param {object} bucket - bucket in which objectMetadata is stored
 * @param {object} objectMetadata - object's metadata
 * @param {object} responseMetaHeaders - contains user meta headers to be passed to response
 * @param {string} object unique identifier
 * @param {function} cb - callback from async.waterfall in objectGet
 */
services.deleteBucket = function (bucket, responseMetaHeaders, metastore, bucketUID, accessKey, cb) {
                        bucket.GETBucketListObjects(null, null, null, null, function (err, objectsListRes) {
                            if (objectsListRes.Contents.length > 0) {
                                return cb('BucketNotEmpty', null, responseMetaHeaders);
                            }

                            delete metastore.buckets[bucketUID];
                            let userBuckets = metastore.users[accessKey].buckets;
                            for (let i = 0, ii = userBuckets.length; i < ii; i++) {
                                if (userBuckets[i].uid === bucketUID) {
                                    userBuckets.splice(i, 1);
                                    break;
                                }
                            }
                            bucket.DELETEBucket(function (err) {
                                if (err) {
                                    return cb(err, null, responseMetaHeaders);
                                }
                                return cb(null, 'Bucket deleted permanently', responseMetaHeaders);
                            });
                        });
                    };


services.bucketMetadataChecks = function (bucket, metadataCheckParams, cb) {
    // todo check request headers and add appropriate response headers
    return cb(null, bucket, metadataCheckParams);
};
/**
 * Checks whether request headers included 'if-modified-since', 'if-unmodified-since', 'if-match' or 'if-none-match'
 * headers.  If so, return appropriate response based on last-modified date of object or Etag.
 * Also pulls user's meta headers from metadata and passes them along to be added to response.
 * @param {object} bucket - bucket in which objectMetadata is stored
 * @param {object} objectMetadata - object's metadata
 * @param {object} metadataCheckParams - contains lowercased headers from request object
 * @param {function} cb - callback from async.waterfall in objectGet
 */

services.metadataChecks = function (bucket, objectMetadata, metadataCheckParams, cb) {

        if (!objectMetadata || objectMetadata['x-amz-delete-marker'] === true) {
            return cb('NoSuchKey');
        }

        let headers = metadataCheckParams.headers;
        let lastModified = objectMetadata['last-modified'].getTime();
        let contentMD5 = objectMetadata['content-md5'];
        let ifModifiedSinceTime = headers['if-modified-since'];
        let ifUnmodifiedSinceTime = headers['if-unmodified-since'];
        let ifEtagMatch = headers['if-match'];
        let ifEtagNoneMatch = headers['if-none-match'];


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

        if (ifEtagMatch) {
            if (ifEtagMatch !== contentMD5) {
                return cb('PreconditionFailed');
            }
        }

        if (ifEtagNoneMatch) {
            if (ifEtagNoneMatch === contentMD5) {
                return cb('NotModified');
            }
        }

        // Add user meta headers from objectMetadata

        let responseMetaHeaders = {};

        for (let k in objectMetadata) {
            if (k.substr(0, 11) === 'x-amz-meta-') {
                responseMetaHeaders[k] = objectMetadata[k];
            }
        }

        // TODO: Add additional response headers -- i.e. x-amz-storage-class and x-amz-server-side-encryption

        responseMetaHeaders['Content-Length'] = objectMetadata['content-length'];
        responseMetaHeaders['Etag'] = objectMetadata['content-md5'];
        responseMetaHeaders['Content-Type'] = objectMetadata['content-type'];

        return cb(null, bucket, objectMetadata, responseMetaHeaders);


    };

/**
 * Gets list of objects in bucket
 * @param {object} bucket - bucket in which objectMetadata is stored
 * @param {object} listingParams - params object passing on needed items from request object
 * @param {function} cb - callback to bucketGet.js
 * @returns {function} callback with either error or JSON response from metastore
 */


services.getObjectListing = function (bucket, listingParams, cb) {
            let delimiter = listingParams.delimiter;
            let marker = listingParams.marker;
            let maxKeys = Number(listingParams.maxKeys);
            let prefix = listingParams.prefix;



            bucket.GETBucketListObjects(prefix, marker, delimiter, maxKeys, function (err, listResponse) {
                if (err) {
                    return cb(err);
                }
                return cb(null, listResponse);
            });


        };

module.exports = services;
