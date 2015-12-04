import async from 'async';
import Bucket from './metadata/in_memory/Bucket';
import { accountsKeyedbyEmail, accountsKeyedbyCanID } from './auth/vault.json';
import utils from './utils';
import { Readable } from 'stream';

export default {
    /**
     * Called by services.dataStore to actually
     * put the key and value in the datastore
     * @param {object} ds - in memory datastore
     * @param {string} key - object key
     * @param {string} value - object value (request.post)
     * @param {function} callback - callback to services.dataStore
     * @return {function} calls callback with error and lcoation as arguments
     */
    putDataStore(ds, key, value, callback) {
        // In memory implementation
        // key === location
        // TODO: Add async call to datastore with possible error response
        ds[key] = value;
        callback(null, key);
    },

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
        const { metastore, bucketUID, objectKey } = params;

        if (metastore.buckets[bucketUID] === undefined) {
            return cb("NoSuchBucket");
        }
        const bucket = metastore.buckets[bucketUID];

        // Check to see if user is authrized to perform
        // particular action on bucket based on ACLs.
        // This assumes accessKey is the canonicalID.
        // TODO: ensure that accessKey being provided here is
        // canonicalID.
        // TODO: Add IAM checks and bucket policy checks.

        let bucketAuthorized = false;

        if (params.requestType === 'bucketGet'
            || params.requestType === 'bucketHead'
            || params.requestType === 'objectGet'
            || params.requestType === 'objectHead') {
            if (bucket.owner === params.accessKey) {
                bucketAuthorized = true;
            } else if (bucket.acl.Canned === 'public-read'
                || bucket.acl.Canned === 'public-read-write'
                || (bucket.acl.Canned === 'authenticated-read'
                    && params.accessKey !==
                        'http://acs.amazonaws.com/groups/global/AllUsers')) {
                bucketAuthorized = true;
            } else if (bucket.acl.FULL_CONTROL.indexOf(params.accessKey) > -1) {
                bucketAuthorized = true;
            } else if (bucket.acl.READ.indexOf(params.accessKey) > -1) {
                bucketAuthorized = true;
            }
        }

        if (params.requestType === 'bucketGetACL') {
            if (bucket.owner === params.accessKey) {
                bucketAuthorized = true;
            } else if (bucket.acl.Canned === 'log-delivery-write'
                && params.accessKey ===
                'http://acs.amazonaws.com/groups/s3/LogDelivery') {
                bucketAuthorized = true;
            } else if (bucket.acl.FULL_CONTROL.indexOf(params.accessKey) > -1) {
                bucketAuthorized = true;
            } else if (bucket.acl.READ_ACP.indexOf(params.accessKey) > -1) {
                bucketAuthorized = true;
            }
        }

        if (params.requestType === 'bucketPutACL') {
            if (bucket.owner === params.accessKey) {
                bucketAuthorized = true;
            } else if (bucket.acl.FULL_CONTROL.indexOf(params.accessKey) > -1) {
                bucketAuthorized = true;
            } else if (bucket.acl.WRITE_ACP.indexOf(params.accessKey) > -1) {
                bucketAuthorized = true;
            }
        }

        if (params.requestType === 'bucketDelete') {
            if (bucket.owner === params.accessKey) {
                bucketAuthorized = true;
            }
        }

        if (params.requestType === 'objectDelete'
            || params.requestType === 'objectPut') {
            if (bucket.owner === params.accessKey) {
                bucketAuthorized = true;
            } else if (bucket.acl.Canned === 'public-read-write') {
                bucketAuthorized = true;
            } else if (bucket.acl.FULL_CONTROL.indexOf(params.accessKey) > -1) {
                bucketAuthorized = true;
            } else if (bucket.acl.WRITE.indexOf(params.accessKey) > -1) {
                bucketAuthorized = true;
            }
        }

        if (params.requestType === 'objectPutACL'
            || params.requestType === 'objectGetACL') {
            bucketAuthorized = true;
        }

        if (!bucketAuthorized) {
            return cb('AccessDenied');
        }
        if (objectKey === undefined) {
            // NEED to pass on three arguments here for the objectPut async
            // waterfall to work
            return cb(null, bucket, null);
        }

        bucket.getObjectMD(objectKey,
            function returnObjectMD(objectMetadataNotFound, objectMD) {
                if (objectMetadataNotFound) {
                    // NEED to pass on three arguments here for the objectPut
                    // async waterfall to work
                    return cb(null, bucket, null);
                }
                // TODO: Add bucket policy and IAM checks
                let objectAuthorized = false;
                if (params.requestType === 'objectGet'
                    || params.requestType === 'objectHead') {
                    if (objectMD['owner-id'] === params.accessKey) {
                        objectAuthorized = true;
                    } else if (objectMD.acl.Canned === 'public-read'
                        || objectMD.acl.Canned === 'public-read-write'
                        || (objectMD.acl.Canned === 'authenticated-read'
                            && params.accessKey !== 'http://acs.amazonaws.com' +
                            '/groups/global/AllUsers')) {
                        objectAuthorized = true;
                    } else if (objectMD.acl.Canned === 'bucket-owner-read'
                        && bucket.owner === params.accessKey) {
                        objectAuthorized = true;
                    } else if (objectMD.acl.Canned
                        === 'bucket-owner-full-control'
                        && bucket.owner === params.accessKey) {
                        objectAuthorized = true;
                    } else if (objectMD.acl.FULL_CONTROL
                            .indexOf(params.accessKey) > -1) {
                        objectAuthorized = true;
                    } else if (objectMD.acl.READ
                            .indexOf(params.accessKey) > -1) {
                        objectAuthorized = true;
                    }
                }

                if (params.requestType === 'objectPut'
                    || params.requestType === 'objectDelete') {
                    if (objectMD['owner-id'] === params.accessKey) {
                        objectAuthorized = true;
                    } else if (objectMD.acl.Canned === 'public-read-write') {
                        objectAuthorized = true;
                    } else if (objectMD.acl.Canned
                            === 'bucket-owner-full-control'
                            && bucket.owner === params.accessKey) {
                        objectAuthorized = true;
                    } else if (objectMD.acl.FULL_CONTROL
                            .indexOf(params.accessKey) > -1) {
                        objectAuthorized = true;
                    } else if (objectMD.acl.WRITE
                            .indexOf(params.accessKey) > -1) {
                        objectAuthorized = true;
                    }
                }

                if (params.requestType === 'objectPutACL') {
                    if (objectMD['owner-id'] === params.accessKey) {
                        objectAuthorized = true;
                    } else if (objectMD.acl.Canned
                        === 'bucket-owner-full-control'
                        && bucket.owner === params.accessKey) {
                        objectAuthorized = true;
                    } else if (objectMD.acl.FULL_CONTROL
                            .indexOf(params.accessKey) > -1) {
                        objectAuthorized = true;
                    } else if (objectMD.acl.WRITE_ACP
                            .indexOf(params.accessKey) > -1) {
                        objectAuthorized = true;
                    }
                }

                if (params.requestType === 'objectGetACL') {
                    if (objectMD['owner-id'] === params.accessKey) {
                        objectAuthorized = true;
                    } else if (objectMD.acl.Canned
                            === 'bucket-owner-full-control') {
                        objectAuthorized = true;
                    } else if (objectMD.acl.FULL_CONTROL
                            .indexOf(params.accessKey) > -1) {
                        objectAuthorized = true;
                    } else if (objectMD.acl.READ_ACP
                            .indexOf(params.accessKey) > -1) {
                        objectAuthorized = true;
                    }
                }

                if (!objectAuthorized) {
                    return cb('AccessDenied');
                }
                return cb(null, bucket, objectMD);
            });
    },

    /**
     * Stores object and responds back with location and storage type
     * @param {object} bucket - bucket in which metadata is stored
     * @param {object} objectMetadata - object's metadata (or if multipart
     * upload, then it is the multipart metadata)
     * @param {object} ds - datastore
     * @param {object} params - custom built object containing resource name,
     * resource body, type, access key etc.
     * @param {function} cb - callback containing result for the next task
     * @return {function} executes cb with either
     * error or bucket as arguments
     */
    dataStore(bucket, objectMetadata, ds, params, cb) {
        const key = params.partUID || params.objectUID;
        // Note: In a multipart upload if a user uploads the
        // same part number twice, the second write should
        // overwrite the first write. By using the partUID as the key,
        // this functionality should be accomplished here (i.e., using
        // the same part number will result in the same partUID and then
        // putting in the datastore will overwrite the key with that partUID).
        this.putDataStore(ds, key, params.value, (err, newLocation) => {
            if (err) {
                cb(err);
            }
            if (newLocation) {
            // Note if this is the upload of a part, objectMetadata is
            // actually the mutlipart upload metadata
                return cb(null, bucket, objectMetadata, newLocation);
            }
            return cb(null, bucket);
        });
    },

    /**
     * Stores object location, custom headers, version etc.
     * @param {object} bucket - bucket in which metadata is stored
     * @param {object} objectMetadata - object's metadata
     * @param {string} newLocation - object's location in datastore
     * @param {object} params - custom built object containing resource details.
     * @param {function} cb - callback containing result for the next task
     * @return {function} executes callback with err or etag as arguments
     */
    metadataStoreObject(
            bucket, objectMetadata, dataLocations, params, cb) {
        const { objectUID, objectKey, accessKey, size,
            contentMD5, metaHeaders, contentType,
            multipart, uploadId, headers } = params;
        // If a non multipart upload object is uploaded, change the
        // format of the location reference to an array with one item
        const dataArray =
            !Array.isArray(dataLocations) ? [dataLocations] : dataLocations;
        let omVal;
        if (objectMetadata) {
            // TODO: Deal with versioning
            // including saving the then current location
            // For now delete the delete marker
            delete objectMetadata['x-amz-delete-marker'];

            omVal = objectMetadata;
        } else {
            omVal = {};
            omVal.Date = new Date().toISOString();
            omVal.objectUID = objectUID;

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
        omVal['last-modified'] = new Date();
        omVal['content-md5'] = contentMD5;
        // Need to complete values
        omVal['x-amz-server-side-encryption'] = "";
        omVal['x-amz-server-version-id'] = "";
        // Hard-coded storage class as Standard.  Could have config option.
        omVal['x-amz-storage-class'] = "Standard";
        omVal['x-amz-website-redirect-location'] = "";
        omVal['x-amz-server-side-encryption-aws-kms-key-id'] = "";
        omVal['x-amz-server-side-encryption-customer-algorithm'] = "";
        omVal.location = dataArray;
        // simple/no version. will expand once object versioning is introduced
        omVal['x-amz-version-id'] = null;
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
            this.parseAclFromHeaders(parseAclParams, (err, parsedACL) => {
                if (err) {
                    return cb(err);
                }
                omVal.acl = parsedACL;
                bucket.putObjectMD(objectKey, omVal, (err) => {
                    if (err) {
                        return cb(err);
                    }
                    return cb(null, contentMD5);
                });
            });
        } else {
            bucket.putObjectMD(objectKey, omVal, (err) => {
                if (err) {
                    return cb(err);
                }
                 // Note: We do not wait to get any confirmation of
                 // deletion of the multipart upload metadata before
                 // moving on
                 // TODO: Consider whether metadata should do both actions
                 // (perform the new metadata write and delete the old multipart
                 // metadata) at one time so do not end up with irrelevant
                 // multipart metadata if the delete fails.
                if (multipart) {
                    this.deleteMultipartUploadEntry(bucket, uploadId);
                }
                return cb(null, contentMD5);
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
     * @param {string} bucketname - name of bucket
     * @param {string} bucketUID - unique identifier for bucket
     * @param {object} headers - request headers
     * @param {string} locationConstraint - locationConstraint
     * provided in request body xml (if provided)
     * @param {object} metastore - global metastore
     * @param {function} callback - callback to bucketPut
     * @return {function} calls callback with error or result as arguments
     */
    createBucket(accessKey, bucketname, bucketUID, headers, locationConstraint,
            metastore, callback) {
        const that = this;
        async.waterfall([
            function waterfall1(next) {
            // TODO Check user policies to see if user is authorized
            // to create a bucket
                if (metastore.buckets[bucketUID] !== undefined) {
                // TODO Check whether user already owns the bucket,
                // if so return "BucketAlreadyOwnedByYou"
                // If not owned by user, return "BucketAlreadyExists"
                    return next("BucketAlreadyExists");
                }
                next();
            },
            function waterfall2(next) {
                const bucket = new Bucket();
                bucket.owner = accessKey;
                bucket.name = bucketname;

                if (locationConstraint !== undefined) {
                    bucket.locationConstraint = locationConstraint;
                }
                const parseAclParams = {
                    headers,
                    resourceType: 'bucket',
                    acl: bucket.acl
                };
                that.parseAclFromHeaders(parseAclParams, (err, parsedACL) => {
                    if (err) {
                        return next(err);
                    }
                    bucket.acl = parsedACL;
                    return next(null, bucket);
                });
            },
            function waterfall3(bucket, next) {
                metastore.buckets[bucketUID] = bucket;
                metastore.users[accessKey].buckets.push({
                    name: bucket.name,
                    creationDate: bucket.creationDate,
                    uid: bucketUID
                });
                next(null, 'Bucket created');
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
     * @param {object} ds - datastore
     * @param {function} cb - callback from async.waterfall in objectGet
     * @return {function} executes cb with error, result and responseMetaHeaders
     * as arguments
     */
    getFromDatastore(bucket, objectMetadata, responseMetaHeaders, ds, cb) {
        // TODO: Handle range requests
        const locations = objectMetadata.location;
        const readStream = new Readable;

        // Call the data store asynchronously in order to get
        // the chunks from each part of the multipart upload
        async.eachSeries(locations, function iterator(location, callback) {
            const result = ds[location];
            if (!result) {
                return cb('InternalError');
            }
            if (Array.isArray(result)) {
                // Add each chunk to the readable stream
                result.forEach((chunk) => {
                    readStream.push(chunk);
                });
            } else {
                readStream.push(result);
            }
            callback();
        }, function (err) {
            if (err) {
                return cb('InternalError');
            }
            // signal end of stream
            readStream.push(null);
            return cb(null, readStream, responseMetaHeaders);
        });
    },

    /**
     * Deletes objects from a bucket
     * @param {object} bucket - bucket in which objectMetadata is stored
     * @param {object} objectMetadata - object's metadata
     * @param {object} responseMetaHeaders - contains user meta headers
     * to be passed to response
     * @param {object} datastore - object data store
     * @param {string} objectUID - object unique identifier
     * @param {function} cb - callback from async.waterfall in objectGet
     * @return {function} executes cb with error,
     * result message and responseMetaHeaders as arguments
     */
    deleteObjectFromBucket(
        bucket, objectMetadata, responseMetaHeaders, datastore, objectUID, cb) {
        if (objectMetadata['x-amz-delete-marker']) {
            responseMetaHeaders['x-amz-delete-marker'] = true;

            bucket.deleteObjectMD(objectUID, (err) => {
                if (err) {
                    return cb(err, null, responseMetaHeaders);
                }
                return cb(null, 'Object deleted permanently',
                        responseMetaHeaders);
            });
        } else if (objectMetadata['x-amz-version-id'] !== undefined) {
            // goes here if bucket has a version_id or the version_id
            // is null (non-versioned bucket)
            objectMetadata['x-amz-delete-marker'] = true;

            const location = objectMetadata.location;
            delete datastore[location];
            return cb(null, 'Object deleted permanently', responseMetaHeaders);
        }
        // if version_id is undefined( not mentioned in the request),
        // then mark object deleted
        // you will get here only if there are versioned buckets
        // and version_id is not mentioned in the request
        objectMetadata['x-amz-delete-marker'] = true;
        return cb(null, 'Object marked as deleted', responseMetaHeaders);
    },

    /**
     * Delete bucket from namespace
     * @param {object} bucket - bucket in which objectMetadata is stored
     * @param {object} metastore - metadata store
     * @param {string} bucketUID - bucket unique identifier
     * @param {string} accessKey - user's access key
     * @param {function} cb - callback from async.waterfall in objectGet
     * @return {function} executes cb with error or success message as arguments
     */
    deleteBucket(bucket, metastore, bucketUID, accessKey, cb) {
        bucket.getBucketListObjects(null, null, null, null,
                function buckDeleter(err, objectsListRes) {
                    if (objectsListRes.Contents.length) {
                        return cb('BucketNotEmpty');
                    }
                    delete metastore.buckets[bucketUID];
                    const userBuckets = metastore.users[accessKey].buckets;
                    for (let i = 0; i < userBuckets.length; i++) {
                        if (userBuckets[i].uid === bucketUID) {
                            userBuckets.splice(i, 1);
                            break;
                        }
                    }
                    bucket.deleteBucketMD((err) => {
                        if (err) {
                            return cb(err);
                        }
                        return cb(null);
                    });
                });
    },

    /**
     * Checks whether request headers included 'if-modified-since',
     * 'if-unmodified-since', 'if-match' or 'if-none-match'
     * headers.  If so, return appropriate response based
     * on last-modified date of object or Etag.
     * Also pulls user's meta headers from metadata and
     * passes them along to be added to response.
     * @param {object} bucket - bucket in which objectMetadata is stored
     * @param {object} objectMetadata - object's metadata
     * @param {object} metadataCheckParams - contains lowercased
     * headers from request object
     * @param {function} cb - callback from async.waterfall in objectGet
     * @return {function} executes cb error, bucket, objectMetada
     * and responseMetaHeaders as arguments
     */
    metadataChecks(bucket, objectMetadata, metadataCheckParams, cb) {
        if (!objectMetadata
                || objectMetadata['x-amz-delete-marker'] === true) {
            return cb('NoSuchKey');
        }

        const headers = metadataCheckParams.headers;
        const lastModified = objectMetadata['last-modified'].getTime();
        const contentMD5 = objectMetadata['content-md5'];
        let ifModifiedSinceTime = headers['if-modified-since'];
        let ifUnmodifiedSinceTime = headers['if-unmodified-since'];
        const ifEtagMatch = headers['if-match'];
        const ifEtagNoneMatch = headers['if-none-match'];
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
        const responseMetaHeaders = {};
        Object.keys(objectMetadata)
            .filter(val => val.substr(0, 11) === 'x-amz-meta-')
            .forEach(id => { responseMetaHeaders[id] = objectMetadata[id]; });

        // TODO: Add additional response headers --
        // i.e. x-amz-storage-class and x-amz-server-side-encryption
        responseMetaHeaders['Content-Length'] =
            objectMetadata['content-length'];
        responseMetaHeaders.Etag = objectMetadata['content-md5'];
        if (objectMetadata['content-type']) {
            responseMetaHeaders['Content-Type'] =
                objectMetadata['content-type'];
        }
        return cb(null, bucket, objectMetadata, responseMetaHeaders);
    },

    /**
     * Gets list of objects in bucket
     * @param {object} bucket - bucket in which objectMetadata is stored
     * @param {object} listingParams - params object passing on
     * needed items from request object
     * @param {function} cb - callback to bucketGet.js
     * @returns {function} callback with either error or
     * JSON response from metastore
     */
    getObjectListing(bucket, listingParams, cb) {
        const { delimiter, marker, prefix } = listingParams;
        const maxKeys = Number(listingParams.maxKeys);
        bucket.getBucketListObjects(prefix, marker, delimiter, maxKeys,
                (err, listResponse) => {
                    if (err) {
                        return cb(err);
                    }
                    return cb(null, listResponse);
                });
    },

    /**
     * Gets canonical ID of account based on email associated with account
     * @param {string} email - account's email address
     * @param {function} cb - callback to bucketPutACL.js
     * @returns {function} callback with either error or
     * canonical ID response from Vault
     */

    getCanonicalID(email, cb) {
        const lowercasedEmail = email.toLowerCase();
        // Placeholder for actual request to Vault/Metadata
        process.nextTick(()=> {
            if (accountsKeyedbyEmail[lowercasedEmail] === undefined) {
                return cb('UnresolvableGrantByEmailAddress');
            }
            // if more than one canonical ID associated
            // with email address, return callback with
            // error 'AmbiguousGrantByEmailAddres'
            // AWS has this error as a possibility.  If we will not have
            // an email address associated with multiple accounts, then
            // error not needed.
            return cb(null, accountsKeyedbyEmail[lowercasedEmail].canonicalID);
        });
    },


    /**
     * Gets canonical ID's for a list of accounts
     * based on email associated with account
     * @param {array} emails - list of email addresses
     * @param {function} cb - callback to calling function
     * @returns {function} callback with either error or
     * canonical ID response from Vault
     */
    getManyCanonicalIDs(emails, cb) {
        process.nextTick(() => {
            let canonicalID;
            let lowercasedEmail;
            const results = [];
            for (let i = 0; i < emails.length; i++) {
                lowercasedEmail = emails[i].toLowerCase();
                if (!accountsKeyedbyEmail[lowercasedEmail]) {
                    return cb('UnresolvableGrantByEmailAddress');
                }
                canonicalID = accountsKeyedbyEmail[lowercasedEmail].canonicalID;
                results.push({
                    email: lowercasedEmail,
                    canonicalID,
                });
            }
            return cb(null, results);
        });
    },

    /**
     * Gets email addresses (referred to as diplay names for getACL's)
     * for a list of accounts
     * based on canonical IDs associated with account
     * @param {array} canonicalIDs - list of canonicalIDs
     * @param {function} cb - callback to calling function
     * @returns {function} callback with either error or
     * array of account objects from Vault containing account canonicalID
     * and email address for each account
     */
    getManyDisplayNames(canonicalIDs, cb) {
        process.nextTick(() => {
            let foundAccount;
            const results = [];
            for (let i = 0; i < canonicalIDs.length; i++) {
                foundAccount = accountsKeyedbyCanID[canonicalIDs[i]];
                // TODO: Determine whether want to return an error message
                // if user no longer found or just skip as done here
                if (!foundAccount) {
                    continue;
                }
                results.push({
                    displayName: foundAccount.email,
                    canonicalID: canonicalIDs[i],
                });
            }
            // TODO: Send back error if no response from Vault
            return cb(null, results);
        });
    },

    addACL(bucket, addACLParams, cb) {
        process.nextTick(() => {
            bucket.acl = addACLParams;
            return cb(null);
        });
    },

    addObjectACL(bucket, objectKey, objectMD, addACLParams, cb) {
        objectMD.acl = addACLParams;
        process.nextTick(() => {
            bucket.putObjectMD(objectKey, objectMD, (err) => {
                if (err) {
                    return cb(err);
                }
                return cb();
            });
        });
    },
    parseAclFromHeaders(params, cb) {
        const headers = params.headers;
        const resourceType = params.resourceType;
        const currentResourceACL = params.acl;
        let resourceACL = {};
        let validCannedACL = [];
        if (resourceType === 'bucket') {
            resourceACL = {
                'Canned': '',
                'FULL_CONTROL': [],
                'WRITE': [],
                'WRITE_ACP': [],
                'READ': [],
                'READ_ACP': [],
            };
            validCannedACL =
                ['private', 'public-read', 'public-read-write',
                'authenticated-read', 'log-delivery-write'];
        } else if (resourceType === 'object') {
            resourceACL = {
                'Canned': '',
                'FULL_CONTROL': [],
                'WRITE_ACP': [],
                'READ': [],
                'READ_ACP': [],
            };
            validCannedACL =
                ['private', 'public-read', 'public-read-write',
                'authenticated-read', 'bucket-owner-read',
                'bucket-owner-full-control'];
        }

        // parse canned acl
        if (headers['x-amz-acl']) {
            const newCannedACL = headers['x-amz-acl'];
            if (validCannedACL.indexOf(newCannedACL) > -1) {
                resourceACL.Canned = newCannedACL;
                return cb(null, resourceACL);
            }
            return cb('InvalidArgument');
        }

        // parse grant headers
        const grantReadHeader =
            utils.parseGrant(headers['x-amz-grant-read'], 'READ');
        let grantWriteHeader = [];
        if (resourceType === 'bucket') {
            grantWriteHeader =
                utils.parseGrant(headers['x-amz-grant-write'], 'WRITE');
        }
        const grantReadACPHeader =
            utils.parseGrant(headers['x-amz-grant-read-acp'], 'READ_ACP');
        const grantWriteACPHeader =
            utils.parseGrant(headers['x-amz-grant-write-acp'], 'WRITE_ACP');
        const grantFullControlHeader =
            utils.parseGrant(
                headers['x-amz-grant-full-control'], 'FULL_CONTROL');
        const allGrantHeaders =
            [].concat(grantReadHeader, grantWriteHeader,
            grantReadACPHeader, grantWriteACPHeader,
            grantFullControlHeader).filter(item => item !== undefined);
        if (allGrantHeaders.length === 0) {
            return cb(null, currentResourceACL);
        }

        const usersIdentifiedByEmail =
            allGrantHeaders.filter((item) => {
                if (item && item.userIDType.toLowerCase()
                    === 'emailaddress') {
                    return true;
                }
            });
        const justEmails = usersIdentifiedByEmail.
            map((item) => item.identifier);
        const usersIdentifiedByGroup =
            allGrantHeaders.filter((item) => {
                if (item && item.userIDType.toLowerCase() === 'uri') {
                    return true;
                }
            });
        const validGroups = [
            'http://acs.amazonaws.com/groups/global/AuthenticatedUsers',
            'http://acs.amazonaws.com/groups/global/AllUsers',
            'http://acs.amazonaws.com/groups/s3/LogDelivery'
        ];
        for (let i = 0; i < usersIdentifiedByGroup.length; i ++) {
            if (validGroups.indexOf(
                    usersIdentifiedByGroup[i].identifier) < 0) {
                return cb('InvalidArgument');
            }
        }
        const usersIdentifiedByID = allGrantHeaders.
            filter(item => (item && item.userIDType.toLowerCase() === 'id'));
        // TODO: Consider whether want to verify with Vault
        // whether canonicalID is associated with existing
        // account before adding to ACL

        // If have to lookup canonicalID's do that asynchronously
        // then add grants to bucket
        if (justEmails.length > 0) {
            this.getManyCanonicalIDs(justEmails, (err, results) => {
                if (err) {
                    return cb(err);
                }
                const reconstructedUsersIdentifiedByEmail = utils.
                    reconstructUsersIdentifiedByEmail(results,
                        usersIdentifiedByEmail);
                const allUsers = [].concat(
                    reconstructedUsersIdentifiedByEmail,
                    usersIdentifiedByGroup,
                    usersIdentifiedByID);
                const revisedACL = utils
                    .sortHeaderGrants(allUsers, resourceACL);
                return cb(null, revisedACL);
            });
        } else {
            // If don't have to look up canonicalID's just sort grants
            // and add to bucket
            const revisedACL = utils.
                sortHeaderGrants(allGrantHeaders, resourceACL);
            return cb(null, revisedACL);
        }
    },
    metadataStoreMPObject(bucket, params, cb) {
        const multipartObjectMD = {};
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
        multipartObjectMD.partLocations = [];
        multipartObjectMD.key = params.objectKey;
        multipartObjectMD.initiated = new Date().toISOString();
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
        // Hard-coded storage class as Standard.  Could have config option.
        multipartObjectMD['x-amz-storage-class'] = 'Standard';
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
        this.parseAclFromHeaders(parseAclParams, (err, parsedACL) => {
            if (err) {
                return cb(err);
            }
            multipartObjectMD.acl = parsedACL;
            bucket.putMPobjectMD(params.uploadId, multipartObjectMD, (err) => {
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
        const { metastore, bucketUID, uploadId,
            accessKey, objectKey, requestType } = params;

        const bucket = metastore.buckets[bucketUID];
        if (bucket === undefined) {
            return cb('NoSuchBucket');
        }

        bucket.getMultipartUploadMD(uploadId,
            function returnMultipartMD(multipartNotFound, multipartUpload) {
                if (multipartNotFound ||
                    multipartUpload.key !== objectKey) {
                    return cb('NoSuchUpload');
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
                    multipartUpload.initiator.id === accessKey ? true : false;
                if (requestType === 'put') {
                    // In order for account to be
                    // authorized must both have putObject
                    // authorization AND be the
                    // initiator of the multipart upload
                    let putAuthorized = false;
                    if (bucket.owner === accessKey) {
                        putAuthorized = true;
                    } else if (bucket.acl.FULL_CONTROL
                        .indexOf(accessKey) > -1) {
                        putAuthorized = true;
                    } else if (bucket.acl.WRITE
                        .indexOf(params.accessKey) > -1) {
                        putAuthorized = true;
                    }
                    // Only the initiator of the multipart
                    // upload can upload a part
                    if (!putAuthorized || !isInitiator) {
                        return cb('AccessDenied');
                    }
                }
                if (requestType === 'delete') {
                    // In order for account/user to be
                    // authorized must either be the
                    // bucket owner or intitator of
                    // the multipart upload request
                    // (or parent account of initiator).
                    // In addition if the bucket policy
                    // designates someone else with
                    // s3:AbortMultipartUpload rights,
                    // that account/user will have the right.
                    // TODO: When implement bucket policies,
                    // provide that anyone with s3:AbortMultipartUpload
                    // rights can also perform this delete action
                    // (and anyone denied the right cannot). GH Issue#76
                    if (bucket.owner !== accessKey && !isInitiator) {
                        return cb('AccessDenied');
                    }
                }
                if (requestType === 'listParts') {
                    // In order for account/user to be
                    // authorized must either be the
                    // bucket owner or intitator of
                    // the multipart upload request
                    // (or parent account of initiator).
                    // In addition if the bucket policy
                    // designates someone else with
                    // s3:ListMultipartUploadParts rights,
                    // that account/user will have the right.
                    // TODO: When implement bucket policies,
                    // provide that anyone with s3:ListMultipartUploadParts
                    // rights can also perform this list action
                    // (and anyone denied the right cannot). GH Issue#76
                    if (bucket.owner !== accessKey && !isInitiator) {
                        return cb('AccessDenied');
                    }
                }
                return cb(null, bucket, multipartUpload);
            });
    },

    /**
     * Stores metadata about a part of a multipart upload
     * @param {object} bucket - bucket metadata
     * @param {object} multipartMetadata - particular metadata for
     * an ongoing multipart upload
     * @param {string} newLocation - location of part in data store
     * @param {metaStoreParams } metaStoreParams - custom built object
     * @param {function} cb - callback to send error or move to next
     * task
     * @return {function} calls callback with either error or null
     */
    metadataStorePart(
        bucket, multipartMetadata, newLocation, metaStoreParams, cb) {
        const { partNumber, contentMD5, size } = metaStoreParams;
        bucket.putPartLocation(partNumber, contentMD5, size,
            newLocation, multipartMetadata, (err) => {
                if (err) {
                    return cb(err);
                }
                return cb(null);
            });
    },

    /**
     * Deletes the multipart metadata for a completed multipart upload
     * or an aborted multipart upload
     * @param {object} bucket - bucket metadata
     * @param {string} uploadId - uploadId for locating multipart metadata
     * @param {function} deleteCallback - callback to
     * call upon completion or error
     */
    deleteMultipartUploadEntry(bucket, uploadId, deleteCallback) {
        bucket.deleteMultipartUploadMD(uploadId, deleteCallback);
    },

    /**
     * Deletes the multipart data for a completed multipart upload
     * @param {array} locations - list of locations where data is stored
     * @param {object} datastore - storage for actual data
     * @param {function} cb - callback that returns error if any
     */
    deleteData(locations, datastore, cb) {
        // Simulates call to data which will accept an array
        // of locations to delete.
        process.nextTick(() => {
            locations.forEach((item) => {
                delete datastore[item];
            });
            return cb(null);
        });
    },
};
