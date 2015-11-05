import async from 'async';
import Bucket from './bucket_mem';
import { accountsKeyedbyEmail, accountsKeyedbyCanID }
from './testdata/vault.json';
import utils from './utils';

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

        // For now, user is authorized if they are bucket owner.
        // TODO: In future implementation
        // will parse xml permissions more particularly.
        if (bucket.owner !== params.accessKey) {
            return cb("AccessDenied");
        }
        if (objectKey === undefined) {
            // NEED to pass on three arguments here for the objectPut async
            // waterfall to work
            return cb(null, bucket, null);
        }
        // TODO: If objectKey is defined, should check whether user has
        // applicable authorization for object.
        // If not, return AccessDenied error.

        bucket.getObjectMD(objectKey, (objectMetadataNotFound, objectMD) => {
            if (objectMetadataNotFound) {
                // NEED to pass on three arguments here for the objectPut
                // async waterfall to work
                return cb(null, bucket, null);
            }
            return cb(null, bucket, objectMD);
        });
    },

    /**
     * Stores object and responds back with location and storage type
     * @param {object} bucket - bucket in which metadata is stored
     * @param {object} objectMetadata - object's metadata
     * @param {object} ds - datastore
     * @param {object} params - custom built object containing resource name,
     * resource body, type, access key etc.
     * @param {function} cb - callback containing result for the next task
     * @return {function} executes cb with either
     * error or bucket as arguments
     */
    dataStore(bucket, objectMetadata, ds, params, cb) {
        if (params.headers['content-md5']) {
            if (params.headers['content-md5'] !== params.contentMD5) {
                cb('InvalidDigest');
            }
        }
        const key = params.objectUID;
        this.putDataStore(ds, key, params.value, (err, newLocation) => {
            if (err) {
                cb(err);
            }
            if (newLocation) {
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
     * @param {object} metastore - metadata store
     * @param {object} params - custom built object containing resource details.
     * @param {function} cb - callback containing result for the next task
     * @return {function} executes callback with err or etag as arguments
     */
    metadataStoreObject(
            bucket, objectMetadata, newLocation, metastore, params, cb) {
        let omVal;
        if (objectMetadata) {
            // TODO: Deal with versioning
            // including saving the then current location
            // For now delete the delete marker
            delete objectMetadata['x-amz-delete-marker'];

            omVal = objectMetadata;
        } else {
            omVal = {};
            omVal.Date = new Date();
            omVal.objectUID = params.objectUID;

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
        omVal.location = newLocation;
        // simple/no version. will expand once object versioning is introduced
        omVal['x-amz-version-id'] = null;

        // Store user provided metadata.  TODO: limit size.
        Object.keys(params.metaHeaders).forEach((val) => {
            omVal[val] = params.metaHeaders[val];
        });
        bucket.putObjectMD(params.objectKey, omVal, (err) => {
            if (err) {
                return cb(err);
            }
            return cb(null, params.contentMD5);
        });
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

                if (headers['x-amz-acl']) {
                    const newCannedACL = headers['x-amz-acl'];
                    const possibleCannedACL =
                        ['private', 'public-read', 'public-read-write',
                        'authenticated-read', 'log-delivery-write'];
                    if (possibleCannedACL.indexOf(newCannedACL) > -1) {
                        bucket.acl.Canned = newCannedACL;
                        return next(null, bucket);
                    }
                    return next('InvalidArgument');
                }
                // TODO: Refactor so less code duplication here
                // and in bucketPutACL.  Consider parsing these headers
                // in utils.normalizeRequest
                const grantReadHeader =
                    utils.parseGrant(headers[
                        'x-amz-grant-read'], 'READ');
                const grantWriteHeader =
                    utils.parseGrant(headers
                        ['x-amz-grant-write'], 'WRITE');
                const grantReadACPHeader =
                    utils.parseGrant(headers
                        ['x-amz-grant-read-acp'], 'READ_ACP');
                const grantWriteACPHeader =
                    utils.parseGrant(headers
                        ['x-amz-grant-write-acp'], 'WRITE_ACP');
                const grantFullControlHeader =
                    utils.parseGrant(headers
                        ['x-amz-grant-full-control'], 'FULL_CONTROL');

                const allGrantHeaders =
                    [].concat(grantReadHeader, grantWriteHeader,
                    grantReadACPHeader, grantWriteACPHeader,
                    grantFullControlHeader);

                if (allGrantHeaders.length === 0) {
                    return next(null, bucket);
                }

                const usersIdentifiedByEmail = allGrantHeaders
                    .filter(item => item && item
                            .userIDType.toLowerCase() === 'emailaddress');
                const justEmails = usersIdentifiedByEmail
                    .map(item => item.identifier);
                const usersIdentifiedByGroup = allGrantHeaders
                    .filter(item => item
                            && item.userIDType.toLowerCase() === 'uri');
                const possibleGroups = [
                    'http://acs.amazonaws.com/groups/global/AuthenticatedUsers',
                    'http://acs.amazonaws.com/groups/global/AllUsers',
                    'http://acs.amazonaws.com/groups/s3/LogDelivery',
                ];
                for (let i = 0; i < usersIdentifiedByGroup.length; i++) {
                    if (possibleGroups.indexOf(
                            usersIdentifiedByGroup[i].identifier) < 0) {
                        return next('InvalidArgument');
                    }
                }
                const usersIdentifiedByID = allGrantHeaders
                    .filter(item => item
                            && item.userIDType.toLowerCase() === 'id');
                // TODO: Consider whether want to verify with Vault
                // whether canonicalID is associated with existing
                // account before adding to ACL
                const addACLParams = {
                    'Canned': '',
                    'FULL_CONTROL': [],
                    'WRITE': [],
                    'WRITE_ACP': [],
                    'READ': [],
                    'READ_ACP': [],
                };

                // If have to lookup canonicalID's do that asynchronously
                // then add grants to bucket
                if (justEmails.length > 0) {
                    this.getManyCanonicalIDs(justEmails,
                        function rebuildGrants(err, results) {
                            if (err) {
                                return next(err);
                            }
                            const reconstructedUsersIdentifiedByEmail = utils.
                                reconstructUsersIdentifiedByEmail(results,
                                    usersIdentifiedByEmail);
                            const allUsers = [].concat(
                                reconstructedUsersIdentifiedByEmail,
                                usersIdentifiedByGroup,
                                usersIdentifiedByID);
                            const revisedAddACLParams = utils
                                .sortHeaderGrants(allUsers, addACLParams);
                            bucket.acl = revisedAddACLParams;
                            return next(null, bucket);
                        });
                }

                // If don't have to look up canonicalID's just sort grants
                // and add to bucket
                if (justEmails.length === 0) {
                    const revisedAddACLParams = utils
                        .sortHeaderGrants(allGrantHeaders, addACLParams);
                    bucket.acl = revisedAddACLParams;
                    return next(null, bucket);
                }
                return next(null, bucket);
            }.bind(this),
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
        const location = objectMetadata.location;
        const result = ds[location];
        if (!result) {
            return cb("InternalError");
        }
        cb(null, result, responseMetaHeaders);
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
        responseMetaHeaders['Content-Type'] = objectMetadata['content-type'];
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
                lowercasedEmail = emails[i];
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
    }
};
