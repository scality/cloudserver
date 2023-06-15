const assert = require('assert');

const async = require('async');
const { errors, s3middleware } = require('arsenal');

const ObjectMD = require('arsenal').models.ObjectMD;
const BucketInfo = require('arsenal').models.BucketInfo;
const ObjectMDArchive = require('arsenal').models.ObjectMDArchive;
const acl = require('./metadata/acl');
const constants = require('../constants');
const { config } = require('./Config');
const { data } = require('./data/wrapper');
const metadata = require('./metadata/wrapper');
const logger = require('./utilities/logger');
const { setObjectLockInformation }
    = require('./api/apiUtils/object/objectLockHelpers');
const removeAWSChunked = require('./api/apiUtils/object/removeAWSChunked');
const { parseTagFromQuery } = s3middleware.tagging;

const usersBucket = constants.usersBucket;
const oldUsersBucket = constants.oldUsersBucket;

const services = {
    getService(authInfo, request, log, splitter, cb, overrideUserbucket) {
        const canonicalID = authInfo.getCanonicalID();
        assert.strictEqual(typeof splitter, 'string');
        const prefix = `${canonicalID}${splitter}`;
        const bucketUsers = overrideUserbucket || usersBucket;
        // Note: we are limiting max keys on a bucket listing to 10000
        // AWS does not limit but they only allow 100 buckets
        // (without special increase)
        // TODO: Consider implementing pagination like object listing
        // with respect to bucket listing so can go beyond 10000
        metadata.listObject(bucketUsers, { prefix, maxKeys: 10000 }, log,
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
                if (err?.is.NoSuchBucket) {
                    log.trace('no buckets found');
                    // If we checked the old user bucket, that means we
                    // already checked the new user bucket. If neither the
                    // old user bucket or the new user bucket exist, no buckets
                    // have yet been created in the namespace so an empty
                    // listing should be returned
                    if (overrideUserbucket) {
                        return cb(null, [], splitter);
                    }
                    // Since there were no results from checking the
                    // new users bucket, we check the old users bucket
                    return this.getService(authInfo, request, log,
                        constants.oldSplitter, cb, oldUsersBucket);
                }
                if (err) {
                    log.error('error from metadata', { error: err });
                    return cb(err);
                }
                return cb(null, listResponse.Contents, splitter);
            });
    },

   /**
    * Check that hashedStream.completedHash matches header contentMd5.
    * @param {object} contentMD5 - content-md5 header
    * @param {string} completedHash - hashed stream once completed
    * @param {RequestLogger} log - the current request logger
    * @return {boolean} - true if contentMD5 matches or is undefined,
    * false otherwise
    */
    checkHashMatchMD5(contentMD5, completedHash, log) {
        if (contentMD5 && completedHash && contentMD5 !== completedHash) {
            log.debug('contentMD5 and completedHash does not match',
            { method: 'checkHashMatchMD5', completedHash, contentMD5 });
            return false;
        }
        return true;
    },

    /**
     * Stores object location, custom headers, version etc.
     * @param {object} bucketName - bucket in which metadata is stored
     * @param {array} dataGetInfo - array of objects with information to
     * retrieve data or null if 0 bytes object
     * @param {object} cipherBundle - server side encryption information
     * @param {object} params - custom built object containing resource details.
     * @param {function} cb - callback containing result for the next task
     * @return {function} executes callback with err or ETag as arguments
     */
    metadataStoreObject(bucketName, dataGetInfo, cipherBundle, params, cb) {
        const { objectKey, authInfo, size, contentMD5, metaHeaders,
            contentType, cacheControl, contentDisposition, contentEncoding,
            expires, multipart, headers, overrideMetadata, log,
            lastModifiedDate, versioning, versionId, uploadId,
            tagging, taggingCopy, replicationInfo, defaultRetention,
            dataStoreName, creationTime, retentionMode, retentionDate,
            legalHold, originOp, updateMicroVersionId, archive, oldReplayId,
            deleteNullKey } = params;
        log.trace('storing object in metadata');
        assert.strictEqual(typeof bucketName, 'string');
        const md = new ObjectMD();
        // This should be object creator's canonical ID.
        md.setOwnerId(authInfo.getCanonicalID())
            .setKey(objectKey)
            .setCacheControl(cacheControl)
            .setContentDisposition(contentDisposition)
            .setContentEncoding(contentEncoding)
            .setExpires(expires)
            .setContentLength(size)
            .setContentType(contentType)
            .setContentMd5(contentMD5)
            .setLocation(dataGetInfo)
            // If an IAM user uploads a resource, the owner should be the parent
            // account. http://docs.aws.amazon.com/AmazonS3/
            // latest/dev/access-control-overview.html
            .setOwnerDisplayName(authInfo.getAccountDisplayName())
            .setDataStoreName(dataStoreName)
            // CreationTime needs to be carried over so that it remains static
            .setCreationTime(creationTime)
            .setOriginOp(originOp);
        // Sending in last modified date in object put copy since need
        // to return the exact date in the response
        if (lastModifiedDate) {
            md.setLastModified(lastModifiedDate);
        }
        if (cipherBundle) {
            md.setAmzServerSideEncryption(cipherBundle.algorithm);
            // configuredMasterKeyId takes precedence
            if (cipherBundle.configuredMasterKeyId || cipherBundle.masterKeyId) {
                md.setAmzEncryptionKeyId(cipherBundle.configuredMasterKeyId || cipherBundle.masterKeyId);
            }
        }
        if (headers && headers['x-amz-website-redirect-location']) {
            md.setRedirectLocation(headers['x-amz-website-redirect-location']);
        }
        if (headers) {
            // Stores retention information if object has its own retention
            // configuration or default retention configuration from its bucket
            const headerMode = headers['x-amz-object-lock-mode'];
            const headerDate = headers['x-amz-object-lock-retain-until-date'];
            const headerLegalHold = headers['x-amz-object-lock-legal-hold'];
            const objectRetention = headers && headerMode && headerDate;
            const objectLegalHold = headers && headerLegalHold;
            if (objectRetention || defaultRetention || objectLegalHold) {
                setObjectLockInformation(headers, md, defaultRetention);
            }
        }
        if (replicationInfo) {
            md.setReplicationInfo(replicationInfo);
        }
        // options to send to metadata to create or overwrite versions
        // when putting the object MD
        const options = {};
        if (versioning) {
            options.versioning = versioning;
        }
        if (versionId || versionId === '') {
            options.versionId = versionId;
        }
        if (uploadId) {
            md.setUploadId(uploadId);
            options.replayId = uploadId;
        }
        // update microVersionId when overwriting metadata.
        if (updateMicroVersionId) {
            md.updateMicroVersionId();
        }
        // update restore
        if (archive) {
            md.setArchive(new ObjectMDArchive(
                archive.archiveInfo,
                archive.restoreRequestedAt,
                archive.restoreRequestedDays,
                archive.restoreCompletedAt,
                archive.restoreWillExpireAt));
            md.setAmzRestore({
                'ongoing-request': false,
                'expiry-date': archive.restoreWillExpireAt,
            });
        }

        if (oldReplayId) {
            options.oldReplayId = oldReplayId;
        }

        if (deleteNullKey) {
            options.deleteNullKey = deleteNullKey;
        }

        // information to store about the version and the null version id
        // in the object metadata

        // NOTE nullVersionId and nullUploadId are only maintained in
        // v0 metadata compatibility mode
        const { isNull, nullVersionId, nullUploadId, isDeleteMarker } = params;
        md.setIsNull(isNull)
            .setNullVersionId(nullVersionId)
            .setNullUploadId(nullUploadId)
            .setIsDeleteMarker(isDeleteMarker);
        if (versionId && versionId !== 'null') {
            md.setVersionId(versionId);
        }
        if (isNull && !config.nullVersionCompatMode) {
            md.setIsNull2(true);
        }
        if (taggingCopy) {
            // If copying tags to an object (taggingCopy) we do not
            // need to validate them again
            md.setTags(taggingCopy);
        } else if (tagging) {
            const validationTagRes = parseTagFromQuery(tagging);
            if (validationTagRes instanceof Error) {
                log.debug('tag validation failed', {
                    error: validationTagRes,
                    method: 'metadataStoreObject',
                });
                return process.nextTick(() => cb(validationTagRes));
            }
            md.setTags(validationTagRes);
        }

        // Store user provided metadata.
        // For multipart upload this also serves to transfer
        // over metadata originally sent with the initiation
        // of the multipart upload (for instance, ACL's).
        // Do NOT move this to before
        // the assignments of metadata above since this loop
        // will reassign some of the above values with the ones
        // from the intiation of the multipart upload
        // (for instance, storage class)
        md.setUserMetadata(metaHeaders);
        if (overrideMetadata) {
            md.overrideMetadataValues(overrideMetadata);
        }
        if (retentionMode && retentionDate) {
            md.setRetentionMode(retentionMode);
            md.setRetentionDate(retentionDate);
        }
        if (legalHold) {
            md.setLegalHold(legalHold);
        }

        log.trace('object metadata', { omVal: md.getValue() });
        // If this is not the completion of a multipart upload or
        // the creation of a delete marker, parse the headers to
        // get the ACL's if any
        return async.waterfall([
            callback => {
                if (multipart || md.getIsDeleteMarker()) {
                    return callback();
                }
                const parseAclParams = {
                    headers,
                    resourceType: 'object',
                    acl: md.getAcl(),
                    log,
                };
                log.trace('parsing acl from headers');
                acl.parseAclFromHeaders(parseAclParams, (err, parsedACL) => {
                    if (err) {
                        log.debug('error parsing acl', { error: err });
                        return callback(err);
                    }
                    md.setAcl(parsedACL);
                    return callback();
                });
                return null;
            },
            callback => metadata.putObjectMD(bucketName, objectKey, md,
                    options, log, callback),
        ], (err, data) => {
            if (err) {
                log.error('error from metadata', { error: err });
                return cb(err);
            }
            log.trace('object successfully stored in metadata');
            // if versioning is enabled, data will be returned from metadata
            // as JSON containing a versionId which some APIs will need sent
            // back to them
            let versionId;
            if (data) {
                if (params.isNull && params.isDeleteMarker) {
                    versionId = 'null';
                } else if (!params.isNull) {
                    versionId = JSON.parse(data).versionId;
                }
            }
            return cb(err, {
                lastModified: md.getLastModified(),
                tags: md.getTags(),
                contentMD5,
                versionId,
            });
        });
    },

    /**
     * Deletes objects from a bucket
     * @param {string} bucketName - bucket in which objectMD is stored
     * @param {object} objectMD - object's metadata
     * @param {string} objectKey - object key name
     * @param {object} options - other instructions, such as { versionId } to
     *                           delete a specific version of the object
     * @param {Log} log - logger instance
     * @param {string} originOp - origin operation
     * @param {function} cb - callback from async.waterfall in objectGet
     * @param {boolean} deferDeletion - true if the object should not be removed
     *                                  from the storage, but be returned
     *                                  instead.
     * @return {undefined}
     */
    deleteObject(bucketName, objectMD, objectKey, options, log, originOp, cb, deferDeletion) {
        log.trace('deleting object from bucket');
        assert.strictEqual(typeof bucketName, 'string');
        assert.strictEqual(typeof objectMD, 'object');

        function deleteMDandData() {
            return metadata.deleteObjectMD(bucketName, objectKey, options, log,
                (err, res) => {
                    if (err) {
                        return cb(err, res);
                    }
                    log.trace('deleteObject: metadata delete OK');
                    const deleteLog =
                        logger.newRequestLoggerFromSerializedUids(
                            log.getSerializedUids());
                    if (objectMD.location === null) {
                        return cb(null, res);
                    }

                    const locations = Array.isArray(objectMD.location)
                        ? objectMD.location : [objectMD.location];

                    if (deferDeletion) {
                        return cb(null, locations);
                    }

                    return data.batchDelete(locations, null, null, deleteLog, err => {
                        if (err) {
                            return cb(err);
                        }
                        return cb(null, res);
                    });
                }, originOp);
        }

        const objGetInfo = objectMD.location;
        // special case that prevents azure blocks from unecessary deletion
        // will return null if no need
        return data.protectAzureBlocks(bucketName, objectKey, objGetInfo,
        log, err => {
            if (err) {
                return cb(err);
            }
            return deleteMDandData();
        });
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
        log.trace('performing metadata get object listing',
            { listingParams });
        metadata.listObject(bucketName, listingParams, log,
            (err, listResponse) => {
                if (err) {
                    log.debug('error from metadata', { error: err });
                    return cb(err);
                }
                return cb(null, listResponse);
            });
    },

    /**
     * Gets list of objects ready to be lifecycled
     * @param {object} bucketName - bucket in which objectMetadata is stored
     * @param {object} listingParams - params object passing on
     * needed items from request object
     * @param {object} log - request logger instance
     * @param {function} cb - callback to bucketGet.js
     * @return {undefined}
     * JSON response from metastore
     */
    getLifecycleListing(bucketName, listingParams, log, cb) {
        assert.strictEqual(typeof bucketName, 'string');
        log.trace('performing metadata get object listing for lifecycle',
            { listingParams });
        metadata.listLifecycleObject(bucketName, listingParams, log,
            (err, listResponse) => {
                if (err) {
                    log.debug('error from metadata', { error: err });
                    return cb(err);
                }
                return cb(null, listResponse);
            });
    },

    metadataStoreMPObject(bucketName, cipherBundle, params, log, cb) {
        assert.strictEqual(typeof bucketName, 'string');
        assert.strictEqual(typeof params.splitter, 'string');
        // TODO: Determine splitter that will not appear in
        // any of these items.  This is GH Issue#218
        // 1) ObjectKey can contain any characters so when initiating
        // the MPU, we restricted the ability to create an object containing
        // the splitter.
        // 2) UploadId's are UUID version 4
        const splitter = params.splitter;
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
            removeAWSChunked(params.headers['content-encoding']);
        multipartObjectMD['content-type'] =
            params.headers['content-type'];
        multipartObjectMD.expires =
            params.headers.expires;
        multipartObjectMD['x-amz-storage-class'] = params.storageClass;
        multipartObjectMD['x-amz-website-redirect-location'] =
            params.headers['x-amz-website-redirect-location'];
        if (cipherBundle) {
            multipartObjectMD['x-amz-server-side-encryption'] =
                cipherBundle.algorithm;
            if (cipherBundle.masterKeyId) {
                multipartObjectMD[
                    'x-amz-server-side-encryption-aws-kms-key-id'] =
                    cipherBundle.masterKeyId;
            }
        }
        multipartObjectMD.controllingLocationConstraint =
            params.controllingLocationConstraint;
        multipartObjectMD.dataStoreName = params.dataStoreName;
        if (params.tagging) {
            const validationTagRes = parseTagFromQuery(params.tagging);
            if (validationTagRes instanceof Error) {
                log.debug('tag validation failed', {
                    error: validationTagRes,
                    method: 'metadataStoreObject',
                });
                process.nextTick(() => cb(validationTagRes));
            }
            multipartObjectMD['x-amz-tagging'] = params.tagging;
        }
        if (params.retentionMode && params.retentionDate) {
            multipartObjectMD.retentionMode = params.retentionMode;
            multipartObjectMD.retentionDate = params.retentionDate;
        }
        if (params.legalHold) {
            multipartObjectMD.legalHold = params.legalHold;
        }
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
        // eslint-disable-next-line consistent-return
        acl.parseAclFromHeaders(parseAclParams, (err, parsedACL) => {
            if (err) {
                return cb(err);
            }
            multipartObjectMD.acl = parsedACL;
            metadata.putObjectMD(bucketName, longMPUIdentifier,
                multipartObjectMD, {}, log, err => {
                    if (err) {
                        log.error('error from metadata', { error: err });
                        return cb(err);
                    }

                    return cb(null, multipartObjectMD);
                });
            return undefined;
        });
    },

    /**
     * Mark the MPU overview key with a flag when starting the
     * CompleteMPU operation, to be checked by "put part" operations
     *
     * @param {object} params - params object
     * @param {string} params.bucketName - name of MPU bucket
     * @param {string} params.objectKey - object key
     * @param {string} params.uploadId - upload ID
     * @param {string} params.splitter - splitter for this overview key
     * @param {object} params.storedMetadata - original metadata of the overview key
     * @param {Logger} log - Logger object
     * @param {function} cb - callback(err)
     * @return {undefined}
     */
    metadataMarkMPObjectForCompletion(params, log, cb) {
        assert.strictEqual(typeof params, 'object');
        assert.strictEqual(typeof params.bucketName, 'string');
        assert.strictEqual(typeof params.objectKey, 'string');
        assert.strictEqual(typeof params.uploadId, 'string');
        assert.strictEqual(typeof params.splitter, 'string');
        assert.strictEqual(typeof params.storedMetadata, 'object');
        const splitter = params.splitter;
        const longMPUIdentifier =
            `overview${splitter}${params.objectKey}${splitter}${params.uploadId}`;
        const multipartObjectMD = Object.assign({}, params.storedMetadata);
        multipartObjectMD.completeInProgress = true;
        metadata.putObjectMD(params.bucketName, longMPUIdentifier, multipartObjectMD,
        {}, log, err => {
            if (err) {
                log.error('error from metadata', { error: err });
                return cb(err);
            }
            return cb();
        });
    },

    /**
     * Returns if a CompleteMPU operation is in progress for this
     * object, by looking at the `completeInProgress` flag stored in
     * the overview key
     *
     * @param {object} params - params object
     * @param {string} params.bucketName - bucket name where object should be stored
     * @param {string} params.objectKey - object key
     * @param {string} params.uploadId - upload ID
     * @param {string} params.splitter - splitter for this overview key
     * @param {object} log - request logger instance
     * @param {function} cb - callback(err, {bool} completeInProgress)
     * @return {undefined}
     */
    isCompleteMPUInProgress(params, log, cb) {
        assert.strictEqual(typeof params, 'object');
        assert.strictEqual(typeof params.bucketName, 'string');
        assert.strictEqual(typeof params.objectKey, 'string');
        assert.strictEqual(typeof params.uploadId, 'string');
        assert.strictEqual(typeof params.splitter, 'string');

        const mpuBucketName = `${constants.mpuBucketPrefix}${params.bucketName}`;
        const splitter = params.splitter;
        const mpuOverviewKey =
            `overview${splitter}${params.objectKey}${splitter}${params.uploadId}`;
        return metadata.getObjectMD(mpuBucketName, mpuOverviewKey, {}, log,
            (err, res) => {
                if (err) {
                    log.error('error getting the overview object from mpu bucket', {
                        error: err,
                        method: 'services.isCompleteMPUInProgress',
                        params,
                    });
                    return cb(err);
                }
                return cb(null, Boolean(res.completeInProgress));
            });
    },

    /**
     * Checks whether bucket exists, multipart upload
     * has been initiated and the user is authorized
     * @param {object} params - custom built object containing
     * bucket name, uploadId, authInfo etc.
     * @param {function} cb - callback containing error and
     * bucket reference for the next task
     * @return {undefined} calls callback with arguments:
     * - error
     * - bucket
     * - the multipart upload metadata
     * - the overview key stored metadata
     */
    metadataValidateMultipart(params, cb) {
        const { bucketName, uploadId, authInfo,
            objectKey, requestType, log } = params;

        assert.strictEqual(typeof bucketName, 'string');
        // This checks whether the mpu bucket exists.
        // If the MPU was initiated, the mpu bucket should exist.
        const mpuBucketName = `${constants.mpuBucketPrefix}${bucketName}`;
        metadata.getBucket(mpuBucketName, log, (err, mpuBucket) => {
            if (err?.is.NoSuchBucket) {
                log.debug('bucket not found in metadata', { error: err,
                    method: 'services.metadataValidateMultipart' });
                return cb(errors.NoSuchUpload);
            }
            if (err) {
                log.error('error from metadata', { error: err,
                    method: 'services.metadataValidateMultipart' });
                return cb(err);
            }

            let splitter = constants.splitter;
            // BACKWARD: Remove to remove the old splitter
            if (mpuBucket.getMdBucketModelVersion() < 2) {
                splitter = constants.oldSplitter;
            }
            const mpuOverviewKey =
                `overview${splitter}${objectKey}${splitter}${uploadId}`;

            metadata.getObjectMD(mpuBucket.getName(), mpuOverviewKey,
                {}, log, (err, storedMetadata) => {
                    if (err) {
                        if (err.is.NoSuchKey) {
                            return cb(errors.NoSuchUpload);
                        }
                        log.error('error from metadata', { error: err });
                        return cb(err);
                    }

                    const initiatorID = storedMetadata.initiator.ID;
                    const ownerID = storedMetadata['owner-id'];
                    const mpuOverview = {
                        key: storedMetadata.key,
                        id: storedMetadata.id,
                        eventualStorageBucket:
                            storedMetadata.eventualStorageBucket,
                        initiatorID,
                        initiatorDisplayName:
                            storedMetadata.initiator.DisplayName,
                        ownerID,
                        ownerDisplayName:
                            storedMetadata['owner-display-name'],
                        storageClass:
                            storedMetadata['x-amz-storage-class'],
                        initiated: storedMetadata.initiated,
                        controllingLocationConstraint:
                            storedMetadata.controllingLocationConstraint,
                    };

                    const tagging = storedMetadata['x-amz-tagging'];
                    if (tagging) {
                        mpuOverview.tagging = tagging;
                    }
                    // If access was provided by the destination bucket's
                    // bucket policies, go ahead.
                    if (requestType === 'bucketPolicyGoAhead') {
                        return cb(null, mpuBucket, mpuOverview, storedMetadata);
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
                    return cb(null, mpuBucket, mpuOverview, storedMetadata);
                });
            return undefined;
        });
    },

    /**
     * Stores metadata about a part of a multipart upload
     * @param {string} mpuBucketName - name of the special mpu bucket
     * @param {object []} partLocations - data retrieval info for part.
     * @param {string} partLocations[].key -
     * key in datastore for part
     * @param {string} partLocations[].dataStoreName - name of dataStore
     * @param {string} [partLocations[].size] - part size
     * @param {string} [partLocations[].sseCryptoScheme] - cryptoScheme
     * @param {string} [partLocations[].sseCipheredDataKey] - cipheredDataKey
     * @param {string} [partLocations[].sseAlgorithm] - encryption algo
     * @param {string} [partLocations[].masterKeyId] - masterKeyId
     * @param {object} metaStoreParams - custom built object
     * @param {object} log - request logger instance
     * @param {function} cb - callback to send error or move to next task
     * @return {undefined}
     */
    metadataStorePart(mpuBucketName, partLocations,
                      metaStoreParams, log, cb) {
        assert.strictEqual(typeof mpuBucketName, 'string');
        const { partNumber, contentMD5, size, uploadId, lastModified, splitter }
            = metaStoreParams;
        const dateModified = typeof lastModified === 'string' ?
            lastModified : new Date().toJSON();
        assert.strictEqual(typeof splitter, 'string');
        const partKey = `${uploadId}${splitter}${partNumber}`;
        const omVal = {
            // Version 3 changes the format of partLocations
            // from an object to an array
            'md-model-version': 3,
            partLocations,
            'key': partKey,
            'last-modified': dateModified,
            'content-md5': contentMD5,
            'content-length': size,
        };
        metadata.putObjectMD(mpuBucketName, partKey, omVal, {}, log, err => {
            if (err) {
                log.error('error from metadata', { error: err });
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
        assert.strictEqual(typeof listingParams.splitter, 'string');

        metadata.getBucket(MPUbucketName, log, (err, bucket) => {
            if (err) {
                log.error('error from metadata', { error: err });
                return cb(err);
            }
            if (bucket === undefined) {
                return cb(null, {
                    IsTruncated: false,
                    NextMarker: undefined,
                    MaxKeys: 0,
                    Uploads: [],
                    CommonPrefixes: [],
                });
            }
            const listParams = {};
            Object.keys(listingParams).forEach(name => {
                listParams[name] = listingParams[name];
            });
            // BACKWARD: Remove to remove the old splitter
            if (bucket.getMdBucketModelVersion() < 2) {
                listParams.splitter = constants.oldSplitter;
            }
            metadata.listMultipartUploads(MPUbucketName, listParams, log,
                                          cb);
            return undefined;
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
            if (err?.is.NoSuchBucket) {
                log.trace('no buckets found');
                const creationDate = new Date().toJSON();
                const mpuBucket = new BucketInfo(MPUBucketName,
                    destinationBucket.getOwner(),
                    destinationBucket.getOwnerDisplayName(), creationDate,
                    BucketInfo.currentModelVersion());
                // Note that unlike during the creation of a normal bucket,
                // we do NOT add this bucket to the lists of a user's buckets.
                // By not adding this bucket to the lists of a user's buckets,
                // a getService request should not return a reference to this
                // bucket.  This is the desired behavior since this should be
                // a hidden bucket.
                return metadata.createBucket(MPUBucketName, mpuBucket, log,
                    err => {
                        if (err) {
                            log.error('error from metadata', { error: err });
                            return cb(err);
                        }
                        return cb(null, mpuBucket);
                    });
            }
            if (err) {
                log.error('error from metadata', {
                    error: err,
                    method: 'services.getMPUBucket',
                });
                return cb(err);
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
        metadata.listObject(mpuBucketName, searchArgs, log, cb);
    },

    getSomeMPUparts(params, cb) {
        const { uploadId, mpuBucketName, maxParts, partNumberMarker, log } =
            params;
        assert.strictEqual(typeof mpuBucketName, 'string');
        assert.strictEqual(typeof params.splitter, 'string');
        const paddedPartNumber = `000000${partNumberMarker}`.substr(-5);
        const searchArgs = {
            prefix: uploadId,
            marker: `${uploadId}${params.splitter}${paddedPartNumber}`,
            delimiter: undefined,
            maxKeys: maxParts,
        };
        metadata.listObject(mpuBucketName, searchArgs, log, cb);
    },

    batchDeleteObjectMetadata(mpuBucketName, keysToDelete, log, cb) {
        // If have efficient way to batch delete metadata, should so this
        // all at once in production implementation
        assert.strictEqual(typeof mpuBucketName, 'string');
        async.eachLimit(keysToDelete, 5, (key, callback) => {
            metadata.deleteObjectMD(mpuBucketName, key, {}, log, callback);
        }, err => cb(err));
    },
};

module.exports = services;
