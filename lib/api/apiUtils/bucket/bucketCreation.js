import async from 'async';
import assert from 'assert';
import { errors } from 'arsenal';

import acl from '../../../metadata/acl';
import BucketInfo from '../../../metadata/BucketInfo';
import constants from '../../../../constants';
import createKeyForUserBucket from './createKeyForUserBucket';
import metadata from '../../../metadata/wrapper';
import kms from '../../../kms/wrapper';

const usersBucket = constants.usersBucket;
const oldUsersBucket = constants.oldUsersBucket;
const userBucketOwner = 'admin';


function addToUsersBucket(canonicalID, bucketName, log, cb) {
    // BACKWARD: Simplify once do not have to deal with old
    // usersbucket name and old splitter

    // Get new format usersBucket to see if it exists
    return metadata.getBucket(usersBucket, log, (err, usersBucketAttrs) => {
        if (err && !err.NoSuchBucket && !err.BucketAlreadyExists) {
            return cb(err);
        }
        const splitter = usersBucketAttrs ?
            constants.splitter : constants.oldSplitter;
        let key = createKeyForUserBucket(canonicalID, splitter, bucketName);
        const omVal = { creationDate: new Date().toJSON() };
        // If the new format usersbucket does not exist, try to put the
        // key in the old usersBucket using the old splitter.
        // Otherwise put the key in the new format usersBucket
        const usersBucketBeingCalled = usersBucketAttrs ?
            usersBucket : oldUsersBucket;
        return metadata.putObjectMD(usersBucketBeingCalled, key,
            omVal, log, err => {
                if (err && err.NoSuchBucket) {
                    // There must be no usersBucket so createBucket
                    // one using the new format
                    log.trace('users bucket does not exist, ' +
                        'creating users bucket');
                    key = `${canonicalID}${constants.splitter}` +
                        `${bucketName}`;
                    const creationDate = new Date().toJSON();
                    const freshBucket = new BucketInfo(usersBucket,
                        userBucketOwner, userBucketOwner, creationDate,
                        BucketInfo.currentModelVersion());
                    return metadata.createBucket(usersBucket,
                        freshBucket, log, err => {
                            // Note: In the event that two
                            // users' requests try to create the
                            // usersBucket at the same time,
                            // this will prevent one of the users
                            // from getting a BucketAlreadyExists
                            // error with respect
                            // to the usersBucket.
                            if (err &&
                                err !==
                                    errors.BucketAlreadyExists) {
                                log.error('error from metadata', {
                                    error: err,
                                });
                                return cb(err);
                            }
                            log.trace('Users bucket created');
                            // Finally put the key in the new format
                            // usersBucket
                            return metadata.putObjectMD(usersBucket,
                                key, omVal, log, cb);
                        });
                }
                return cb(err);
            });
    });
}

function removeTransientOrDeletedLabel(bucket, log, callback) {
    log.trace('removing transient or deleted label from bucket attributes');
    const bucketName = bucket.getName();
    bucket.removeTransientFlag();
    bucket.removeDeletedFlag();
    return metadata.updateBucket(bucketName, bucket, log, callback);
}

function freshStartCreateBucket(bucket, canonicalID, log, callback) {
    const bucketName = bucket.getName();
    metadata.createBucket(bucketName, bucket, log, err => {
        if (err) {
            log.debug('error from metadata', { error: err });
            return callback(err);
        }
        log.trace('created bucket in metadata');
        return addToUsersBucket(canonicalID, bucketName, log, err => {
            if (err) {
                return callback(err);
            }
            return removeTransientOrDeletedLabel(bucket, log, callback);
        });
    });
}

/**
 * Finishes creating a bucket in transient state
 * by putting an object in users bucket representing the created bucket
 * and removing transient attribute of the created bucket
 * @param {object} bucketMD - either the bucket metadata sent in the new request
 * or the existing metadata if no new metadata sent
 * (for example in an objectPut)
 * @param {string} canonicalID - bucket owner's canonicalID
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback with error or null as arguments
 * @return {undefined}
 */
export function cleanUpBucket(bucketMD, canonicalID, log, callback) {
    const bucketName = bucketMD.getName();
    return addToUsersBucket(canonicalID, bucketName, log, err => {
        if (err) {
            return callback(err);
        }
        return removeTransientOrDeletedLabel(bucketMD, log, callback);
    });
}

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
export function createBucket(authInfo, bucketName, headers,
    locationConstraint, log, cb) {
    log.trace('Creating bucket');
    assert.strictEqual(typeof bucketName, 'string');
    const canonicalID = authInfo.getCanonicalID();
    const ownerDisplayName =
        authInfo.getAccountDisplayName();
    const creationDate = new Date().toJSON();
    const bucket = new BucketInfo(bucketName,
        canonicalID, ownerDisplayName, creationDate,
        BucketInfo.currentModelVersion());

    if (locationConstraint !== undefined) {
        bucket.setLocationConstraint(locationConstraint);
    }
    const parseAclParams = {
        headers,
        resourceType: 'bucket',
        acl: bucket.acl,
        log,
    };
    async.parallel({
        serverSideEncryption: function serverSideEncryption(callback) {
            kms.bucketLevelEncryption(
                bucketName, headers, log, (err, sseInfo) => {
                    if (err) {
                        log.debug('error getting bucket encryption info', {
                            error: err,
                        });
                        return callback(err);
                    }
                    return callback(null, sseInfo);
                });
        },
        prepareNewBucketMD: function prepareNewBucketMD(callback) {
            acl.parseAclFromHeaders(parseAclParams, (err, parsedACL) => {
                if (err) {
                    log.debug('error parsing acl from headers', {
                        error: err,
                    });
                    return callback(err);
                }
                bucket.setFullAcl(parsedACL);
                return callback(null, bucket);
            });
        },
        getAnyExistingBucketInfo: function getAnyExistingBucketInfo(callback) {
            metadata.getBucket(bucketName, log, (err, data) => {
                if (err && err.NoSuchBucket) {
                    return callback(null, 'NoBucketYet');
                }
                if (err) {
                    return callback(err);
                }
                return callback(null, data);
            });
        },
    },
    // Function to run upon finishing both parallel requests
    (err, results) => {
        if (err) {
            return cb(err);
        }
        const existingBucketMD = results.getAnyExistingBucketInfo;
        if (existingBucketMD instanceof BucketInfo &&
            existingBucketMD.getOwner() !== canonicalID) {
            return cb(errors.BucketAlreadyExists);
        }
        const newBucketMD = results.prepareNewBucketMD;
        const serverSideEncryption = results.serverSideEncryption;
        if (serverSideEncryption) {
            newBucketMD.setServerSideEncryption(serverSideEncryption);
        }
        if (existingBucketMD === 'NoBucketYet') {
            log.trace('new bucket without flags; adding transient label');
            newBucketMD.addTransientFlag();

            return freshStartCreateBucket(newBucketMD, canonicalID, log, cb);
        }
        if (existingBucketMD.hasTransientFlag() ||
            existingBucketMD.hasDeletedFlag()) {
            log.trace('bucket has transient flag or deleted flag. cleaning up');
            return cleanUpBucket(newBucketMD, canonicalID, log, cb);
        }
        // If bucket exists in non-transient and non-deleted
        // state and owned by requester:
        return cb(errors.BucketAlreadyOwnedByYou);
    });
}
