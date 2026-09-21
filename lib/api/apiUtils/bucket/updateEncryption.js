const { getVersionSpecificMetadataOptions } = require('../object/versioning');
// const getReplicationInfo = require('../object/getReplicationInfo');
const { config } = require('../../../Config');
const kms = require('../../../kms/wrapper');
const metadata = require('../../../metadata/wrapper');
const { isScalityKmsArn, makeScalityArnPrefix } = require('arsenal/build/lib/network/KMSInterface');

// Bucket need a key from the new KMS, not a simple reformating
function updateBucketEncryption(bucket, log, cb) {
    const sse = bucket.getServerSideEncryption();

    if (!sse) {
        return cb(null, bucket);
    }

    const masterKey = sse.masterKeyId;
    const configuredKey = sse.configuredMasterKeyId;

    // Note: if migration is from an external to an external, absence of arn is not enough
    // a comparison of arn will be necessary but config validation blocks this for now
    const updateMaster = masterKey && !isScalityKmsArn(masterKey);
    const updateConfigured = configuredKey && !isScalityKmsArn(configuredKey);

    if (!updateMaster && !updateConfigured) {
        return cb(null, bucket);
    }
    log.debug('trying to update bucket encryption', { oldKey: masterKey || configuredKey });
    // this should trigger vault account key update as well
    return kms.createBucketKey(bucket, log, (err, newSse) => {
        if (err) {
            return cb(err, bucket);
        }
        // if both keys needs migration, it is ok the use the same KMS key
        // as the configured one should be used and the only way to use the
        // masterKeyId is to PutBucketEncryption to AES256 but then nothing
        // will break and the same KMS key will continue to be used.
        // And the key is managed (created) by Scality, not passed from input.
        if (updateMaster) {
            sse.masterKeyId = newSse.masterKeyArn;
        }
        if (updateConfigured) {
            sse.configuredMasterKeyId = newSse.masterKeyArn;
        }
        // KMS account key will not be deleted when bucket is deleted
        if (newSse.isAccountEncryptionEnabled) {
            sse.isAccountEncryptionEnabled = newSse.isAccountEncryptionEnabled;
        }

        log.info('updating bucket encryption', {
            oldKey: masterKey || configuredKey,
            newKey: newSse.masterKeyArn,
            isAccount: newSse.isAccountEncryptionEnabled,
        });
        return metadata.updateBucket(bucket.getName(), bucket, log, err => cb(err, bucket));
    });
}

// Only reformat the key, don't generate a new one.
// Use opts.skipObjectUpdate to only prepare objMD without sending the update to metadata
// if a metadata.putObjectMD is expected later in call flow. (Downside: update skipped if error)
function updateObjectEncryption(bucket, objMD, objectKey, log, keyArnPrefix, opts, cb) {
    if (!objMD) {
        return cb(null, bucket, objMD);
    }

    const key = objMD['x-amz-server-side-encryption-aws-kms-key-id'];

    if (!key || isScalityKmsArn(key)) {
        return cb(null, bucket, objMD);
    }
    const newKey = `${keyArnPrefix}${key}`;
    // eslint-disable-next-line no-param-reassign
    objMD['x-amz-server-side-encryption-aws-kms-key-id'] = newKey;
    // Doesn't seem to be used but update as well
    for (const dataLocator of objMD.location || []) {
        if (dataLocator.masterKeyId) {
            dataLocator.masterKeyId = `${keyArnPrefix}${dataLocator.masterKeyId}`;
        }
    }
    // eslint-disable-next-line no-param-reassign
    objMD.originOp = 's3:ObjectCreated:Copy';
    // Copy should be tested for 9.5 in INTGR-1038
    // to make sure it does not impact backbeat CRR / bucket notif
    const params = getVersionSpecificMetadataOptions(objMD, config.nullVersionCompatMode);

    log.info('reformating object encryption key', { oldKey: key, newKey, skipUpdate: opts.skipObjectUpdate });
    if (opts.skipObjectUpdate) {
        return cb(null, bucket, objMD);
    }
    return metadata.putObjectMD(bucket.getName(), objectKey, objMD, params, log, err => cb(err, bucket, objMD));
}

/**
 * Update encryption of bucket and object if kms provider changed
 *
 * @param {Error} err - error coming from metadata validate before the action handling
 * @param {BucketInfo} bucket - bucket
 * @param {Object} [objMD] - object metadata
 * @param {string} objectKey  - objectKey from request.
 * @param {Logger} log - request logger
 * @param {Object} opts - options for sseMigration
 * @param {boolean} [opts.skipObject] - ignore object update
 * @param {boolean} [opts.skipObjectUpdate] - don't update metadata but prepare objMD for later update
 * @param {Function} cb - callback (err, bucket, objMD)
 * @returns {undefined}
 */
function updateEncryption(err, bucket, objMD, objectKey, log, opts, cb) {
    // Error passed here to call the function inbetween the metadataValidate and its callback
    if (err) {
        return cb(err);
    }
    // if objMD missing, still try updateBucketEncryption
    if (!config.sseMigration) {
        return cb(null, bucket, objMD);
    }

    const { previousKeyType, previousKeyProtocol, previousKeyProvider } = config.sseMigration;
    // previousKeyType is required and validated in Config.js
    // for now it is the only implementation we need.
    // See TAD Seamless decryption with internal and external KMS: https://scality.atlassian.net/wiki/x/EgADu
    // for other method of migration without a previousKeyType

    const keyArnPrefix = makeScalityArnPrefix(previousKeyType, previousKeyProtocol, previousKeyProvider);

    return updateBucketEncryption(bucket, log, (err, bucket) => {
        // Any error in updating encryption at bucket or object level is returned to client.
        // Other possibilities: ignore error, include sse migration notice in error message.
        if (err) {
            return cb(err, bucket, objMD);
        }
        if (opts.skipObject) {
            return cb(err, bucket, objMD);
        }
        return updateObjectEncryption(bucket, objMD, objectKey, log, keyArnPrefix, opts, cb);
    });
}

module.exports = {
    updateEncryption,
};
