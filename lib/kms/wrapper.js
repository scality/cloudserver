const async = require('async');

const { errors } = require('arsenal');

const { config } = require('../Config');
const logger = require('../utilities/logger');
const inMemory = require('./in_memory/backend').backend;
const file = require('./file/backend');
const KMIPClient = require('arsenal').network.kmipClient;
const { KmsAWSClient } = require('arsenal').network;
const Common = require('./common');
const vault = require('../auth/vault');
let scalityKMS;
let scalityKMSImpl;
try {
     // eslint-disable-next-line import/no-unresolved
    const ScalityKMS = require('scality-kms');
    scalityKMS = new ScalityKMS(config.kms);
    scalityKMSImpl = 'scalityKms';
} catch (error) {
    logger.warn('scality kms unavailable. ' +
      'Using file kms backend unless mem specified.',
      { error });
    scalityKMS = file;
    scalityKMSImpl = 'fileKms';
}

let client;
let implName;

if (config.backends.kms === 'mem') {
    client = inMemory;
    implName = 'memoryKms';
} else if (config.backends.kms === 'file' || config.backends.kms === 'cdmi') {
    client = file;
    implName = 'fileKms';
} else if (config.backends.kms === 'scality') {
    client = scalityKMS;
    implName = scalityKMSImpl;
} else if (config.backends.kms === 'kmip') {
    const kmipConfig = { kmip: config.kmip };
    if (!kmipConfig.kmip) {
        throw new Error('KMIP KMS driver configuration is missing.');
    }
    client = new KMIPClient(kmipConfig);
    implName = 'kmip';
} else if (config.backends.kms === 'aws') {
    const awsConfig = { kmsAWS: config.kmsAWS };
    client = new KmsAWSClient(awsConfig);
    implName = 'aws';
} else {
    throw new Error(`KMS backend is not configured: ${config.backends.kms}`);
}

class KMS {
     /**
      * Create a new bucket encryption key.
      *
      * This function is responsible for creating an encryption key for a bucket.
      * If the client supports using a default master encryption key per account
      * and one is configured, the key is managed at the account level by Vault.
      * Otherwise, a bucket-level encryption key is created for legacy support.
      *
      * @param {BucketInfo} bucket - bucket info
      * @param {object} log - logger object
      * @param {function} cb - callback
      * @returns {undefined}
      * @callback called with (err, { masterKeyId: string, isAccountEncryptionEnabled: boolean })
      */
    static createBucketKey(bucket, log, cb) {
        log.debug('creating a new bucket key');
        // Check if the client supports the use of a default master encryption key per account
        // and one is configured.
        // If so, retrieve or create the encryption key for the account from Vault.
        // Later its id will be stored at the bucket metadata level.
        if (client.supportsDefaultKeyPerAccount && config.defaultEncryptionKeyPerAccount) {
            return vault.getOrCreateEncryptionKeyId(bucket.getOwner(), log, (err, data) => {
                if (err) {
                    log.debug('error retrieving or creating the default encryption key at the account level from vault',
                        { implName, error: err });
                    return cb(err);
                }

                const { encryptionKeyId, action } = data;
                log.trace('default encryption key retrieved or created at the account level from vault',
                    { implName, encryptionKeyId, action });
                return cb(null, { masterKeyId: encryptionKeyId, isAccountEncryptionEnabled: true });
            });
        }
        // Otherwise, create a default master encryption key, later its id will be stored at the bucket metadata level.
        return client.createBucketKey(bucket.getName(), log, (err, masterKeyId) => {
            if (err) {
                log.debug('error from kms', { implName, error: err });
                return cb(err);
            }
            log.trace('bucket key created in kms');
            return cb(null, { masterKeyId });
        });
    }

     /**
      *
      * @param {BucketInfo} bucket - bucket info
      * @param {object} sseConfig - SSE configuration
      * @param {object} log - logger object
      * @param {function} cb - callback
      * @returns {undefined}
      * @callback called with (err, serverSideEncryptionInfo: object)
      */
    static bucketLevelEncryption(bucket, sseConfig, log, cb) {
        /*
        The purpose of bucket level encryption is so that the client does not
        have to send appropriate headers to trigger encryption on each object
        put in an "encrypted bucket". Customer provided keys are not
        feasible in this system because we do not want to store this key
        in the bucket metadata.
        */
        const { algorithm, configuredMasterKeyId, mandatory } = sseConfig;
        const _mandatory = mandatory === true;
        if (algorithm === 'AES256' || algorithm === 'aws:kms') {
            const serverSideEncryptionInfo = {
                cryptoScheme: 1,
                algorithm,
                mandatory: _mandatory,
            };

            if (algorithm === 'aws:kms' && configuredMasterKeyId) {
                serverSideEncryptionInfo.configuredMasterKeyId = configuredMasterKeyId;

                return process.nextTick(() => cb(null, serverSideEncryptionInfo));
            }

            return this.createBucketKey(bucket, log, (err, data) => {
                if (err) {
                    return cb(err);
                }

                const { masterKeyId, isAccountEncryptionEnabled } = data;
                serverSideEncryptionInfo.masterKeyId = masterKeyId;

                if (isAccountEncryptionEnabled) {
                    serverSideEncryptionInfo.isAccountEncryptionEnabled = isAccountEncryptionEnabled;
                }

                return cb(null, serverSideEncryptionInfo);
            });
        }
       /*
        * no encryption
        */
        return cb(null, null);
    }

    /**
     *
     * @param {string} bucketKeyId - the Id of the bucket key
     * @param {object} log - logger object
     * @param {function} cb - callback
     * @returns {undefined}
     * @callback called with (err)
     */
    static destroyBucketKey(bucketKeyId, log, cb) {
        log.debug('deleting bucket key', { bucketKeyId });
        client.destroyBucketKey(bucketKeyId, log, err => {
            if (err) {
                log.debug('error from kms', { implName, error: err });
                return cb(err);
            }
            log.trace('bucket key destroyed in kms');
            return cb(null);
        });
    }

     /**
      * createCipherBundle
      * @param {object} serverSideEncryptionInfo - info for encryption
      * @param {number} serverSideEncryptionInfo.cryptoScheme -
      * cryptoScheme used
      * @param {string} serverSideEncryptionInfo.algorithm -
      * algorithm to use
      * @param {string} serverSideEncryptionInfo.masterKeyId -
      * key to get master key
      * @param {boolean} serverSideEncryptionInfo.mandatory -
      * true for mandatory encryption
      * @param {object} log - logger object
      * @param {function} cb - cb from external call
      * @returns {undefined}
      * @callback called with (err, cipherBundle)
      */
    static createCipherBundle(serverSideEncryptionInfo,
                              log, cb) {
        const { algorithm, configuredMasterKeyId, masterKeyId: bucketMasterKeyId } = serverSideEncryptionInfo;

        let masterKeyId = bucketMasterKeyId;
        if (configuredMasterKeyId) {
            log.debug('using user configured kms master key id');
            masterKeyId = configuredMasterKeyId;
        }

        const cipherBundle = {
            algorithm,
            masterKeyId,
            cryptoScheme: 1,
            cipheredDataKey: null,
            cipher: null,
        };

        async.waterfall([
            function generateDataKey(next) {
                /* There are 2 ways of generating a datakey :
                  - using the generateDataKey of the KMS backend if it exists
                    (currently only implemented for the AWS KMS backend). This is
                    the preferred solution since a dedicated KMS should offer a better
                    entropy for generating random content.
                  - using local random number generation, and then use the KMS to
                    encrypt the datakey. This method is used when the KMS backend doesn't
                    provide the generateDataKey method.
                */
                let res;
                if (client.generateDataKey) {
                    log.debug('creating a data key using the KMS');
                    res = client.generateDataKey(cipherBundle.cryptoScheme,
                        cipherBundle.masterKeyId,
                        log, (err, plainTextDataKey, cipheredDataKey) => {
                            if (err) {
                                log.debug('error generating a new data key from KMS',
                                    { implName, error: err });
                                return next(err);
                            }
                            log.trace('data key generated by the kms');
                            return next(null, plainTextDataKey, cipheredDataKey);
                        });
                } else {
                    log.debug('creating a new data key');
                    const plainTextDataKey = Common.createDataKey();

                    log.debug('ciphering the data key');
                    res = client.cipherDataKey(cipherBundle.cryptoScheme,
                        cipherBundle.masterKeyId,
                        plainTextDataKey, log, (err, cipheredDataKey) => {
                            if (err) {
                                log.debug('error encrypting the data key using KMS',
                                    { implName, error: err });
                                return next(err);
                            }
                            log.trace('data key ciphered by the kms');
                            return next(null, plainTextDataKey, cipheredDataKey);
                        });
                }
                return res;
            },
            function createCipher(plainTextDataKey, cipheredDataKey, next) {
                log.debug('creating a cipher');
                cipherBundle.cipheredDataKey =
                    cipheredDataKey.toString('base64');
                return Common.createCipher(cipherBundle.cryptoScheme,
                    plainTextDataKey, 0, log, (err, cipher) => {
                        plainTextDataKey.fill(0);
                        if (err) {
                            log.debug('error from kms',
                            { implName, error: err });
                            return next(err);
                        }
                        log.trace('cipher created by the kms');
                        return next(null, cipher);
                    });
            },
            function finishCipherBundle(cipher, next) {
                cipherBundle.cipher = cipher;
                return next(null, cipherBundle);
            },
        ], (err, cipherBundle) => {
            if (err) {
                log.error('error processing cipher bundle',
                          { implName, error: err });
            }
            return cb(err, cipherBundle);
        });
    }

     /**
      * createDecipherBundle
      * @param {object} serverSideEncryptionInfo - info for decryption
      * @param {number} serverSideEncryptionInfo.cryptoScheme -
      * cryptoScheme used
      * @param {string} serverSideEncryptionInfo.algorithm -
      * algorithm to use
      * @param {string} serverSideEncryptionInfo.masterKeyId -
      * key to get master key
      * @param {boolean} serverSideEncryptionInfo.mandatory -
      * true for mandatory encryption
      * @param {buffer} serverSideEncryptionInfo.cipheredDataKey -
      * ciphered data key
      * @param {number} offset - offset for decryption
      * @param {object} log - logger object
      * @param {function} cb - cb from external call
      * @returns {undefined}
      * @callback called with (err, decipherBundle)
      */
    static createDecipherBundle(serverSideEncryptionInfo, offset,
                                log, cb) {
        if (!serverSideEncryptionInfo.masterKeyId ||
            !serverSideEncryptionInfo.cipheredDataKey ||
            !serverSideEncryptionInfo.cryptoScheme) {
            log.error('Invalid cryptographic information', { implName });
            return cb(errors.InternalError);
        }
        const decipherBundle = {
            cryptoScheme: serverSideEncryptionInfo.cryptoScheme,
            decipher: null,
        };
        return async.waterfall([
            function decipherDataKey(next) {
                return client.decipherDataKey(
                    decipherBundle.cryptoScheme,
                    serverSideEncryptionInfo.masterKeyId,
                    serverSideEncryptionInfo.cipheredDataKey,
                    log, (err, plainTextDataKey) => {
                        log.debug('deciphering a data key');
                        if (err) {
                            log.debug('error from kms',
                                     { implName, error: err });
                            return next(err);
                        }
                        log.trace('data key deciphered by the kms');
                        return next(null, plainTextDataKey);
                    });
            },
            function createDecipher(plainTextDataKey, next) {
                log.debug('creating a decipher');
                return Common.createDecipher(decipherBundle.cryptoScheme,
                    plainTextDataKey, offset, log, (err, decipher) => {
                        plainTextDataKey.fill(0);
                        if (err) {
                            log.debug('error from kms',
                            { implName, error: err });
                            return next(err);
                        }
                        log.trace('decipher created by the kms');
                        return next(null, decipher);
                    });
            },
            function finishDecipherBundle(decipher, next) {
                decipherBundle.decipher = decipher;
                return next(null, decipherBundle);
            },
        ], (err, decipherBundle) => {
            if (err) {
                log.error('error processing decipher bundle',
                          { implName, error: err });
                return cb(err);
            }
            return cb(err, decipherBundle);
        });
    }

    static checkHealth(log, cb) {
        if (!client.healthcheck) {
            return cb(null, {
                [implName]: { code: 200, message: 'OK' },
            });
        }
        return client.healthcheck(log, err => {
            const respBody = {};
            if (err) {
                respBody[implName] = {
                    error: err.description,
                    code: err.code,
                };
            } else {
                respBody[implName] = {
                    code: 200,
                    message: 'OK',
                };
            }
            return cb(null, respBody);
        });
    }
}

module.exports = KMS;
