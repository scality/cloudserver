const async = require('async');

const { errors } = require('arsenal');

const { config } = require('../Config');
const logger = require('../utilities/logger');
const inMemory = require('./in_memory/backend').backend;
const file = require('./file/backend');
const KMIPClient = require('arsenal').network.kmipClient;
const Common = require('./common');
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
} else {
    throw new Error('KMS backend is not configured');
}

class KMS {
     /**
      *
      * @param {string} bucketName - bucket name
      * @param {object} log - logger object
      * @param {function} cb - callback
      * @returns {undefined}
      * @callback called with (err, masterKeyId: string)
      */
    static createBucketKey(bucketName, log, cb) {
        log.debug('creating a new bucket key');
        client.createBucketKey(bucketName, log, (err, masterKeyId) => {
            if (err) {
                log.debug('error from kms', { implName, error: err });
                return cb(err);
            }
            log.trace('bucket key created in kms');
            return cb(null, masterKeyId);
        });
    }

     /**
      *
      * @param {string} bucketName - bucket name
      * @param {object} sseConfig - SSE configuration
      * @param {object} log - logger object
      * @param {function} cb - callback
      * @returns {undefined}
      * @callback called with (err, serverSideEncryptionInfo: object)
      */
    static bucketLevelEncryption(bucketName, sseConfig, log, cb) {
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
            return this.createBucketKey(bucketName, log, (err, masterKeyId) => {
                if (err) {
                    return cb(err);
                }

                const serverSideEncryptionInfo = {
                    cryptoScheme: 1,
                    algorithm,
                    masterKeyId,
                    mandatory: _mandatory,
                };

                if (algorithm === 'aws:kms' && configuredMasterKeyId) {
                    serverSideEncryptionInfo.configuredMasterKeyId = configuredMasterKeyId;
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
     *
     * @param {object} log - logger object
     * @returns {buffer} newKey - a data key
     */
    static createDataKey(log) {
        log.debug('creating a new data key');
        const newKey = Common.createDataKey();
        log.trace('data key created by the kms');
        return newKey;
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
        const dataKey = this.createDataKey(log);

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
            function cipherDataKey(next) {
                log.debug('ciphering a data key');
                return client.cipherDataKey(cipherBundle.cryptoScheme,
                    cipherBundle.masterKeyId,
                    dataKey, log, (err, cipheredDataKey) => {
                        if (err) {
                            log.debug('error from kms',
                                { implName, error: err });
                            return next(err);
                        }
                        log.trace('data key ciphered by the kms');
                        return next(null, cipheredDataKey);
                    });
            },
            function createCipher(cipheredDataKey, next) {
                log.debug('creating a cipher');
                cipherBundle.cipheredDataKey =
                    cipheredDataKey.toString('base64');
                return Common.createCipher(cipherBundle.cryptoScheme,
                    dataKey, 0, log, (err, cipher) => {
                        dataKey.fill(0);
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
}

module.exports = KMS;
