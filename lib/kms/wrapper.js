const async = require('async');

const { errors } = require('arsenal');

const { config } = require('../Config');
const logger = require('../utilities/logger');
const inMemory = require('./in_memory/backend').backend;
const file = require('./file/backend');
const Common = require('./common');
let scalityKMS;
let scalityKMSImpl;
try {
    const ScalityKMS = require('scality-kms');
    scalityKMS = new ScalityKMS(config.kms);
    scalityKMSImpl = 'scalityKms';
} catch (err) {
    logger.warn('scality kms unavailable. ' +
      'Using file kms backend unless mem specified.');
    scalityKMS = file;
    scalityKMSImpl = 'fileKms';
}

let client;
let implName;

if (config.backends.kms === 'mem') {
    client = inMemory;
    implName = 'memoryKms';
} else if (config.backends.kms === 'file') {
    client = file;
    implName = 'fileKms';
} else if (config.backends.kms === 'scality') {
    client = scalityKMS;
    implName = scalityKMSImpl;
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
      * @param {object} headers - request headers
      * @param {object} log - logger object
      * @param {function} cb - callback
      * @returns {undefined}
      * @callback called with (err, serverSideEncryptionInfo: object)
      */
    static bucketLevelEncryption(bucketName, headers, log, cb) {
        const sseAlgorithm = headers['x-amz-scal-server-side-encryption'];
        const sseMasterKeyId =
                  headers['x-amz-scal-server-side-encryption-aws-kms-key-id'];
        /*
        The purpose of bucket level encryption is so that the client does not
        have to send appropriate headers to trigger encryption on each object
        put in an "encrypted bucket". Customer provided keys are not
        feasible in this system because we do not want to store this key
        in the bucket metadata.
         */
        if (sseAlgorithm === 'AES256' ||
            (sseAlgorithm === 'aws:kms' && sseMasterKeyId === undefined)) {
            this.createBucketKey(bucketName, log, (err, masterKeyId) => {
                if (err) {
                    cb(err);
                    return;
                }
                const serverSideEncryptionInfo = {
                    cryptoScheme: 1,
                    algorithm: sseAlgorithm,
                    masterKeyId,
                    mandatory: true,
                };
                cb(null, serverSideEncryptionInfo);
            });
        } else if (sseAlgorithm === 'aws:kms') {
            const serverSideEncryptionInfo = {
                cryptoScheme: 1,
                algorithm: sseAlgorithm,
                masterKeyId: sseMasterKeyId,
                mandatory: true,
            };
            cb(null, serverSideEncryptionInfo);
        } else {
            /*
             * no encryption
             */
            cb(null, null);
        }
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
        const cipherBundle = {
            algorithm: serverSideEncryptionInfo.algorithm,
            masterKeyId: serverSideEncryptionInfo.masterKeyId,
            cryptoScheme: 1,
            cipheredDataKey: null,
            cipher: null,
        };

        async.waterfall([
            function cipherDataKey(next) {
                log.debug('ciphering a data key');
                return client.cipherDataKey(cipherBundle.cryptoScheme,
                    serverSideEncryptionInfo.masterKeyId,
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
