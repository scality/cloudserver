const async = require('async');

const { errors } = require('arsenal');

const { config } = require('../Config');
const logger = require('../utilities/logger');
const inMemory = require('./in_memory/backend').backend;
const file = require('./file/backend');
const KMIPClient = require('arsenal').network.kmipClient;
const KMIPClusterClient = require('arsenal').network.kmipClusterClient;
const { KmsAWSClient } = require('arsenal').network;
const Common = require('./common');
const vault = require('../auth/vault');
const Cache = require('./Cache');
const cache = new Cache();
const {
    KmsProtocol,
    makeBackend,
    isScalityKmsArn,
    extractDetailFromArn,
    validateKeyDetail,
} = require('arsenal/build/lib/network/KMSInterface');

function getScalityKms() {
    let scalityKMS;
    let scalityKMSImpl;

    try {
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
    return { scalityKMS, scalityKMSImpl };
}

const kmsFactory = {
    mem: () => ({ client: inMemory, implName: 'memoryKms' }),
    file: () => ({ client: file, implName: 'fileKms' }),
    cdmi: () => ({ client: file, implName: 'fileKms' }),
    scality: () => {
        const { scalityKMS, scalityKMSImpl } = getScalityKms();
        return { client: scalityKMS, implName: scalityKMSImpl };
    },
    kmip: () => {
        if (!config.kmip) {
            throw new Error('KMIP KMS driver configuration is missing.');
        }
        const client = Array.isArray(config.kmip.transport)
            ? new KMIPClusterClient({ kmip: config.kmip })
            : new KMIPClient({ kmip: config.kmip });
        return { client, implName: 'kmip' };
    },
    aws: () => {
        if (!config.kmsAWS) {
            throw new Error('AWS KMS driver configuration is missing.');
        }
        return { client: new KmsAWSClient({ kmsAWS: config.kmsAWS }), implName: 'aws' };
    },
};

function getClient(kms) {
    const impl = kmsFactory[kms];
    if (!impl) {
        throw new Error(`KMS backend is not configured: ${kms}`);
    }
    return impl();
}

/**
 * Note: non current instance from previous keys won't be healthchecked
 * `{ [`type:protocol:provider`]: clientDetails }`
 */
const clientInstances = {};

const { client, implName } = getClient(config.backends.kms);
const { type, protocol, provider } = client.backend;
const currentIdentifier = `${type}:${protocol}:${provider}`;
clientInstances[currentIdentifier] = { client, implName };

const availableBackends = [client.backend];

const mapKmsProtocolToClient = {
    [KmsProtocol.aws_kms]: 'aws',
    // others already match
};

let previousBackend;
let previousIdentifier;

if (config.sseMigration) {
    previousBackend = makeBackend(
        config.sseMigration.previousKeyType,
        config.sseMigration.previousKeyProtocol,
        config.sseMigration.previousKeyProvider
    );
    availableBackends.push(previousBackend);
    previousIdentifier = `${previousBackend.type
        }:${previousBackend.protocol
        }:${previousBackend.provider}`;

    // Pre instantiate previous backend as for now only internal backend (file) is supported
    // for future multiple external backend we should consider keeping open connection to
    // external backend, healthcheck and idle timeout (if migration is finished)
    // a config flag could help toggle this behavior
    const previousKms = mapKmsProtocolToClient[previousBackend.protocol] || previousBackend.protocol;
    const previousInstance = getClient(previousKms);
    clientInstances[previousIdentifier] = previousInstance;
}

/**
 * Extract backend provider from key, validate arn for errors.
 * @param {string} key KeyId or KeyArn
 * @param {object} log logger
 * @returns {object} error or client with extracted KeyId
 */
function getClientForKey(key, log) {
    // if extraction only return the id, it is not a scality arnPrefix
    const detail = extractDetailFromArn(key);
    let clientIdentifier;
    if (detail.type) {
        // if type was extracted, it is a scality arnPrefix, it needs validation
        // might throw if arn malformed or backend not available
        // for any request (PUT or GET)
        const error = validateKeyDetail(detail, availableBackends);
        if (error) {
            log.error('KMS key arn is invalid', { key, detail, availableBackends });
            return { error };
        }
        clientIdentifier = `${detail.type}:${detail.protocol}:${detail.provider}`;
    } else if (config.sseMigration) {
        // if not a scality arnPrefix but migration from previous KMS
        clientIdentifier = previousIdentifier;
    } else {
        // if not a scality arnPrefix and no migration
        clientIdentifier = currentIdentifier;
    }

    const instance = clientInstances[clientIdentifier];

    if (instance) {
        // was already instantiated
        // return the extracted key id to avoid further processing of potential arn
        // return clientIdentifier to allow usage restriction
        return { ...instance, clientIdentifier, key: detail.id };
    }

    // Only pre instantiated previous KMS from sseMigration is supported now
    // Here we could instantiate other provider on the fly to manage multi providers
    log.error('KMS key doesn\'t match any KMS instance', { key, detail, availableBackends });
    return { error: new errors.InvalidArgument
        // eslint-disable-next-line new-cap
        .customizeDescription(`KMS unknown provider for key ${key}`),
    };
}

class KMS {
    /** Used for keys from current client */
    static get arnPrefix() {
        return client.backend.arnPrefix;
    }

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
      * @callback called with (err, { masterKeyId: string, masterKeyArn: string, isAccountEncryptionEnabled: boolean })
      */
    static createBucketKey(bucket, log, cb) {
        // always use current client for create
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
                return cb(null, {
                    // vault only return arn
                    masterKeyId: encryptionKeyId,
                    masterKeyArn: encryptionKeyId,
                    isAccountEncryptionEnabled: true,
                });
            });
        }
        // Otherwise, create a default master encryption key, later its id will be stored at the bucket metadata level.
        return client.createBucketKey(bucket.getName(), log, (err, masterKeyId, masterKeyArn) => {
            if (err) {
                log.debug('error from kms', { implName, error: err });
                return cb(err);
            }
            log.trace('bucket key created in kms');
            return cb(null, { masterKeyId, masterKeyArn });
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
                // If input key is scality arn format it needs validation
                // otherwise prepend the current KMS client arnPrefix
                if (isScalityKmsArn(configuredMasterKeyId)) {
                    const detail = extractDetailFromArn(configuredMasterKeyId);
                    const error = validateKeyDetail(detail, availableBackends);
                    if (error) {
                        return cb(error);
                    }
                    serverSideEncryptionInfo.configuredMasterKeyId = configuredMasterKeyId;
                } else {
                    serverSideEncryptionInfo.configuredMasterKeyId =
                        `${client.backend.arnPrefix}${configuredMasterKeyId}`;
                }

                return process.nextTick(() => cb(null, serverSideEncryptionInfo));
            }

            return this.createBucketKey(bucket, log, (err, data) => {
                if (err) {
                    return cb(err);
                }

                const { masterKeyId, masterKeyArn, isAccountEncryptionEnabled } = data;
                serverSideEncryptionInfo.masterKeyId = masterKeyArn || masterKeyId;

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
        // shadowing global client for key
        const { error, client, implName, key } = getClientForKey(bucketKeyId, log);
        if (error) {
            return cb(error);
        }
        return client.destroyBucketKey(key, log, err => {
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
      * @param {object} [opts] - additional options
      * @param {boolean} [opts.previousOk] - allow usage of previous KMS (for ongoing MPU not migrated)
      * @returns {undefined}
      * @callback called with (err, cipherBundle)
      */
    static createCipherBundle(serverSideEncryptionInfo,
                              log, cb, opts) {
        const { algorithm, configuredMasterKeyId, masterKeyId: bucketMasterKeyId } = serverSideEncryptionInfo;

        let masterKeyId = bucketMasterKeyId;
        if (configuredMasterKeyId) {
            log.debug('using user configured kms master key id');
            masterKeyId = configuredMasterKeyId;
        }
        // shadowing global client for key
        // but should not happen to cipher for another client as Puts should use current KMS
        // still extract KeyId and validate arn
        const { error, client, implName, clientIdentifier, key } = getClientForKey(masterKeyId, log);
        if (error) {
            return cb(error);
        }
        if (previousIdentifier
            && clientIdentifier === previousIdentifier
            && clientIdentifier !== currentIdentifier
            && (opts && !opts.previousOk)
        ) {
            return cb(errors.InvalidArgument
                .customizeDescription(
                    'KMS cannot use previous provider to encrypt new objects if a new provider is configured'));
        }

        const cipherBundle = {
            algorithm,
            masterKeyId, // keep arnPrefix in cipherBundle as it is returned to callback
            cryptoScheme: 1,
            cipheredDataKey: null,
            cipher: null,
        };

        return async.waterfall([
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
                        key,
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
                        key,
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

        // shadowing global client for key - implName already used can't be shadowed here
        const { error, client, implName: _impl, key } = getClientForKey(
            serverSideEncryptionInfo.masterKeyId, log);
        if (error) {
            return cb(error);
        }

        return async.waterfall([
            function decipherDataKey(next) {
                return client.decipherDataKey(
                    decipherBundle.cryptoScheme,
                    key,
                    serverSideEncryptionInfo.cipheredDataKey,
                    log, (err, plainTextDataKey) => {
                        log.debug('deciphering a data key');
                        if (err) {
                            log.debug('error from kms',
                                     { implName: _impl, error: err });
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
                            { implName: _impl, error: err });
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
                          { implName: _impl, error: err });
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

        const cachedResult = cache.getResult();
        logger.debug('current KMS cache state', { result: cachedResult });

        const shouldRefreshCache = cache.shouldRefresh();

        if (shouldRefreshCache) {
            logger.debug('health check for KMS backend');
            return client.healthcheck(log, err => {
                let res;
                if (err) {
                    res = {
                        // The following response makes sure that if KMS is down,
                        // cloudserver health check is still healthy.
                        // Simply including an error code in the response won't cause the health check to fail.
                        // Instead, the healthCheck logic detects errors by checking for the "error" field.
                        code: err.code,
                        message: 'KMS health check failed',
                        description: err.description,
                    };
                    logger.warn('KMS health check failed', { errorCode: err.code, error: err.description });
                } else {
                    res = {
                        code: 200,
                        message: 'OK',
                    };
                    logger.info('KMS health check succeeded', { res });
                }

                cache.setResult(res);
                const updatedResult = cache.getResult();
                logger.debug('updated KMS cache:', { result: updatedResult });

                const respBody = { [implName]: updatedResult };
                return cb(null, respBody);
            });
        }

        // Use cached healthcheck result if within a 1-hour window
        logger.debug('using cached KMS health check', { cachedResult });
        return cb(null, { [implName]: cachedResult });
    }
}

module.exports = KMS;
