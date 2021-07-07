const { errors } = require('arsenal');
const metadata = require('../../../metadata/wrapper');
const kms = require('../../../kms/wrapper');
const { parseString } = require('xml2js');

/**
 * ServerSideEncryptionInfo - user configuration for server side encryption
 * @typedef {Object} ServerSideEncryptionInfo
 * @property {string} algorithm - Algorithm to use for encryption. Either AES256 or aws:kms.
 * @property {string} masterKeyId - Key id for the kms key used to encrypt data keys.
 * @property {string} configuredMasterKeyId - User configured master key id.
 * @property {boolean} mandatory - Whether a default encryption policy has been enabled.
*/

/**
 * @callback ServerSideEncryptionInfo~callback
 * @param {Object} error - Instance of Arsenal error
 * @param {ServerSideEncryptionInfo} - SSE configuration
 */

/**
 * parseEncryptionXml - Parses and validates a ServerSideEncryptionConfiguration xml document
 * @param {object} xml - ServerSideEncryptionConfiguration doc
 * @param {object} log - logger
 * @param {ServerSideEncryptionInfo~callback} cb - callback
 * @returns {undefined}
 */
function parseEncryptionXml(xml, log, cb) {
    return parseString(xml, (err, parsed) => {
        if (err) {
            log.trace('xml parsing failed', {
                error: err,
                method: 'parseEncryptionXml',
            });
            log.debug('invalid xml', { xml });
            return cb(errors.MalformedXML);
        }

        if (!parsed
            || !parsed.ServerSideEncryptionConfiguration
            || !parsed.ServerSideEncryptionConfiguration.Rule) {
            log.trace('error in sse config, invalid ServerSideEncryptionConfiguration section', {
                method: 'parseEncryptionXml',
            });
            return cb(errors.MalformedXML);
        }

        const { Rule } = parsed.ServerSideEncryptionConfiguration;

        if (!Array.isArray(Rule)
            || Rule.length > 1
            || !Rule[0]
            || !Rule[0].ApplyServerSideEncryptionByDefault
            || !Rule[0].ApplyServerSideEncryptionByDefault[0]) {
            log.trace('error in sse config, invalid ApplyServerSideEncryptionByDefault section', {
                method: 'parseEncryptionXml',
            });
            return cb(errors.MalformedXML);
        }

        const [encConfig] = Rule[0].ApplyServerSideEncryptionByDefault;

        if (!encConfig.SSEAlgorithm || !encConfig.SSEAlgorithm[0]) {
            log.trace('error in sse config, no SSEAlgorithm provided', {
                method: 'parseEncryptionXml',
            });
            return cb(errors.MalformedXML);
        }

        const [algorithm] = encConfig.SSEAlgorithm;

        if (algorithm !== 'AES256' && algorithm !== 'aws:kms') {
            log.trace('error in sse config, unknown SSEAlgorithm', {
                method: 'parseEncryptionXml',
            });
            return cb(errors.MalformedXML);
        }

        const result = { algorithm, mandatory: true };

        if (encConfig.KMSMasterKeyID) {
            if (algorithm === 'AES256') {
                log.trace('error in sse config, can not specify KMSMasterKeyID when using AES256', {
                    method: 'parseEncryptionXml',
                });
                return cb(errors.InvalidArgument.customizeDescription(
                    'a KMSMasterKeyID is not applicable if the default sse algorithm is not aws:kms'));
            }

            if (!encConfig.KMSMasterKeyID[0] || typeof encConfig.KMSMasterKeyID[0] !== 'string') {
                log.trace('error in sse config, invalid KMSMasterKeyID', {
                    method: 'parseEncryptionXml',
                });
                return cb(errors.MalformedXML);
            }

            result.configuredMasterKeyId = encConfig.KMSMasterKeyID[0];
        }
        return cb(null, result);
    });
}

/**
 * hydrateEncryptionConfig - Constructs a ServerSideEncryptionInfo object from arguments
 * ensuring no invalid or undefined keys are added
 *
 * @param {string} algorithm - Algorithm to use for encryption. Either AES256 or aws:kms.
 * @param {string} configuredMasterKeyId - User configured master key id.
 * @param {boolean} [mandatory] - Whether a default encryption policy has been enabled.
 * @returns {ServerSideEncryptionInfo} - SSE configuration
 */
function hydrateEncryptionConfig(algorithm, configuredMasterKeyId, mandatory = null) {
    if (algorithm !== 'AES256' && algorithm !== 'aws:kms') {
        return {
            algorithm: null,
        };
    }

    const sseConfig = { algorithm, mandatory };

    if (algorithm === 'aws:kms' && configuredMasterKeyId) {
        sseConfig.configuredMasterKeyId = configuredMasterKeyId;
    }

    if (mandatory !== null) {
        sseConfig.mandatory = mandatory;
    }

    return sseConfig;
}

/**
 * parseBucketEncryptionHeaders - retrieves bucket level sse configuration from request headers
 * @param {object} headers - Request headers
 * @returns {ServerSideEncryptionInfo} - SSE configuration
 */
function parseBucketEncryptionHeaders(headers) {
    const sseAlgorithm = headers['x-amz-scal-server-side-encryption'];
    const configuredMasterKeyId = headers['x-amz-scal-server-side-encryption-aws-kms-key-id'] || null;
    return hydrateEncryptionConfig(sseAlgorithm, configuredMasterKeyId, true);
}

/**
 * parseObjectEncryptionHeaders - retrieves bucket level sse configuration from request headers
 * @param {object} headers - Request headers
 * @returns {ServerSideEncryptionInfo} - SSE configuration
 */
function parseObjectEncryptionHeaders(headers) {
    const sseAlgorithm = headers['x-amz-server-side-encryption'];
    const configuredMasterKeyId = headers['x-amz-server-side-encryption-aws-kms-key-id'] || null;

    if (sseAlgorithm && sseAlgorithm !== 'AES256' && sseAlgorithm !== 'aws:kms') {
        return {
            error: errors.InvalidArgument.customizeDescription('The encryption method specified is not supported'),
        };
    }

    if (sseAlgorithm !== 'aws:kms' && configuredMasterKeyId) {
        return {
            error: errors.InvalidArgument.customizeDescription(
                'a KMSMasterKeyID is not applicable if the default sse algorithm is not aws:kms'),
        };
    }
    return { objectSSE: hydrateEncryptionConfig(sseAlgorithm, configuredMasterKeyId) };
}

/**
 * createDefaultBucketEncryptionMetadata - Creates master key and sets up default server side encryption configuration
 * @param {BucketInfo} bucket - bucket metadata
 * @param {object} log - werelogs logger
 * @param {ServerSideEncryptionInfo~callback} cb - callback
 * @returns {undefined}
 */
function createDefaultBucketEncryptionMetadata(bucket, log, cb) {
    return kms.bucketLevelEncryption(
        bucket.getName(),
        { algorithm: 'AES256', mandatory: false },
        log,
        (error, sseConfig) => {
            if (error) {
                return cb(error);
            }
            bucket.setServerSideEncryption(sseConfig);
            return metadata.updateBucket(bucket.getName(), bucket, log, err => cb(err, sseConfig));
        });
}

/**
 *
 * @param {object} headers - request headers
 * @param {BucketInfo} bucket - BucketInfo model
 * @param {*} log - werelogs logger
 * @param {ServerSideEncryptionInfo~callback} cb - callback
 * @returns {undefined}
 */
function getObjectSSEConfiguration(headers, bucket, log, cb) {
    const bucketSSE = bucket.getServerSideEncryption();
    const { error, objectSSE } = parseObjectEncryptionHeaders(headers);
    if (error) {
        return cb(error);
    }

    // If a per object sse algo has been passed through
    // x-amz-server-side-encryption
    if (objectSSE.algorithm) {
        // If aws:kms and a custom key id
        // pass it through without updating the bucket md
        if (objectSSE.algorithm === 'aws:kms' && objectSSE.configuredMasterKeyId) {
            return cb(null, objectSSE);
        }

        // If the client has not specified a key id,
        // and we have a default config, then we reuse
        // it and pass it through
        if (!objectSSE.configuredMasterKeyId && bucketSSE) {
            // The default configs algo is overridden with the one passed in the
            // request headers. Our implementations of AES256 and aws:kms are the
            // same underneath so this is only cosmetic change.
            const sseConfig = Object.assign({}, bucketSSE, { algorithm: objectSSE.algorithm });
            return cb(null, sseConfig);
        }

        // If the client has not specified a key id, and we
        // don't have a default config, generate it
        if (!objectSSE.configuredMasterKeyId && !bucketSSE) {
            return createDefaultBucketEncryptionMetadata(bucket, log, (error, sseConfig) => {
                if (error) {
                    return cb(error);
                }
                // Override the algorithm, for the same reasons as above.
                Object.assign(sseConfig, { algorithm: objectSSE.algorithm });
                return cb(null, sseConfig);
            });
        }
    }

    // If the bucket has a default encryption config, and it is mandatory
    // (created with putBucketEncryption or legacy headers)
    // pass it through
    if (bucketSSE && bucketSSE.mandatory) {
        return cb(null, bucketSSE);
    }

    // No encryption config
    return cb(null, null);
}

module.exports = {
    createDefaultBucketEncryptionMetadata,
    getObjectSSEConfiguration,
    hydrateEncryptionConfig,
    parseEncryptionXml,
    parseBucketEncryptionHeaders,
    parseObjectEncryptionHeaders,
};
