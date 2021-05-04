const { errors } = require('arsenal');
const { parseString } = require('xml2js');

/**
 * parseEncryptionXml - Parses and validates a ServerSideEncryptionConfiguration xml document
 * @param {object} xml - ServerSideEncryptionConfiguration doc
 * @param {object} log - logger
 * @param {function} cb - callback
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

        const result = { algorithm };

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
 * parseBucketEncryptionHeaders - retrieves bucket level sse configuration from request headers
 * @param {object} headers - Request headers
 * @returns {object} SSE configuration
 */
function parseBucketEncryptionHeaders(headers) {
    const sseAlgorithm = headers['x-amz-scal-server-side-encryption'];
    const configuredMasterKeyId = headers['x-amz-scal-server-side-encryption-aws-kms-key-id'] || null;

    if (sseAlgorithm === 'AES256') {
        return {
            algorithm: sseAlgorithm,
        };
    } else if (sseAlgorithm === 'aws:kms') {
        return {
            algorithm: sseAlgorithm,
            configuredMasterKeyId,
        };
    }

    return {
        algorithm: null,
    };
}

module.exports = {
    parseEncryptionXml,
    parseBucketEncryptionHeaders,
};
