const assert = require('assert');
const joi = require('@hapi/joi');

const envVar = 'S3_NOTIFICATION_DESTINATION_API_ENABLED';

// The metadata backends that hold the destination records. To support
// another backend, add it here.
const servingBackends = ['scality', 'file', 'mem'];

// joi refuses any key that the schema does not name. Thus a typo in the
// block stops the server instead of being ignored, whatever the flag. This
// schema checks only the keys of bucketd: while the flag is off, the parser
// reads no other field of the block.
const notificationDestinationApiSchema = joi
    .object({
        enabled: joi.boolean().default(false),
        bucketd: joi.object({ bootstrap: joi.any() }),
    })
    .default();

// The bucketd list, read only when the flag is on with scality
const bucketdSchema = joi.object({
    bucketd: joi.object({
        bootstrap: joi.array().items(joi.string()).min(1),
    }),
});

/**
 * Parse the notificationDestinationApi block. This block holds the one flag
 * of the bucket notification destination API. The
 * S3_NOTIFICATION_DESTINATION_API_ENABLED environment variable, "true" or
 * "false", overrides its enabled field. On a metadata backend that does not
 * serve the API, the flag is always off, with no warning.
 * @param {object} [block] - the block of config.json
 * @param {string} metadata - the metadata backend
 * @return {object} - { enabled, bucketd }. bucketd is null unless the flag
 * is on with the scality backend.
 */
function parseNotificationDestinationApiConfig(block, metadata) {
    // The record store exists only for the backends of the list. On any
    // other backend, the flag is off, nothing in this block stops the server
    // from starting, and the API answers NotImplemented.
    if (!servingBackends.includes(metadata)) {
        return { enabled: false, bucketd: null };
    }
    const { error, value } = notificationDestinationApiSchema.validate(block, { convert: false });
    if (error) {
        throw new Error(`bad config: notificationDestinationApi: ${error.message}`);
    }
    let { enabled } = value;
    const envEnabled = process.env[envVar];
    if (envEnabled !== undefined) {
        assert(
            envEnabled === 'true' || envEnabled === 'false',
            `bad environment variable: ${envVar} must be "true" or "false"`,
        );
        enabled = envEnabled === 'true';
    }
    if (!enabled || metadata !== 'scality') {
        return { enabled, bucketd: null };
    }
    const bootstrap = value.bucketd?.bootstrap;
    assert(
        bootstrap !== undefined,
        'bad config: notificationDestinationApi.bucketd.bootstrap is required when the API is on with scality',
    );
    const { error: bucketdError } = bucketdSchema.validate({ bucketd: value.bucketd }, { convert: false });
    if (bucketdError) {
        throw new Error(`bad config: notificationDestinationApi: ${bucketdError.message}`);
    }
    return { enabled, bucketd: { bootstrap } };
}

module.exports = {
    parseNotificationDestinationApiConfig,
};
